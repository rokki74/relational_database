package myDatabase

import (
	"encoding/binary"
	"log"
	"os"
	"strings"
)

type LSN uint64

const (
  WAL_INSERT uint8 = iota
	WAL_DELETE
	WAL_UPDATE
	WAL_UPDATEINPLACE
	WAL_PAGE_SPLIT
)

type WalRecord struct{
	LSN uint64
	ResourceType ResourceType
	TableName string
	PageId uint32
	Operation uint8
	DataSize uint16
	Data []byte
}

type WalManager struct{
	file *os.File
	currentLSN uint64
	CheckFile string
	CheckPoint uint64
	SoftPoint uint64
}

func NewWalManager(path string) *WalManager{
	f, _ := os.OpenFile(path+"/wal.log",
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	defer f.Close()
	return &WalManager{
		file: f,
		currentLSN: 0,
	}
}

func (wal *WalManager) Log(record *WalRecord) uint64{
	record.LSN = wal.currentLSN
  log.Printf("logging a record ..") 
	log.Printf("The received record for logging %v", *record)
	data := serializeRecord(record)
	wal.file.Write(data)
	wal.file.Sync()

	wal.currentLSN += uint64(len(data))

	return record.LSN
}

func writeFull(f *os.File, data []byte) error {
	total := 0
	for total < len(data) {
		n, err := f.Write(data[total:])
		if err != nil {
			return err
		}
		total += n
	}
	return nil
}

func serializeRecord(rec *WalRecord) []byte{
	recBytes := make([]byte, 0)

	recBytes = append(recBytes, byte(rec.LSN))
  recBytes = append(recBytes, byte(rec.ResourceType))
	tableNameLen := uint8(len(rec.TableName))
	recBytes = append(recBytes, byte(tableNameLen))

	buf := []byte(rec.TableName)
  recBytes = append(recBytes, buf...)
	//The rest of offsets can align themselves well for the data after recording tablename
	recBytes = append(recBytes, byte(rec.PageId))
	recBytes = append(recBytes, rec.Operation)
	recBytes = append(recBytes, byte(rec.DataSize))
	recBytes = append(recBytes, rec.Data...)


	log.Printf("current length of recBytes in serializing records: %v", len(recBytes))
  return recBytes
}

func deserializeRecord(recBytes []byte) *WalRecord{
	rec := WalRecord{}

	offset := 0
	binary.LittleEndian.PutUint64(recBytes[offset:8], rec.LSN)
	offset += 8
	var resType [1]byte
	copy(resType[:], recBytes[offset:offset+1])
	rec.ResourceType = ResourceType(resType[offset])
	offset += 1
	var tableNameLen [1]byte
	copy(tableNameLen[:], recBytes[offset:offset+1])
	offset += 1
	copy([]byte(rec.TableName), recBytes[offset:offset+int(tableNameLen[0])])
  offset += int(tableNameLen[0])

	binary.LittleEndian.PutUint32(recBytes[offset:offset+4], rec.PageId) 
	offset += 4
	var bufByte [1]byte
	copy(bufByte[:],	recBytes[offset:offset+1])
	rec.Operation = uint8(bufByte[0])
	offset += 1
	binary.LittleEndian.PutUint16(recBytes[offset:offset+2] , rec.DataSize)
	offset += 2
	copy(rec.Data, recBytes[offset:offset+int(rec.DataSize)])

	return &rec
}

func (wal *WalManager) NewCheckPoint(checkPoint uint64){
  wal.CheckPoint = checkPoint
}

func (wal *WalManager) SaveCheckPoint(db *Database_Manager) bool {
  db.BufferPool.FlushAll()
	f, err := os.OpenFile(wal.CheckFile, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		return false
	}
	defer f.Close()

	// Move to end of file
	_, err = f.Seek(0, 2) // 2 = io.SeekEnd
	if err != nil {
		return false
	}

	// Convert uint64 to bytes
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint64(buf, wal.CheckPoint)

	// Write 8 bytes
	_, err = f.Write(buf)
	if err != nil {
		return false
	}

	f.Sync()
	return true
}

func (wal *WalManager) ReadCheckPoint() uint64 {
	f, err := os.Open(wal.CheckFile)
	if err != nil {
		return 0
	}
	defer f.Close()

	// Move to 8 bytes before end
	_, err = f.Seek(-8, 2) // 2 = io.SeekEnd
	if err != nil {
		return 0
	}

	buf := make([]byte, 8)
	_, err = f.Read(buf)
	if err != nil {
		return 0
	}

	checkPoint := binary.LittleEndian.Uint64(buf)
	return checkPoint
}

func (wal *WalManager) Recover(db *Database_Manager, startLSN uint64) {
  checkPoint := uint64(0)
  if startLSN >0{
	  checkPoint = startLSN
	}else if wal.CheckPoint > wal.SoftPoint{
	  checkPoint = wal.CheckPoint
	}else{
	  checkPoint = wal.SoftPoint
	}
	records := wal.readAll(checkPoint)

	for _, rec := range records {

		filename := db.GetTablePath(rec.TableName)

		if rec.ResourceType == IndexRes{
		  parts := strings.Split(filename, ".")
			filename = parts[0]+".idx"
		}
		page, found := db.BufferPool.FetchPage(rec.PageId, filename)
		if !found{
		  continue
		}

    header := page.Read_header()
		if rec.LSN <= header.PageLSN {
			continue
		}

		wal.applyRedo(*page, rec)

		header.PageLSN = rec.LSN
	}

	wal.CheckPoint = checkPoint
	wal.SaveCheckPoint(db)
}

func (tm *TransactionManager) GetTxnLogs(txnId uint8) ([]WalRecord, bool){
    records := make([]WalRecord, 0)

		txn, alive := tm.ActiveTxns[txnId]
		if !alive{
			return records, false
		}

		startLSN := uint64(txn.StartLSN)
    if startLSN <= tm.DbManager.WAL.CheckPoint{
		  return records, false
		}

		return tm.DbManager.WAL.readMatchingTxnlogs(startLSN)
}

func (wal *WalManager) readMatchingTxnlogs(startLsn uint64) ([]WalRecord, bool){
	records := make([]WalRecord, 0)
  
	newOff := int64(startLsn)
	for{
		off, recordBytes, err := wal.readFileWalRecord(newOff)
		if err != nil{
			break
		}

		record := deserializeRecord(recordBytes)
		records = append(records, *record)
		newOff = off
	} 

	return records, true
}

func (wal *WalManager) readAll(startLSN uint64) []WalRecord{
	newOff := int64(startLSN)

	records := make([]WalRecord, 0)
	for{
		off, recordBytes, err := wal.readFileWalRecord(newOff)
		if err != nil{
			break
		}

		record := deserializeRecord(recordBytes)
		records = append(records, *record)

		newOff = off
	} 

	return records
}

func (wal *WalManager) readFileWalRecord(lsn int64) (int64, []byte, error){
	f := wal.file
	var recordSize  [4]byte
	_, e := f.ReadAt(recordSize[:], int64(lsn))
	if e != nil{
		return lsn, nil, e
	}
	recordBytes := make([]byte, 0)
	currentOffset := int64(lsn)+int64(binary.LittleEndian.Uint32(recordBytes))
	_, err := f.ReadAt(recordBytes, currentOffset)
	if err != nil{
		return lsn, nil, err
	}

	return currentOffset, recordBytes, nil
}

func (wal *WalManager) applyRedo(page Page, rec WalRecord) {
	switch rec.Operation {
  
	case WAL_INSERT:
		page.Insert_row(rec.Data)

	case WAL_DELETE:
	  s,exists := page.WalkPage(rec.Data)
		if !exists{
		  return
		}
		page.Delete_row(s)
	}
}



func (wal *WalManager) Flush(db *Database_Manager){
   startLsn := wal.SoftPoint
   wal.Recover(db, startLsn)
}

func (wal *WalManager) FlushLog(log WalRecord, db *Database_Manager){
		fileName := db.GetTablePath(log.TableName)
		if log.ResourceType == IndexRes{
		  parts := strings.Split(fileName, ".")
			fileName = parts[0]+".idx"
		}
		page, found := db.BufferPool.FetchPage(log.PageId, fileName)
		if !found{
		  return
		}

		wal.applyRedo(*page, log)

}

func (wal *WalManager) Undo(log WalRecord, db *Database_Manager){
		fileName := db.GetTablePath(log.TableName)
		if log.ResourceType == IndexRes{
		  parts := strings.Split(fileName, ".")
			fileName = parts[0]+".idx"
		}
		page, found := db.BufferPool.FetchPage(log.PageId, fileName)
		if !found{
		  return
		}
	  s,exists := page.WalkPage(log.Data)
		if !exists{
		  return
		}
		page.Delete_row(s)
}

func (wal *WalManager) LogDelete(tableName string, resType ResourceType, rowId RowId, deletedBytes []byte){
  rec := &WalRecord{
		ResourceType: resType,
		TableName: tableName,
		PageId: rowId.PageId,
		Operation: WAL_DELETE,
		DataSize: uint16(len(deletedBytes)),
		Data: deletedBytes,
	}

	wal.Log(rec)
}

func (wal *WalManager) LogUpdateInPlace(tableName string, resType ResourceType, newRowId RowId, newData []byte, oldData []byte){
  rec := &WalRecord{
		ResourceType: resType,
		TableName: tableName,
		PageId: newRowId.PageId,
		Operation: WAL_UPDATEINPLACE,
		DataSize: uint16(len(newData)),
		Data: newData,
	}

	wal.Log(rec)
}

func (wal *WalManager) LogInsert(tableName string, resType ResourceType, newRowId RowId, writtenData []byte) uint64{
  rec := &WalRecord{
		ResourceType: resType,
		TableName: tableName,
		PageId: newRowId.PageId,
		Operation: WAL_INSERT,
		DataSize: uint16(len(writtenData)),
		Data: writtenData,
	}

	wal.Log(rec)

	return wal.currentLSN
}



