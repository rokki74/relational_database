package myDatabase
import(
	"os"
)

type LSN uint64

const(
	WAL_INSERT = 1
	WAL_DELETE =2
	WAL_UPDATE =3
	WAL_PAGE_SPLIT =4
)

type WalRecord struct{
	LSN uint64
	PrevLSN uint64
	TableId uint64
	PageId uint32
	Operation uint8
	DataSize uint32
	Data []byte
}

type WalManager struct{
	file *os.File
	currentLSN uint64
}

func NewWalManager(path string) *WalManager{
	f, _ := os.OpenFile(path+"/wal.log",
		os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)

	defer f.Close()
	return &WALManager{
		file: f,
		currentLSN: 0,
	}
}

func (wal *WalManager) Log(record *WalRecord) uint64{
	record.LSN = wal.currentLSN

	data := serializeRecord(record)
	wal.file.Write(data)
	wal.file.Sync()

	wal.currentLSN += uint64(len(data))

	return record.LSN
}

func (wal *WALManager) Recover() {

	records := wal.readAll()

	for _, rec := range records {

		page := bufferPool.fetch_page(rec.PageId, rec.TableId)

		if rec.LSN <= page.pageLSN {
			continue
		}

		applyRedo(page, rec)

		page.pageLSN = rec.LSN
	}
}

func applyRedo(page *Page, rec WALRecord) {

	switch rec.Operation {

	case WAL_INSERT:
		page.insert_row(string(rec.Data))

	case WAL_DELETE:
		page.delete_row(rec.Data)

	}
}

func (wal *WALManager) Checkpoint(bufferPool *BufferPool) {

	bufferPool.FlushAll()

	rec := WALRecord{
		Operation: WAL_CHECKPOINT,
	}

	wal.Log(&rec)
}

