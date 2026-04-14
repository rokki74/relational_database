package myDatabase

import (
	"encoding/binary"
	"log"
	"strings"
	"strconv"
)

//A change in tables logic, you must always GetTable first so as to use it
type ColumnType int 
type TotalPagesSZ uint32
type SchemaSizeSZ uint32
type ColumnCountSZ uint16
type ColumnTypeSZ uint16
type columnNameLengthSZ uint16 

const(
	BOOLEAN ColumnType = iota
	INT
	STRING
)

const SEPARATOR = ","

type Column struct{
	ColumnName string
	ColumnType ColumnType
	nullable bool
}

type Schema struct{
	Columns []Column
}

type FsmEntry struct{
	PageId uint32
	freeBytes uint16
}

type Table struct {
    TableName string
		TableSchema Schema
		LastPageId uint32
		FirstFramePageId uint32
		bufferpool BufferPool
    Indexes  map[string]*Index
		TxnMngr *TransactionManager
		Db *Database_Manager
}

func (db *Database_Manager) CreateTable(name string, columns []Column) (Table, bool){
  clgEntry := db.Catalog.CatalogEntry[db.Dbname]
	_, exists := clgEntry.Tables[name]
	if !exists{
		log.Printf("Table already exists!")
		return Table{}, false
	}

		schm := Schema{
		   Columns: columns,
     	}

   table :=  Table{
		   TableName: name,
			 TableSchema: schm,
			 LastPageId: 0,
	   }

	clgEntry.Tables[name] = &table
	db.SaveTable(&table)

	return Table{}, false
}

func (db *Database_Manager) GetTable(name string) (Table, bool){
  clgEntry := db.Catalog.CatalogEntry[db.Dbname]
	_, exists := clgEntry.Tables[name]
	if !exists{
		log.Printf("Table Doesn't exist!")
		return Table{}, false
	}

  return *clgEntry.Tables[name], true
}

//After a scan get the pageId's that are free and by how many bytes then parse to this function to write to the fsm 
func (tb *Table) writeFsm(pageId uint32, freeBytes uint16){
	//The tail is always the latest information thus needs to be in such a way that i scan from the last fsm to find page with free page then, yeah
	lastFsmPageId, ok := tb.Db.BufferPool.Fsm.TablesRecorded[tb.TableName]
	if !ok{
		tb.Db.BufferPool.Fsm.TablesRecorded[tb.TableName] = tb.FirstFramePageId
		lastFsmPageId = tb.FirstFramePageId
	}

	fsmPath, exists := tb.Db.GetFsmPath(tb.TableName)
	if !exists{
	   return
	}

	page, ok := tb.Db.BufferPool.FetchPage(lastFsmPageId, fsmPath)
	if !ok{
	 return
	}
	header := page.Read_header()

	binary.LittleEndian.PutUint32(page.data[header.FreeSpaceOffset:header.FreeSpaceOffset+4], uint32(pageId))
  header.FreeSpaceOffset += 4
	binary.LittleEndian.PutUint16(page.data[header.FreeSpaceOffset:header.FreeSpaceOffset+2], uint16(freeBytes))
	header.FreeSpaceOffset += 2
	page.Write_header(&header)

	tb.Db.BufferPool.SavePage(fsmPath, *page)
}

func (tb *Table)  SaveTable(){
	tb.Db.SaveTable(tb)
}

func (tb *Table) DeleteTable(){
	tb.Db.DeleteTable(tb)
}

func (tl *Table) read(pageId uint32) ([]byte, bool){
	rows := make([]byte, 0)
	filename := tl.TableName +".tbl"
	pg, exists := tl.Db.BufferPool.FetchPage(pageId, filename)
	if !exists{
	   return rows, false
	}
	header := pg.Read_header()
	for i :=0; i<int(header.RowCount); i++{
		row := pg.Read_row(i)
		rows = append(rows, row...)
	}

	return rows, true
}

func (tl *Table) Scan(ScanPages uint8, c chan *[]Page ){
	if ScanPages >10{
		ScanPages =10
	}

	table_pages := make([]Page, ScanPages)
	for p := 0; p<=int(tl.LastPageId); p++{
		pg := Page{}
		data, ok := tl.read(uint32(p))
		if !ok{
		  continue
		}

		copy(pg.data[:], data[:4096])
		table_pages = append(table_pages, pg)

		if len(table_pages) >= int(ScanPages){
			c <- &table_pages

			table_pages = make([]Page, 0, ScanPages)
		}
	}

	if len(table_pages)>0{
		c <- &table_pages
	}
}

func (tl *Table) compact_table(){

}

func (tl *Table) close_table(){

}

func (tl *Table) SerializeColumnValues(parts []string, colTypes []ColumnType) []byte{
	buf := make([]byte, 4)

	for pos, val := range parts{
    colType := colTypes[pos]
		switch colType{
		case BOOLEAN:
			if strings.ToLower(val) == "true"{
				buf = append(buf, byte(1))//i shall deduce size from TableSchema later on deserialize
			}else{
				buf = append(buf, byte(1))
			}
		case INT:
			v, _ := strconv.Atoi(val)
			tmp := make([]byte, 0)
			binary.LittleEndian.PutUint32(tmp, uint32(v))
			buf = append(buf, tmp...)

		case STRING:
			strBytes := []byte(val)
			col_len := make([]byte, 2)
			binary.LittleEndian.PutUint16(col_len, uint16(len(strBytes)))
			buf = append(buf, col_len...)
			buf = append(buf, strBytes...)
		}
	}
	return buf
}

func (tl *Table) DeserializeColumnValues(rowBytes []byte) string{
	offset := 0

	var rowString string
	vals := make([]string, 0)
	
	colType := rowBytes[offset]
	switch colType{
	 case 0:
		 value := rowBytes[offset]
			var col_value string
			if value ==1{
				col_value = "true"
			}else{
				col_value = "false"
			}

			vals = append(vals, col_value)
			offset += 1
	 case 1:
			col_value := string(rowBytes[offset:offset+4])
			vals = append(vals, col_value)
			offset += 4
	 case 2:
			var str_len uint16
			str_len = binary.LittleEndian.Uint16(rowBytes[offset:offset+2])
			offset += 2
			col_value := string(rowBytes[offset:offset+int(str_len)])

			vals = append(vals, col_value)
	}

  //building back my user facing string
	rowString = strings.Join(vals, ",")
	return rowString
}

func (tb *Table) Insert(row string, txn *Transaction) {
	 parts := strings.Split(row, ",")
	 colTypes := make([]ColumnType, 0)
	 for pos, _ := range parts{
		 col := tb.TableSchema.Columns[pos]
		 colTypes = append(colTypes, col.ColumnType)
	 }

	 row_bytes := tb.SerializeColumnValues(parts, colTypes)
   pageId, _, ok := tb.Db.BufferPool.FittingPage(tb, uint16(len(row_bytes)))
	 if !ok{
		 pageId = tb.LastPageId
	 }
   tablepath, _ := tb.Db.GetTablePath(tb.TableName)
	 page, _ := tb.Db.BufferPool.FetchPage(pageId, tablepath)
   header := page.Read_header()	

	 rowId := RowId{pageId, uint16(header.RowCount)}
	 lsn := tb.Db.WAL.LogInsert(tb.TableName, ResourceType(TABLETYPE), rowId, row_bytes)
	 //the normal transaction flow can resume after wal being prioritized
	 header.PageLSN = lsn
	 ptr, _ := page.Insert_row(row_bytes)
	 tb.bufferpool.MarkDirty(tb.TableName, header.PageId)


	 if len(tb.Indexes)>0{
		for _, idx := range tb.Indexes {

			key := tb.extractColumnValue(row, idx.ColumnPos)

			colType := colTypes[idx.ColumnPos]
			ky := EncodeKey(key, colType)
			idx.MemTree.Insert(ky, *ptr)
		}
		//After struggling here for the last 6 minutes it's finally figured out, the way one can loose context in his or her own project once it starts to grow, thanks example here i was missing the encoding the key function and was almost starting to handle it from scratch again
	}

	tb.TxnMngr.Commit(txn)
}
//All bugs in table.go are resolved i am hoping so, let's save 

func (tb *Table) extractColumnValue(row string, colPos uint8) interface{} {

    parts := strings.Split(row, SEPARATOR)

    colType, _, _ := tb.FindColumnTypeAndNameFromPos(int(colPos))
    value := parts[colPos]

    switch colType {

    case INT:
        v, _ := strconv.Atoi(value)
        return int32(v)

    case BOOLEAN:
        return value == "true"

    case STRING:
        return value
    }

    return nil
}

func (tb *Table) FindColumnTypeAndPos(col string) (ColumnType, uint8, bool){
	columns := tb.TableSchema.Columns
	for i:=0; i<len(columns); i++{
		if columns[i].ColumnName == col{
			return columns[i].ColumnType, uint8(i), true
		}
	}
	log.Printf("The column [%col] couldn't be found in table schema!", col)

	return -1, uint8(0), false
}

func (tb *Table) FindColumnTypeAndNameFromPos(pos int) (ColumnType, string, bool){
	columns := tb.TableSchema.Columns
	if pos > len(columns){
		return -1, "", false
	}
	col := columns[pos]
	return col.ColumnType, col.ColumnName, true
}



func (tb *Table) CreateIndex(name string, columnNames []string) {
  for _, colName := range columnNames{
		fileName := tb.TableName + "_" + colName + ".idx"

		colType, colPos, ok := tb.FindColumnTypeAndPos(colName)
		if !ok{
			return
		}
	
		indexHeader := IndexHeader{
			RootPageId: 0,
			ColumnPos: colPos,
			IsUnique: false,
			KeyType: colType,
		}

		tree := BPlusTree{
			IndexHeader: &indexHeader,
		}


		index := &Index{
			Name: name,
			TableName: tb.TableName,
			ColumnPos: uint8(colPos),
			FileName: fileName,
			MemTree: &tree,
		}

		tree.TreePath = index.FileName

		tb.Indexes[colName] = index
  }
}

func (db *Database_Manager) MakeIndexes(tb *Table){
	tablePath, ok := db.GetTablePath(tb.TableName)
	if !ok{
		log.Printf("Cannot make indexes for a non existent table!")
		return
	}
	for _, index := range(tb.Indexes){
		tb.MakeIndexMemTreeFromTableFile(index, tablePath)
	}
}

func (tb *Table) MakeIndexMemTreeFromTableFile(idx *Index, tablePath string) {

    for pageId := uint32(0); pageId <= tb.LastPageId; pageId++ {

        page, ok := tb.bufferpool.FetchPage(pageId, tablePath)
        if !ok{
					continue
				}
        header := page.Read_header()

        for slot := 0; slot < int(header.RowCount); slot++ {

            if page.SlotDead(slot) {
                continue
            }

            row := page.Read_row(slot)

            key := tb.extractColumnValue(string(row), idx.ColumnPos)

            encoded := EncodeKey(key, idx.MemTree.IndexHeader.KeyType)

            ptr := RowId{
                PageId: pageId,
                SlotId: uint16(slot),
            }

            idx.MemTree.Insert(encoded, ptr)
					}
    }
}

func (db *Database_Manager) FindByIndex(column string, key []byte, t *Table) []byte {

	idx := t.Indexes[column]

	ptr := idx.MemTree.Search(key)

	if ptr == nil {
		return nil
	}
  
	tablePath, _ := db.GetTablePath(t.TableName)
	page, _ := db.BufferPool.FetchPage(ptr.PageId, tablePath)

	return page.Read_row(int(ptr.SlotId))
}

//Index key extraction logic
func EncodeKey(value interface{}, t ColumnType) []byte {
    switch t {
    case INT:
        return encodeInt(value.(int32))
    case STRING:
        return encodeString(value.(string))
		default:
			 return encodeInt64(value.(int64))
    }
}

func encodeString(s string) []byte {
    buf := make([]byte, 32)

    copy(buf, []byte(s))

    return buf
}

func encodeInt(v int32) []byte {
    buf := make([]byte, 4)
    binary.BigEndian.PutUint32(buf, uint32(v))
    return buf
}

func encodeInt64(v int64) []byte{
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf
}

//later i shall finish the compaction of tables/pages/slots

