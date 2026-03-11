package myDatabase

import (
	"encoding/binary"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

type ColumnType int 
type TotalPagesSZ uint32
type SchemaSizeSZ uint32
type ColumnCountSZ uint16
type ColumnTypeSZ uint16
type columnNameLengthSZ uint16 

const(
	INT ColumnType = iota
	STRING 
	BOOLEAN
)

type Column struct{
	columnName string
	columnType ColumnType
}

type Schema struct{
	columns []Column
}

type FsmEntry struct{
	PageId uint32,
	freeBytes uint16,
}

type Table struct {
    TableName       string,
		TableSchema Schema,
		LastPageId uint32,
		LastFramePageId uint32,
		bufferpool BufferPool,
    Index  map[string]*Index,
		WAL *WalManager,
}

func (db *Database_Manager) createTable(name string, columns []Column) Table{
		schm := Schema{
		   columns: columns,
     	}

   table :=  Table{
		   TableName: name,
			 TableSchema: schm,
	   }

  pgr := Pager{}
	//i think this next shld have been returning table's filename so we updated in in our tablesmap
	if pgr.SaveTable(*table, db.dbPath){
		return table
	}
} 

//After a scan get the pageId's that are free and by how many bytes then parse to this function to write to the fsm 
func (tb *Table) writeFsm(pageId, freeBytes uint16) bool{
	//What if a table only ever had it's tableId and uses it only then the bufferpool knows the real name for it using the id
	page := tb.bufferpool.fetch_page(tableId, LastFramePageId)
	header := page.read_header()

	binary.LittleEndian.PutUint16(page[header.freeSpaceOffset:header.freeSpaceOffset+4], uint32(pageId))
  header.freeSpaceOffset += 4
	binary.LittleEndian.PutUint32(page[header.freeSpaceOffset:header.freeSpaceOffset+2], uint16(freeBytes))
	header.freeSpaceOffset += 2
	page.write_header(&header)

	tb.bufferpool.SavePage(tableId, page)
}

func (tb *Table) deleteTable(){
	tb.bufferpool.DeleteTableById(tb.tableId)
}

func (tl *Table) read(){

}

func (tl *Table) compact_table(){

}

func (tl *Table) close_table(){

}

func (t *Table) Insert(row string) {
   page := tb.bufferpool.FittingPage(tb.tableId, len(row))

	 rec := &WalRecord{
		 TableId: t.TableName,
		 PageId: page.PageId,
		 Operation: WAL_INSERT,
		 DataSize: len(row),
		 Data: []byte(row),
	 }

	 lsn := t.WAL.Log(&rec)
	 //the normal transaction flow can resume after wal being prioritized
	 page.PageLSN = lsn
	 pageId, SlotId := page.insert_row(row)
	 tb.bufferpool.MarkDirty(tb.tableId, page.PageID)

	 ptr := &RowId{pageId, SlotId}

	for col, idx := range t.Indexes.indexes {

		key := extractColumnValue(row, col)

		idx.Tree.Insert(key, ptr)
	}
}

func extractColumnValue(row string, col string) interface{} {

    parts := strings.Split(row, ",")

    pos, colType := findColumnPosAndType(col)

    value := parts[pos]

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

func (tb *Table)findColumnPosAndType(col string) (bool, int, ColumnType){
	columns := tb.TableSchema.columns
	for i=0; i<len(columns); i++{
		if columns[i].columnName == col{
			return true, i,columns[i].columnType
		}
	}
	log.Printf("The column [%col] couldn't be found in table schema!", col)

	return
}

func (tb *Table) CreateIndex(name string, column string) {

	fileName := t.Name + "_" + column + ".idx"

	ok, colPos, colType := tb.findColumnPosAndType(column)
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
		IndexHeader: *indexHeader,
		BufferPool: *tb.bufferPool,
	}


	index := &Index{
		Name: name,
		TableId: t.TableId,
		Column: column,
		FileName: fileName,
		MemTree: tree,
	}

	tb.Index[column] = index
}

func (tb *Table) buildIndex(idx *Index) {

    for pageId := uint32(0); pageId <= tb.LastPageId; pageId++ {

        page := tb.bufferpool.FetchPage(tb.TableId, pageId)

        header := page.read_header()

        for slot := 0; slot < int(header.rowCount); slot++ {

            if page.isSlotDead(slot) {
                continue
            }

            row := page.read_row(slot)

            key := extractColumnValue(row, idx.Column)

            encoded := EncodeKey(key, idx.MemTree.IndexHeader.KeyType)

            ptr := RowId{
                PageId: pageId,
                SlotId: uint16(slot),
            }

            idx.MemTree.Insert(encoded, ptr)
        }
    }
}

func (t *Table) FindByIndex(column string, key []byte) *Row {

	idx := t.Indexes.indexes[column]

	ptr := idx.Tree.Search(key)

	if ptr == nil {
		return nil
	}

	page := t.BufferPool.FetchPage(t.TableId, ptr.PageId)

	return page.GetRow(ptr.SlotId)
}

//Index key extraction logic
func EncodeKey(value interface{}, t ColumnType) []byte {
    switch t {
    case INT:
        return encodeInt(value.(int32))
    case STRING:
        return encodeString(value.(string))
    case TIMESTAMP:
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
	buf :make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(v))
	return buf
}

