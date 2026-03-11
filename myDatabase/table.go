package myDatabase

import (
	"encoding/binary"
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

type TableFileHeader struct{
	TableSchema Schema,
	LastPageId Uint32,
}

type FsmEntry struct{
	PageId uint32,
	freeBytes uint16,
}

type Table struct {
    TableName       string,
		Manager    *TableManager,
		LastPageId uint32,
		LastFramePageId uint32,
		bufferpool BufferPool,
    Index  map[string]*Index, 
}

func (db *Database_Manager) createTable(name string, columns []Column) Table{
		schm := Schema{
		   columns: columns,
     	}

   table :=  Table{
		   TableName: name,
	   }

	pgr := table.pager
	pgr.Header = TableFileHeader{
		TableSchema: schm,
		//then the schemaSize
	}

	if pgr.SaveTable(*table, tlm.dbPath){
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

func (tb *Table) insert(row string) RowId{
	 page := tb.bufferpool.FittingPage(tb.tableId, len(row))
	 pageId, SlotId := page.insert_row(row)
	 tb.bufferpool.MarkDirty(tb.tableId, page.PageID)

	 return &RowId{pageId, SlotId}
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
	ptr := t.insertRow(row)

	for col, idx := range t.Indexes.indexes {

		key := extractColumnValue(row, col)

		idx.Tree.Insert(key, ptr)
	}
}

func extractColumnValue(row string, col string){

}

func (t *Table) CreateIndex(name string, column string) {

	fileName := t.Name + "_" + column + ".idx"

	tree := &BPlusTree{
		TableId: t.TableId,
		FileName: fileName,
		BufferPool: t.BufferPool,
	}

	index := &Index{
		Name: name,
		TableId: t.TableId,
		FileName: fileName,
		HeaderMeta: IndexHeader,
		MemTree: tree,
	}

	t.Index[column] = index
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

