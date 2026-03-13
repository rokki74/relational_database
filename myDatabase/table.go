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
	BOOLEAN ColumnType = iota
	INT
	STRING
)

const SEPARATOR = ","

type Column struct{
	columnName string
	columnType ColumnType
	nullable bool
}

type Schema struct{
	columns []Column
}

type FsmEntry struct{
	PageId uint32
	freeBytes uint16
}

type Table struct {
    TableName string
		TableSchema Schema
		LastPageId uint32
		LastFramePageId uint32
		bufferpool BufferPool
    Index  map[string]*Index
		WAL *WalManager
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

func (tl *Table) read(pageId uint32) []string{
	rows := make([]byte, 0)
	filename := tl.TableName +".tbl"
	pg := tl.BufferPool.fetch_page(pageId, filename)

	header := pg.header()
	for i :=0; i<header.rowCount; i++{
		row := pg.read_row(i)
		rows = append(rows, row)
	}

	return rows
}

func (tl *Table) Scan(ScanPages uint8, c chan *[]Page ){
	if scanPages >10{
		ScanPages =10
	}

	table_pages := make([]Page, ScanPages)
	for p := 0; p<=int(tl.LastPageId); p++{
		pg := Page{}
		pg.data = tl.read(p)
		table_pages = append(table_pages, pg)

		if len(table_pages) >= int(ScanPages){
			c <- &table_pages

			table_pages = make([]Page, 0, scanLimit)
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

func (tl *Table) SerializeColumn(row string) []byte{
  parts := strings.Split(row, SEPARATOR)
	cols := tl.TableSchema.columns

	buf := make([]byte, 0)
	for pos, col := range parts{
		val := parts[pos]

		switch col.ColumnType{
		case BOOLEAN:
			if strings.ToLower(val) == "true"{
				buf = append(buf, byte(1))
				buf = append(buf, byte(1))
			}else{
				buf = append(buf, 1)
				buf = append(buf, byte(1))
			}
		case INT:
			v, _ := strings.strconv.Atoi(val)
			buf = append(buf, len(v))
			tmp := make([]byte, 4)
			binary.LittleEndian.PutUint32(tmp, v)
			buf = append(buf, tmp)
		case STRING:
			strBytes := []byte(val)
			col_len := make([]byte, 2)
			binary.LittleEndian.PutUint16(col_len, len(strBytes))
			buf = append(buf, col_len...)
			buf = append(buf, strBytes...)
		}
	}
	return buf
}

func (tl *Table) DeserializeColumns(rowBytes []byte) string{
	cols := tl.TableSchema.columns
	offset := 0

	var rowString string
	for col_pos, col : range cols{
		col_val := ""
		switch col.ColumnType{
		case BOOLEAN:
			value := rowBytes[offset:offset+1]
			if value ==1{
				col_value = "true"
			}else{
				col_value = "false"
			}

			offset += 1
		case INT:
			value := rowBytes[offset:offset+4]
			col_value = uint32(value)

			offset += 4
		case STRING:
			str_len := rowBytes[offset:offset+2]
			offset += 2
			col_value + string(rowBytes[offset:offset+str_len])
		}

		//building back my user facing string
		rowString = rowString+ SEPARATOR + col_value
	}

	return rowString
}

func (t *Table) Insert(row string) {
	 row_bytes := t.SerializeColumns(row)
   page := tb.bufferpool.FittingPage(tb.tableId, len(row_bytes))

	 rec := &WalRecord{
		 TableId: t.TableName,
		 PageId: page.PageId,
		 Operation: WAL_INSERT,
		 DataSize: len(row_bytes),
		 Data: row_data,
	 }

	 lsn := t.WAL.Log(&rec)
	 //the normal transaction flow can resume after wal being prioritized
	 page.PageLSN = lsn
	 pageId, SlotId := page.insert_row(row_bytes)
	 tb.bufferpool.MarkDirty(tb.tableId, page.PageID)

	 ptr := &RowId{pageId, SlotId}

	for col, idx := range t.Indexes.indexes {

		key := extractColumnValue(row, col)

		idx.Tree.Insert(key, ptr)
	}
}

func extractColumnValue(row string, col string) interface{} {

    parts := strings.Split(row, SEPARATOR)

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

func (tbl *Table) Scan(){

}

