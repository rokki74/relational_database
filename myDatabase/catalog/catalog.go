package catalog

import (
	"log"
	"os"
	"real_dbms/myDatabase"
)

const cat_tables_file = "sys_tables.tbl"
const cat_indexes_file = "sys_indexes.tbl"

type TableCata struct{
	TableName string
	LastPageId uint32
	TableSchema myDatabase.Schema
}

type IndexCata struct{
	IndexFile string
	IndexName string
	IndexedTable string
	ColumnPos uint8
}

type CatalogManager struct{
	Tables map[string]myDatabase.Table
}

func (clg *CatalogManager) NewCatalog(fullDBPath string) bool{
	_, err1 := os.Create(fullDBPath+cat_tables_file)
	_, err2 := os.Create(fullDBPath+cat_indexes_file)

	if err1 | err2 !=nil{return false}
	return true
}

func (clg *CatalogManager) LoadTables() bool{
	clg.Tables := make(map[string]*myDatabase.Table)
	f, err := os.Open(cat_tables_file)
	if err != nil{
		log.Printf("Cannot load tables due to, %v", err)
		return false
	}

    pg myDatabase.Page := Page{}
		for nextBuf := 4096; readOffset, err := f.ReadAt(&pg.data, int64(nextBuf)){
			if err !=nil{
				if err == io.EOF{
					log.Printf("End of file")
					return
				} 
				return
			}

			header := pg.read_header()
			for rowC := 0; rowC < header.rowCount{
				row_data := pg.read_row(rowC)
				tableCata := parseTableMeta(row_data)
				clg.Tables := append(clg.Tables, *tableCata)
			}
		  nextBuf = nextbuf + 4096
	  }
}

func (clg *CatalogManager) parseTableMeta(row_data string) *TableCata{
	tableName := 
	lastPageId :=
	schema := 

	TableCata := TableCata{
		TableName: tableName,
		LastPageId: lastPageId,
		TableSchema: schema,
	}
}

func (clg *CatalogManager) PurgeTable(){

}




