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
	
	myDatabase.Column{
		columnName: 
		columnType: 
		nullable: false,
	}
	table.TableSchema := 
	c chan *[]myDatabase.Page
	table.Scan(8, c)

	tableMetas := make([]TableCata,0)
	for data := range c{
		pg := myDatabase.Page{}
		pg.data = data

		header := pg.read_header()
		for r := 0; r<=header.rowCount;r++{
			table := myDatabase.Table{}
			tableSchema := myDatabase.Schema{}
			tableSchema.columns := make([]myDatabase.Column,0)

			nameColumn := myDatabase.Column{
				columnName: "tableName",
				columType: myDatabase.ColumnType.STRING,
				nullable: false,
			}
			lastPageIdColumn := myDatabase.Column{
				columnName: "lastPageId",
				columnType: myDatabase.ColumnType.INT,
				nullable: false,
			}
			tableSchema.columns = append(tableSchema.columns, nameColumn)
      tableSchema.columns = append(tableSchema.columns, lastPageIdColumn)

			//how to read out the rest of the array now as this should have the schema for the actual table columns
			

			tableName := 
			lastPageId :=
			schema := 

			TableCata := TableCata{
				TableName: tableName,
				LastPageId: lastPageId,
				TableSchema: schema,
			}

			tableMetas = append(tableMetas, &TableCata)
		}

  }
}

func (clg *CatalogManager) PurgeTable(){

}




