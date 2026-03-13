package catalog

import (
	"log"
	"os"
	"real_dbms/myDatabase"
)

const cat_tables_file = "sys_tables.tbl"
const cat_indexes_file = "sys_indexes.tbl"

const lenOffset = 1 
const typeOffset = 1
const lastPageIdLen := 4 

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

func (clg *CatalogManager) parseTableMeta(row_data string) []TableCata{
	
	clg.Tables := make(map[string]*myDatabase.Table)
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
      
			currOffset := 0
			tableNameLen := uint8(r[currOffset:currOffset+lenOffset])
			currOffset += lenOffset

			tableName := string(r[currOffset:currOffset+tableNameLen])
			currOffset += tableNameLen
			lastPageId := uint32(r[currOffset:currOffset+lastPageIdLen])
			currOffset := currOffset += lastPageIdLen

			//The next data bytes have two preceeding meta before them len and type both 1 bytes as the catalogs needed to track themselves here unlike my normal user tables 
			//where columns or rather schema begins is an extra byte to inform how many cols there are
			totalCols := uint8(r[currOffset:currOffset+1])
			currOffset +=1

			schema := myDatabase.Schema{}
			schemaCols := make([]myDatabase.Column, 0)
			for colNo := 1; colNo <= totalCols; colNo++{
				colLen := uint8(r[currOffset:currOffset+lenOffset])
				currOffset += lenOffset
				colType := uint8(r[currOff:currOffset+typeOffset])
				currOffset += typeOffset
	      
				switch colType{
				case 1:
					//haha i previously read it into an int then i was struggling to find the column name, realized it was like i was using two columnTypes separately yet it was meant the first offset to infer the column type already
					colName := string(r[currOffset:currOffset+colLen])
					currOffset += colLen

					column := Column{
						columnName : colName,
						columnType : myDatabase.BOOLEAN,
						nullable : false,
					}
				
					schemaCols = append(schemaCols, column)

				case 2:
					colName := string(r[currOffset:currOffset+colLen])
					currOffset += colLen

					column := Column{
						columnName : colName,
						columnType : myDatabase.INT,
						nullable : false,
					}
					schemaCols = append(schemaCols, column)

				case 3:
					colName := string(r[currOffset:currOffset+colLen])
					currOffset += colLen

					column := Column{
						columnName: colName,
						columnType: myDatabase.STRING,
						nullable: false,
					}
					schemaCols = append(schemaCols, column)
				}
			}

			schema.columns = schemaCols
			tableCata := TableCata{
				TableName: tableName,
				LastPageId: lastPageId,
				TableSchema: schema,
			}

			tableMetas = append(tableMetas, &tableCata)
		}
  }
	return tableMetas
}

func (clg *CatalogManager) PurgeTable(){

}




