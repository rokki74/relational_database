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
	FirstFramePageID uint32
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

func (clg *CatalogManager) LoadIndexMeta() map[string]*IndexCata{
	c chan *[]myDatabase.lastPageId
	clg.ScanFile(cat_indexes_file, 8, c)
	indexMetas := make(map[string]*IndexCata)
	for data := range c{
	  pg := Page{}
		pg.data = data

		header := pg.read_header()
		currOffset := 0
		for r := 0; r <= header.rowCount; r++{
		  table := myDatabase.Table{}
			tableSchema := myDatabase.Schema{}
			tableSchema.columns := make([]myDatabase.Column,0)

      indexFileLen := uint8(r[currOffset: currOffset+1])
		  currOffset += 1
			indexFile := string(r[currOffset: currOffset+indexFileLen])
			currOffset += indexFileLen
			indexNameLen := uint8(r[currOffset: currOffset+1])
			currOffset += 1
			indexName := string(r[currOffset:currOffset+indexNameLen])
			currOffset += indexNameLen
			indexedTableLen := uint8(r[:currOffset+indexNameLen])
			currOffset += 1
			indexedTable := string(r[currOffset:currOffset+indexedTableLen])
			currOffset += indexedTableLen
			columnPos := uint8(r[:currOffset+1])

			indexCata := IndexCata{
			   IndexFile: indexFile,
				 IndexName: indexName,
				 IndexedTable: indexedTable,
				 ColumnPos: columnPos,
			}

      indexMetas = append(indexMetas, &indexCata)
		}
	}

	return &indexMetas
}


func (clg *CatalogManager) LoadTableMeta() []TableCata{
  indexMetasMap := clg.LoadIndexMeta()
	clg.Tables := make(map[string]*myDatabase.Table)
	
	c chan *[]myDatabase.Page
	clg.ScanFile(cat_tables_file, 8, c)

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
			firstFramePageId := uint32(r[currOffset:currOffset+lastPageIdLen])

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

      indexCata := indexMetas[tableName]
      index := Index{
			   Name: indexCata.IndexName,
				 FileName: indexCata.IndexFileName,
				 ColumnPos: indexCata.ColumnPos,
			}
			schema.columns = schemaCols

			tableCata := TableCata{
				TableName: tableName,
				LastPageId: lastPageId,
				FirstFramePageID: firstFramePageId,
				TableSchema: schema,
				Index: 
			}

			tableMetas = append(tableMetas, &tableCata)
		}
  }
	return tableMetas
}


func (clg *CatalogManager) PurgeTable(){

}

func (clg *CatalogManager) ScanFile(fileName string, ScanPages uint8, c chan *[]Page ) bool{
	if scanPages >10{
		ScanPages =10
	}

	table_pages := make([]Page, ScanPages)
	f, err := os.open(fileName)
	if err != nil{
		log.Printf("Error reading catalog table, ", err)
		return false
	}
	for {
		pg := Page{}
		pg.data := make([]byte, 4096) 
		n, err := f.Read(pg.data)
		if err != nil{
			log.Printf("Error occured reading file, %v", err)
			if err ==io.EOF{
				c <- &table_pages
				return
			}
		}

		table_pages = append(table_pages, pg)

		if len(table_pages) >= int(ScanPages){
			c <- &table_pages

			table_pages = make([]Page, 0, scanLimit)
		}
	}
}


