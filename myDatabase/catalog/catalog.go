package catalog

import (
	"log"
	"os"
	"real_dbms/myDatabase"
)

/* My mental flow for this catalog logic, --helps me boostrap faster later when using:
  clgMngr := NewCatalog()
	clgMngr.LoadDatabaseCatalog()

	For create database:
	   clgMngr.AddDatabaseCatalog(dbName)
*/

const systemPath = myDatabase.system.GetSystemPath() 
const cat_sys_database_file = systemPath +"/sys_database.tbl"

const lenOffset = 1 
const typeOffset = 1
const lastPageIdLen = 4 

type TableCata struct{
	TableName string
	LastPageId uint32
	FirstFramePageID uint32
	TableSchema myDatabase.Schema
}

type IndexCata struct{
	IndexFile string
	IndexName string
	ColumnPos uint8
}

type IndexFrame struct{
	IndexedTable string
	IndexName string
}

type CatalogEntry struct{
	Tables map[string]*myDatabase.Table
	IndexMetas map[IndexFrame]IndexCata
}

//clg manager here hasn't built the indexes instead using small meta, --this shall be a reminder for me later
type CatalogManager struct{
	CatalogEntry map[string]CatalogEntry
}

func NewCatalog() (*CatalogManager, bool){
	clgMngr := &CatalogManager{}
	_, err := os.Create(cat_sys_database_file)
  if err != nil{
		log.Printf("system init may have failed, checking for catalog.., ERROR: %v",err)
		if os.Exists(cat_sys_database_file){
			log.Printf("system issue alleviated, process flow continues")
			return clgMngr, true
		}
		log.Fatal("System Failure, Shutting down")
	}

  return clgMngr,true
}

//For create database workflow
//Every db and tbl shall be responsible for persisting their catas to this catalog, so hard to manage from here intead the caller can just use a combination of clg pointer and extra steps to do it
func (clg *CatalogManager) AddDatabaseCatalog(dbName string){
	clg.CatalogEntry[dbName] = CatalogEntry{} 
	return initDatabaseCatalog()
}

//For the system starting
//Same with a call to fill the catalog entries
func (clg *CatalogManager) LoadDatabaseCatalog(){
	c chan *[]myDatabase.Page
	clg.ScanFile(cat_tables_file, 8, c) 

	for data := range(c){
		pg := &Page{}
		pg.data = data

		header := pg.Read_header()
		for s :=0; s<header.RowCount; s++{
			row := pg.Read_row(s)

			offset = 0
			dbNameLen :=0
			DBName :=""
			copy(dbNameLen, row[:1])
			offset +=1
			copy(DBName, row[offset:offset+dbNameLen])
			
			DBTablesCataFile = dbName+"/_tables.tbl"
	    DBIndexesCataFile = dbName+"/_indexes.tbl"
      
			clgEntry := CatalogEntry{}
			//load index and tables catalogs
			clg.LoadIndexMeta(DBIndexesCataFile, &clgEntry)
			clg.LoadTableMeta(DBTablesCataFile, &clgEntry)

			//so the tables and indexes cata are well aligned the only issue would be the database, what if this db has more than one row or overflows into next page etc? I still think such a case is very difficult as every row just stored one string the database name in the sys_database_file file. I will need to confirm later
			clg.CatalogEntry[DBName] = clgEntry
		}
	}

	//return clg
}

func (clg *CatalogManager) LoadIndexMeta(dbIndexesPath string, catalogEntry *CatalogEntry){
	c chan *[]myDatabase.lastPageId
	clg.ScanFile(dbIndexesPath, 8, c)
	
	catalogEntry.IndexMetas := make(map[IndexFrame]IndexCata)
	for data := range c{
	  pg := Page{}
		pg.data = data

		header := pg.read_header()
		currOffset := 0
		for r := 0; r <= header.rowCount; r++{
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
				 ColumnPos: columnPos,
			}

			//incase a table had not just one indexes
			indexFrame = IndexFrame{indexedTable, indexName}
			catalogEntry.IndexMetas[indexFrame] = indexCata
		}
	}
}

func (clg *CatalogManager) BuildIndexesIntoTable(tableName string, dbName string){
	clgEntry := clg.CatalogEntry[dbName]

	table := clgEntry.Tables[tableName]
	table.Indexes := make([]Index, 0)

	for k, cata := range clgEntry.IndexMetas{
		if k.IndexedTable != table.TableName{
			continue
		}

		index := &Index{}

		index.ColumnPos := cata.ColumnPos
		index.FileName := cata.IndexFile
		index.Name := cata.IndexFile
    index.MemTree := index.BuildMemTreeFromIndexFile()

		table.Indexes := append(indexes, index)
	}
}

func (clg *CatalogManager) LoadTableMeta(dbTablesPath string, catalogEntry *CatalogEntry){
	catalogEntry.Tables := make(map[string]*myDatabase.Table, 0)
	
	c chan *[]myDatabase.Page
	clg.ScanFile(dbTablesPath, 8, c)

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

			schema.columns = schemaCols

			table = Table{
				TableName: tableName,
				LastPageId: lastPageId,
				FirstFramePageID: firstFramePageId,
				TableSchema: schema,
			}

		}
  }
}

func (clg *CatalogManager) LoadCatalog(){
	clg.CatalogEntry.Tables := make(map[string], 0)
	clg.CatalogEntry.IndexMetas := clg.LoadIndexMeta()

	tableMetas := clg.LoadTableMeta()
	for tableMeta := tableMetas{
		table := Table{
			TableName : tableMeta.TableName,
			LastPageId: tableMeta.LastPageId,
			FirstFramePageID: tableMeta.LastFramePageId,
			TableSchema: tableMeta.TableSchema,
		}

		clg.CatalogEntry.Tables[tableMeta.TableName] = &table
	}
}

func (clg *CatalogManager) PurgeTable(){

}

func (clg *CatalogManager) ScanFile(fileName string, ScanPages uint8, c chan *[]Page ) bool{
	if scanPages >10{
		ScanPages =10
	}

	table_pages := make([]Page, ScanPages)
	f, err := os.Open(fileName)
	defer f.Close()
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


