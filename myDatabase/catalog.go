package myDatabase

import (
	"encoding/binary"
	"errors"
	"io"
	"io/fs"
	"log"
	"os"
)

/* My mental flow for this catalog logic, --helps me boostrap faster later when using:
  clgMngr := NewCatalog()
	clgMngr.LoadDatabaseCatalog()

	For create database:
	   clgMngr.AddDatabaseCatalog(dbName)
*/

var systemPath = GetSystemPath() 
var cat_sys_database_file = systemPath +"/sys_databases.tbl"
var cat_tables_file = systemPath +"/sys_tables.tbl"

const lenOffset = 1 
const typeOffset = 1
const LastPageIdLen = 4 

type TableCata struct{
	TableName string
	LastPageId uint32
	FirstFramePageID uint32
	TableSchema Schema
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
	Tables map[string]*Table
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
		if errors.Is(err, fs.ErrExist){
			log.Printf("system issue alleviated, process flow continues")
			return clgMngr, true
		}
		log.Fatal("System Failure, Shutting down")
	}

  return clgMngr,true
}

//For create database workflow
//Every db and tbl shall be responsible for persisting their catas to this catalog, so hard to manage from here intead the caller can just use a combination of clg pointer and extra steps to do it
func (clg *CatalogManager) UpdateDatabaseCatalog(db *Database_Manager, catEntry *CatalogEntry){
	entry, ok := clg.CatalogEntry[db.Dbname]
	if ok{
    entry.UpdateCatalogEntryWith(catEntry)
		return
	}

	clg.CatalogEntry[db.Dbname] = *catEntry
}

//For the system starting
//Same with a call to fill the catalog entries
func (clg *CatalogManager) LoadDatabaseCatalog(){
	c := make(chan []Page)
	clg.ScanFile(cat_tables_file, 8, c) 

	for pages := range(c){
		for _,page := range pages{
				pg := page

				header := pg.Read_header()
				for s :=0; s<int(header.RowCount); s++{
					row := pg.Read_row(s)

					offset := 0
					dbNameLen := int(row[offset])
					offset +=1
					dbName := string(row[offset:offset+dbNameLen])
					DBTablesCataFile := dbName+"/_tables.tbl"
					DBIndexesCataFile := dbName+"/_indexes.tbl"
					
					clgEntry := CatalogEntry{}
					//load index and tables catalogs
					clg.LoadIndexMeta(DBIndexesCataFile, &clgEntry)
					clg.LoadTableMeta(DBTablesCataFile, &clgEntry)

					//so the tables and indexes cata are well aligned the only issue would be the database, what if this db has more than one row or overflows into next page etc? I still think such a case is very difficult as every row just stored one string the database name in the sys_database_file file. I will need to confirm later
					clg.CatalogEntry[dbName] = clgEntry
				}
			}
   }
	//return clg
}

func (clg *CatalogManager) LoadIndexMeta(dbIndexesPath string, catalogEntry *CatalogEntry){
	c := make(chan []Page)
	clg.ScanFile(dbIndexesPath, 8, c)
	
	catalogEntry.IndexMetas = make(map[IndexFrame]IndexCata)
	for pages := range c{
		for _, page := range pages{
      pg := &page
			header := pg.Read_header()
			currOffset := 0
			rB := pg.data
			for s := 0; s <= int(header.RowCount); s++{
				indexFileLen := uint8(rB[currOffset])
				currOffset += 1
				indexFB := make([]byte, 0)
				copy(indexFB, rB[currOffset: currOffset+int(indexFileLen)])
				indexFile := string(indexFB)
				currOffset += int(indexFileLen)
				indexNameLen := uint8(rB[currOffset])
				currOffset += 1
				indexName := string(rB[currOffset:currOffset+int(indexNameLen)])
				currOffset += int(indexNameLen)
				indexedTableLen := uint8(rB[currOffset])
				currOffset += 1
				indexedTable := string(rB[currOffset:currOffset+int(indexedTableLen)])
				currOffset += int(indexedTableLen)
				columnPos := uint8(rB[currOffset])

				indexCata := IndexCata{
					 IndexFile: indexFile,
					 ColumnPos: columnPos,
				}

				//incase a table had not just one indexes
				indexFrame := IndexFrame{indexedTable, indexName}
				catalogEntry.IndexMetas[indexFrame] = indexCata
			}
		}
	}
}

func (clg CatalogManager) SaveTable(dbName string, table *Table){
	dbEntry, ok := clg.CatalogEntry[dbName]
	if !ok{
		log.Printf("Can't save table into an uninitialised database in the catalog")
		return
	}

	_, k := dbEntry.Tables[table.TableName]
	if k{
		log.Printf("Table already exists!")
		return
	} 

	dbEntry.Tables[table.TableName] = table
}

func (clg CatalogManager) DeleteTable(dbName string, table *Table){
	dbEntry, ok := clg.CatalogEntry[dbName]
	if !ok{
		log.Printf("Can't delete table into an uninitialised database in the catalog")
		return
	}

	_, k := dbEntry.Tables[table.TableName]
	if k{
		delete(dbEntry.Tables, table.TableName)
		return
	} 
}

func (clg *CatalogManager) BuildIndexesIntoTable(tableName string, dbName string){
	clgEntry := clg.CatalogEntry[dbName]

	table := clgEntry.Tables[tableName]
	table.Indexes = make(map[string]*Index, 0)

	for k, cata := range clgEntry.IndexMetas{
		if k.IndexedTable != table.TableName{
			continue
		}

		index := &Index{}

		index.ColumnPos = cata.ColumnPos
		index.FileName = cata.IndexFile
		index.Name = cata.IndexFile
    index.BuildMemTreeFromIndexFile()
		col := table.TableSchema.Columns[index.ColumnPos]
		table.Indexes[col.ColumnName] = index
	}
}

func (clg *CatalogManager) LoadTableMeta(dbTablesPath string, catalogEntry *CatalogEntry){
	catalogEntry.Tables = make(map[string]*Table, 0)
	
	c := make(chan []Page)
	clg.ScanFile(dbTablesPath, 8, c)

	for pages := range c{
		for _,page := range pages{
			pg := &page
			header := pg.Read_header()
			for s := 0; s<=int(header.RowCount);s++{
				table := Table{}
				rB := page.Read_row(s)
				tableSchema := Schema{}
				tableSchema.Columns = make([]Column,0)
				
				currOffset := 0
				tableNameLen := uint8(rB[currOffset])
				currOffset += lenOffset
				tableName := string(rB[currOffset:currOffset+int(tableNameLen)])
				currOffset += int(tableNameLen)
				lastPageId := binary.LittleEndian.Uint32(rB[currOffset:currOffset+4])
				currOffset += 4
				firstFramePageId := binary.LittleEndian.Uint32(rB[currOffset:currOffset+4])
        currOffset += 4 
				//The next data bytes have two preceeding meta before them len and type both 1 bytes as the catalogs needed to track themselves here unlike my normal user tables 
				//where columns or rather schema begins is an extra byte to inform how many cols there are
				totalCols := uint8(rB[currOffset])
				currOffset +=1

				schema := Schema{}
				schemaCols := make([]Column, 0)
				for colNo := 1; colNo <= int(totalCols); colNo++{
					colType := uint8(rB[currOffset])
					currOffset += 1
					colLen := uint8(rB[currOffset])
					currOffset += 1
					
					switch colType{
					case 1:
						//haha i previously read it into an int then i was struggling to find the column name, realized it was like i was using two columnTypes separately yet it was meant the first offset to infer the column type already
						colName := string(rB[currOffset:currOffset+int(colLen)])
						currOffset += int(colLen)

						column := Column{
							ColumnName : colName,
							ColumnType : BOOLEAN,
							nullable : false,
						}
					
						schemaCols = append(schemaCols, column)

					case 2:
						colName := string(rB[currOffset:currOffset+int(colLen)])
						currOffset += int(colLen)

						column := Column{
							ColumnName : colName,
							ColumnType : INT,
							nullable : false,
						}
						schemaCols = append(schemaCols, column)

					case 3:
						colName := string(rB[currOffset:currOffset+int(colLen)])
						currOffset += int(colLen)

						column := Column{
							ColumnName: colName,
							ColumnType: STRING,
							nullable: false,
						}
						schemaCols = append(schemaCols, column)
					}
				}

				schema.Columns = schemaCols

				table = Table{
					TableName: tableName,
					LastPageId: lastPageId,
					FirstFramePageId: firstFramePageId,
					TableSchema: schema,
				}
        catalogEntry.Tables[tableName]=&table
			}
		}
  }
}

func (clg *CatalogManager) LoadCatalog(){
	clg.CatalogEntry = make(map[string]CatalogEntry, 0)
  clg.LoadDatabaseCatalog()	
}

func (clg *CatalogManager) PurgeTable(){

}

func (clg *CatalogManager) ScanFile(fileName string, ScanPages uint8, c chan []Page ){
	if ScanPages >10{
		ScanPages =10
	}

	table_pages := make([]Page, ScanPages)
	f, err := os.Open(fileName)
	defer f.Close()
	if err != nil{
		log.Printf("Error reading catalog table, ", err)
		return
	}
	for {
		pg := Page{}
		n, err := f.Read(pg.data[:])
		if err != nil{
			log.Printf("Error occured reading file, %v", err)
			if err ==io.EOF{
				c <- table_pages
				return
			}
		}
    log.Printf("Bytes read: %v", n)
		table_pages = append(table_pages, pg)

		if len(table_pages) >= int(ScanPages){
			c <- table_pages

			table_pages = make([]Page, 0, ScanPages)
		}
	}
}

func (clg *CatalogManager) AddDatabaseCatalog(dbName string){
	if _, ok := clg.CatalogEntry[dbName]; ok{
		log.Printf("Database already exists!")
		return
	}
	clg.CatalogEntry[dbName] = CatalogEntry{}
}

func (ce *CatalogEntry)UpdateCatalogEntryWith(catEntry *CatalogEntry){
	//update tables 
	 for updatek, updatev := range catEntry.Tables{
		 existentTable, keyExisted := ce.Tables[updatek]
		 if keyExisted{
			 existentTable.Indexes = updatev.Indexes
			 existentTable.TableSchema = updatev.TableSchema
			 existentTable.LastPageId = updatev.LastPageId
			 existentTable.FirstFramePageId = updatev.FirstFramePageId

			 ce.Tables[updatek] = existentTable
			 break
		 }

		 ce.Tables[updatek] = updatev
	}
}

