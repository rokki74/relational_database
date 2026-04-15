package myDatabase

import (
	"encoding/binary"
	"io"
	"log"
	"os"
	"path/filepath"
)

/* My mental flow for this catalog logic, --helps me boostrap faster later when using:
  clgMngr := NewCatalog()
	clgMngr.LoadDatabaseCatalog()

	For create database:
	   clgMngr.AddDatabaseCatalog(dbName)
*/

/*THE MAIN TABLES IN SYS DATABASE
sys_tables_meta.tbl
sys_indexes_meta.tbl
sys_databases_meta.tbl
*/
const sys_tables_m = "sys_tables_meta.tbl"
const sys_indexes_m = "sys_indexes_meta.tbl"
const sys_databases_m = "sys_databases_meta.tbl"

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

//This was my suspected culprit, everything may be working but then the maps within this CatalogEntry can't be accessed as they
//haven't be initialized yet thus i need to use make on the clEntry variable i recently used! going back there
type CatalogEntry struct{
	Tables map[string]*Table
	IndexMetas map[IndexFrame]IndexCata
}

//clg manager here hasn't built the indexes instead using small meta, --this shall be a reminder for me later
type CatalogManager struct{
	CatalogEntry map[string]CatalogEntry
	SysDBDir string
}

func NewCatalog() (*CatalogManager, bool){
	clgMngr := &CatalogManager{}
	clgMngr.SysDBDir = filepath.Join(GetSystemPath()+"/sys")
	err := os.MkdirAll(clgMngr.SysDBDir, 0755)
  if err != nil{
		log.Printf("system init may have failed, ERROR: %v",err)
		log.Fatal("System Failure, Shutting down")
	}

	log.Printf("The catalog init was successful, ..yet to load, have a look: %v", clgMngr)
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
	fl := filepath.Join(clg.SysDBDir, sys_databases_m)
	tabl := &Table{}
	tabl.TableName = "Databases"
	log.Printf("ready to scan the sys_databases_m file")
	goodStat := clg.ScanFile(fl, 8, c) 
  if !goodStat{
		return
	}
	totalPages := 0
	log.Printf("Preparing to start ranging the chan")
	for pages := range(c){
		log.Printf("ranging the chan now")
		for _,page := range pages{
			  totalPages += 1
				pg := page

				header := pg.Read_header()
				for s :=0; s<int(header.RowCount); s++{
					row := pg.Read_row(s)

					offset := 0
					dbNameLen := int(row[offset])
					offset +=1
					dbName := string(row[offset:offset+dbNameLen])
					DBTablesCataFile := filepath.Join(dbName,"/_tables.tbl")
					DBIndexesCataFile := filepath.Join(dbName,"/_indexes.tbl")
					
					clgEntry := CatalogEntry{}
					//load index and tables catalogs
					clg.LoadIndexMeta(DBIndexesCataFile, &clgEntry)
					clg.LoadTableMeta(DBTablesCataFile, &clgEntry)

					//so the tables and indexes cata are well aligned the only issue would be the database, what if this db has more than one row or overflows into next page etc? I still think such a case is very difficult as every row just stored one string the database name in the sys_database_file file. I will need to confirm later
					clg.CatalogEntry[dbName] = clgEntry
					tabl.LastPageId = uint32(totalPages - 1)

				}
			}
   }
	//The catalog is storing info about itself also and an in-mem so it can later utilise bufferpool
	sysCatEntry := CatalogEntry{}
	sysCatEntry.Tables[tabl.TableName] = tabl
	clg.CatalogEntry["sys"] = sysCatEntry
}

func (clg *CatalogManager) LoadIndexMeta(dbIndexesPath string, catalogEntry *CatalogEntry){
	c := make(chan []Page)
	fl := filepath.Join(clg.SysDBDir, filepath.Join("/", sys_databases_m))
	clg.ScanFile(fl, 8, c)
  tabl := &Table{}
	tabl.TableName ="Indexes"

	catalogEntry.IndexMetas = make(map[IndexFrame]IndexCata)
	totalPages := 0
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

			totalPages += 1
			tabl.LastPageId = uint32(totalPages - 1)
		}
	}

	existent, ok := clg.CatalogEntry["sys"]
	if !ok{
		existent.Tables[tabl.TableName] = tabl
		clg.CatalogEntry["sys"] = existent

		return
	}

	existent.Tables[tabl.TableName] = tabl
}

func PersistDB(){

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
	fl := filepath.Join(clg.SysDBDir, filepath.Join("/",sys_tables_m))
	clg.ScanFile(fl, 8, c)

  tabl := &Table{}
	tabl.TableName ="Tables"
	totalPages := 0
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

			totalPages += 1
			tabl.LastPageId = uint32(totalPages - 1)
		}
  }

	existent, ok := clg.CatalogEntry["sys"]
	if !ok{
    existent.Tables[tabl.TableName] = tabl
		clg.CatalogEntry["sys"] = existent
		return
	}

	existent.Tables[tabl.TableName] = tabl
}

func (clg *CatalogManager) LoadCatalog(){
	clg.CatalogEntry = make(map[string]CatalogEntry, 0)
  clg.LoadDatabaseCatalog()	
}

func (clg *CatalogManager) PurgeTable(){

}

func (clg *CatalogManager) ScanFile(fileName string, ScanPages uint8, c chan []Page )bool{
	if ScanPages >10{
		ScanPages =10
	}

	table_pages := make([]Page, ScanPages)
	f, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE, 0666)
	info, e := f.Stat()
	if e != nil {
			log.Printf("Could not retrieve file stats: %v", err)
	}

	if info.Size() == 0 {
			log.Println("File is empty. Skipping read loop.")
			return false
	}

	if err != nil{
		log.Printf("Error reading catalog table, %v", err)
		if err ==io.EOF{
			log.Printf("didn't even hit the chan seems the file is so so new!")
			return true
		}
	}

	defer f.Close()
	for {
		pg := Page{}
		n, err := f.Read(pg.data[:])
		if n > 0 {
        log.Printf("Bytes read: %v", n)
        table_pages = append(table_pages, pg)
    }
		if err != nil{
			log.Printf("Error occured reading file, %v", err)
			if err ==io.EOF{
				c <- table_pages
				return true
			}

			log.Printf("Terminal error reading file %v", err)
		}

		if len(table_pages) >= int(ScanPages){
			c <- table_pages

			table_pages = make([]Page, 0, ScanPages)
		}
	}
}

func (clg *CatalogManager) AddDatabaseCatalog(dbName string){
	log.Printf("Add database catalog hit, in a bid to add the dbName into in-mem catalog")
	if _, ok := clg.CatalogEntry[dbName]; ok{
		log.Printf("Database already exists!")
		return
	}
	clEntry := CatalogEntry{}
  clEntry.Tables = make(map[string]*Table, 0)
	clEntry.IndexMetas = make(map[IndexFrame]IndexCata, 0)
	clg.CatalogEntry[dbName] = clEntry
	log.Printf("Db added to the catalog successfully!")
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


func FetchSysPage(clg *CatalogManager, bf *BufferPool, tableName string, pageId uint32) (*Page, bool){
   switch tableName{
	   case "DATABASES":
			 flpath := clg.SysDBDir+ sys_databases_m
       return bf.FetchPage(pageId, flpath)
		 case "TABLES":
       flpath := clg.SysDBDir+ sys_tables_m

       return bf.FetchPage(pageId, flpath)
		 case "INDEXES":
			 flpath := clg.SysDBDir+ sys_indexes_m

       return bf.FetchPage(pageId, flpath)
		 default:
			 log.Printf("The table provided is not of the sys directory")
			 return &Page{}, false
	 }
}

func SaveSysPage(clg *CatalogManager,bf *BufferPool, tableName string, page *Page){
		switch tableName{
	   case "DATABASES":
			 flpath := clg.SysDBDir+ sys_databases_m
			 bf.SavePage(flpath, *page)
		 case "TABLES":
       flpath := clg.SysDBDir+ sys_tables_m
			 bf.SavePage(flpath, *page)
		 case "INDEXES":
			 flpath := clg.SysDBDir+ sys_indexes_m
			 bf.SavePage(flpath, *page)
		 default:
			 log.Printf("The table provided is not of the sys directory")
	 }
}

