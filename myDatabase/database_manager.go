package myDatabase

import (
	"log"
	"os"
	"bytes"
)

type system DBSystem

type Database_Manager struct{
	Dbname string
	BufferPool *BufferPool
	Catalog *CatalogManager
	Pager *Pager
	WAL *WalManager
	TransactionManager TransactionManager
	DbPath string
}

func (syst *DBSystem) CreateDatabase(name string) bool{
 //i cannot ascertain as of now whether the ModeDir is really used corectly to create dir
 err := os.Mkdir(GetSystemPath()+"/"+name, 0755)
 if err !=nil{
	 log.Printf("Could not build the database dir! %",err)
	 return false
 }
 
 log.Printf("The database created successfully, updating it into the catalog")
 syst.Catalog.AddDatabaseCatalog(name)
 return true
}

//someone is Initing DB each time making the databse loose it's catalog then..
func (db *Database_Manager) InitDB(syst *DBSystem){
	db.WAL = NewWalManager(GetSystemPath()+"/"+db.Dbname)
	db.DbPath = GetSystemPath()+"/"+db.Dbname
	db.Pager = &syst.Pager

	db.BufferPool = &BufferPool{
		Pager: db.Pager,
		capacity: 0,
		Fsm: NewFsmManager(),
	}

	db.NewTransactionManager()
	db.Catalog = syst.Catalog
	db.FillFSM()
	log.Printf("database successfully initialized!")
}

type ObjectType int
const (
  TABLETYPE ObjectType = iota
	INDEXTYPE
	FSMTYPE
)

func (db *Database_Manager) GetObjectPath(objectName string, objType ObjectType) (string, bool){
	switch objType{ 
	   case TABLETYPE:
		    return db.GetTablePath(objectName), true
		 case INDEXTYPE:
		    return db.GetIndexPath(objectName)
		 case FSMTYPE:
		    return db.GetFsmPath(objectName)
		 default:
		    log.Printf("Object type unspecified, fitting it to table")
				return db.GetTablePath(objectName), true
	}
}

func (db *Database_Manager) GetTablePath(tableName string) string{
  tb, exists := db.GetTable(tableName)
	if !exists{
		return ""
	}

	return  db.DbPath+"/"+tb.TableName+".tbl"
}

//to remove the unnecessary bool returned
func (db *Database_Manager) GetFsmPath(tableName string) (string, bool){
	log.Printf("Building the fsmPath string..")
	if db != nil{
	fsmPath := db.DbPath+"/"+tableName+".fsm"

	log.Printf("returning fsm path[%v]",fsmPath)
	return fsmPath, true
  }

	log.Printf("the db struct is a nil at the GetFsmPath")
	return "", false
}

func (db Database_Manager) SaveTable(tb *Table){

	tablePath := db.DbPath+"/"+tb.TableName+".tbl" 
	log.Printf("db.SaveTable hit, saving into path: %v", tablePath)
	db.BufferPool.FlushTable(tablePath, tb)
	db.Catalog.SaveTable(db.Dbname, tb)

	fsmPath := db.DbPath+"/"+tb.TableName+".fsm"
	log.Printf("creating fsm for the new table..")
	_, fsmerr := os.Create(fsmPath)
	if fsmerr != nil{
		log.Printf("the fsm file won't be created!, Err: %v", fsmerr)
	}
  fsmPage := Page{}
	fsmPage.Init(uint32(0))
  db.BufferPool.FlushFsmPage(fsmPath, &fsmPage) 
  log.Printf("fsm for the intended table created and initial page persisted buy pager")
}

func (db *Database_Manager) DeleteTable(tb *Table){
	db.Catalog.DeleteTable(db.Dbname, tb)

	tablePath := db.GetTablePath(tb.TableName)
	db.BufferPool.DeleteTableName(tablePath)
}

func (db *Database_Manager) GetIndexPath(tableName string) (string, bool){
  _, exists := db.GetTable(tableName)
	if !exists{
		return "", false
	}

  return db.DbPath+"/"+tableName+".idx", true
}

func (db *Database_Manager) Recover(){
  db.WAL.Recover(db, 0)
}

func (db *Database_Manager) NewTransactionManager(){
  lckMngr := LockManager{}
	lckMngr.lockTable = make(map[ResourceKey]Lock,0)
  transactionManager := TransactionManager{
	                    nextTxnId: uint8(0),
											ActiveTxns: make(map[uint8]*Transaction,0),
											DbManager: db,
											lockMngr: &lckMngr,
	                 }

	db.TransactionManager = transactionManager
}

func (db *Database_Manager) FillFSM(){
  tableNames := make([]string, 0)
	for k, _ := range db.Catalog.CatalogEntry[db.Dbname].Tables{
	   tableNames = append(tableNames, k)
	}
  db.BufferPool.Fsm.FillFsms(db, tableNames)
}

func extractKey(row []byte, colPos uint8, colType ColumnType) []byte {
	offset := 0
	switch colType {
	case INT:
		return row[offset : offset+4]

	case STRING:
		length := int(row[offset])
		offset += 1
		return row[offset : offset+length]
	}

	return nil
}

func (db *Database_Manager) DeleteFromIndexes(
	tb *Table,
	rowId RowId,
	row []byte,
) {
	for _, index := range tb.Indexes {

		tree := index.MemTree

		key := extractKey(row, tree.IndexHeader.ColumnPos, tree.IndexHeader.KeyType)

		tree.Delete(key, rowId)
	}
}

func (db *Database_Manager) InsertIntoIndexes(
	tb *Table,
	rowId RowId,
	row []byte,
) {
	for _, index := range tb.Indexes {

		tree := index.MemTree

		key := extractKey(row, tree.IndexHeader.ColumnPos, tree.IndexHeader.KeyType)

		tree.Insert(key, rowId)
	}
}

func (db *Database_Manager) UpdateIndexes(
	tb *Table,
	rowId RowId,
	oldRow []byte,
	newRow []byte,
	updatedCols []string,
) {

	if len(tb.Indexes) == 0 {
		return
	}

	for _, colName := range updatedCols {

		index, ok := tb.Indexes[colName]
		if !ok {
			continue
		}

		tree := index.MemTree

		colPos := tree.IndexHeader.ColumnPos
		colType := tree.IndexHeader.KeyType

		oldKey := extractKey(oldRow, colPos, colType)
		newKey := extractKey(newRow, colPos, colType)

		// If value didn't change → skip
		if bytes.Equal(oldKey, newKey) {
			continue
		}

		tree.Delete(oldKey, rowId)

		tree.Insert(newKey, rowId)
	}
}



