package myDatabase

import(
	"os"
	"log"
	"real_dbms/myDatabase/system"
	"real_dbms/myDatabase/catalog"
)

type DBSystem system.DBSystem
type CatalogManager catalog.CatalogManager

type Database_Manager struct{
	Dbname string
	BufferPool *BufferPool
	Catalog *CatalogManager
	Pager *Pager
	WAL *WalManager
	FsmManager *FSMManager
	TransactionManager TransactionManager
	DbPath string
}

func (syst *DBSystem) CreateDatabase(name string) bool{
 //i cannot ascertain as of now whether the ModeDir is really used corectly to create dir
 err := os.Mkdir(system.GetSystemPath()+"/"+name, fs.ModeDir)
 if err !=nil{
	 log.Printf("Could not build the database dir! %",err)
	 return false
 }

 syst.Catalog.AddDatabaseCatalog(name)
 return true
}

func (db *Database_Manager) InitDB(){
	db.WAL = NewWalManager(system.GetSystemPath()+"/"+db.Dbname)
	db.DbPath = system.GetSystemPath()+"/"+db.Dbname
	db.FsmManager = NewFsmManager()

	db.BufferPool = &BufferPool{
		Pager: db.Pager,
		capacity: 0,
		fsm: db.FsmManager,
	}

	db.NewTransactionManager()
	db.FillFSM()
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
		    return db.GetTablePath(objectName)
		 case INDEXTYPE:
		    return db.GetIndexPath(objectName)
		 case FSMTYPE:
		    return db.GetFsmPath(objectName)
		 default:
		    log.Printf("Object type unspecified, fitting it to table")
				return db.GetTablePath(objectName)
	}
}

func (db *Database_Manager) GetTablePath(tableName string) (string, bool){
  _, exists := db.GetTable(tableName)
	if !exists{
		return "", false
	}

  return db.DbPath+"/"+tableName+".tbl", true
}

func (db *Database_Manager) GetFsmPath(tableName string) (string, bool){
  _, exists := db.GetTable(tableName)
	if !exists{
		return "", false
	}

  return db.DbPath+"/"+tableName+".fsm", true
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
	for k, v := range db.Catalog.CatalogEntry[db.Dbname].Tables{
	   tableNames = append(tableNames, k)
	}
  db.FsmManager.FillFsms(db, tableNames)
}

func UpdateIndexes(table *Table, values, pageID uint32, slot Slot){

}


