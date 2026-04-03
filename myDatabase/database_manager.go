package myDatabase

import(
	"os"
)

type Database_Manager struct{
	dbName string
	BufferPool *BufferPool
	Catalog *CatalogManager
	Pager *Pager
	WAL *WalManager
}

func (syst *DBSystem) CreateDatabase(name string) (*Database_Manager, bool){
 //i cannot ascertain as of now whether the ModeDir is really used corectly to create dir
 err := os.Mkdir(GetSystemPath()+"/"+name, fs.ModeDir)
 if err !=nil{
	 log.Printf("Could not build the database dir! %",err)
	 return nil, false
 }

 syst.Catalog.AddDatabaseCatalog(name)
 //Need to udate catalog entry to record this event
 dbMngr := &Database_Manager{dbName: name, BufferPool: &sys.BufferPool, Pager: &sys.Pager}
 return dbMngr, true
}

func (db *Database_Manager) GetTablePath(tbName string) string{
	return GetSystemPath()+"/"+db.dbName+"/"+tbName+".tbl"
}

