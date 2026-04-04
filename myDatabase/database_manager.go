package myDatabase

import(
	"os"
	"log"
)

type Database_Manager struct{
	Dbname string
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
 dbMngr := &Database_Manager{Dbname: name, BufferPool: &syst.BufferPool, Pager: &syst.Pager}
 return dbMngr, true
}

func (db *Database_Manager) InitDbWal(){
	db.WAL = NewWalManager(GetSystemPath+"/"+db.Dbname)
}

func (db *Database_Manager) GetTablePath(tbName string) (string, bool){
  _, exists := db.GetTable(tbName)
	if !exists{
		return "", false
	}

  return GetSystemPath()+"/"+db.Dbname+"/"+tbName+".tbl", true
}

func (db *Database_Manager) Recover(){
  db.WAL.Recover(db, 0)
}

