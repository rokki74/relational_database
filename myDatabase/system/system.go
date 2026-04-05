package system

import(
	"log"
	"real_dbms/myDatabase"
	"real_dbms/myDatabase/catalog"
)

const sysPath = "/home/nines/Desktop/gon/TestDB"

type DBSystem struct{
	Catalog *catalog.CatalogManager
	Pager myDatabase.Pager
	Executor myDatabase.Executor
	Sessions map[string]*myDatabase.Database_Manager
}

func GetSystemPath() string{
	return sysPath
}

func InitSystem() *DBSystem{
	log.Printf("SYSTEM STARTING..")
	clgMngr ,_ := catalog.NewCatalog()
	clgMngr.LoadDatabaseCatalog()

	log.Printf("Started successfully!")
  return &DBSystem{
    Catalog: clgMngr,
	}
}

func (syst *DBSystem) GetDatabase(dbName string) (*myDatabase.Database_Manager, bool){
	cata, ok := syst.Catalog.CatalogEntry[dbName]
	if !ok{
		return nil, false
	}

	dbMngr := &myDatabase.Database_Manager{}
	dbMngr.Catalog = cata
	dbMngr.Dbname = dbName
	dbMngr.Pager = &syst.Pager
	dbMngr.InitDb()
  
	syst.NewSession(dbMngr)
	return dbMngr, true
}

func (syst *DBSystem) NewSession(db *myDatabase.Database_Manager){
  if syst.InSession(db){
	  return
	}
  syst.Sessions[db.Dbname] = db
}

func (syst *DBSystem) InSession(db *myDatabase.Database_Manager) bool{
  _, in := syst.Sessions[db.Dbname]
	return in
}

func (syst *DBSystem) EndSession(db *myDatabase.Database_Manager){
  delete(syst.Sessions, db.Dbname)
}

