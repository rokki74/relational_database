package myDatabase

import(
	"log"
)

const sysPath = "/home/nines/Desktop/gon/TestDB"

type DBSystem struct{
	Catalog *catalog.CatalogManager
	Pager Pager
	Executor Executor
	Sessions map[string]*Database_Manager
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

func (syst *DBSystem) GetDatabase(dbName string) (*Database_Manager, bool){
	cata, ok := syst.Catalog.CatalogEntry[dbName]
	if !ok{
		return nil, false
	}

	dbMngr := &Database_Manager{}
	dbMngr.Catalog = cata
	dbMngr.Dbname = dbName
	dbMngr.Pager = &syst.Pager
	dbMngr.InitDb()
  
	syst.NewSession(dbMngr)
	return dbMngr, true
}

func (syst *DBSystem) NewSession(db *Database_Manager){
  if syst.InSession(db){
	  return
	}
  syst.Sessions[db.Dbname] = db
}

func (syst *DBSystem) InSession(db *Database_Manager) bool{
  _, in := syst.Sessions[db.Dbname]
	return in
}

func (syst *DBSystem) EndSession(db *Database_Manager){
  delete(syst.Sessions, db.Dbname)
}

