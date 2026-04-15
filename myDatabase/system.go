package myDatabase

import (
	"log"
)

const sysPath = "/home/nines/Desktop/gon/TestDB"

type DBSystem struct{
	Catalog *CatalogManager
	Pager Pager
	Sessions map[string]*Database_Manager
}

func GetSystemPath() string{
	return sysPath
}

func InitSystem() *DBSystem{
	log.Printf("SYSTEM STARTING..")
	clgMngr ,_ := NewCatalog()
	log.Printf("NewCatalog done!")
	clgMngr.LoadCatalog()
//this should be the perfect solution here should really handle the panic on a nil map i think,lets look at the rest of runtime erros suggestion
	sess := make(map[string]*Database_Manager, 0)
	log.Printf("Started successfully!")
  return &DBSystem{
    Catalog: clgMngr,
		Sessions: sess,
	}
}

func (syst *DBSystem) GetDatabase(dbName string) (*Database_Manager, bool){
	_, ok := syst.Catalog.CatalogEntry[dbName]
	if !ok{
		log.Printf("Database does not exist!")
		log.Printf("or the Database id is yet to updated into the catalog map!")
		return nil, false
	}

	catalog := CatalogManager{}
	dbMngr := &Database_Manager{}
	dbMngr.Catalog = &catalog
	dbMngr.Dbname = dbName
	dbMngr.Pager = &syst.Pager
	dbMngr.InitDB()
  
	syst.NewSession(dbMngr)
	return dbMngr, true
}

func (syst *DBSystem) NewSession(db *Database_Manager){
  if syst.InSession(db){
	  return
	}

	log.Printf("Adding the db[%v] to sessions as it didn't exist", db.Dbname)
  syst.Sessions[db.Dbname] = db
}

func (syst *DBSystem) InSession(db *Database_Manager) bool{
  _, in := syst.Sessions[db.Dbname]
	return in
}

func (syst *DBSystem) EndSession(db *Database_Manager){
  delete(syst.Sessions, db.Dbname)
}

