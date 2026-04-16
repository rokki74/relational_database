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

	log.Printf("The system knows the database[%v] exists", dbName)
  
	return syst.NewSession(dbName), true
}

func (syst *DBSystem) NewSession(databaseName string) *Database_Manager{
	probeDb, sessioned := syst.InSession(databaseName)
	if sessioned{
		log.Printf("database already in session, skipping initializing it..")
	  return probeDb
	}

	log.Printf("Adding the db[%v] to sessions as it didn't exist", databaseName)

	log.Printf("initializing then later adding database into session..")
	dbMngr := &Database_Manager{Dbname: databaseName}
	dbMngr.InitDB(syst)
  syst.Sessions[databaseName] = dbMngr 

	return dbMngr
}

func (syst *DBSystem) InSession(dbName string) (*Database_Manager , bool){
  dbMngr, in := syst.Sessions[dbName]
	return dbMngr,in
}

func (syst *DBSystem) EndSession(db *Database_Manager){
  delete(syst.Sessions, db.Dbname)
}

