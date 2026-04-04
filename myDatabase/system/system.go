package system

import(
	"log"
	"real_dbms/myDatabase"
	"real_dbms/myDatabase/catalog"
)

const sysPath = "/home/nines/Desktop/gon/TestDB"

type DBSystem struct{
	Catalog *catalog.CatalogManager
	Wal myDatabase.WalManager
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
		Wal: myDatabase.WalManager{},
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
	dbMngr.InitDbWal()

	return dbMngr, true
}

