package system

import(
	"log"
	"real_dbms/myDatabase"
	"real_dbms/myDatabase/catalog"
)

const sysPath = ""

type DBSystem struct{
	Catalog *catalog.CatalogManager
	BufferPool myDatabase.BufferPool
	Pager myDatabase.Pager
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
		BufferPool: myDatabase.BufferPool{},
		Pager: myDatabase.Pager{},
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
	dbMngr.dbName = dbName

	return dbMngr, true
}

