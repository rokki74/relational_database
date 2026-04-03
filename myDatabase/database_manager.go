package myDatabase

import(
	"catalog/CatalogManager"
	"os"
)

type Database_Manager struct{
	dbName string
	BufferPool *BufferPool
	Catalog *CatalogManager
	Pager *Pager
}

func (sys *DBSystem) CreateDatabase(name string) (*Database_Manager, bool){
 //i cannot ascertain as of now whether the ModeDir is really used corectly to create dir
 err := os.Mkdir(sys.SysPath+"/"+name, fs.ModeDir)
 if err !=nil{
	 log.Printf("Could not build the database dir! %",err)
	 return nil, false
 }

 sys.Catalog.AddDatabaseCatalog(name)
 //Need to udate catalog entry to record this event
 dbMngr := &Database_Manager{dbName: name, BufferPool: &sys.BufferPool, Pager: &sys.Pager}
 return dbMngr, true
}

func (sys *DBSystem) OpenDatabase(name string) (*Database_Manager, bool){
	dbCata, ok := sys.Catalog.CatalogEntry[name]

	if !ok{
		return nil, false
	}

	dbMngr := &Database_Manager{
		dbName: name,
		BufferPool: &sys.BufferPool,
		Catalog: &dbCata,
		Pager: &sys.Pager,
	}

	return dbMngr, true
}

