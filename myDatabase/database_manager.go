package myDatabase

type Database_Manager struct{
	dbName string,
	dbPath string,
	BufferPool BufferPool
	Tables map[string]*Table,
	Pager *Pager
}

func (db *Database_Manager) CreateDatabase(name string) string{
 wd, e := os.Getwd()
 if e !=nil{
	 log.Printf("Cannot get working dir, error %v",e)
	 return
 }

 dbPath := wd + name
 //i cannot ascertain as of now whether the ModeDir is really used corectly to create dir
 err := os.Mkdir(dbPath, fs.ModeDir)
 if err !=nil{
	 log.Printf("Could not build the database dir! %",err)
	 return
 }
  
 f, er := os.Create(dbPath+"/"+name+".meta")
 if er!=nil{
	 log.Printf("Could not create the metadata fir for the db, due to: %v", er)
	 return
 }

 //The problem as i realised is that meta and other informational data required by db can be written into db but need special ways to decode it back to the meaningful info for the system unlike user rows which are really naked and direct
 //unfished logic for persisting the db's metadata
 f.Write()
 return string dbPath
}

func OpenDatabase(name string) *Database_Manager{
 	lookPath := os.Getwd() + "/"+name

	f, err := os.Open(lookPath+".meta")
	if err !=nil{
		log.Printf("Could not open the database due to error %", err)
		//Somehow the user needs to be getting these errors as the database is his and he himself/herself should be debbuging
		//I therefore need a database error struct to store and return meaningful errors to user
		return
	}

	//read back the meta, a very tricky aspect of the system, if i misread then it's misinformation and breakage!
	//unfished implementation, how do i read back the metadata?
	f.Read()
  
	pgr := Pager{}
	//then fillback the Database_Manager struct
	return &Database_Manager{
		dbName: ,
		dbPath: ,
		Tables: make(map[string]*Table, 3)
		Pager: &Pgr
	}
}

