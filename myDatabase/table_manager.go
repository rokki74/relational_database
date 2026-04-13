package myDatabase

import(
	"os"
)

const TablesFile = "tables_meta.txt"

type TblManager struct{
   file *os.File
}


