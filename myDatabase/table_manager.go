package myDatabase

import(
	"os"
)

const TablesFile = "tables_meta.txt"

type TblManager struct{
   file *os.File
	 in_mem 
}
func init(){
  f := os.Open(TablesFile)
	defer f.CLose()
	tbl_manager := Tbl
}
func save_table(){

}
func fetch_tables(){

}


