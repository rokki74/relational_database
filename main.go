package main

import(
	"real_dbms/myDatabase"
)
func main() {
	server := myDatabase.NewServer()
	server.Start(":5432")
}
