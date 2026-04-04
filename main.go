package main

import(
	"real_dbms/myDatabase/server"
)
func main() {
	server := server.NewServer()
	server.Start(":5432")
}
