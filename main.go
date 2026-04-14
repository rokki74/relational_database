package main

import(
	"real_dbms/server"
)
func main() {
	server := server.NewServer()
	server.Start(":5432")
}
