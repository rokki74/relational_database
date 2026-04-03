package main

import(
	"real_dbms/myDatabase/server"
)

func main() {
	server := NewServer("data")
	server.Start(":5432")
}
