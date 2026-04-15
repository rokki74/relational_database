package server

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"real_dbms/sqlCompiler"
	"real_dbms/myDatabase"
	"log"
)

type Server struct {
	RealDB *myDatabase.DBSystem
}

func NewServer() *Server {
	return &Server{
		RealDB: myDatabase.InitSystem(),
	}
}

func RunServerManager(){
	server := NewServer()
	server.Start(":5439")
}

var cnts int = 0
func (s *Server) Start(port string) {
	cnts++
	log.Printf("called the %v", cnts)
	ln, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("error from server trying to listen to port, %v", err)
	}
	defer ln.Close()

	fmt.Println("DB Server running on port", port)

	fmt.Println("Server up and running, Waiting for requests..")
	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}

		go s.handleClient(conn)
	}
}

func (s *Server) handleClient(conn net.Conn) {
	defer conn.Close()

	fmt.Println("Client connected:", conn.RemoteAddr())

	reader := bufio.NewReader(conn)
  executor := &sqlCompiler.Executor{
		Syst: s.RealDB,
	}
	for {
		// Prompt
		conn.Write([]byte("db > "))

		// Read SQL until semicolon
		sql, err := reader.ReadString(';')
		if err != nil {
			fmt.Println("Client disconnected")
			return
		}

		sql = strings.TrimSpace(sql)
		if sql == "" {
			continue
		}

		// Run SQL pipeline
		result, err := s.executeSQL(sql, executor)
		if err != nil {
			conn.Write([]byte("ERROR: " + err.Error() + "\n"))
			continue
		}

		// Send result
		s.sendResult(conn, result)
	}
}

func (s *Server) executeSQL(sql string, executor *sqlCompiler.Executor) ([][]string, error) {
	lexer := sqlCompiler.NewLexer(sql)
	parser := sqlCompiler.NewParser(lexer)



	stmt := parser.ParseStatement()
	if stmt == nil {
		return nil, fmt.Errorf("failed to parse SQL, It might be empty: %v", stmt)
	}

	result := executor.Execute(stmt)

	return result, nil
}

func (s *Server) sendResult(conn net.Conn, rows [][]string) {
	if rows == nil {
		conn.Write([]byte("OK\n"))
		return
	}

	for _, row := range rows {
		line := strings.Join(row, " | ")
		conn.Write([]byte(line + "\n"))
	}

	conn.Write([]byte("\n"))
}

