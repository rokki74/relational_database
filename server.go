package myDatabase

import (
	"bufio"
	"fmt"
	"net"
	"strings"
	"real_dbms/sqlCompiler"
	"log"
)

type Server struct {
	RealDB *system
}

func NewServer() *Server {
	return &Server{
		RealDB: system.InitSystem(),
	}
}

func (s *Server) Start(port string) {
	ln, err := net.Listen("tcp", port)
	if err != nil {
		log.Printf("error from server trying to listen to port")
		panic(err)
	}
	defer ln.Close()

	fmt.Println("DB Server running on port", port)

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
		result, err := s.executeSQL(sql)
		if err != nil {
			conn.Write([]byte("ERROR: " + err.Error() + "\n"))
			continue
		}

		// Send result
		s.sendResult(conn, result)
	}
}

func (s *Server) executeSQL(sql string) ([][]string, error) {
	lexer := sqlCompiler.NewLexer(sql)
	parser := sqlCompiler.NewParser(lexer)
	session := system.Session{}
	parser.ParseUse(&session)

  executor := sqlCompiler.Executor{session, &s.RealDB}
	db, exists := executor.syst.GetDatabase(executor.session.CurrentDB)
	if !exists{
			log.Printf("Cannot execute database does not exist!")
	}

	stmt := parser.ParseStatement()
	if stmt == nil {
		return nil, fmt.Errorf("failed to parse SQL")
	}

	result := executor.Execute(stmt, db)

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

