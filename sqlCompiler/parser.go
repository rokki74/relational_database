package sqlCompiler

import (
	"log"
	"real_dbms/myDatabase"
	"strings"
)

type Statement interface{
	stmtNode()
}

type UseStmt struct{
	DBName string
}

type CreateDBStmt struct{
  DBName string
}

type CreateTBLStmt struct{
  TBLName string
	Columns []myDatabase.Column
}

type CreateIDXStmt struct{
  ParentTableName string
  IDXName string
	Columns []string
}

type InsertStmt struct {
	  TBLName   string
    Columns []string
    Values  []Expr
}

type DeleteStmt struct {
    TBLName string
    Where Expr
}

type ObjectType int
const (
	DATABASE ObjectType = iota
	TABLE 
	INDEX
)

type UpdateStmt struct {
	TBLName string
	Set   map[string]Expr
	Where Expr
}

type SelectStmt struct{
	Columns []string 
	TBLName string 
	Where Expr
}

func (*SelectStmt) stmtNode(){}
func (*UpdateStmt) stmtNode(){}
func (*DeleteStmt) stmtNode(){}
func (*InsertStmt) stmtNode(){}
func (*CreateDBStmt) stmtNode(){}
func (*CreateIDXStmt) stmtNode(){}
func (*CreateTBLStmt) stmtNode(){}
func (*UseStmt) stmtNode(){}

type Expr interface{}

type Identifier struct{
	Name string
}

type NumberLiteral struct{
	Value string
}

type StringLiteral struct{
	Value string
}

type BinaryExpr struct{
	Left Expr 
	Op TokenType
	Right Expr
}

type Parser struct{
	lexer *Lexer
	curToken Token
	peekToken Token
}

func NewParser(l *Lexer) *Parser {
    p := &Parser{lexer: l}

    // Load two tokens
    p.nextToken()
    p.nextToken()

    return p
}

func (p *Parser) nextToken() {
    p.curToken = p.peekToken
    p.peekToken = p.lexer.NextToken()
}

func (p *Parser) expect(t TokenType) {
    if p.curToken.Type != t {
			//This is becoming ambigous, how do i make it get me human readable types only?
			//How i regret it i should have redirected the runtime logs to a file
				log.Printf("Expected a %v found value %v of type %v", t,p.curToken.Value,p.curToken.Type)
				return
    }
    p.nextToken()
}

func (p *Parser) match(t TokenType) bool {
    if p.curToken.Type == t {
        p.nextToken()
        return true
    }
    return false
}

func (p *Parser) parseColumns() []string {
    var cols []string

    for {
        cols = append(cols, p.curToken.Value)
        p.expect(IDENT)

        if !p.match(COMMA) {
            break
        }
    }

    return cols
}

var precedences = map[TokenType]int{
    OR:  1,
    AND: 2,
    EQ:  3,
    NEQ: 3,
    LT:  3,
    GT:  3,
    LTE: 3,
    GTE: 3,
}

func (p *Parser) curPrecedence() int {
    if prec, ok := precedences[p.curToken.Type]; ok {
        return prec
    }
    return 0
}

func (p *Parser) parseExpression(precedence int) (Expr, bool) {

    left, ok := p.parsePrimary()
		if !ok{
			return nil, false
		}

    for p.curToken.Type != SEMICOLON && precedence < p.curPrecedence() {
        op := p.curToken.Type
        p.nextToken()

        right, ok := p.parseExpression(precedences[op])

				if !ok{
					return nil, false
				}

        left = &BinaryExpr{
            Left:  left,
            Op:    op,
            Right: right,
        }
    }

    return left, true
}

func (p *Parser) parsePrimary() (Expr, bool) {
    tok := p.curToken

    switch tok.Type {

    case IDENT:
        p.nextToken()
        return &Identifier{Name: tok.Value}, true

    case NUMBER:
        p.nextToken()
        return &NumberLiteral{Value: tok.Value}, true

    case STRING:
        p.nextToken()
        return &StringLiteral{Value: tok.Value}, true

    case LPAREN:
        p.nextToken()
        expr, ok := p.parseExpression(0)
				if !ok{
					return nil, false
				}
        p.expect(RPAREN)
        return expr, true

    default:
        log.Printf("unexpected token in expression: " + tok.Value)
				return nil, false
    }
}

func (p *Parser) ParseStatement() Statement{
	switch p.curToken.Type{
	case USE:
		log.Printf("found a use statement, ready to handle it")
		return p.ParseUse()
	case SELECT:
		return p.parseSelect()
	case UPDATE:
		return p.parseUpdate()
	case INSERT:
		return p.parseInsert()
	case DELETE:
		return p.parseDelete()
	case CREATE:
		return p.parseCreate()
	default:
		log.Printf("Unexpected statement %v", p.curToken.Value)
		return nil
 	}
}

func (p *Parser) ParseUse() Statement{
	p.expect(USE)
	stmt := &UseStmt{}
	stmt.DBName = p.curToken.Value
	p.expect(IDENT)
	return stmt
}

func (p *Parser) parseSelect() Statement{
	stmt := &SelectStmt{}
	p.expect(SELECT)
	columns := p.parseColumns()
	stmt.Columns = columns

	p.expect(FROM)
	table := p.curToken.Value
	p.expect(IDENT)
	stmt.TBLName = table
	
	if p.match(WHERE){
		expr, ok := p.parseExpression(0)
		if ok{
			stmt.Where = expr
		}
	}

	return stmt
}

func (p *Parser) parseUpdate() Statement{
	stmt := &UpdateStmt{}
	stmt.Set = make(map[string]Expr)

  p.expect(UPDATE)
	stmt.TBLName = p.curToken.Value
	p.expect(IDENT)

	p.expect(SET)

	for{
		col := p.curToken.Value
		p.nextToken()

		p.expect(EQ)
		expr, ok := p.parseExpression(0)
		if !ok{
			return nil
		}

		stmt.Set[col] =expr
		if !p.match(COMMA){
			break;
		}
	}

	if p.match(WHERE){
		expr, prst := p.parseExpression(0)
		if prst{
			stmt.Where = expr
		}
	}

	return stmt
}

func (p *Parser) parseInsert() Statement{
	stmt := &InsertStmt{}

	p.expect(INSERT)
	p.expect(INTO)

	stmt.TBLName = p.curToken.Value
	p.expect(IDENT)
	p.expect(LPAREN)

	stmt.Columns = p.parseColumns()
	p.expect(RPAREN)
	p.expect(VALUES)
	p.expect(LPAREN)

	for{
		expr, ok := p.parseExpression(0)
		if !ok{
			return nil
		}
		stmt.Values = append(stmt.Values, expr)

		if !p.match(COMMA){
			break
		}
	}

	p.expect(RPAREN)
	return stmt
}

func (p *Parser) parseDelete() Statement{
	stmt := &DeleteStmt{}

	p.expect(DELETE)
	p.expect(FROM)
	stmt.TBLName = p.curToken.Value
	p.expect(IDENT)
	
	if p.match(WHERE){
		expr, ok := p.parseExpression(0)
		if ok{
			stmt.Where = expr
		}
	}

	return stmt
}

func (p *Parser) parseCreate() Statement{
	log.Printf("parse create was hit, expecting a CREATE keyword now!")
	p.expect(CREATE)
	identType := p.curToken.Value
	identType = strings.ToUpper(identType)
	log.Printf("done expecting CREATE keyword, looking at the identType, it is a: %v", identType)
	log.Printf("expecting an IDENT from now!")
	p.expect(IDENT)
	switch identType{
	case "TABLE":
		return p.parseCreateTable()
	case "DATABASE":
		return p.parseCreateDatabase()
	case "INDEX":
		return p.parseCreateIndex()
	default:
	  log.Printf("Cannot create unknown object on create statement, %v", identType)
		return nil
	}
}

func (p *Parser) parseCreateDatabase() Statement{
 databaseName := p.curToken.Value
 return &CreateDBStmt{
   DBName: databaseName,
 } 
}

func (p *Parser) parseCreateTable() Statement{
	log.Printf("parse create table hit, expecting a TABLE keyword as an IDENT now!")
 tableName := p.curToken.Value
 log.Printf("Upto this point we got a tableName from the statement, it is: %v", tableName)
 stmt := &CreateTBLStmt{
   TBLName: tableName,
 }

 log.Printf("Expecting the table name so next we can expect an LPAREN")
 p.expect(IDENT)
 p.expect(LPAREN)
 log.Printf("a LPAREN is already expected, moving into deserializing the columns now")
 log.Printf("the LPAREN id no is: %v", LPAREN)
 columns := make([]myDatabase.Column,0)

 for{
	 colName := p.curToken.Value
   log.Printf("The net put into the columns river caught a fish column called: %v", colName)
	 p.expect(IDENT)
	 temType := strings.ToUpper(p.curToken.Value)
	 log.Printf("The anticipated column type is: %v", temType)
	 var colType myDatabase.ColumnType 
	 switch temType{
	 case "INT":
		 log.Printf("The int type case")
		 colType = myDatabase.INT
	 case "STRING":
		 colType = myDatabase.STRING
	 case "BOOLEAN":
		 colType = myDatabase.BOOLEAN
	 }

	 col := myDatabase.Column{
		 ColumnName: colName,
		 ColumnType: colType,
	 }

	 columns = append(columns, col)
	 log.Printf("Expecting the column type")
	 p.expect(IDENT)
	 log.Printf("ColumnType expected, now a comma")
	 if !p.match(COMMA){
		 log.Printf("the token didn't match a comma, breaking..")
		 break
	 }

		 log.Printf("continuing with the column loop..")
 }
 p.expect(RPAREN)

 stmt.Columns = columns
 log.Printf("parseCreateTable finished successfully!")
 return stmt
}

func (p *Parser) parseCreateIndex() Statement{
  indexName := p.curToken.Value
	p.expect(IDENT)
	p.expect(ON)
	tableName := p.curToken.Value
	p.expect(LPAREN)
	cols := make([]string, 0)
	for {
	  col := p.curToken.Value
		cols = append(cols, col)

		if !p.match(COMMA){
		  break
		}
	}
	p.expect(RPAREN)
	return &CreateIDXStmt{
		ParentTableName: tableName,
	  IDXName: indexName,
		Columns: cols,
	}
}

