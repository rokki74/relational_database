package sqlCompiler

import(
	"log"
	"real_dbms/myDatabase/system"
)

type Statement interface{
	stmtNode()
}

type UseStmt struct{
	DBName string
}

type InsertStmt struct {
    Table   string
    Columns []string
    Values  []Expr
}

type DeleteStmt struct {
    Table string
    Where Expr
}

type ObjectType int
const (
	DATABASE ObjectType = iota
	TABLE 
	INDEX
)

type CreateStmt struct{
	objectType ObjectType
	objectName string
}

type UpdateStmt struct {
    Table string
    Set   map[string]Expr
    Where Expr
}

type SelectStmt struct{
	Columns []string 
	Table string 
	Where Expr
}

func (*SelectStmt) stmtNode(){}
func (*UpdateStmt) stmtNode(){}
func (*DeleteStmt) stmtNode(){}
func (*InsertStmt) stmtNode(){}
func (*CreateStmt) stmtNode(){}

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

func (p *Parser) ParseUse(session *system.Session){
	p.expect(USE)
	session.currentDB = p.curToken.Value
	p.expect(IDENT)
}

func (p *Parser) parseSelect() Statement{
	stmt := &SelectStmt{}

	p.expect(SELECT)
	columns := p.parseColumns()
	stmt.Columns = columns

	p.expect(FROM)
	table := p.curToken.Value
	p.expect(IDENT)
	stmt.Table = table
	
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
	stmt.Table = p.curToken.Value
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

	stmt.Table = p.curToken.Value
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
	stmt.Table = p.curToken.Value
	p.expect(IDENT)
	
	if p.match(WHERE){
		expr, ok := p.parseExpression(0)
		if ok{
			stmt.Where = expr
		}
	}

	return stmt
}

//CAUTION! incomplete
func (p *Parser) parseCreate() Statement{
	stmt := &CreateStmt{}
	return stmt
}





