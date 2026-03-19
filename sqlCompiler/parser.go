package sqlcompiler

import(
	"log"
)

type InsertStmt struct {
    Table   string
    Columns []string
    Values  []Expr
}

type DeleteStmt struct {
    Table string
    Where Expr
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

type Expr interface{}

type Identifier struct{
	Name string
}

type NumberLiteral struct{
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
        log.Printf("unexpected token: " + p.curToken.Value)
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
        p.expect(INDENT)

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

func (p *Parser) parseExpression(precedence int) Expr {

    left := p.parsePrimary()

    for p.curToken.Type != SEMICOLON && precedence < p.curPrecedence() {
        op := p.curToken.Type
        p.nextToken()

        right := p.parseExpression(precedences[op])

        left = &BinaryExpr{
            Left:  left,
            Op:    op,
            Right: right,
        }
    }

    return left
}

func (p *Parser) parsePrimary() Expr {
    tok := p.curToken

    switch tok.Type {

    case IDENT:
        p.nextToken()
        return &Identifier{Name: tok.Value}

    case NUMBER:
        p.nextToken()
        return &NumberLiteral{Value: tok.Value}

    case STRING:
        p.nextToken()
        return &StringLiteral{Value: tok.Value}

    case LPAREN:
        p.nextToken()
        expr := p.parseExpression(0)
        p.expect(RPAREN)
        return expr

    default:
        log.Printf("unexpected token in expression: " + tok.Value)
    }
}












