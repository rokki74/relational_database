package sqlcompiler

import(
	"strings"
)

type TokenType int 
const (
	ILLEGAL TokenType = iota
	IDENT

	SELECT
	UPDATE
	INSERT
	DELETE

	NUMBER
	STRING
	
	COMMA
	STAR
	LPAREN
	RPAREN
	SEMICOLON

  ON 
	CREATE
	VALUES
	INTO
	WHERE
	FROM

  LT 
	GT 
	LTE 
	GTE 
	EQ
	NEQ

	EOF
)

var keywords = map[string]TokenType{
    "SELECT": SELECT,
    "INSERT": INSERT,
    "UPDATE": UPDATE,
    "DELETE": DELETE,
    "FROM":   FROM,
    "WHERE":  WHERE,
    "INTO":   INTO,
    "VALUES": VALUES,
    "CREATE": CREATE,
    "ON":     ON,
}

type Lexer struct {
    input        string
    position     int  // current position
    readPosition int  // next position
    ch           byte // current char
}

type Token struct {
    Type  TokenType
    Value string
}

func NewLexer(input string) *Lexer {
    l := &Lexer{input: input}
    l.readChar()
    return l
}

func (l *Lexer) readChar() {
    if l.readPosition >= len(l.input) {
        l.ch = 0 // EOF
    } else {
        l.ch = l.input[l.readPosition]
    }
    l.position = l.readPosition
    l.readPosition++
}

func (l *Lexer) peekChar() byte {
    if l.readPosition >= len(l.input) {
        return 0
    }
    return l.input[l.readPosition]
}

func (l *Lexer) skipWhitespace() {
    for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
        l.readChar()
    }
}

func isLetter(ch byte) bool {
    return (ch >= 'a' && ch <= 'z') ||
           (ch >= 'A' && ch <= 'Z') ||
           ch == '_'
}

func isDigit(ch byte) bool {
    return ch >= '0' && ch <= '9'
}

func (l *Lexer) readIdentifier() string {
    start := l.position
    for isLetter(l.ch) || isDigit(l.ch) {
        l.readChar()
    }
    return l.input[start:l.position]
}

func lookupIdent(ident string) TokenType {
    upper := strings.ToUpper(ident)
    if tok, ok := keywords[upper]; ok {
        return tok
    }
    return IDENT
}

func (l *Lexer) readNumber() string {
    start := l.position
    for isDigit(l.ch) {
        l.readChar()
    }
    return l.input[start:l.position]
}

func (l *Lexer) readString() string {
    l.readChar() // skip opening '

    start := l.position

    for l.ch != '\'' && l.ch != 0 {
        l.readChar()
    }

    value := l.input[start:l.position]

    l.readChar() // skip closing '
    return value
}

func (l *Lexer) NextToken() Token {
    l.skipWhitespace()

    var tok Token

    switch l.ch {

    case '=':
        tok = Token{Type: EQ, Value: "="}

    case '!':
        if l.peekChar() == '=' {
            ch := l.ch
            l.readChar()
            tok = Token{Type: NEQ, Value: string(ch) + string(l.ch)}
        } else {
            tok = Token{Type: ILLEGAL, Value: string(l.ch)}
        }

    case '<':
        if l.peekChar() == '=' {
            l.readChar()
            tok = Token{Type: LTE, Value: "<="}
        } else {
            tok = Token{Type: LT, Value: "<"}
        }

    case '>':
        if l.peekChar() == '=' {
            l.readChar()
            tok = Token{Type: GTE, Value: ">="}
        } else {
            tok = Token{Type: GT, Value: ">"}
        }

    case ',':
        tok = Token{Type: COMMA, Value: ","}

    case ';':
        tok = Token{Type: SEMICOLON, Value: ";"}

    case '(':
        tok = Token{Type: LPAREN, Value: "("}

    case ')':
        tok = Token{Type: RPAREN, Value: ")"}

    case '*':
        tok = Token{Type: STAR, Value: "*"}

    case '\'':
        tok.Type = STRING
        tok.Value = l.readString()
        return tok

    case 0:
        tok = Token{Type: EOF, Value: ""}

    default:
        if isLetter(l.ch) {
            literal := l.readIdentifier()
            tokType := lookupIdent(literal)
            return Token{Type: tokType, Value: literal}
        } else if isDigit(l.ch) {
            return Token{Type: NUMBER, Value: l.readNumber()}
        } else {
            tok = Token{Type: ILLEGAL, Value: string(l.ch)}
        }
    }

    l.readChar()
    return tok
}



