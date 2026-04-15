package sqlCompiler

import(
	"strings"
)

type TokenType int 
const (
	ILLEGAL TokenType = iota
	IDENT

	USE
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
	SET

  LT 
	GT 
	LTE 
	GTE 
	EQ
	NEQ
	OR
	AND

	EOF
)

var keywords = map[string]TokenType{
    "SELECT": SELECT,
    "INSERT": INSERT,
    "UPDATE": UPDATE,
    "DELETE": DELETE,
    "CREATE": CREATE,
    "FROM":   FROM,
    "WHERE":  WHERE,
    "INTO":   INTO,
    "VALUES": VALUES,
    "ON":     ON,
		"USE": USE,
	}

type Lexer struct {
    input        string
    leftPointer     int  // current position
    rightPointer int  // next leftPointer
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
    if l.rightPointer >= len(l.input) {
        l.ch = 0 // EOF
    } else {
        l.ch = l.input[l.rightPointer]
    }
    l.leftPointer = l.rightPointer
    l.rightPointer++
}

func (l *Lexer) peekChar() byte {
    if l.rightPointer >= len(l.input) {
        return 0
    }
    return l.input[l.rightPointer]
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
    start := l.leftPointer
    for isLetter(l.ch) || isDigit(l.ch) {
        l.readChar()
    }
    return l.input[start:l.leftPointer]
}

func lookupIdent(ident string) TokenType {
    upper := strings.ToUpper(ident)
    if tok, ok := keywords[upper]; ok {
        return tok
    }
    return IDENT
}

func (l *Lexer) readNumber() string {
    start := l.leftPointer
    for isDigit(l.ch) {
        l.readChar()
    }
    return l.input[start:l.leftPointer]
}

func (l *Lexer) readString() string {
    l.readChar() // skip opening '

    start := l.leftPointer

		for l.ch !=0{
			if l.ch =='\''{
				if l.peekChar() == '\''{
					l.readChar()
				}else{
					break
				}
			}

			l.readChar()
		}

    value := l.input[start:l.leftPointer]

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



