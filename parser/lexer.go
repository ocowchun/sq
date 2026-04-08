package parser

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/ocowchun/sq/token"
)

type lexer struct {
	source  string
	start   int
	current int
	line    int
	column  int
}

func newLexer(source string) *lexer {
	return &lexer{
		source:  source,
		start:   0,
		current: 0,
		line:    1,
		column:  1,
	}
}

func (l *lexer) Tokens() ([]token.Token, error) {
	tokens := make([]token.Token, 0)

	for !l.isAtEnd() {
		t, err := l.NextToken()
		if err != nil {
			return tokens, err
		}
		if t.IsTokenType(token.TokenTypeEOF) {
			break
		}

		tokens = append(tokens, t)
	}
	return tokens, nil
}

func (l *lexer) isAtEnd() bool {
	return l.current >= len(l.source)
}

func (l *lexer) advance() byte {
	if l.isAtEnd() {
		panic("can't called Advance when lexer is at end")
	}
	c := l.source[l.current]
	l.current++
	l.column++
	return c
}

func (l *lexer) match(expected byte) bool {
	if l.isAtEnd() {
		return false
	}

	if l.source[l.current] != expected {
		return false
	}

	l.advance()
	return true
}

func (l *lexer) peek() byte {
	if l.isAtEnd() {
		return 0
	}

	return l.source[l.current]
}

func (l *lexer) pos() token.Position {
	return token.Position{
		Offset: l.start,
		Line:   l.line,
		Column: l.column,
	}
}

func (l *lexer) peekNext() byte {
	if l.current+1 >= len(l.source) {
		return 0
	}

	return l.source[l.current+1]
}

func (l *lexer) NextToken() (token.Token, error) {

	for !l.isAtEnd() {
		l.start = l.current

		pos := l.pos()
		c := l.advance()
		switch c {
		case '(':
			return token.Token{Type: token.TokenTypeLeftParen, Lexeme: string(c), Literal: nil, Pos: pos}, nil
		case ')':
			return token.Token{Type: token.TokenTypeRightParen, Lexeme: string(c), Literal: nil, Pos: pos}, nil
		case ',':
			return token.Token{Type: token.TokenTypeComma, Lexeme: string(c), Literal: nil, Pos: pos}, nil
		case '.':
			return token.Token{Type: token.TokenTypeDot, Lexeme: string(c), Literal: nil, Pos: pos}, nil
		case '-':
			if l.match('-') {
				for l.peek() != '\n' && !l.isAtEnd() {
					l.advance()
				}
			} else {
				return token.Token{Type: token.TokenTypeMinus, Lexeme: string(c), Literal: nil, Pos: pos}, nil
			}
		case '+':
			return token.Token{Type: token.TokenTypePlus, Lexeme: string(c), Literal: nil, Pos: pos}, nil
		case '*':
			return token.Token{Type: token.TokenTypeStar, Lexeme: string(c), Literal: nil, Pos: pos}, nil
		case '/':
			return token.Token{Type: token.TokenTypeSlash, Lexeme: string(c), Literal: nil, Pos: pos}, nil
		case ';':
			return token.Token{Type: token.TokenTypeSemicolon, Lexeme: string(c), Literal: nil, Pos: pos}, nil
		case '!':
			if l.match('=') {
				return token.Token{Type: token.TokenTypeBangEqual, Lexeme: "!=", Literal: nil, Pos: pos}, nil
			}
		case '=':
			return token.Token{Type: token.TokenTypeEqual, Lexeme: "=", Literal: nil, Pos: pos}, nil

		case '>':
			if l.match('=') {
				return token.Token{Type: token.TokenTypeGreaterEqual, Lexeme: ">=", Literal: nil, Pos: pos}, nil
			} else {
				return token.Token{Type: token.TokenTypeGreater, Lexeme: ">", Literal: nil, Pos: pos}, nil
			}
		case '<':
			if l.match('=') {
				return token.Token{Type: token.TokenTypeLessEqual, Lexeme: "<=", Literal: nil, Pos: pos}, nil
			} else {
				return token.Token{Type: token.TokenTypeLess, Lexeme: "<", Literal: nil, Pos: pos}, nil
			}
		case ' ':
			noop()
		case '\r':
			noop()
		case '\t':
			noop()
		case '\n':
			l.line++
			l.column = 1
		case '"':
			str, err := l.nextString('"')
			if err != nil {
				return token.Token{Type: token.TokenTypeString, Lexeme: str, Literal: str, Pos: pos}, err
			}
			return token.Token{Type: token.TokenTypeString, Lexeme: str, Literal: str, Pos: pos}, nil
		case '\'':
			str, err := l.nextString('\'')
			if err != nil {
				return token.Token{Type: token.TokenTypeString, Lexeme: str, Literal: str, Pos: pos}, err
			}
			return token.Token{Type: token.TokenTypeString, Lexeme: str, Literal: str, Pos: pos}, nil

		default:
			if isDigit(c) {
				return l.nextNumber(pos)
			} else if isAlpha(c) {
				return l.nextKeywordOrIdentifier(pos)
			}
			return token.Token{Type: token.TokenTypeEOF, Lexeme: string(c), Literal: nil, Pos: pos}, fmt.Errorf("Unexpected character %x", c)

		}
	}

	return token.Token{Type: token.TokenTypeEOF, Lexeme: "", Literal: nil, Pos: l.pos()}, nil
}

func isAlpha(c byte) bool {
	return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_'
}

func (l *lexer) nextKeywordOrIdentifier(pos token.Position) (token.Token, error) {
	for isAlpha(l.peek()) || isDigit(l.peek()) {
		l.advance()
	}

	str := l.source[l.start:l.current]

	switch strings.ToLower(str) {
	case "and":
		return token.Token{Type: token.TokenTypeAnd, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "false":
		return token.Token{Type: token.TokenTypeFalse, Lexeme: str, Literal: false, Pos: pos}, nil
	case "null":
		return token.Token{Type: token.TokenTypeNull, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "or":
		return token.Token{Type: token.TokenTypeOr, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "not":
		return token.Token{Type: token.TokenTypeNot, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "true":
		return token.Token{Type: token.TokenTypeTrue, Lexeme: str, Literal: true, Pos: pos}, nil

	case "as":
		return token.Token{Type: token.TokenTypeAs, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "select":
		return token.Token{Type: token.TokenTypeSelect, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "from":
		return token.Token{Type: token.TokenTypeFrom, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "where":
		return token.Token{Type: token.TokenTypeWhere, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "like":
		return token.Token{Type: token.TokenTypeLike, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "in":
		return token.Token{Type: token.TokenTypeIn, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "on":
		return token.Token{Type: token.TokenTypeOn, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "is":
		return token.Token{Type: token.TokenTypeIs, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "left":
		return token.Token{Type: token.TokenTypeLeft, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "inner":
		return token.Token{Type: token.TokenTypeInner, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "join":
		return token.Token{Type: token.TokenTypeJoin, Lexeme: str, Literal: nil, Pos: pos}, nil
	case "with":
		return token.Token{Type: token.TokenTypeWith, Lexeme: str, Literal: nil, Pos: pos}, nil

	default:
		return token.Token{Type: token.TokenTypeIdentifier, Lexeme: str, Literal: nil, Pos: pos}, nil
	}
}

func isDigit(c byte) bool {
	return c >= '0' && c <= '9'
}

func (l *lexer) nextNumber(pos token.Position) (token.Token, error) {
	for isDigit(l.peek()) {
		l.advance()
	}

	isFloat := false
	if l.peek() == '.' && isDigit(l.peekNext()) {
		isFloat = true
		l.advance()

		for isDigit(l.peek()) {
			l.advance()
		}
	}

	str := l.source[l.start:l.current]
	if isFloat {
		num, err := strconv.ParseFloat(str, 64)
		if err != nil {
			return token.Token{Type: token.TokenTypeDouble, Lexeme: str, Literal: nil, Pos: pos}, err
		}
		return token.Token{Type: token.TokenTypeDouble, Lexeme: str, Literal: num, Pos: pos}, nil
	} else {
		num, err := strconv.ParseInt(str, 10, 64)
		if err != nil {
			return token.Token{Type: token.TokenTypeInt, Lexeme: str, Literal: nil, Pos: pos}, err
		}
		return token.Token{Type: token.TokenTypeInt, Lexeme: str, Literal: num, Pos: pos}, nil

	}
}

func (l *lexer) nextString(endChar byte) (string, error) {
	for l.peek() != endChar && !l.isAtEnd() {
		if l.peek() == '\n' {
			l.line++
		}
		l.advance()
	}
	if l.isAtEnd() {
		return "", errors.New("unterminated string.")
	}

	l.advance()

	str := l.source[l.start+1 : l.current-1]
	return str, nil
}

func noop() {

}
