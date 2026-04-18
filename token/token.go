package token

import "fmt"

type TokenType int

const (
	TokenTypeLeftParen TokenType = iota
	TokenTypeRightParen
	TokenTypeWith
	TokenTypeSelect
	TokenTypeWhere
	TokenTypeFrom
	TokenTypeLike
	TokenTypeIn
	TokenTypeOn
	TokenTypeIs
	TokenTypeLeft
	TokenTypeInner
	TokenTypeJoin
	TokenTypeComma
	TokenTypeDot
	TokenTypeMinus
	TokenTypePlus
	TokenTypeSemicolon
	TokenTypeStar
	TokenTypeSlash
	TokenTypeBangEqual
	TokenTypeEqual
	TokenTypeGreater
	TokenTypeGreaterEqual
	TokenTypeLess
	TokenTypeLessEqual
	TokenTypeIdentifier
	TokenTypeString
	TokenTypeInt
	TokenTypeDouble
	TokenTypeAnd
	TokenTypeOr
	TokenTypeNot
	TokenTypeTrue
	TokenTypeFalse
	TokenTypeNull
	TokenTypeAs
	TokenTypeLimit
	TokenTypeOrder
	TokenTypeBy
	TokenTypeAsc
	TokenTypeDesc
	TokenTypeEOF
)

func (t TokenType) String() string {
	switch t {
	case TokenTypeLeftParen:
		return "LEFT_PAREN"
	case TokenTypeRightParen:
		return "RIGHT_PAREN"
	case TokenTypeWith:
		return "WITH"
	case TokenTypeSelect:
		return "SELECT"
	case TokenTypeWhere:
		return "WHERE"
	case TokenTypeFrom:
		return "FROM"
	case TokenTypeLike:
		return "LIKE"
	case TokenTypeComma:
		return "COMMA"
	case TokenTypeIn:
		return "IN"
	case TokenTypeOn:
		return "ON"
	case TokenTypeIs:
		return "IS"
	case TokenTypeLeft:
		return "LEFT"
	case TokenTypeInner:
		return "INNER"
	case TokenTypeJoin:
		return "JOIN"
	case TokenTypeDot:
		return "DOT"
	case TokenTypeMinus:
		return "MINUS"
	case TokenTypePlus:
		return "PLUS"
	case TokenTypeSemicolon:
		return "SEMICOLON"
	case TokenTypeStar:
		return "STAR"
	case TokenTypeSlash:
		return "SLASH"
	case TokenTypeBangEqual:
		return "BANG_EQUAL"
	case TokenTypeEqual:
		return "EQUAL"
	case TokenTypeGreater:
		return "GREATER"
	case TokenTypeGreaterEqual:
		return "GREATER_EQUAL"
	case TokenTypeLess:
		return "LESS"
	case TokenTypeLessEqual:
		return "LESS_EQUAL"
	case TokenTypeIdentifier:
		return "IDENTIFIER"
	case TokenTypeString:
		return "STRING"
	case TokenTypeInt:
		return "INT"
	case TokenTypeDouble:
		return "DOUBLE"
	case TokenTypeAnd:
		return "AND"
	case TokenTypeNull:
		return "NULL"
	case TokenTypeOr:
		return "OR"
	case TokenTypeNot:
		return "NOT"
	case TokenTypeTrue:
		return "True"
	case TokenTypeFalse:
		return "False"
	case TokenTypeAs:
		return "AS"
	case TokenTypeLimit:
		return "LIMIT"
	case TokenTypeOrder:
		return "ORDER"
	case TokenTypeBy:
		return "BY"
	case TokenTypeAsc:
		return "ASC"
	case TokenTypeDesc:
		return "Desc"
	case TokenTypeEOF:
		return "EOF"
	default:
		return "UNKNOWN"
	}
}

type Position struct {
	Offset int
	Line   int
	Column int
}

type Token struct {
	Type    TokenType
	Lexeme  string
	Literal interface{}
	Pos     Position
}

func (t Token) IsTokenType(targetType TokenType) bool {
	return t.Type == targetType
}

func (t Token) String() string {
	return fmt.Sprintf("%s %s %v", t.Type, t.Lexeme, t.Literal)
}
