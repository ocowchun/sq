package parser

import (
	"math"
	"testing"

	"github.com/ocowchun/sq/token"
)

func TestLexer(t *testing.T) {
	input := `
( ) , . - + * ; != =  < <= > >= 1.2 1234 "hola a" foo and false null or true
inner left join select where 'ramen' limit order by asc desc
`
	l := newLexer(input)

	expectedTokens := []token.Token{
		{Type: token.TokenTypeLeftParen, Pos: token.Position{Offset: 1, Line: 2, Column: 1}},
		{Type: token.TokenTypeRightParen, Pos: token.Position{Offset: 3, Line: 2, Column: 3}},
		{Type: token.TokenTypeComma, Pos: token.Position{Offset: 5, Line: 2, Column: 5}},
		{Type: token.TokenTypeDot, Pos: token.Position{Offset: 7, Line: 2, Column: 7}},
		{Type: token.TokenTypeMinus, Pos: token.Position{Offset: 9, Line: 2, Column: 9}},
		{Type: token.TokenTypePlus, Pos: token.Position{Offset: 11, Line: 2, Column: 11}},
		{Type: token.TokenTypeStar, Pos: token.Position{Offset: 13, Line: 2, Column: 13}},
		{Type: token.TokenTypeSemicolon, Pos: token.Position{Offset: 15, Line: 2, Column: 15}},
		{Type: token.TokenTypeBangEqual, Pos: token.Position{Offset: 17, Line: 2, Column: 17}},
		{Type: token.TokenTypeEqual, Pos: token.Position{Offset: 20, Line: 2, Column: 20}},
		{Type: token.TokenTypeLess, Pos: token.Position{Offset: 23, Line: 2, Column: 23}},
		{Type: token.TokenTypeLessEqual, Pos: token.Position{Offset: 25, Line: 2, Column: 25}},
		{Type: token.TokenTypeGreater, Pos: token.Position{Offset: 28, Line: 2, Column: 28}},
		{Type: token.TokenTypeGreaterEqual, Pos: token.Position{Offset: 30, Line: 2, Column: 30}},
		{Type: token.TokenTypeDouble, Literal: 1.2, Pos: token.Position{Offset: 33, Line: 2, Column: 33}},
		{Type: token.TokenTypeInt, Literal: 1234, Pos: token.Position{Offset: 37, Line: 2, Column: 37}},
		{Type: token.TokenTypeString, Literal: "hola a", Pos: token.Position{Offset: 42, Line: 2, Column: 42}},
		{Type: token.TokenTypeIdentifier, Lexeme: "foo", Pos: token.Position{Offset: 51, Line: 2, Column: 51}},
		{Type: token.TokenTypeAnd, Pos: token.Position{Offset: 55, Line: 2, Column: 55}},
		{Type: token.TokenTypeFalse, Pos: token.Position{Offset: 59, Line: 2, Column: 59}},
		{Type: token.TokenTypeNull, Pos: token.Position{Offset: 65, Line: 2, Column: 65}},
		{Type: token.TokenTypeOr, Pos: token.Position{Offset: 70, Line: 2, Column: 70}},
		{Type: token.TokenTypeTrue, Pos: token.Position{Offset: 73, Line: 2, Column: 73}},
		{Type: token.TokenTypeInner, Pos: token.Position{Offset: 78, Line: 3, Column: 1}},
		{Type: token.TokenTypeLeft, Pos: token.Position{Offset: 84, Line: 3, Column: 7}},
		{Type: token.TokenTypeJoin, Pos: token.Position{Offset: 89, Line: 3, Column: 12}},
		{Type: token.TokenTypeSelect, Pos: token.Position{Offset: 94, Line: 3, Column: 17}},
		{Type: token.TokenTypeWhere, Pos: token.Position{Offset: 101, Line: 3, Column: 24}},
		{Type: token.TokenTypeString, Pos: token.Position{Offset: 107, Line: 3, Column: 30}, Literal: "ramen"},
		{Type: token.TokenTypeLimit, Pos: token.Position{Offset: 115, Line: 3, Column: 38}},
		{Type: token.TokenTypeOrder, Pos: token.Position{Offset: 121, Line: 3, Column: 44}},
		{Type: token.TokenTypeBy, Pos: token.Position{Offset: 127, Line: 3, Column: 50}},
		{Type: token.TokenTypeAsc, Pos: token.Position{Offset: 130, Line: 3, Column: 53}},
		{Type: token.TokenTypeDesc, Pos: token.Position{Offset: 134, Line: 3, Column: 57}},
		{Type: token.TokenTypeEOF, Pos: token.Position{Offset: 138, Line: 4, Column: 1}},
	}

	i := 0
	for !l.isAtEnd() {
		tok, err := l.NextToken()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		assertToken(t, tok, expectedTokens[i])
		i++
	}

}

const float64EqualityThreshold = 1e-9

func assertToken(t *testing.T, actualToken token.Token, expectedToken token.Token) {
	if actualToken.Pos.Line != expectedToken.Pos.Line {
		t.Fatalf("Line mismatch: expected %v, got %v", expectedToken.Pos.Line, actualToken.Pos.Line)
	}
	if actualToken.Pos.Column != expectedToken.Pos.Column {
		t.Fatalf("Column mismatch: expected %v, got %v", expectedToken.Pos.Column, actualToken.Pos.Column)
	}
	if actualToken.Pos.Offset != expectedToken.Pos.Offset {
		t.Fatalf("Offset mismatch: expected %v, got %v", expectedToken.Pos.Offset, actualToken.Pos.Offset)
	}

	if actualToken.Type != expectedToken.Type {
		t.Fatalf("Expected token to be %s, got %s", expectedToken.Type, actualToken.Type)
	}

	if actualToken.Type == token.TokenTypeDouble {
		actualLiteral, ok := actualToken.Literal.(float64)
		if !ok {
			t.Fatalf("failed to convert actual literal to float64, literal: %v", actualToken.Literal)
		}

		expectedLiteral, ok := expectedToken.Literal.(float64)
		if !ok {
			t.Fatalf("failed to convert expected literal to float64, literal: %v", expectedToken.Literal)
		}

		if math.Abs(actualLiteral-expectedLiteral) > float64EqualityThreshold {
			t.Fatalf("literal not matched, expected = %v, actual = %v  ", expectedLiteral, actualLiteral)

		}
	} else if actualToken.Type == token.TokenTypeString {
		actualLiteral, ok := actualToken.Literal.(string)
		if !ok {
			t.Fatalf("failed to convert actual literal to string, literal: %v", actualToken.Literal)
		}

		expectedLiteral, ok := expectedToken.Literal.(string)
		if !ok {
			t.Fatalf("failed to convert expected literal to string, literal: %v", expectedToken.Literal)
		}

		if actualLiteral != expectedLiteral {
			t.Fatalf("literal not matched, expected = %v, actual = %v  ", expectedLiteral, actualLiteral)
		}
	} else if actualToken.Type == token.TokenTypeIdentifier {
		if actualToken.Lexeme != expectedToken.Lexeme {
			t.Fatalf("Lexeme not matched, expected = %v, actual = %v  ", expectedToken.Lexeme, actualToken.Lexeme)
		}
	}
}
