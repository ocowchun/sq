package parser

import (
	"fmt"

	"github.com/ocowchun/sq/ast"
	"github.com/ocowchun/sq/token"
)

// https://learn.microsoft.com/en-us/sql/t-sql/queries/search-condition-transact-sql?view=sql-server-ver17
// <search_condition> ::=
//
//	<search_condition_without_match> | <search_condition> AND <search_condition>
//
// <search_condition_without_match> ::=
//
//	{ [ NOT ] <predicate> | ( <search_condition_without_match> ) }
//	[ { AND | OR } [ NOT ] { <predicate> | ( <search_condition_without_match> ) } ]
//
// [ ...n ]
//
// <predicate> ::=
//
//	  { expression { = | <> | != | > | >= | !> | < | <= | !< } expression
//	  | string_expression [ NOT ] LIKE string_expression
//	[ ESCAPE 'escape_character' ]
//	  | expression [ NOT ] BETWEEN expression AND expression
//	  | expression IS [ NOT ] NULL
//	  | expression IS [ NOT ] DISTINCT FROM
//	  | CONTAINS
//	( { column | * } , '<contains_search_condition>' )
//	  | FREETEXT ( { column | * } , 'freetext_string' )
//	  | expression [ NOT ] IN ( subquery | expression [ , ...n ] )
//	  | expression { = | < > | != | > | >= | ! > | < | <= | ! < }
//	{ ALL | SOME | ANY } ( subquery )
//	  | EXISTS ( subquery )     }
//
// https://learn.microsoft.com/en-us/sql/t-sql/language-elements/expressions-transact-sql?view=sql-server-ver17
// { constant | scalar_function | [ table_name. ] column | variable
//
//	   | ( expression ) | ( scalar_subquery )
//	   | { unary_operator } expression
//	   | expression { binary_operator } expression
//	   | ranking_windowed_function | aggregate_windowed_function
//	}
//
// <predicate> ::=
//
//	  { expression { = | <> | != | > | >= | !> | < | <= | !< } expression
//	  | string_expression [ NOT ] LIKE string_expression
//	[ ESCAPE 'escape_character' ]
//	  | expression [ NOT ] BETWEEN expression AND expression
//	  | expression IS [ NOT ] NULL
//	  | expression IS [ NOT ] DISTINCT FROM
//	  | CONTAINS
//	( { column | * } , '<contains_search_condition>' )
//	  | FREETEXT ( { column | * } , 'freetext_string' )
//	  | expression [ NOT ] IN ( subquery | expression [ , ...n ] )
//	  | expression { = | < > | != | > | >= | ! > | < | <= | ! < }
//	{ ALL | SOME | ANY } ( subquery )
//	  | EXISTS ( subquery )     }
func (p *Parser) parsePredicate() (ast.Predicate, error) {
	left, err := p.parseExpr()
	if err != nil {
		return nil, err
	}

	if p.currentTokenIs(token.TokenTypeIs) {
		p.advance()
		hasNot := false
		if p.currentTokenIs(token.TokenTypeNot) {
			p.advance()
			hasNot = true
		}
		if !p.currentTokenIs(token.TokenTypeNull) {
			return nil, fmt.Errorf("expected nil, found %s", p.currentToken())
		}
		p.advance()
		return &ast.IsNullPredicate{Pos: left.Position(), Expression: left, Not: hasNot}, nil
	}

	hasNot := false
	if p.currentTokenIs(token.TokenTypeNot) {
		p.advance()
		hasNot = true
	}

	if p.currentTokenIs(token.TokenTypeLike) {
		p.advance()
		right, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		switch lit := right.(type) {
		case *ast.LiteralExpr:
			if lit.LiteralType != ast.LiteralTypeString {
				return nil, newParserError(right.Position(), "expected string")
			}
			pattern := lit.Value.(string)
			return &ast.LikePredicate{Pos: left.Position(), Left: left, Not: hasNot, Right: pattern}, nil
		default:
			return nil, newParserError(right.Position(), "expected string")
		}
	}

	if p.currentTokenIs(token.TokenTypeIn) {
		p.advance()
		if !p.currentTokenIs(token.TokenTypeLeftParen) {
			return nil, newParserError(p.currentToken().Pos, fmt.Sprintf("expected `(` but got %s", p.currentToken()))
		}
		exprs := make([]*ast.LiteralExpr, 0)
		if !p.currentTokenIs(token.TokenTypeRightParen) {
			expr, err := p.parseExpr()
			if err != nil {
				return nil, err
			}
			for p.currentTokenIs(token.TokenTypeComma) {
				p.advance()
				expr, err = p.parseExpr()
				if err != nil {
					return nil, err
				}
				lit, ok := expr.(*ast.LiteralExpr)
				if !ok {
					return nil, newParserError(expr.Position(), "expected literal")
				}
				exprs = append(exprs, lit)
			}
		}

		if !p.currentTokenIs(token.TokenTypeRightParen) {
			return nil, newParserError(p.currentToken().Pos, fmt.Sprintf("expected `)` but got %s", p.currentToken()))
		}
		return &ast.InPredicate{
			Pos:         left.Position(),
			Left:        left,
			Not:         hasNot,
			Expressions: exprs,
		}, nil
	}

	return &ast.ExprPredicate{Pos: left.Position(), Expr: left}, nil
}

func (p *Parser) parseSearchCondition() (ast.SearchCondition, error) {
	return p.parseOr()
}

func (p *Parser) parseOr() (ast.SearchCondition, error) {
	expr, err := p.parseAnd()
	if err != nil {
		return nil, err
	}
	for p.currentTokenIs(token.TokenTypeOr) {
		p.advance()

		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}

		expr = &ast.OrSearchCondition{
			LeftCondition:  expr,
			RightCondition: right,
		}
	}

	return expr, nil
}

func (p *Parser) parseAnd() (ast.SearchCondition, error) {
	var left ast.SearchCondition
	var err error
	left, err = p.parsePredicate()
	if err != nil {
		return nil, err
	}
	for p.currentTokenIs(token.TokenTypeAnd) {
		p.advance()

		right, err := p.parsePredicate()
		if err != nil {
			return nil, err
		}

		left = &ast.AndSearchCondition{
			LeftCondition:  left,
			RightCondition: right,
		}
	}

	return left, nil
}
