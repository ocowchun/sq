package parser

import (
	"fmt"
	"slices"

	"github.com/ocowchun/sq/ast"
	"github.com/ocowchun/sq/token"
)

func Parse(input string) (ast.Statement, error) {
	l := newLexer(input)
	tokens, err := l.Tokens()
	if err != nil {
		return nil, err
	}
	p := Parser{
		tokens:  tokens,
		current: 0,
	}
	statement, err := p.parse()
	return statement, err
}

type Parser struct {
	tokens  []token.Token
	current int
}

type ParseError struct {
	Pos     token.Position
	Message string
}

func newParserError(pos token.Position, msg string) *ParseError {
	return &ParseError{
		Pos:     pos,
		Message: msg,
	}
}

func (e *ParseError) Error() string {
	return fmt.Sprintf("Parse error at line %d, column %d: %s", e.Pos.Line, e.Pos.Column, e.Message)
}

func (p *Parser) currentToken() token.Token {
	if p.current >= len(p.tokens) {
		return token.Token{
			Type: token.TokenTypeEOF,
		}
	}
	return p.tokens[p.current]
}

func (p *Parser) currentTokenIs(tokenTypes ...token.TokenType) bool {
	return slices.Contains(tokenTypes, p.currentToken().Type)
}

func (p *Parser) nextTokenIs(tokenTypes ...token.TokenType) bool {
	return slices.Contains(tokenTypes, p.peekNext().Type)
}

func (p *Parser) advance() token.Token {
	if p.current >= len(p.tokens) {
		return token.Token{
			Type: token.TokenTypeEOF,
		}
	}

	token := p.tokens[p.current]
	p.current++
	return token
}

func (p *Parser) consume(tokenType token.TokenType, errorMessage string) (token.Token, error) {
	if p.currentTokenIs(tokenType) {
		t := p.advance()
		return t, nil
	}
	return token.Token{}, newParserError(p.currentToken().Pos, fmt.Sprintf("%s got token %s", errorMessage, p.currentToken().Lexeme))
}

func (p *Parser) peekNext() token.Token {
	if p.current+1 >= len(p.tokens) {
		return token.Token{
			Type: token.TokenTypeEOF,
		}
	}
	return p.tokens[p.current+1]
}

func (p *Parser) parse() (ast.Statement, error) {
	t := p.advance()
	switch t.Type {
	case token.TokenTypeSelect:
		return p.parseSelectStatement()
	case token.TokenTypeWith:
		// TODO: parse with
		ctes, err := p.parseCTEs()
		if err != nil {
			return nil, err
		}

		_, err = p.consume(token.TokenTypeSelect, "expected select")
		if err != nil {
			return nil, err
		}

		stmt, err := p.parseSelectStatement()
		if err != nil {
			return nil, err
		}
		stmt.CTEs = ctes
		return stmt, nil
	default:
		return nil, newParserError(t.Pos, fmt.Sprintf("unexpected token %s", t.Lexeme))
	}
}

func (p *Parser) parseCTEs() ([]ast.CTE, error) {
	ctes := make([]ast.CTE, 0)
	for p.currentTokenIs(token.TokenTypeIdentifier) {
		t := p.advance()
		name := t.Lexeme
		if !p.currentTokenIs(token.TokenTypeAs) {
			return nil, newParserError(p.currentToken().Pos, fmt.Sprintf("expected as, got %s", p.currentToken().Lexeme))
		}
		p.advance()
		if !p.currentTokenIs(token.TokenTypeLeftParen) {
			return nil, newParserError(p.currentToken().Pos, fmt.Sprintf("expected (, got %s", p.currentToken().Lexeme))
		}
		p.advance()

		if !p.currentTokenIs(token.TokenTypeSelect) {
			return nil, newParserError(p.currentToken().Pos, fmt.Sprintf("expected select, got %s", p.currentToken().Lexeme))
		}
		p.advance()

		query, err := p.parseSelectStatement()
		if err != nil {
			return nil, err
		}

		if !p.currentTokenIs(token.TokenTypeRightParen) {
			return nil, newParserError(p.currentToken().Pos, fmt.Sprintf("expected ), got %s", p.currentToken().Lexeme))
		}
		p.advance()

		ctes = append(ctes, ast.CTE{
			Name:  name,
			Query: query,
		})

		if !p.currentTokenIs(token.TokenTypeComma) {
			break
		}
		p.advance()
	}
	return ctes, nil
}

func (p *Parser) parseSelectStatement() (*ast.SelectStatement, error) {
	selectExprs := make([]ast.SelectExpr, 0)
	for {
		expr, err := p.parseSelectExpr()
		if err != nil {
			return nil, err
		}
		// TODO: handle as
		selectExprs = append(selectExprs, expr)
		if p.currentTokenIs(token.TokenTypeComma) {
			p.advance()
		} else if p.currentTokenIs(token.TokenTypeFrom) {
			p.advance()
			break
		}
	}

	if len(selectExprs) == 0 {
		return nil, newParserError(p.currentToken().Pos, fmt.Sprintf("expected at least one select expression"))
	}

	// parse from
	from, err := p.parseFrom()
	if err != nil {
		return nil, err
	}

	// parse where
	var searchCondition ast.SearchCondition
	if p.currentTokenIs(token.TokenTypeWhere) {
		p.advance()
		searchCondition, err = p.parseSearchCondition()
		if err != nil {
			return nil, err
		}
	}

	selectStatement := &ast.SelectStatement{
		SelectExprs: selectExprs,
		From:        from,
		Where:       searchCondition,
	}

	return selectStatement, nil
}

func (p *Parser) parseSelectExpr() (ast.SelectExpr, error) {
	expr, err := p.parseExpr()
	if err != nil {
		return ast.SelectExpr{}, err
	}
	selectExpr := ast.SelectExpr{
		Expr: expr,
	}

	if p.currentTokenIs(token.TokenTypeAs) {
		p.advance()
		t, err := p.consume(token.TokenTypeIdentifier, "expected identifier")
		if err != nil {
			return ast.SelectExpr{}, err
		}
		selectExpr.Alias = t.Lexeme
	}

	return selectExpr, nil
}

func (p *Parser) parseExpr() (ast.Expr, error) {
	//ParseEquality
	//parseComparison
	//parseTerm
	//parseFactor
	//parseUnary
	//parseCall
	//parsePrimary
	return p.parseEquality()
}

func (p *Parser) parseEquality() (ast.Expr, error) {
	var left ast.Expr
	left, err := p.parseComparison()
	if err != nil {
		return nil, err
	}

	for p.currentTokenIs(token.TokenTypeBangEqual, token.TokenTypeEqual) {
		t := p.advance()
		op, err := toBinaryOperator(t)
		if err != nil {
			return nil, err
		}

		right, err := p.parseComparison()
		if err != nil {
			return nil, err
		}

		left = &ast.BinaryExpr{
			Pos:      left.Position(),
			Left:     left,
			Operator: op,
			Right:    right,
		}
	}

	return left, nil
}

func toBinaryOperator(t token.Token) (ast.BinaryOp, error) {
	switch t.Type {
	case token.TokenTypePlus:
		return ast.BinaryOpAdd, nil
	case token.TokenTypeMinus:
		return ast.BinaryOpSub, nil
	case token.TokenTypeStar:
		return ast.BinaryOpMul, nil
	case token.TokenTypeSlash:
		return ast.BinaryOpDiv, nil
	case token.TokenTypeEqual:
		return ast.BinaryOpEqual, nil
	case token.TokenTypeBangEqual:
		return ast.BinaryOpNotEqual, nil
	case token.TokenTypeGreater:
		return ast.BinaryOpGreater, nil
	case token.TokenTypeGreaterEqual:
		return ast.BinaryOpGreaterEqual, nil
	case token.TokenTypeLess:
		return ast.BinaryOpLess, nil
	case token.TokenTypeLessEqual:
		return ast.BinaryOpLessEqual, nil
	default:
		return ast.BinaryOpAdd, newParserError(t.Pos, fmt.Sprintf("unexpected token %s", t.Lexeme))

	}
}

func (p *Parser) parseComparison() (ast.Expr, error) {
	var left ast.Expr
	left, err := p.parseTerm()
	if err != nil {
		return nil, err
	}

	for p.currentTokenIs(token.TokenTypeGreater, token.TokenTypeGreaterEqual, token.TokenTypeLess, token.TokenTypeLessEqual) {
		t := p.advance()
		op, err := toBinaryOperator(t)
		if err != nil {
			return nil, err
		}

		right, err := p.parseTerm()
		if err != nil {
			return nil, err
		}

		left = &ast.BinaryExpr{
			Left:     left,
			Operator: op,
			Right:    right,
		}

	}

	return left, nil
}

func (p *Parser) parseTerm() (ast.Expr, error) {
	var left ast.Expr
	left, err := p.parseFactor()
	if err != nil {
		return nil, err
	}

	for p.currentTokenIs(token.TokenTypePlus, token.TokenTypeMinus) {
		t := p.advance()
		op, err := toBinaryOperator(t)
		if err != nil {
			return nil, err
		}

		right, err := p.parseFactor()
		if err != nil {
			return nil, err
		}

		left = &ast.BinaryExpr{
			Left:     left,
			Operator: op,
			Right:    right,
		}
	}

	return left, nil
}

func (p *Parser) parseFactor() (ast.Expr, error) {
	var left ast.Expr
	left, err := p.parseUnary()
	if err != nil {
		return nil, err
	}

	for p.currentTokenIs(token.TokenTypeStar, token.TokenTypeSlash) {
		t := p.advance()
		op, err := toBinaryOperator(t)
		if err != nil {
			return nil, err
		}

		right, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		left = &ast.BinaryExpr{
			Left:     left,
			Operator: op,
			Right:    right,
		}
	}

	return left, nil
}

func (p *Parser) parseUnary() (ast.Expr, error) {
	if p.currentTokenIs(token.TokenTypeMinus) {
		p.advance()

		expr, err := p.parseUnary()
		if err != nil {
			return nil, err
		}

		return &ast.UnaryExpr{
			Pos:      expr.Position(),
			Operator: ast.UnaryOpNegate,
			Expr:     expr,
		}, nil
	}

	return p.parseCall()
}

func (p *Parser) parseCall() (ast.Expr, error) {
	callee, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	if identifier, ok := callee.(*ast.IdentifierExpr); ok && p.currentTokenIs(token.TokenTypeLeftParen) {
		p.advance()
		args := make([]ast.Expr, 0)
		for !p.currentTokenIs(token.TokenTypeRightParen) {
			arg, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			args = append(args, arg)
			if p.currentTokenIs(token.TokenTypeComma) {
				p.advance()
			}
		}
		p.advance()
		return &ast.CallExpr{
			Pos:    identifier.Pos,
			Callee: identifier.Name,
			Args:   args,
		}, nil
	}

	return callee, nil
}

func (p *Parser) parsePrimary() (ast.Expr, error) {
	if p.currentTokenIs(token.TokenTypeTrue) {
		t := p.advance()
		return &ast.LiteralExpr{Pos: t.Pos, Value: true, LiteralType: ast.LiteralTypeBool}, nil
	}

	if p.currentTokenIs(token.TokenTypeFalse) {
		t := p.advance()
		return &ast.LiteralExpr{Pos: t.Pos, Value: false, LiteralType: ast.LiteralTypeBool}, nil
	}

	if p.currentTokenIs(token.TokenTypeNull) {
		t := p.advance()
		return &ast.LiteralExpr{Pos: t.Pos, Value: nil, LiteralType: ast.LiteralTypeNull}, nil
	}

	if p.currentTokenIs(token.TokenTypeInt, token.TokenTypeDouble, token.TokenTypeString) {
		t := p.advance()
		literalType := ast.LiteralTypeString
		if t.Type == token.TokenTypeDouble {
			literalType = ast.LiteralTypeInt
		} else if t.Type == token.TokenTypeInt {
			literalType = ast.LiteralTypeInt
		}
		return &ast.LiteralExpr{Pos: t.Pos, Value: t.Literal, LiteralType: literalType}, nil
	}

	if p.currentTokenIs(token.TokenTypeLeftParen) {
		p.advance()

		exp, err := p.parseExpr()
		if err != nil {
			return nil, err
		}

		if p.currentTokenIs(token.TokenTypeRightParen) {
			p.advance()
			return exp, nil
		}
		return nil, newParserError(p.currentToken().Pos, fmt.Sprintf("expected `)` but got token %s", p.currentToken().Lexeme))
	}

	if p.currentTokenIs(token.TokenTypeStar) {
		t := p.advance()
		return &ast.StarExpr{Pos: t.Pos}, nil
	}

	if p.currentTokenIs(token.TokenTypeIdentifier) {
		t := p.advance()
		firstName := t.Lexeme

		if p.currentTokenIs(token.TokenTypeDot) {
			p.advance()
			secondToken := p.advance()
			if secondToken.Type == token.TokenTypeIdentifier {
				return &ast.QualifiedRef{
					Pos:       t.Pos,
					TableName: firstName,
					Name:      secondToken.Lexeme,
				}, nil

			} else if secondToken.Type == token.TokenTypeStar {
				return &ast.QualifiedStarExpr{
					Pos:       t.Pos,
					TableName: firstName,
				}, nil

			}
			return nil, newParserError(secondToken.Pos, fmt.Sprintf("expected identifier or * after `.` but got token %s", secondToken.Lexeme))
		}

		return &ast.IdentifierExpr{
			Pos:  t.Pos,
			Name: firstName,
		}, nil
	}
	return nil, newParserError(p.currentToken().Pos, fmt.Sprintf("expect expression got %s", p.currentToken().Type))

}

func (p *Parser) parseFrom() (ast.From, error) {
	baseToken, err := p.consume(token.TokenTypeIdentifier, "expected identfier")
	if err != nil {
		return ast.From{}, err
	}

	baseRel := ast.Relation{
		Name: baseToken.Lexeme,
	}
	if p.currentTokenIs(token.TokenTypeAs) {
		p.advance()

		t, err := p.consume(token.TokenTypeIdentifier, "expected identifier")
		if err != nil {
			return ast.From{}, err
		}
		baseRel.Alias = t.Lexeme
	}

	joins := make([]ast.Join, 0)
	for p.currentTokenIs(token.TokenTypeInner, token.TokenTypeLeft) {
		t := p.advance()
		pos := t.Pos
		_, err = p.consume(token.TokenTypeJoin, "expected join")
		if err != nil {
			return ast.From{}, err
		}
		joinType, err := toJoinType(t)
		if err != nil {
			return ast.From{}, err
		}
		relToken, err := p.consume(token.TokenTypeIdentifier, "expected identifier")
		if err != nil {
			return ast.From{}, err
		}
		right := ast.Relation{
			Name: relToken.Lexeme,
		}

		if p.currentTokenIs(token.TokenTypeAs) {
			p.advance()
			t, err = p.consume(token.TokenTypeIdentifier, "expected identifier")
			if err != nil {
				return ast.From{}, err
			}
			right.Alias = t.Lexeme
		}

		// select * from a join b on (xxxx) join c on (ooo)
		_, err = p.consume(token.TokenTypeOn, "expected on")
		if err != nil {
			return ast.From{}, err
		}
		searchCondition, err := p.parseSearchCondition()
		if err != nil {
			return ast.From{}, err
		}
		joins = append(joins, ast.Join{
			Pos:      pos,
			Right:    right,
			JoinType: joinType,
			On:       searchCondition,
		})

	}
	return ast.From{
		Pos:      baseToken.Pos,
		Relation: baseRel,
		Joins:    joins,
	}, nil
}

func toJoinType(tok token.Token) (ast.JoinType, error) {
	switch tok.Type {
	case token.TokenTypeLeft:
		return ast.JoinTypeLeftJoin, nil
	case token.TokenTypeInner:
		return ast.JoinTypeInnerJoin, nil
	default:
		return ast.JoinTypeInnerJoin, newParserError(tok.Pos, fmt.Sprintf("expected join type but got token %s", tok.Lexeme))
	}
}
