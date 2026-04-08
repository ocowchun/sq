package ast

import "github.com/ocowchun/sq/token"

type Expr interface {
	exprNode()
	Position() token.Position
}

type BinaryOp uint8

const (
	BinaryOpAdd BinaryOp = iota
	BinaryOpSub
	BinaryOpMul
	BinaryOpDiv
	BinaryOpEqual
	BinaryOpNotEqual
	BinaryOpGreater
	BinaryOpGreaterEqual
	BinaryOpLess
	BinaryOpLessEqual
)

type IdentifierExpr struct {
	Pos  token.Position
	Name string
}

func (e *IdentifierExpr) exprNode() {}
func (e *IdentifierExpr) Position() token.Position {
	return e.Pos
}

type QualifiedRef struct {
	Pos       token.Position
	TableName string
	Name      string
}

func (r *QualifiedRef) exprNode() {}
func (r *QualifiedRef) Position() token.Position {
	return r.Pos
}

type BinaryExpr struct {
	Pos      token.Position
	Left     Expr
	Right    Expr
	Operator BinaryOp
}

func (e *BinaryExpr) exprNode() {}
func (e *BinaryExpr) Position() token.Position {
	return e.Pos
}

type UnaryOp uint8

const (
	UnaryOpNegate UnaryOp = iota
)

type UnaryExpr struct {
	Pos      token.Position
	Operator UnaryOp
	Expr     Expr
}

func (e *UnaryExpr) exprNode() {}
func (e *UnaryExpr) Position() token.Position {
	return e.Pos
}

type LiteralType uint8

const (
	LiteralTypeString LiteralType = iota
	LiteralTypeBool
	LiteralTypeInt
	LiteralTypeDouble
	LiteralTypeDatetime
	LiteralTypeNull
)

type LiteralExpr struct {
	Pos         token.Position
	Value       any
	LiteralType LiteralType
}

func (e *LiteralExpr) exprNode() {}
func (e *LiteralExpr) Position() token.Position {
	return e.Pos
}

type StarExpr struct {
	Pos token.Position
}

func (e *StarExpr) exprNode() {}
func (e *StarExpr) Position() token.Position {
	return e.Pos
}

type QualifiedStarExpr struct {
	Pos       token.Position
	TableName string
}

func (e *QualifiedStarExpr) exprNode() {}
func (e *QualifiedStarExpr) Position() token.Position {
	return e.Pos
}
