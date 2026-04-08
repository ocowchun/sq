package binder

import (
	"github.com/ocowchun/sq/ast"
	"github.com/ocowchun/sq/catalog"
)

type Statement interface {
	statement()
}

type CTE struct {
	Name   string
	Query  *Query
	Schema catalog.Schema
}

type Query struct {
	CTEs        []CTE
	SelectExprs []SelectExpr
	From        From
	Where       SearchCondition
	Schema      catalog.Schema
}

func (q Query) statement() {}

type SelectExpr struct {
	Expr  Expr
	Alias string
}

type Expr interface {
	exprNode()
	Type() catalog.ColumnType
}

type Table struct {
	Name       string
	Alias      string
	Schema     catalog.Schema
	Source     TableSource
	AccessKind catalog.AccessKind
}

type From struct {
	Base  Table
	Joins []Join
}

type Join struct {
	JoinType ast.JoinType
	Right    Table
	On       SearchCondition
}

type Literal struct {
	Value       any
	LiteralType catalog.ColumnType
}

func (literal *Literal) exprNode() {}
func (literal *Literal) Type() catalog.ColumnType {
	return literal.LiteralType
}

type ColumnRef struct {
	TableName   string
	TableAlias  string
	ColumnName  string
	ColumnIndex int
	ColumnType  catalog.ColumnType
}

func (c *ColumnRef) exprNode() {}
func (c *ColumnRef) Type() catalog.ColumnType {
	return c.ColumnType
}

type UnaryExpr struct {
	Operator   ast.UnaryOp
	Expr       Expr
	ColumnType catalog.ColumnType
}

func (e *UnaryExpr) exprNode() {}
func (e *UnaryExpr) Type() catalog.ColumnType {
	return e.ColumnType
}

type BinaryExpr struct {
	Operator   ast.BinaryOp
	Left       Expr
	Right      Expr
	ColumnType catalog.ColumnType
}

func (e *BinaryExpr) exprNode() {}
func (e *BinaryExpr) Type() catalog.ColumnType {
	return e.ColumnType
}

type SearchCondition interface {
	searchCondition()
}

type OrSearchCondition struct {
	LeftCondition  SearchCondition
	RightCondition SearchCondition
}

func (o *OrSearchCondition) searchCondition() {}

type AndSearchCondition struct {
	LeftCondition  SearchCondition
	RightCondition SearchCondition
}

func (a *AndSearchCondition) searchCondition() {}

type Predicate interface {
	predicate()
	SearchCondition
}

type LikePredicate struct {
	Left  Expr
	Not   bool
	Right string
}

func (p *LikePredicate) predicate()       {}
func (p *LikePredicate) searchCondition() {}

type InPredicate struct {
	Left        Expr
	Not         bool
	Expressions []Expr
}

func (p *InPredicate) predicate()       {}
func (p *InPredicate) searchCondition() {}

type IsNullPredicate struct {
	Expression Expr
	Not        bool
}

func (p *IsNullPredicate) predicate()       {}
func (p *IsNullPredicate) searchCondition() {}

type ExprPredicate struct {
	Expr Expr
}

func (p *ExprPredicate) predicate()       {}
func (p *ExprPredicate) searchCondition() {}
