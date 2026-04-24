package binder

import (
	"fmt"
	"strings"

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
	OrderBy     []Ordering
	Limit       *uint32
}

type Ordering struct {
	Expr Expr
	Desc bool
}

func (q Query) statement() {}

type SelectExpr struct {
	Expr  Expr
	Alias string
}

type Expr interface {
	exprNode()
	Type() catalog.ColumnType
	String() string
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

func (literal *Literal) String() string {
	switch literal.LiteralType {
	case catalog.ColumnTypeInt:
		return fmt.Sprintf("%d", literal.Value.(int64))
	case catalog.ColumnTypeDouble:
		return fmt.Sprintf("%f", literal.Value.(float64))
	case catalog.ColumnTypeString:
		return fmt.Sprintf("%q", literal.Value.(string))
	case catalog.ColumnTypeBool:
		return fmt.Sprintf("%t", literal.Value.(bool))
	case catalog.ColumnTypeNull:
		return "NULL"
	default:
		return fmt.Sprintf("%v", literal.LiteralType)
	}
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
func (c *ColumnRef) String() string {
	return c.ColumnName
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

func (e *UnaryExpr) String() string {
	return fmt.Sprintf("-%s", e.Expr.String())
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
func (e *BinaryExpr) String() string {
	var sb strings.Builder
	sb.WriteString(e.Left.String())
	sb.WriteString(" ")
	sb.WriteString(e.Operator.String())
	sb.WriteString(" ")
	sb.WriteString(e.Right.String())
	return sb.String()
}

type CallExpr struct {
	Callee     string
	Args       []Expr
	ColumnType catalog.ColumnType
}

func (e *CallExpr) exprNode() {}
func (e *CallExpr) Type() catalog.ColumnType {
	return e.ColumnType
}
func (e *CallExpr) String() string {
	var sb strings.Builder
	sb.WriteString(e.Callee)
	sb.WriteString("(")
	for i, arg := range e.Args {
		sb.WriteString(arg.String())
		if i < len(e.Args)-1 {
			sb.WriteString(", ")
		}
	}
	sb.WriteString(")")
	return sb.String()
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
	Expressions []*Literal
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
