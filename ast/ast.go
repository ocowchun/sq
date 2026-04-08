package ast

import "github.com/ocowchun/sq/token"

type Statement interface {
	Statement()
}

type CTE struct {
	Name  string
	Query *SelectStatement
}

type SelectStatement struct {
	CTEs        []CTE
	SelectExprs []SelectExpr
	From        From
	Where       SearchCondition
}

type SelectExpr struct {
	Expr  Expr
	Alias string
}

func (s SelectStatement) Statement() {}

type From struct {
	Pos      token.Position
	Relation Relation
	Joins    []Join
}

type Relation struct {
	Name  string
	Alias string
}

type Join struct {
	Pos      token.Position
	Right    Relation
	JoinType JoinType
	On       SearchCondition
}

type JoinType uint8

const (
	JoinTypeInnerJoin JoinType = iota
	JoinTypeLeftJoin
	JoinTypeRightJoin
	JoinTypeFullJoin
)

type SearchCondition interface {
	searchCondition()
	Position() token.Position
}

type OrSearchCondition struct {
	Pos            token.Position
	LeftCondition  SearchCondition
	RightCondition SearchCondition
}

func (o *OrSearchCondition) searchCondition() {}
func (o *OrSearchCondition) Position() token.Position {
	return o.LeftCondition.Position()
}

type AndSearchCondition struct {
	Pos            token.Position
	LeftCondition  SearchCondition
	RightCondition SearchCondition
}

func (a *AndSearchCondition) searchCondition() {}
func (a *AndSearchCondition) Position() token.Position {
	return a.LeftCondition.Position()
}

type Predicate interface {
	predicate()
	SearchCondition
}

type LikePredicate struct {
	Pos   token.Position
	Left  Expr
	Not   bool
	Right string
}

func (p *LikePredicate) predicate()       {}
func (p *LikePredicate) searchCondition() {}
func (p *LikePredicate) Position() token.Position {
	return p.Pos
}

type InPredicate struct {
	Pos  token.Position
	Left Expr
	Not  bool
	// TODO: support a in (select b from c) later
	Expressions []Expr
}

func (p *InPredicate) predicate()       {}
func (p *InPredicate) searchCondition() {}
func (p *InPredicate) Position() token.Position {
	return p.Left.Position()
}

type IsNullPredicate struct {
	Pos        token.Position
	Expression Expr
	Not        bool
}

func (p *IsNullPredicate) predicate()       {}
func (p *IsNullPredicate) searchCondition() {}
func (p *IsNullPredicate) Position() token.Position {
	return p.Expression.Position()
}

type ExprPredicate struct {
	Pos  token.Position
	Expr Expr
}

func (p *ExprPredicate) predicate()       {}
func (p *ExprPredicate) searchCondition() {}
func (p *ExprPredicate) Position() token.Position {
	return p.Expr.Position()
}

//type ComparisonOperator uint8
//
//const (
//	ComparisonOperatorEqual ComparisonOperator = iota
//	ComparisonOperatorNotEqual
//	ComparisonOperatorLessThan
//	ComparisonOperatorLessThanOrEqual
//	ComparisonOperatorGreaterThan
//	ComparisonOperatorGreaterThanOrEqual
//	ComparisonOperatorIn
//	ComparisonOperatorNotIn
//	ComparisonOperatorLike
//	ComparisonOperatorNotLike
//)
//
//type ComparisonSearchCondition struct {
//	LeftExpr  Expr
//	RightExpr Expr
//	Operator  ComparisonOperator
//}
//
//func (c *ComparisonSearchCondition) SearchCondition() {}
