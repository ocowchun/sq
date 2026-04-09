package logical

import (
	"slices"

	"github.com/ocowchun/sq/ast"
	"github.com/ocowchun/sq/catalog"
)

type Node interface {
	Schema() catalog.Schema
}

type CTEDefinition struct {
	Name   string
	Query  Node
	Schema catalog.Schema
}

type Table struct {
	Name       string
	Alias      string
	Schema     catalog.Schema
	AccessKind catalog.AccessKind
}

type CTERef struct {
	Name   string
	Schema catalog.Schema
}

type Statement struct {
	CTEs []CTEDefinition
	Root Node
}

func (s *Statement) Schema() catalog.Schema {
	return s.Root.Schema()
}

type Expr interface {
	exprNode()
	Type() catalog.ColumnType
}

type LiteralExpr struct {
	Value       any
	LiteralType catalog.ColumnType
}

func (e *LiteralExpr) exprNode() {}
func (e *LiteralExpr) Type() catalog.ColumnType {
	return e.LiteralType
}

type ColumnRef struct {
	RelationID   string
	RelationName string
	TableName    string
	TableAlias   string
	ColumnName   string
	ColumnIndex  int
	ColumnType   catalog.ColumnType
}

func (c *ColumnRef) exprNode() {}
func (c *ColumnRef) Type() catalog.ColumnType {
	return c.ColumnType
}

type UnaryExpr struct {
	Op         ast.UnaryOp
	Expr       Expr
	ColumnType catalog.ColumnType
}

func (e *UnaryExpr) exprNode() {}
func (e *UnaryExpr) Type() catalog.ColumnType {
	return e.ColumnType
}

type BinaryExpr struct {
	Op         ast.BinaryOp
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
	Left  SearchCondition
	Right SearchCondition
}

func (o *OrSearchCondition) searchCondition() {}

type AndSearchCondition struct {
	Left  SearchCondition
	Right SearchCondition
}

func (a *AndSearchCondition) searchCondition() {}

type Predicate interface {
	predicate()
	SearchCondition
}

type LikePredicate struct {
	Left    Expr
	Not     bool
	Pattern string
}

func (p *LikePredicate) predicate()       {}
func (p *LikePredicate) searchCondition() {}

type InPredicate struct {
	Left  Expr
	Not   bool
	Exprs []*LiteralExpr
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

type SelectExpr struct {
	Expr  Expr
	Alias string
}

type Scan struct {
	RelationID   string
	RelationName string
	Table        Table
	CTE          *CTERef
}

func (s *Scan) Schema() catalog.Schema {
	if s.CTE != nil {
		return s.CTE.Schema
	}
	return s.Table.Schema
}

type S3ObjectScan struct {
	RelationID   string
	RelationName string
	Table        Table
	BucketName   string
	KeyPrefix    *string
}

func (s *S3ObjectScan) Schema() catalog.Schema {
	return s.Table.Schema
}

type Filter struct {
	Input     Node
	Predicate SearchCondition
}

func (f *Filter) Schema() catalog.Schema {
	return f.Input.Schema()
}

type Project struct {
	Input        Node
	SelectExprs  []SelectExpr
	OutputSchema catalog.Schema
}

func (p *Project) Schema() catalog.Schema {
	return p.OutputSchema
}

type Join struct {
	Left         Node
	Right        Node
	Type         ast.JoinType
	On           SearchCondition
	OutputSchema catalog.Schema
}

func (j *Join) Schema() catalog.Schema {
	return j.OutputSchema
}

func SearchConditionRelationIds(searchCondition SearchCondition) []string {
	switch sc := searchCondition.(type) {
	case *OrSearchCondition:
		return mergeRelationIDs(SearchConditionRelationIds(sc.Left), SearchConditionRelationIds(sc.Right))
	case *AndSearchCondition:
		return mergeRelationIDs(SearchConditionRelationIds(sc.Left), SearchConditionRelationIds(sc.Right))
	case *LikePredicate:
		return ExprRelationIds(sc.Left)
	case *InPredicate:
		return ExprRelationIds(sc.Left)
	case *IsNullPredicate:
		return ExprRelationIds(sc.Expression)
	case *ExprPredicate:
		return ExprRelationIds(sc.Expr)
	default:
		return nil
	}
}

func ExprRelationIds(expr Expr) []string {
	switch e := expr.(type) {
	case *LiteralExpr:
		return nil
	case *ColumnRef:
		if e.RelationID == "" {
			return nil
		}

		return []string{e.RelationID}
	case *UnaryExpr:
		return ExprRelationIds(e.Expr)
	case *BinaryExpr:
		return mergeRelationIDs(ExprRelationIds(e.Left), ExprRelationIds(e.Right))
	default:
		return nil
	}
}

func mergeRelationIDs(left, right []string) []string {
	if len(left) == 0 {
		return slices.Clone(right)
	}
	result := slices.Clone(left)
	for _, id := range right {
		if !slices.Contains(result, id) {
			result = append(result, id)
		}
	}
	return result
}
