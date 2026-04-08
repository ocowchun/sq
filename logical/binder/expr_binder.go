package binder

import (
	"fmt"

	"github.com/ocowchun/sq/ast"
	"github.com/ocowchun/sq/catalog"
)

type exprBinder struct {
	scope   *scope
	catalog *catalog.Catalog
}

func newExprBinder(
	scope *scope,
	catalog *catalog.Catalog,
) *exprBinder {
	return &exprBinder{
		scope:   scope,
		catalog: catalog,
	}
}

func (b *exprBinder) bindSearchCondition(searchCondition ast.SearchCondition) (SearchCondition, error) {
	switch sc := searchCondition.(type) {
	case *ast.OrSearchCondition:
		left, err := b.bindSearchCondition(sc.LeftCondition)
		if err != nil {
			return nil, err
		}
		right, err := b.bindSearchCondition(sc.RightCondition)
		if err != nil {
			return nil, err
		}
		return &OrSearchCondition{
			LeftCondition:  left,
			RightCondition: right,
		}, nil
	case *ast.AndSearchCondition:
		left, err := b.bindSearchCondition(sc.LeftCondition)
		if err != nil {
			return nil, err
		}
		right, err := b.bindSearchCondition(sc.RightCondition)
		if err != nil {
			return nil, err
		}
		return &AndSearchCondition{
			LeftCondition:  left,
			RightCondition: right,
		}, nil

	case *ast.LikePredicate:
		left, err := b.bind(sc.Left)
		if err != nil {
			return nil, err
		}
		return &LikePredicate{
			Left:  left,
			Not:   sc.Not,
			Right: sc.Right,
		}, nil

	case *ast.InPredicate:
		left, err := b.bind(sc.Left)
		if err != nil {
			return nil, err
		}

		exprs := make([]Expr, len(sc.Expressions))
		for i, e := range sc.Expressions {
			expr, err := b.bind(e)
			if err != nil {
				return nil, err
			}
			exprs[i] = expr
		}
		return &InPredicate{
			Left:        left,
			Not:         sc.Not,
			Expressions: exprs,
		}, nil

	case *ast.IsNullPredicate:
		expr, err := b.bind(sc.Expression)
		if err != nil {
			return nil, err
		}
		return &IsNullPredicate{
			Expression: expr,
			Not:        sc.Not,
		}, nil

	case *ast.ExprPredicate:
		expr, err := b.bind(sc.Expr)
		if err != nil {
			return nil, err
		}

		if expr.Type() != catalog.ColumnTypeBool {
			return nil, newBindError(sc.Expr.Position(), "expression must be of boolean type")
		}
		return &ExprPredicate{
			Expr: expr,
		}, nil
	default:
		return nil, newBindError(searchCondition.Position(), fmt.Sprintf("unsupported search condition type: %T", sc))
	}
}

func (b *exprBinder) bind(expr ast.Expr) (Expr, error) {
	switch e := expr.(type) {
	case *ast.IdentifierExpr:
		return b.bindIdentifier(e)
	case *ast.QualifiedRef:
		return b.bindQualifiedRef(e)
	case *ast.LiteralExpr:
		return b.bindLiteral(e)
	case *ast.UnaryExpr:
		return b.bindUnary(e)
	case *ast.BinaryExpr:
		return b.bindBinaryExpr(e)
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}

func (b *exprBinder) bindBinaryExpr(expr *ast.BinaryExpr) (Expr, error) {
	left, err := b.bind(expr.Left)
	if err != nil {
		return nil, err
	}
	right, err := b.bind(expr.Right)
	if err != nil {
		return nil, err
	}

	switch expr.Operator {
	case ast.BinaryOpAdd, ast.BinaryOpSub, ast.BinaryOpMul, ast.BinaryOpDiv:
		if left.Type() != catalog.ColumnTypeInt {
			return nil, newBindError(expr.Position(), "left operand is not an integer")
		}
		if right.Type() != catalog.ColumnTypeInt {
			return nil, newBindError(expr.Position(), "right operand is not an integer")
		}
		return &BinaryExpr{
			Operator:   expr.Operator,
			Left:       left,
			Right:      right,
			ColumnType: catalog.ColumnTypeInt,
		}, nil
	case ast.BinaryOpEqual, ast.BinaryOpNotEqual, ast.BinaryOpGreater, ast.BinaryOpGreaterEqual, ast.BinaryOpLess, ast.BinaryOpLessEqual:
		if left.Type() != right.Type() {
			return nil, newBindError(expr.Position(), "operands must be of the same type")
		}

		return &BinaryExpr{
			Operator:   expr.Operator,
			Left:       left,
			Right:      right,
			ColumnType: catalog.ColumnTypeBool,
		}, nil
	default:
		return nil, newBindError(expr.Position(), "operator is not supported")
	}
}

func (b *exprBinder) bindUnary(expr *ast.UnaryExpr) (Expr, error) {
	inner, err := b.bind(expr.Expr)
	if err != nil {
		return nil, err
	}
	return &UnaryExpr{
		Operator:   expr.Operator,
		Expr:       inner,
		ColumnType: inner.Type(),
	}, nil
}

func (b *exprBinder) bindLiteral(expr *ast.LiteralExpr) (Expr, error) {
	switch expr.LiteralType {
	case ast.LiteralTypeBool:
		return &Literal{Value: expr.Value, LiteralType: catalog.ColumnTypeBool}, nil
	case ast.LiteralTypeInt:
		return &Literal{Value: expr.Value, LiteralType: catalog.ColumnTypeInt}, nil
	case ast.LiteralTypeDouble:
		return &Literal{Value: expr.Value, LiteralType: catalog.ColumnTypeDouble}, nil
	case ast.LiteralTypeDatetime:
		return &Literal{Value: expr.Value, LiteralType: catalog.ColumnTypeDatetime}, nil
	case ast.LiteralTypeNull:
		return &Literal{Value: expr.Value, LiteralType: catalog.ColumnTypeNull}, nil
	case ast.LiteralTypeString:
		return &Literal{Value: expr.Value, LiteralType: catalog.ColumnTypeString}, nil
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr.LiteralType)
	}
}

func (b *exprBinder) bindQualifiedRef(expr *ast.QualifiedRef) (Expr, error) {
	column, err := b.scope.resolveQualified(expr.TableName, expr.Name)
	if err != nil {
		return nil, err
	}
	return b.columnRef(column), nil
}

func (b *exprBinder) bindIdentifier(expr *ast.IdentifierExpr) (Expr, error) {
	column, err := b.scope.resolveColumn(expr.Name)
	if err != nil {
		return nil, err
	}
	return b.columnRef(column), nil
}

func (b *exprBinder) columnRef(column *scopeColumn) Expr {
	return &ColumnRef{
		TableName:   column.table.name,
		TableAlias:  column.table.alias,
		ColumnName:  column.name,
		ColumnIndex: column.index,
		ColumnType:  column.columnType,
	}
}
