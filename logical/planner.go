package logical

import (
	"fmt"

	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical/binder"
	"github.com/ocowchun/sq/parser"
)

func BuildLogicalOptimizedPlan(c *catalog.Catalog, sql string) (Node, error) {
	plan, err := buildLogicalPlan(c, sql)
	if err != nil {
		return nil, err
	}

	optimized, err := OptimizeLogical(plan)
	if err != nil {
		return nil, err
	}
	return optimized, nil
}

func buildLogicalPlan(c *catalog.Catalog, sql string) (Node, error) {
	stmt, err := parser.Parse(sql)
	if err != nil {
		return nil, err
	}
	boundStatement, err := binder.Bind(c, stmt)
	if err != nil {
		return nil, err
	}

	return buildLogicalStatement(boundStatement)
}

func buildLogicalStatement(statement binder.Statement) (Node, error) {
	builder := &logicalBuilder{nextRelationID: 1}
	switch stmt := statement.(type) {
	case *binder.Query:
		return builder.buildLogicalQuery(stmt)
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

type logicalBuilder struct {
	nextRelationID int
}

func (b *logicalBuilder) buildLogicalQuery(query *binder.Query) (Node, error) {
	definitions := make([]CTEDefinition, len(query.CTEs))
	for i, cte := range query.CTEs {
		q, err := b.buildLogicalQuery(cte.Query)
		if err != nil {
			return nil, err
		}
		definitions[i] = CTEDefinition{
			Name:   cte.Name,
			Query:  q,
			Schema: cte.Schema,
		}
	}

	plan, relationIds, err := b.buildFrom(query.From)
	if err != nil {
		return nil, err
	}
	if query.Where != nil {
		predicate, err := lowerSearchCondition(query.Where, relationIds)
		if err != nil {
			return nil, err
		}
		plan = &Filter{
			Input:     plan,
			Predicate: predicate,
		}
	}

	selectExprs, err := lowerSelectExprs(query.SelectExprs, relationIds)
	if err != nil {
		return nil, err
	}
	plan = &Project{
		Input:        plan,
		SelectExprs:  selectExprs,
		OutputSchema: query.Schema,
	}

	if len(definitions) == 0 {
		return plan, nil
	}
	return &Statement{
		CTEs: definitions,
		Root: plan,
	}, nil
}

func lowerSelectExprs(exprs []binder.SelectExpr, relationIDs map[string]string) ([]SelectExpr, error) {
	result := make([]SelectExpr, len(exprs))
	for i, e := range exprs {
		expr, err := lowerExpr(e.Expr, relationIDs)
		if err != nil {
			return nil, err
		}
		result[i] = SelectExpr{
			Expr:  expr,
			Alias: e.Alias,
		}
	}

	return result, nil
}

func (b *logicalBuilder) buildFrom(from binder.From) (Node, map[string]string, error) {
	relationIDs := make(map[string]string)
	plan := b.lowerScan(from.Base, relationIDs)
	for _, join := range from.Joins {
		right := b.lowerScan(join.Right, relationIDs)
		on, err := lowerSearchCondition(join.On, relationIDs)
		if err != nil {
			return nil, nil, err
		}
		plan = &Join{
			Left:  plan,
			Right: right,
			Type:  join.JoinType,
			On:    on,
		}
	}

	return plan, relationIDs, nil
}

func lowerTable(table binder.Table) Table {
	return Table{
		Name:       table.Name,
		Alias:      table.Alias,
		Schema:     table.Schema,
		AccessKind: table.AccessKind,
	}
}

func (b *logicalBuilder) lowerScan(table binder.Table, relationIDs map[string]string) Node {
	relationID := b.allocateRelationID()
	relationIDs[visibleRelationName(table)] = relationID
	scan := &Scan{
		RelationID:   relationID,
		RelationName: visibleRelationName(table),
		Table:        lowerTable(table),
	}
	if table.Source == binder.TableSourceCTE {
		scan.CTE = &CTERef{
			Name:   table.Name,
			Schema: table.Schema,
		}
	}

	return scan
}

func (b *logicalBuilder) allocateRelationID() string {
	relationID := fmt.Sprintf("rel#%d", b.nextRelationID)
	b.nextRelationID++
	return relationID
}

func visibleRelationName(table binder.Table) string {
	if table.Alias != "" {
		return table.Alias
	}
	return table.Name
}

func lowerSearchCondition(searchCondition binder.SearchCondition, relationIDs map[string]string) (SearchCondition, error) {
	switch condition := searchCondition.(type) {
	case *binder.OrSearchCondition:
		left, err := lowerSearchCondition(condition.LeftCondition, relationIDs)
		if err != nil {
			return nil, err
		}
		right, err := lowerSearchCondition(condition.RightCondition, relationIDs)
		if err != nil {
			return nil, err
		}

		return &OrSearchCondition{
			Left:  left,
			Right: right,
		}, nil

	case *binder.AndSearchCondition:
		left, err := lowerSearchCondition(condition.LeftCondition, relationIDs)
		if err != nil {
			return nil, err
		}
		right, err := lowerSearchCondition(condition.RightCondition, relationIDs)
		if err != nil {
			return nil, err
		}

		return &AndSearchCondition{
			Left:  left,
			Right: right,
		}, nil
	case *binder.LikePredicate:
		left, err := lowerExpr(condition.Left, relationIDs)
		if err != nil {
			return nil, err
		}
		return &LikePredicate{
			Left:    left,
			Not:     condition.Not,
			Pattern: condition.Right,
		}, nil

	case *binder.InPredicate:
		left, err := lowerExpr(condition.Left, relationIDs)
		if err != nil {
			return nil, err
		}
		exprs := make([]Expr, len(condition.Expressions))
		for i, e := range condition.Expressions {
			expr, err := lowerExpr(e, relationIDs)
			if err != nil {
				return nil, err
			}
			exprs[i] = expr
		}
		return &InPredicate{
			Left:  left,
			Not:   condition.Not,
			Exprs: exprs,
		}, nil

	case *binder.IsNullPredicate:
		expr, err := lowerExpr(condition.Expression, relationIDs)
		if err != nil {
			return nil, err
		}
		return &IsNullPredicate{
			Expression: expr,
			Not:        condition.Not,
		}, nil
	case *binder.ExprPredicate:
		expr, err := lowerExpr(condition.Expr, relationIDs)
		if err != nil {
			return nil, err
		}
		return &ExprPredicate{
			Expr: expr,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported condition type: %T", condition)
	}
}

func lowerExpr(expr binder.Expr, relationIDs map[string]string) (Expr, error) {
	switch e := expr.(type) {
	case *binder.Literal:
		return &LiteralExpr{
			Value:       e.Value,
			LiteralType: e.LiteralType,
		}, nil
	case *binder.ColumnRef:
		relationID, err := relationIDFromBoundColumnRef(e, relationIDs)
		if err != nil {
			return nil, err
		}
		return &ColumnRef{
			RelationID:   relationID,
			RelationName: visibleNameFromColumnRef(e),
			TableName:    e.TableName,
			TableAlias:   e.TableAlias,
			ColumnName:   e.ColumnName,
			ColumnIndex:  e.ColumnIndex,
			ColumnType:   e.ColumnType,
		}, nil
	case *binder.UnaryExpr:
		inner, err := lowerExpr(e.Expr, relationIDs)
		if err != nil {
			return nil, err
		}
		return &UnaryExpr{
			Op:         e.Operator,
			Expr:       inner,
			ColumnType: e.ColumnType,
		}, nil
	case *binder.BinaryExpr:
		left, err := lowerExpr(e.Left, relationIDs)
		if err != nil {
			return nil, err
		}
		right, err := lowerExpr(e.Right, relationIDs)
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{
			Op:         e.Operator,
			Left:       left,
			Right:      right,
			ColumnType: e.ColumnType,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported expression type: %T", e)
	}
}

func relationIDFromBoundColumnRef(column *binder.ColumnRef, relationIDs map[string]string) (string, error) {
	visibleName := visibleNameFromColumnRef(column)
	relationID, ok := relationIDs[visibleName]
	if !ok {
		return "", fmt.Errorf("no relation ID found for columnRef %s.%s", visibleName, column.ColumnName)
	}
	return relationID, nil
}

func visibleNameFromColumnRef(column *binder.ColumnRef) string {
	if column.TableAlias != "" {
		return column.TableAlias
	}
	return column.TableName
}
