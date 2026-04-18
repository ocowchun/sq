package binder

import (
	"fmt"

	"github.com/ocowchun/sq/ast"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/token"
)

func Bind(c *catalog.Catalog, statement ast.Statement) (Statement, error) {
	b := newBinder(c)
	return b.Bind(statement)
}

type Binder struct {
	catalog *catalog.Catalog
}

func newBinder(c *catalog.Catalog) *Binder {
	return &Binder{
		catalog: c,
	}
}

func (b *Binder) Bind(statement ast.Statement) (Statement, error) {
	switch statement.(type) {
	case *ast.SelectStatement:
		stmt := statement.(*ast.SelectStatement)
		visibleCTEs := make(map[string]CTE)
		query, err := b.bindSelect(stmt, visibleCTEs, "")
		if err != nil {
			return nil, err
		}
		return query, nil
	default:
		return nil, fmt.Errorf("unsupported statement type: %T", statement)
	}
}

type BindError struct {
	Pos     token.Position
	Message string
}

func (e *BindError) Error() string {
	return fmt.Sprintf("Bind error at line %d, column %d: %s", e.Pos.Line, e.Pos.Column, e.Message)
}
func newBindError(pos token.Position, msg string) *BindError {
	return &BindError{
		Pos:     pos,
		Message: msg,
	}
}

func (b *Binder) bindSelect(statement *ast.SelectStatement, visibleCTEs map[string]CTE, currentCTE string) (*Query, error) {
	localCTEs := cloneCTEs(visibleCTEs)
	boundCTEs := make([]CTE, len(statement.CTEs))
	for i, cte := range statement.CTEs {

		bodyVisibleCTEs := cloneCTEs(localCTEs)
		query, err := b.bindSelect(cte.Query, bodyVisibleCTEs, cte.Name)
		if err != nil {
			return nil, err
		}
		boundCTE := CTE{
			Name:   cte.Name,
			Query:  query,
			Schema: query.Schema,
		}
		boundCTEs[i] = boundCTE
		localCTEs[cte.Name] = boundCTE
	}

	scope := newScope()
	from, err := b.bindFrom(scope, statement.From, localCTEs, currentCTE)
	if err != nil {
		return nil, err
	}

	selectExprs, err := b.bindSelectExprs(scope, statement.SelectExprs)
	if err != nil {
		return nil, err
	}

	where, err := b.bindWhere(scope, statement.Where)
	if err != nil {
		return nil, err
	}

	orderBy, err := b.bindOrderBy(scope, statement.OrderBy, selectExprs)
	if err != nil {
		return nil, err
	}

	var limit *uint32
	if statement.Limit != nil {
		if *statement.Limit < 0 {
			return nil, fmt.Errorf("limit must be non-negative")
		}
		l := uint32(*statement.Limit)
		limit = &l
	}

	return &Query{
		CTEs:        boundCTEs,
		SelectExprs: selectExprs,
		From:        from,
		Where:       where,
		Schema:      buildSchema(selectExprs),
		OrderBy:     orderBy,
		Limit:       limit,
	}, nil
}

func (b *Binder) bindOrderBy(scope *scope, orderings []ast.Ordering, selectExprs []SelectExpr) ([]Ordering, error) {
	orderBy := make([]Ordering, len(orderings))
	if orderings == nil || len(orderings) == 0 {
		return orderBy, nil
	}

	aliasIndexes := make(map[string]int)
	for i, selectExpr := range selectExprs {
		if selectExpr.Alias != "" {
			aliasIndexes[selectExpr.Alias] = i
		}
	}

	selectBinder := newExprBinder(scope, b.catalog)
	for i, ordering := range orderings {
		switch expr := ordering.Expr.(type) {
		case *ast.LiteralExpr:
			if expr.LiteralType == ast.LiteralTypeInt {
				index := int(expr.Value.(int64))
				if index < 0 || index >= len(scope.tables) {
					return nil, newBindError(expr.Pos, "index out of range")
				}

				o := Ordering{
					Expr: selectExprs[index].Expr,
					Desc: ordering.Desc,
				}
				orderBy[i] = o
			} else {
				return orderBy, newBindError(expr.Pos, fmt.Sprintf("unsupported order expression"))
			}
		case *ast.IdentifierExpr:
			if index, ok := aliasIndexes[expr.Name]; ok {
				o := Ordering{
					Expr: selectExprs[index].Expr,
					Desc: ordering.Desc,
				}
				orderBy[i] = o
				continue
			}

			columnRef, err := selectBinder.bindIdentifier(expr)
			if err != nil {
				return nil, err
			}
			o := Ordering{
				Expr: columnRef,
				Desc: ordering.Desc,
			}
			orderBy[i] = o
		default:
			exp, err := selectBinder.bind(ordering.Expr)
			if err != nil {
				return nil, err
			}
			o := Ordering{
				Expr: exp,
				Desc: ordering.Desc,
			}
			orderBy[i] = o
		}
	}

	return orderBy, nil
}

func buildSchema(items []SelectExpr) catalog.Schema {
	schema := catalog.Schema{
		Columns: make([]catalog.Column, 0, len(items)),
	}
	for _, item := range items {
		name := item.Alias
		if name == "" {
			name = exprName(item.Expr)
		}
		schema.Columns = append(schema.Columns, catalog.Column{
			Name: name,
			Type: item.Expr.Type(),
		})
	}
	return schema
}

func exprName(expr Expr) string {
	switch e := expr.(type) {
	case *ColumnRef:
		return e.ColumnName
	default:
		return fmt.Sprintf("%T", expr)
	}
}

func (b *Binder) bindSelectExprs(scope *scope, selectExprs []ast.SelectExpr) ([]SelectExpr, error) {
	if len(selectExprs) == 0 {
		return nil, fmt.Errorf("no select expressions found")
	}

	selectBinder := newExprBinder(scope, b.catalog)
	res := make([]SelectExpr, 0, len(selectExprs))
	for _, e := range selectExprs {
		switch expr := e.Expr.(type) {
		case *ast.StarExpr:
			for _, column := range scope.visibleColumns() {
				res = append(res, SelectExpr{
					Expr: column.ColumnRef(),
				})
			}
		case *ast.QualifiedStarExpr:
			columns, err := scope.tableColumns(expr.TableName)
			if err != nil {
				return nil, newBindError(expr.Position(), err.Error())
			}
			for _, column := range columns {
				res = append(res, SelectExpr{
					Expr: column.ColumnRef(),
				})
			}
		default:
			boundExpr, err := selectBinder.bind(expr)
			if err != nil {
				return nil, err
			}
			res = append(res, SelectExpr{
				Expr:  boundExpr,
				Alias: e.Alias,
			})
		}
	}

	return res, nil
}

func (b *Binder) bindWhere(scope *scope, where ast.SearchCondition) (SearchCondition, error) {
	if where == nil {
		return nil, nil
	}

	whereBinder := newExprBinder(scope, b.catalog)
	searchCondition, err := whereBinder.bindSearchCondition(where)
	if err != nil {
		return nil, err
	}
	return searchCondition, nil
}

func cloneCTEs(ctes map[string]CTE) map[string]CTE {
	if len(ctes) == 0 {
		return make(map[string]CTE)
	}

	clone := make(map[string]CTE, len(ctes))
	for name, cte := range ctes {
		clone[name] = cte
	}
	return clone
}

func (b *Binder) bindFrom(scope *scope, from ast.From, visibleCTEs map[string]CTE, currentCTE string) (From, error) {
	rel, err := b.lookupRelation(from.Relation.Name, visibleCTEs, currentCTE)
	if err != nil {
		return From{}, err
	}

	baseScopeTable, err := scope.addRelation(
		rel.name,
		rel.schema,
		from.Relation.Alias,
		rel.source,
		rel.accessKind,
	)
	if err != nil {
		return From{}, err
	}

	joins := make([]Join, len(from.Joins))
	for i, join := range from.Joins {
		rightRe, err := b.lookupRelation(join.Right.Name, visibleCTEs, currentCTE)
		if err != nil {
			return From{}, err
		}
		rightScopeTable, err := scope.addRelation(
			rightRe.name,
			rightRe.schema,
			join.Right.Alias,
			rightRe.source,
			rightRe.accessKind,
		)
		if err != nil {
			return From{}, err
		}
		onBinder := newExprBinder(scope, b.catalog)
		on, err := onBinder.bindSearchCondition(join.On)
		if err != nil {
			return From{}, err
		}
		joins[i] = Join{
			JoinType: join.JoinType,
			Right:    toBoundTable(rightScopeTable),
			On:       on,
		}
	}

	return From{
		Base:  toBoundTable(baseScopeTable),
		Joins: joins,
	}, nil

}
func toBoundTable(table *scopeTable) Table {
	return Table{
		Name:       table.name,
		Alias:      table.alias,
		Schema:     table.schema,
		Source:     table.source,
		AccessKind: table.accessKind,
	}
}

type relation struct {
	name       string
	schema     catalog.Schema
	source     TableSource
	accessKind catalog.AccessKind
}

func (b *Binder) lookupRelation(name string, visibleCTEs map[string]CTE, currentCTE string) (relation, error) {
	if currentCTE != "" && name == currentCTE {
		return relation{}, fmt.Errorf("recursive reference to CTE %s is not allowed", name)
	}
	if visibleCTEs != nil {
		if cte, ok := visibleCTEs[name]; ok {
			return relation{
				name:   cte.Name,
				schema: cte.Schema,
				source: TableSourceCTE,
			}, nil
		}
	}
	table, ok := b.catalog.GetTable(name)
	if !ok {
		return relation{}, fmt.Errorf("table %s not found", name)
	}
	return relation{
		name:       table.Name,
		schema:     table.Schema,
		source:     TableSourceCatalog,
		accessKind: table.AccessKind,
	}, nil
}
