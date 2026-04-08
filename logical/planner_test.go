package logical

import (
	"testing"

	"github.com/ocowchun/sq/ast"
	"github.com/ocowchun/sq/catalog"
)

func TestBuildLogicalPlanForJoinAggregateQuery(t *testing.T) {
	sql := "SELECT t.name AS team_name, u.name AS user_name FROM users as u LEFT JOIN teams as t ON u.team_id = t.id WHERE t.active = true"
	plan, err := buildLogicalPlan(testCatalog(), sql)
	if err != nil {
		t.Fatal(err)
	}

	project, ok := plan.(*Project)
	if !ok {
		t.Fatalf("plan = %T, want *Project", plan)
	}
	if len(project.SelectExprs) != 2 {
		t.Fatalf("select item count = %d, want 2", len(project.SelectExprs))
	}
	firstSelect, ok := project.SelectExprs[0].Expr.(*ColumnRef)
	if !ok {
		t.Fatalf("first select expr = %T, want *ColumnRef", project.SelectExprs[0].Expr)
	}
	if project.SelectExprs[0].Alias != "team_name" || firstSelect.TableAlias != "t" || firstSelect.ColumnName != "name" {
		t.Fatalf("unexpected first select item: %+v", project.SelectExprs[0])
	}

	secondSelect, ok := project.SelectExprs[1].Expr.(*ColumnRef)
	if !ok {
		t.Fatalf("second select expr = %T, want *ColumnRef", project.SelectExprs[1].Expr)
	}
	if project.SelectExprs[1].Alias != "user_name" || secondSelect.TableAlias != "u" || secondSelect.ColumnName != "name" {
		t.Fatalf("unexpected second select item: %+v", project.SelectExprs[1])
	}

	where, ok := project.Input.(*Filter)
	if !ok {
		t.Fatalf("project input = %T, want *Filter", project.Input)
	}
	wherePredicate, ok := where.Predicate.(*ExprPredicate)
	if !ok {
		t.Fatalf("where predicate = %T, want *ExprPredicate", where.Predicate)
	}
	whereExpr, ok := wherePredicate.Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("where expr = %T, want *BinaryExpr", wherePredicate.Expr)
	}
	whereLeft, ok := whereExpr.Left.(*ColumnRef)
	if !ok {
		t.Fatalf("where left = %T, want *ColumnRef", whereExpr.Left)
	}
	whereRight, ok := whereExpr.Right.(*LiteralExpr)
	if !ok {
		t.Fatalf("where right= %T, want *ColumnRef", whereExpr.Right)
	}
	if whereExpr.Op != ast.BinaryOpEqual || whereLeft.TableAlias != "t" || whereLeft.ColumnName != "active" || whereRight.LiteralType != catalog.ColumnTypeBool || whereRight.Value != true {
		t.Fatalf("unexpected where predicate: %+v", where.Predicate)
	}

	join, ok := where.Input.(*Join)
	if !ok {
		t.Fatalf("where input = %T, want *Join", where.Input)
	}
	if join.Type != ast.JoinTypeLeftJoin {
		t.Fatalf("join type = %v, want LEFT", join.Type)
	}
	joinOn, ok := join.On.(*ExprPredicate)
	if !ok {
		t.Fatalf("join predicate = %T, want *ExprPredicate", join.On)
	}
	joinExpr, ok := joinOn.Expr.(*BinaryExpr)
	if !ok {
		t.Fatalf("join expr= %T, want *BinaryExpr", join.On)
	}
	joinLeft, ok := joinExpr.Left.(*ColumnRef)
	if !ok {
		t.Fatalf("join left = %T, want *ColumnRef", joinExpr.Left)
	}
	joinRight, ok := joinExpr.Right.(*ColumnRef)
	if !ok {
		t.Fatalf("join right = %T, want *ColumnRef", joinExpr.Right)
	}
	if joinExpr.Op != ast.BinaryOpEqual || joinLeft.TableAlias != "u" || joinLeft.ColumnName != "team_id" || joinRight.TableAlias != "t" || joinRight.ColumnName != "id" {
		t.Fatalf("unexpected join predicate: %+v", join.On)
	}

	leftScan, ok := join.Left.(*Scan)
	if !ok {
		t.Fatalf("join left = %T, want *Scan", join.Left)
	}
	if leftScan.Table.Name != "users" || leftScan.Table.Alias != "u" {
		t.Fatalf("unexpected left scan table: %+v", leftScan.Table)
	}

	rightScan, ok := join.Right.(*Scan)
	if !ok {
		t.Fatalf("join right = %T, want *Scan", join.Right)
	}
	if rightScan.Table.Name != "teams" || rightScan.Table.Alias != "t" {
		t.Fatalf("unexpected right scan table: %+v", rightScan.Table)
	}
}

func testCatalog() *catalog.Catalog {
	c := catalog.New()
	c.RegisterTable(catalog.Table{
		Name: "users",
		Schema: catalog.Schema{
			Columns: []catalog.Column{
				{Name: "id", Type: catalog.ColumnTypeInt},
				{Name: "name", Type: catalog.ColumnTypeString},
				{Name: "score", Type: catalog.ColumnTypeInt},
				{Name: "team_id", Type: catalog.ColumnTypeInt},
			},
		},
		AccessKind: catalog.AccessKindDefault,
	})
	c.RegisterTable(catalog.Table{
		Name: "teams",
		Schema: catalog.Schema{
			Columns: []catalog.Column{
				{Name: "id", Type: catalog.ColumnTypeInt},
				{Name: "name", Type: catalog.ColumnTypeString},
				{Name: "active", Type: catalog.ColumnTypeBool},
			},
		},
		AccessKind: catalog.AccessKindDefault,
	})
	c.RegisterTable(catalog.Table{
		Name: "objects",
		Schema: catalog.Schema{
			Columns: []catalog.Column{
				{Name: "key", Type: catalog.ColumnTypeString},
				{Name: "bucket_name", Type: catalog.ColumnTypeString},
				{Name: "last_modified", Type: catalog.ColumnTypeDatetime},
				{Name: "size", Type: catalog.ColumnTypeInt},
				{Name: "storage_class", Type: catalog.ColumnTypeString},
			},
		},
		AccessKind: catalog.AccessKindS3Sdk,
	})
	return c
}
