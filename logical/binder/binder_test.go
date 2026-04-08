package binder

import (
	"testing"

	"github.com/ocowchun/sq/ast"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/parser"
)

func Test_BindResolvesQualifiedAndUnqualifiedColumns(t *testing.T) {
	c := testCatalog()

	stmt, err := Bind(c, mustParse(t, "SELECT name, users.id FROM users ORDER BY name"))
	if err != nil {
		t.Fatal(err)
	}
	query, ok := stmt.(*Query)
	if !ok {
		t.Fatal("expected Query")
	}

	if len(query.SelectExprs) != 2 {
		t.Fatalf("select item count = %d, want 2", len(query.SelectExprs))
	}

	first, ok := query.SelectExprs[0].Expr.(*ColumnRef)
	if !ok {
		t.Fatalf("first select expr = %T, want *bound.ColumnRef", query.SelectExprs[0].Expr)
	}
	if first.Type() != catalog.ColumnTypeString {
		t.Fatalf("first select type = %v, want %v", first.Type(), catalog.ColumnTypeString)
	}
	if first.TableName != "users" || first.ColumnName != "name" || first.ColumnIndex != 1 {
		t.Fatalf("unexpected first select binding: %+v", first)
	}

	second, ok := query.SelectExprs[1].Expr.(*ColumnRef)
	if !ok {
		t.Fatalf("second select expr = %T, want *bound.ColumnRef", query.SelectExprs[1].Expr)
	}
	if second.TableName != "users" || second.ColumnName != "id" || second.ColumnIndex != 0 {
		t.Fatalf("unexpected second select binding: %+v", second)
	}
}

func Test_BindRejectInvalidCases(t *testing.T) {
	c := testCatalog()

	tests := []struct {
		caseName string
		sql      string
	}{
		{"unknown table", "SELECT * FROM unknown_table"},
		{"unknown column", "SELECT unknown_column FROM users"},
		{"unknown qualifier", "SELECT a.id FROM users"},
		{"expression is not boolean", "SELECT id FROM users where id + 10"},
		{"operand type doesn't matched", "SELECT id FROM users where id = name"},
	}
	for _, tt := range tests {
		t.Run(tt.caseName, func(t *testing.T) {
			_, err := Bind(c, mustParse(t, tt.sql))
			if err == nil {
				t.Fatalf("%s must fail", tt.caseName)
			}
		})
	}
}

func mustParse(t *testing.T, sql string) ast.Statement {
	t.Helper()

	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("parse %q: %v", sql, err)
	}
	return stmt
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
	})
	return c
}
