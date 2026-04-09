package physical

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/ast"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical"
)

func Test_EvalSearchCondition(t *testing.T) {
	allocator := memory.NewGoAllocator()
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "columnA", Type: arrow.PrimitiveTypes.Int64},
		{Name: "columnB", Type: arrow.BinaryTypes.String},
	}, nil)

	bA := array.NewInt64Builder(allocator)
	defer bA.Release()
	bA.AppendValues([]int64{5, 12, 3, 3, 11}, nil)
	bA.AppendNull()
	colA := bA.NewArray()

	bB := array.NewStringBuilder(allocator)
	defer bB.Release()
	bB.AppendValues([]string{"andy timmons", "bela fleck", "chick corea", "david gilmour", "eric clapton"}, nil)
	bB.AppendNull()
	colB := bB.NewArray()

	testCases := []struct {
		caseName  string
		condition logical.SearchCondition
		expected  []bool
	}{
		{
			caseName: "columnA = 5 or columnB = chick corea",
			condition: &logical.OrSearchCondition{
				Left: &logical.ExprPredicate{
					Expr: &logical.BinaryExpr{
						Op: ast.BinaryOpEqual,
						Left: &logical.ColumnRef{
							RelationID:   "rel#1",
							RelationName: "table1",
							TableName:    "table1",
							ColumnName:   "columnA",
							ColumnIndex:  0,
							ColumnType:   catalog.ColumnTypeInt,
						},
						Right: &logical.LiteralExpr{
							Value:       int64(5),
							LiteralType: catalog.ColumnTypeInt,
						},
					},
				},
				Right: &logical.ExprPredicate{
					Expr: &logical.BinaryExpr{
						Op: ast.BinaryOpEqual,
						Left: &logical.ColumnRef{
							RelationID:   "rel#1",
							RelationName: "table1",
							TableName:    "table1",
							ColumnName:   "columnB",
							ColumnIndex:  1,
							ColumnType:   catalog.ColumnTypeString,
						},
						Right: &logical.LiteralExpr{
							Value:       "chick corea",
							LiteralType: catalog.ColumnTypeString,
						},
					},
				},
			},
			expected: []bool{true, false, true, false, false, false},
		},
		{
			caseName: "columnA = 3 and columnB = david glimour",
			condition: &logical.AndSearchCondition{
				Left: &logical.ExprPredicate{
					Expr: &logical.BinaryExpr{
						Op: ast.BinaryOpEqual,
						Left: &logical.ColumnRef{
							RelationID:   "rel#1",
							RelationName: "table1",
							TableName:    "table1",
							ColumnName:   "columnA",
							ColumnIndex:  0,
							ColumnType:   catalog.ColumnTypeInt,
						},
						Right: &logical.LiteralExpr{
							Value:       int64(3),
							LiteralType: catalog.ColumnTypeInt,
						},
					},
				},
				Right: &logical.ExprPredicate{
					Expr: &logical.BinaryExpr{
						Op: ast.BinaryOpEqual,
						Left: &logical.ColumnRef{
							RelationID:   "rel#1",
							RelationName: "table1",
							TableName:    "table1",
							ColumnName:   "columnB",
							ColumnIndex:  1,
							ColumnType:   catalog.ColumnTypeString,
						},
						Right: &logical.LiteralExpr{
							Value:       "david gilmour",
							LiteralType: catalog.ColumnTypeString,
						},
					},
				},
			},
			expected: []bool{false, false, false, true, false, false},
		},
		{
			caseName: "columnA = 3",
			condition: &logical.ExprPredicate{
				Expr: &logical.BinaryExpr{
					Op: ast.BinaryOpEqual,
					Left: &logical.ColumnRef{
						RelationID:   "rel#1",
						RelationName: "table1",
						TableName:    "table1",
						ColumnName:   "columnA",
						ColumnIndex:  0,
						ColumnType:   catalog.ColumnTypeInt,
					},
					Right: &logical.LiteralExpr{
						Value:       int64(3),
						LiteralType: catalog.ColumnTypeInt,
					},
				},
			},
			expected: []bool{false, false, true, true, false, false},
		},
		{
			caseName: "columnA != 3",
			condition: &logical.ExprPredicate{
				Expr: &logical.BinaryExpr{
					Op: ast.BinaryOpNotEqual,
					Left: &logical.ColumnRef{
						RelationID:   "rel#1",
						RelationName: "table1",
						TableName:    "table1",
						ColumnName:   "columnA",
						ColumnIndex:  0,
						ColumnType:   catalog.ColumnTypeInt,
					},
					Right: &logical.LiteralExpr{
						Value:       int64(3),
						LiteralType: catalog.ColumnTypeInt,
					},
				},
			},
			expected: []bool{true, true, false, false, true, true},
		},
		{
			caseName: "columnA > 3",
			condition: &logical.ExprPredicate{
				Expr: &logical.BinaryExpr{
					Op: ast.BinaryOpGreater,
					Left: &logical.ColumnRef{
						RelationID:   "rel#1",
						RelationName: "table1",
						TableName:    "table1",
						ColumnName:   "columnA",
						ColumnIndex:  0,
						ColumnType:   catalog.ColumnTypeInt,
					},
					Right: &logical.LiteralExpr{
						Value:       int64(3),
						LiteralType: catalog.ColumnTypeInt,
					},
				},
			},
			expected: []bool{true, true, false, false, true, false},
		},
		{
			caseName: "columnA >= 11",
			condition: &logical.ExprPredicate{
				Expr: &logical.BinaryExpr{
					Op: ast.BinaryOpGreaterEqual,
					Left: &logical.ColumnRef{
						RelationID:   "rel#1",
						RelationName: "table1",
						TableName:    "table1",
						ColumnName:   "columnA",
						ColumnIndex:  0,
						ColumnType:   catalog.ColumnTypeInt,
					},
					Right: &logical.LiteralExpr{
						Value:       int64(11),
						LiteralType: catalog.ColumnTypeInt,
					},
				},
			},
			expected: []bool{false, true, false, false, true, false},
		},
		{
			caseName: "columnA < 11",
			condition: &logical.ExprPredicate{
				Expr: &logical.BinaryExpr{
					Op: ast.BinaryOpLess,
					Left: &logical.ColumnRef{
						RelationID:   "rel#1",
						RelationName: "table1",
						TableName:    "table1",
						ColumnName:   "columnA",
						ColumnIndex:  0,
						ColumnType:   catalog.ColumnTypeInt,
					},
					Right: &logical.LiteralExpr{
						Value:       int64(11),
						LiteralType: catalog.ColumnTypeInt,
					},
				},
			},
			expected: []bool{true, false, true, true, false, true},
		},

		{
			caseName: "columnA <= 11",
			condition: &logical.ExprPredicate{
				Expr: &logical.BinaryExpr{
					Op: ast.BinaryOpLessEqual,
					Left: &logical.ColumnRef{
						RelationID:   "rel#1",
						RelationName: "table1",
						TableName:    "table1",
						ColumnName:   "columnA",
						ColumnIndex:  0,
						ColumnType:   catalog.ColumnTypeInt,
					},
					Right: &logical.LiteralExpr{
						Value:       int64(11),
						LiteralType: catalog.ColumnTypeInt,
					},
				},
			},
			expected: []bool{true, false, true, true, true, true},
		},
		{
			caseName: "columnA is null",
			condition: &logical.IsNullPredicate{
				Expression: &logical.ColumnRef{
					RelationID:   "rel#1",
					RelationName: "table1",
					TableName:    "table1",
					ColumnName:   "columnA",
					ColumnIndex:  0,
					ColumnType:   catalog.ColumnTypeInt,
				},
				Not: false,
			},
			expected: []bool{false, false, false, false, false, true},
		},
		{
			caseName: "columnA is not null",
			condition: &logical.IsNullPredicate{
				Expression: &logical.ColumnRef{
					RelationID:   "rel#1",
					RelationName: "table1",
					TableName:    "table1",
					ColumnName:   "columnA",
					ColumnIndex:  0,
					ColumnType:   catalog.ColumnTypeInt,
				},
				Not: true,
			},
			expected: []bool{true, true, true, true, true, false},
		},
		{
			caseName: "columnB like andy%",
			condition: &logical.LikePredicate{
				Left: &logical.ColumnRef{
					RelationID:   "rel#1",
					RelationName: "table1",
					TableName:    "table1",
					ColumnName:   "columnB",
					ColumnIndex:  1,
					ColumnType:   catalog.ColumnTypeString,
				},
				Not:     false,
				Pattern: "andy%",
			},
			expected: []bool{true, false, false, false, false, false},
		},
		{
			caseName: "columnB like %fleck",
			condition: &logical.LikePredicate{
				Left: &logical.ColumnRef{
					RelationID:   "rel#1",
					RelationName: "table1",
					TableName:    "table1",
					ColumnName:   "columnB",
					ColumnIndex:  1,
					ColumnType:   catalog.ColumnTypeString,
				},
				Not:     false,
				Pattern: "%fleck",
			},
			expected: []bool{false, true, false, false, false, false},
		},
		{
			caseName: "columnB like %ric%",
			condition: &logical.LikePredicate{
				Left: &logical.ColumnRef{
					RelationID:   "rel#1",
					RelationName: "table1",
					TableName:    "table1",
					ColumnName:   "columnB",
					ColumnIndex:  1,
					ColumnType:   catalog.ColumnTypeString,
				},
				Not:     false,
				Pattern: "%ric%",
			},
			expected: []bool{false, false, false, false, true, false},
		},
		{
			caseName: "columnB in (chick corea, andy timmons)",
			condition: &logical.InPredicate{
				Left: &logical.ColumnRef{
					RelationID:   "rel#1",
					RelationName: "table1",
					TableName:    "table1",
					ColumnName:   "columnB",
					ColumnIndex:  1,
					ColumnType:   catalog.ColumnTypeString,
				},
				Not: false,
				Exprs: []*logical.LiteralExpr{
					{Value: "chick corea", LiteralType: catalog.ColumnTypeString},
					{Value: "andy timmons", LiteralType: catalog.ColumnTypeString},
				},
			},
			expected: []bool{true, false, true, false, false, false},
		},
		{
			caseName: "columnB not in (chick corea, andy timmons)",
			condition: &logical.InPredicate{
				Left: &logical.ColumnRef{
					RelationID:   "rel#1",
					RelationName: "table1",
					TableName:    "table1",
					ColumnName:   "columnB",
					ColumnIndex:  1,
					ColumnType:   catalog.ColumnTypeString,
				},
				Not: true,
				Exprs: []*logical.LiteralExpr{
					{Value: "chick corea", LiteralType: catalog.ColumnTypeString},
					{Value: "andy timmons", LiteralType: catalog.ColumnTypeString},
				},
			},
			expected: []bool{false, true, false, true, true, false},
		},
	}

	batch := array.NewRecordBatch(schema, []arrow.Array{colA, colB}, int64(colA.Len()))

	eval := newEvaluator(allocator)

	for _, tt := range testCases {
		t.Run(tt.caseName, func(t *testing.T) {
			ary, err := eval.evaluateSearchCondition(tt.condition, batch)
			if err != nil {
				t.Fatal(err)
			}
			assertBooleanArray(t, ary, tt.expected)
		})
	}
}

func assertBooleanArray(t *testing.T, actual *array.Boolean, expected []bool) {
	if actual.Len() != len(expected) {
		t.Fatalf("actual length is %d while expected length is %d", actual.Len(), len(expected))
	}
	for i := 0; i < len(expected); i++ {
		if actual.Value(i) != expected[i] {
			t.Fatalf("actual[%d](%v) does not match expected[%d](%v)  ", i, actual.Value(i), i, expected[i])
		}
	}
}
