package physical

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/ast"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical"
)

func TestLoopJoinInnerJoinHappyCase(t *testing.T) {
	allocator := memory.NewGoAllocator()
	leftSchema := arrow.NewSchema([]arrow.Field{
		{Name: "rel#1.id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "rel#1.name", Type: arrow.BinaryTypes.String},
	}, nil)
	rightSchema := arrow.NewSchema([]arrow.Field{
		{Name: "rel#2.user_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "rel#2.team", Type: arrow.BinaryTypes.String},
	}, nil)

	leftIDBuilder := array.NewInt64Builder(allocator)
	defer leftIDBuilder.Release()
	leftIDBuilder.AppendValues([]int64{1, 2, 3}, nil)
	leftID := leftIDBuilder.NewArray()
	defer leftID.Release()

	leftNameBuilder := array.NewStringBuilder(allocator)
	defer leftNameBuilder.Release()
	leftNameBuilder.AppendValues([]string{"alice", "bob", "carol"}, nil)
	leftName := leftNameBuilder.NewArray()
	defer leftName.Release()

	rightUserIDBuilder := array.NewInt64Builder(allocator)
	defer rightUserIDBuilder.Release()
	rightUserIDBuilder.AppendValues([]int64{2, 3, 4}, nil)
	rightUserID := rightUserIDBuilder.NewArray()
	defer rightUserID.Release()

	rightTeamBuilder := array.NewStringBuilder(allocator)
	defer rightTeamBuilder.Release()
	rightTeamBuilder.AppendValues([]string{"blue", "green", "red"}, nil)
	rightTeam := rightTeamBuilder.NewArray()
	defer rightTeam.Release()

	leftBatch := array.NewRecordBatch(leftSchema, []arrow.Array{leftID, leftName}, int64(leftID.Len()))
	rightBatch := array.NewRecordBatch(rightSchema, []arrow.Array{rightUserID, rightTeam}, int64(rightUserID.Len()))

	left := newTestIterator(allocator, leftBatch, catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.ColumnTypeInt},
			{Name: "name", Type: catalog.ColumnTypeString},
		},
	})
	right := newTestIterator(allocator, rightBatch, catalog.Schema{
		Columns: []catalog.Column{
			{Name: "user_id", Type: catalog.ColumnTypeInt},
			{Name: "team", Type: catalog.ColumnTypeString},
		},
	})

	join := newLoopJoin(left, right, &logical.Join{
		Type: ast.JoinTypeInnerJoin,
		On: &logical.ExprPredicate{
			Expr: &logical.BinaryExpr{
				Op: ast.BinaryOpEqual,
				Left: &logical.ColumnRef{
					RelationID:   "rel#1",
					RelationName: "users",
					TableName:    "users",
					ColumnName:   "id",
					ColumnIndex:  0,
					ColumnType:   catalog.ColumnTypeInt,
				},
				Right: &logical.ColumnRef{
					RelationID:   "rel#2",
					RelationName: "memberships",
					TableName:    "memberships",
					ColumnName:   "user_id",
					ColumnIndex:  0,
					ColumnType:   catalog.ColumnTypeInt,
				},
			},
		},
	}, allocator)

	if err := join.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := join.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	batches := make([]arrow.RecordBatch, 0)
	for {
		res := join.Next(context.Background())
		if res.Err != nil {
			t.Fatal(res.Err)
		}
		batches = append(batches, res.Batch)
		if !res.HasNext {
			break
		}
	}
	defer func() {
		for _, batch := range batches {
			batch.Release()
		}
	}()

	joined, err := mergeBatches(batches, allocator)
	if err != nil {
		t.Fatal(err)
	}
	defer joined.Release()

	if got, want := joined.NumRows(), int64(2); got != want {
		t.Fatalf("joined row count = %d, want %d", got, want)
	}

	expected := []struct {
		id     int64
		name   string
		userID int64
		team   string
	}{
		{id: 2, name: "bob", userID: 2, team: "blue"},
		{id: 3, name: "carol", userID: 3, team: "green"},
	}

	for i := range expected {
		if got, want := joined.Column(0).(*array.Int64).Value(i), expected[i].id; got != want {
			t.Fatalf("row %d left id = %d, want %d", i, got, want)
		}
		if got, want := joined.Column(1).(*array.String).Value(i), expected[i].name; got != want {
			t.Fatalf("row %d left name = %q, want %q", i, got, want)
		}
		if got, want := joined.Column(2).(*array.Int64).Value(i), expected[i].userID; got != want {
			t.Fatalf("row %d right user_id = %d, want %d", i, got, want)
		}
		if got, want := joined.Column(3).(*array.String).Value(i), expected[i].team; got != want {
			t.Fatalf("row %d right team = %q, want %q", i, got, want)
		}
	}
}

func TestLoopJoinLeftJoinHappyCase(t *testing.T) {
	allocator := memory.NewGoAllocator()

	leftSchema := arrow.NewSchema([]arrow.Field{
		{Name: "rel#1.id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "rel#1.name", Type: arrow.BinaryTypes.String},
	}, nil)
	rightSchema := arrow.NewSchema([]arrow.Field{
		{Name: "rel#2.user_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "rel#2.team", Type: arrow.BinaryTypes.String},
	}, nil)

	leftIDBuilder := array.NewInt64Builder(allocator)
	defer leftIDBuilder.Release()
	leftIDBuilder.AppendValues([]int64{1, 2}, nil)
	leftID := leftIDBuilder.NewArray()
	defer leftID.Release()

	leftNameBuilder := array.NewStringBuilder(allocator)
	defer leftNameBuilder.Release()
	leftNameBuilder.AppendValues([]string{"alice", "bob"}, nil)
	leftName := leftNameBuilder.NewArray()
	defer leftName.Release()

	rightUserIDBuilder := array.NewInt64Builder(allocator)
	defer rightUserIDBuilder.Release()
	rightUserIDBuilder.AppendValues([]int64{2}, nil)
	rightUserID := rightUserIDBuilder.NewArray()
	defer rightUserID.Release()

	rightTeamBuilder := array.NewStringBuilder(allocator)
	defer rightTeamBuilder.Release()
	rightTeamBuilder.AppendValues([]string{"blue"}, nil)
	rightTeam := rightTeamBuilder.NewArray()
	defer rightTeam.Release()

	leftBatch := array.NewRecordBatch(leftSchema, []arrow.Array{leftID, leftName}, int64(leftID.Len()))
	rightBatch := array.NewRecordBatch(rightSchema, []arrow.Array{rightUserID, rightTeam}, int64(rightUserID.Len()))

	left := newTestIterator(allocator, leftBatch, catalog.Schema{
		Columns: []catalog.Column{
			{Name: "id", Type: catalog.ColumnTypeInt},
			{Name: "name", Type: catalog.ColumnTypeString},
		},
	})
	right := newTestIterator(allocator, rightBatch, catalog.Schema{
		Columns: []catalog.Column{
			{Name: "user_id", Type: catalog.ColumnTypeInt},
			{Name: "team", Type: catalog.ColumnTypeString},
		},
	})

	join := newLoopJoin(left, right, &logical.Join{
		Type: ast.JoinTypeLeftJoin,
		On: &logical.ExprPredicate{
			Expr: &logical.BinaryExpr{
				Op: ast.BinaryOpEqual,
				Left: &logical.ColumnRef{
					RelationID:   "rel#1",
					RelationName: "users",
					TableName:    "users",
					ColumnName:   "id",
					ColumnIndex:  0,
					ColumnType:   catalog.ColumnTypeInt,
				},
				Right: &logical.ColumnRef{
					RelationID:   "rel#2",
					RelationName: "memberships",
					TableName:    "memberships",
					ColumnName:   "user_id",
					ColumnIndex:  0,
					ColumnType:   catalog.ColumnTypeInt,
				},
			},
		},
	}, allocator)

	if err := join.Open(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := join.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	batches := make([]arrow.RecordBatch, 0)
	for {
		res := join.Next(context.Background())
		if res.Err != nil {
			t.Fatal(res.Err)
		}
		batches = append(batches, res.Batch)
		if !res.HasNext {
			break
		}
	}
	defer func() {
		for _, batch := range batches {
			batch.Release()
		}
	}()

	joined, err := mergeBatches(batches, allocator)
	if err != nil {
		t.Fatal(err)
	}
	defer joined.Release()

	if got, want := joined.NumRows(), int64(2); got != want {
		t.Fatalf("joined row count = %d, want %d", got, want)
	}

	if got, want := joined.Column(0).(*array.Int64).Value(0), int64(1); got != want {
		t.Fatalf("row 0 left id = %d, want %d", got, want)
	}
	if got, want := joined.Column(1).(*array.String).Value(0), "alice"; got != want {
		t.Fatalf("row 0 left name = %q, want %q", got, want)
	}
	if !joined.Column(2).IsNull(0) {
		t.Fatal("row 0 right user_id should be null")
	}
	if !joined.Column(3).IsNull(0) {
		t.Fatal("row 0 right team should be null")
	}

	if got, want := joined.Column(0).(*array.Int64).Value(1), int64(2); got != want {
		t.Fatalf("row 1 left id = %d, want %d", got, want)
	}
	if got, want := joined.Column(1).(*array.String).Value(1), "bob"; got != want {
		t.Fatalf("row 1 left name = %q, want %q", got, want)
	}
	if got, want := joined.Column(2).(*array.Int64).Value(1), int64(2); got != want {
		t.Fatalf("row 1 right user_id = %d, want %d", got, want)
	}
	if got, want := joined.Column(3).(*array.String).Value(1), "blue"; got != want {
		t.Fatalf("row 1 right team = %q, want %q", got, want)
	}
}
