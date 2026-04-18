package physical

import (
	"context"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical"
)

type testIterator struct {
	allocator memory.Allocator
	batch     arrow.RecordBatch
	schema    catalog.Schema
	current   int64
}

func newTestIterator(allocator memory.Allocator, batch arrow.RecordBatch, schema catalog.Schema) *testIterator {
	return &testIterator{
		allocator: allocator,
		batch:     batch,
		schema:    schema,
		current:   0,
	}
}

func (i *testIterator) Open() error {
	return nil
}

func (i *testIterator) Close() error {
	if i.batch != nil {
		i.batch.Release()
	}
	return nil
}

func (i *testIterator) Next(ctx context.Context) NextResponse {
	if i.current >= i.batch.NumRows() {
		slice := i.batch.NewSlice(i.current, i.current)
		return NextResponse{
			Batch:   slice,
			Err:     nil,
			HasNext: false,
		}
	}

	slice := i.batch.NewSlice(i.current, i.current+1)
	i.current++

	return NextResponse{
		Batch:   slice,
		Err:     nil,
		HasNext: i.current < i.batch.NumRows(),
	}
}

func (i *testIterator) Schema() *catalog.Schema {
	return &i.schema
}

func Test_OrderBy(t *testing.T) {
	allocator := memory.NewGoAllocator()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "rel#1.columnA", Type: arrow.PrimitiveTypes.Int64},
		{Name: "rel#1.columnB", Type: arrow.BinaryTypes.String},
	}, nil)

	bA := array.NewInt64Builder(allocator)
	defer bA.Release()
	bA.AppendValues([]int64{5, 12, 8, 20, 11, 12}, nil)
	colA := bA.NewArray()
	defer colA.Release()

	bB := array.NewStringBuilder(allocator)
	defer bB.Release()
	bB.AppendValues([]string{"a", "b", "c", "d", "e", "a"}, nil)
	colB := bB.NewArray()
	defer colB.Release()

	batch := array.NewRecordBatch(arrowSchema, []arrow.Array{colA, colB}, int64(colA.Len()))

	input := newTestIterator(allocator, batch, catalog.Schema{
		Columns: []catalog.Column{
			{Name: "columnA", Type: catalog.ColumnTypeInt},
			{Name: "columnB", Type: catalog.ColumnTypeString},
		},
	})

	orderings := []logical.Ordering{
		{
			Expr: &logical.ColumnRef{
				RelationID:  "rel#1",
				ColumnName:  "columnA",
				ColumnIndex: 0,
				ColumnType:  catalog.ColumnTypeInt,
			},
		},
		{
			Expr: &logical.ColumnRef{
				RelationID:  "rel#1",
				ColumnName:  "columnB",
				ColumnIndex: 0,
				ColumnType:  catalog.ColumnTypeString,
			},
		},
	}
	o := newOrderBy(input, orderings, allocator)

	err := o.Open()
	if err != nil {
		t.Fatal(err)
	}

	batches := make([]arrow.RecordBatch, 0)
	for {
		innerRes := o.Next(context.Background())
		if innerRes.Err != nil {
			t.Fatal(innerRes.Err)
		}

		batches = append(batches, innerRes.Batch)

		if !innerRes.HasNext {
			break
		}
	}

	sorted, err := mergeBatches(batches, allocator)
	if err != nil {
		t.Fatal(err)
	}

	if sorted.NumRows() != batch.NumRows() {
		t.Fatalf("wrong number of rows returned: %d != %d", sorted.NumRows(), batch.NumRows())
	}

	expectedItems := []struct {
		columnA int64
		columnB string
	}{
		{columnA: 5, columnB: "a"},
		{columnA: 8, columnB: "c"},
		{columnA: 11, columnB: "e"},
		{columnA: 12, columnB: "a"},
		{columnA: 12, columnB: "b"},
		{columnA: 20, columnB: "d"},
	}
	for i := int64(0); i < sorted.NumRows(); i++ {
		expectedItem := expectedItems[i]
		actualColumnA := sorted.Column(0).(*array.Int64).Value(int(i))
		if actualColumnA != expectedItem.columnA {
			t.Fatalf("unexpected value in columnA at row %d: actual %d != expected %d", i, actualColumnA, expectedItem.columnA)
		}
		actualColumnB := sorted.Column(1).(*array.String).Value(int(i))
		if actualColumnB != expectedItem.columnB {
			t.Fatalf("unexpected value in columnB at row %d: actual %s != expected %s", i, actualColumnB, expectedItem.columnB)
		}
	}
}
