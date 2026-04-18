package physical

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical"
)

type orderBy struct {
	allocator memory.Allocator
	input     Iterator
	order     []logical.Order
	start     bool
	sorted    arrow.RecordBatch
	current   int64
}

func newOrderBy(input Iterator, orders []logical.Order, allocator memory.Allocator) *orderBy {
	return &orderBy{
		allocator: allocator,
		input:     input,
		order:     orders,
		start:     false,
		current:   0,
	}
}

func (o *orderBy) Open() error {
	o.start = true
	err := o.input.Open()
	if err != nil {
		return err
	}

	totalBatch, err := o.drain()
	if err != nil {
		return err
	}

	sortCols := make([]arrow.Array, len(o.order))
	eval := newEvaluator(o.allocator)
	for i, expr := range o.order {
		exprRes := eval.evaluateExpr(expr.Expr, totalBatch)
		if exprRes.err != nil {
			return exprRes.err
		}
		sortCols[i] = exprRes.array
	}

	rows := make([]int, totalBatch.NumRows())
	for i := range rows {
		rows[i] = i
	}

	slices.SortStableFunc(rows, func(left int, right int) int {
		for idx, col := range sortCols {
			res := compare(col, o.order[idx], left, right)
			if res != 0 {
				return res
			}
		}
		return 0
	})

	b := array.NewUint64Builder(o.allocator)
	defer b.Release()
	for _, row := range rows {
		b.Append(uint64(row))
	}
	indices := b.NewUint64Array()
	defer indices.Release()

	out, err := compute.Take(
		context.Background(),
		compute.TakeOptions{},
		&compute.RecordDatum{Value: totalBatch},
		&compute.ArrayDatum{Value: indices.Data()},
	)
	if err != nil {
		return err
	}
	defer out.Release()

	sorted := out.(*compute.RecordDatum).Value
	sorted.Retain()
	o.sorted = sorted

	return nil
}

func (o *orderBy) Next(ctx context.Context) NextResponse {
	n := min(o.sorted.NumRows(), o.current+batchSize)
	slice := o.sorted.NewSlice(o.current, n)
	o.current = n

	return NextResponse{
		Batch:   slice,
		Err:     nil,
		HasNext: o.current < o.sorted.NumRows(),
	}
}

func (o *orderBy) Close() error {
	err := o.input.Close()
	if o.sorted != nil {
		o.sorted.Release()
	}

	return err
}

func (o *orderBy) Schema() *catalog.Schema {
	return o.input.Schema()
}

func compare(ary arrow.Array, order logical.Order, left int, right int) int {
	res := 0
	if ary.IsNull(left) && ary.IsNull(right) {
		res = 0
	} else if ary.IsNull(left) {
		res = -1
	} else if ary.IsNull(right) {
		res = 1
	}

	switch order.Expr.Type() {
	case catalog.ColumnTypeInt:
		a := ary.(*array.Int64)
		leftVal := a.Value(left)
		rightVal := a.Value(right)
		if leftVal > rightVal {
			res = 1
		} else if leftVal < rightVal {
			res = -1
		}
	case catalog.ColumnTypeDouble:
		a := ary.(*array.Float64)
		leftVal := a.Value(left)
		rightVal := a.Value(right)
		if leftVal > rightVal {
			res = 1
		} else if leftVal < rightVal {
			res = -1
		}
	case catalog.ColumnTypeString:
		a := ary.(*array.String)
		leftVal := a.Value(left)
		rightVal := a.Value(right)
		res = strings.Compare(leftVal, rightVal)
	case catalog.ColumnTypeBool:
		a := ary.(*array.Boolean)
		leftVal := a.Value(left)
		rightVal := a.Value(right)
		if leftVal == true && rightVal == false {
			res = 1
		} else if leftVal == false && rightVal == true {
			res = -1
		}
	default:
		return 0
	}

	if order.Desc {
		res = -res
	}
	return res
}

func (o *orderBy) drain() (arrow.RecordBatch, error) {
	batches := make([]arrow.RecordBatch, 0)
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()
	ctx := context.Background()
	for {
		innerRes := o.input.Next(ctx)
		if innerRes.Err != nil {
			return nil, innerRes.Err
		}
		batches = append(batches, innerRes.Batch)
		if !innerRes.HasNext {
			break
		}
	}

	merged, err := mergeBatches(batches, o.allocator)
	if err != nil {
		return nil, err
	}
	return merged, nil
}

func mergeBatches(batches []arrow.RecordBatch, allocator memory.Allocator) (arrow.RecordBatch, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches")
	}

	schema := batches[0].Schema()
	numCols := int(batches[0].NumCols())
	cols := make([]arrow.Array, numCols)
	totalRows := int64(0)
	defer func() {
		for _, col := range cols {
			if col != nil {
				col.Release()
			}
		}
	}()

	for _, batch := range batches {
		totalRows += batch.NumRows()
	}
	for i := 0; i < numCols; i++ {
		pieces := make([]arrow.Array, len(batches))
		for j, batch := range batches {
			pieces[j] = batch.Column(i)
		}
		merged, err := array.Concatenate(pieces, allocator)
		if err != nil {
			return nil, err
		}
		cols[i] = merged
	}

	out := array.NewRecordBatch(schema, cols, totalRows)
	return out, nil
}
