package physical

import (
	"context"
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
	orderings []logical.Ordering
	sorted    arrow.RecordBatch
	current   int64
	opened    bool
}

func newOrderBy(input Iterator, orderings []logical.Ordering, allocator memory.Allocator) *orderBy {
	return &orderBy{
		allocator: allocator,
		input:     input,
		orderings: orderings,
		current:   0,
		opened:    false,
	}
}

func (o *orderBy) Open() error {
	if o.opened {
		panic("orderBy already open")
	}
	o.opened = true

	if err := o.input.Open(); err != nil {
		return err
	}

	totalBatch, err := drain(o.input, o.allocator)
	if err != nil {
		return err
	}
	defer totalBatch.Release()

	sortCols := make([]arrow.Array, 0, len(o.orderings))
	defer func() {
		for _, col := range sortCols {
			col.Release()
		}
	}()

	eval := newEvaluator(o.allocator)
	for _, expr := range o.orderings {
		exprRes := eval.evaluateExpr(expr.Expr, totalBatch)
		if exprRes.err != nil {
			return exprRes.err
		}
		sortCols = append(sortCols, exprRes.array)
		//sortCols[i] = exprRes.array
	}

	rows := make([]int, totalBatch.NumRows())
	for i := range rows {
		rows[i] = i
	}

	slices.SortStableFunc(rows, func(left int, right int) int {
		for idx, col := range sortCols {
			res := compare(col, o.orderings[idx], left, right)
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

func compare(ary arrow.Array, ordering logical.Ordering, left int, right int) int {
	res := 0
	if ary.IsNull(left) && ary.IsNull(right) {
		res = 0
	} else if ary.IsNull(left) {
		res = -1
	} else if ary.IsNull(right) {
		res = 1
	}

	switch ordering.Expr.Type() {
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

	if ordering.Desc {
		res = -res
	}
	return res
}
