package physical

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical"
)

type filter struct {
	allocator       memory.Allocator
	input           Iterator
	searchCondition logical.SearchCondition
	//selectExprs
	output catalog.Schema
}

func newFilter(input Iterator, node *logical.Filter, allocator memory.Allocator) *filter {
	return &filter{
		allocator:       allocator,
		input:           input,
		searchCondition: node.Predicate,
		output:          node.Schema(),
	}
}

func (f *filter) Next(ctx context.Context) NextResponse {
	innerRes := f.input.Next(ctx)
	if innerRes.Err != nil {
		return NextResponse{Err: innerRes.Err}
	}

	defer innerRes.Batch.Release()
	eval := newEvaluator(f.allocator)

	mask, err := eval.evaluateSearchCondition(f.searchCondition, innerRes.Batch)
	if err != nil {
		return NextResponse{Err: err}
	}

	filtered, err := compute.FilterRecordBatch(
		ctx,
		innerRes.Batch,
		mask,
		compute.DefaultFilterOptions(),
	)
	if err != nil {
		return NextResponse{Err: err}
	}

	return NextResponse{
		Batch:   filtered,
		Err:     nil,
		HasNext: innerRes.HasNext,
	}
}

func (f *filter) Schema() *catalog.Schema {
	return &f.output
}

// select * from objects where a = b

// Open() error
// Next() NextResponse
// Close() error
// Schema() *catalog.Schema
// how to do filter?
func foo(batch arrow.RecordBatch, filter *logical.Filter) {
	//predicate := filter.Predicate
	eval := newEvaluator(memory.NewGoAllocator())
	mask, err := eval.evaluateSearchCondition(filter.Predicate, batch)
	if err != nil {
		panic(err)
	}
	filtered, err := compute.FilterRecordBatch(
		context.TODO(),
		batch,
		mask,
		compute.DefaultFilterOptions(),
	)
	if err != nil {
		panic(err)
	}
	defer filtered.Release()

}

func debugArray(ary *array.Boolean) {
	//var sb strings.Builder
	res := make([]bool, 0)
	for i := 0; i < ary.Len(); i++ {
		res = append(res, ary.Value(i))
	}
	fmt.Println(res)
}
