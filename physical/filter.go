package physical

import (
	"context"

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

func (f *filter) Open() error {
	return f.input.Open()
}

func (f *filter) Close() error {
	return f.input.Close()
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
	defer mask.Release()

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
