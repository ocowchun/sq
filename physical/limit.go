package physical

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/catalog"
)

type limit struct {
	allocator memory.Allocator
	input     Iterator
	count     uint32
	current   uint32
}

func newLimit(input Iterator, count uint32, allocator memory.Allocator) *limit {
	return &limit{
		allocator: allocator,
		input:     input,
		count:     count,
		current:   0,
	}
}

func (l *limit) Open() error {
	return l.input.Open()
}

func (l *limit) Close() error {
	return l.input.Close()
}

func (l *limit) Next(ctx context.Context) NextResponse {
	innerRes := l.input.Next(ctx)
	if innerRes.Err != nil {
		return NextResponse{Err: innerRes.Err}
	}
	defer innerRes.Batch.Release()

	innerRes.Batch.NumRows()
	n := min(innerRes.Batch.NumRows(), int64(l.count-l.current))
	slice := innerRes.Batch.NewSlice(0, n)

	l.count += uint32(n)

	return NextResponse{
		Batch:   slice,
		Err:     nil,
		HasNext: innerRes.HasNext && l.count > l.current,
	}
}

func (l *limit) Schema() *catalog.Schema {
	return l.input.Schema()
}
