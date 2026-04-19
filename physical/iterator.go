package physical

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/catalog"
)

type Iterator interface {
	Open() error
	Close() error
	Next(ctx context.Context) NextResponse
	Schema() *catalog.Schema
}

type NextResponse struct {
	Batch   arrow.RecordBatch
	Err     error
	HasNext bool
}

const batchSize = 100

func drain(iter Iterator, allocator memory.Allocator) (arrow.RecordBatch, error) {
	err := iter.Open()
	if err != nil {
		return nil, err
	}

	batches := make([]arrow.RecordBatch, 0)
	defer func() {
		for _, b := range batches {
			b.Release()
		}
	}()
	ctx := context.Background()
	for {
		innerRes := iter.Next(ctx)
		if innerRes.Err != nil {
			return nil, innerRes.Err
		}
		batches = append(batches, innerRes.Batch)
		if !innerRes.HasNext {
			break
		}
	}

	merged, err := mergeBatches(batches, allocator)
	if err != nil {
		return nil, err
	}
	return merged, nil
}
