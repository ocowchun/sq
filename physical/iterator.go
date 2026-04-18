package physical

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
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
