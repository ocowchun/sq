package physical

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/ocowchun/sq/catalog"
)

type Iterator interface {
	Next(ctx context.Context) NextResponse
	Schema() *catalog.Schema
}

type NextResponse struct {
	Batch   arrow.RecordBatch
	Err     error
	HasNext bool
}
