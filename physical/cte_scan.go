package physical

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical"
)

type cteScan struct {
	allocator memory.Allocator

	cteName        string
	executionState *ExecutionState
	output         catalog.Schema
	index          int
}

func newScan(node *logical.Scan, executionState *ExecutionState, allocator memory.Allocator) *cteScan {
	return &cteScan{
		allocator:      allocator,
		cteName:        node.CTE.Name,
		executionState: executionState,
		output:         node.Schema(),
		index:          0,
	}
}

func (s *cteScan) Open() error {
	return nil
}

func (s *cteScan) Close() error {
	return nil
}

func (s *cteScan) Next(ctx context.Context) NextResponse {
	cte, ok := s.executionState.GetCTE(s.cteName)
	if !ok {
		return NextResponse{
			Err: fmt.Errorf("unable to find CTE %q", s.cteName),
		}
	}
	if s.index >= len(cte.records) {
		return NextResponse{
			Batch:   array.NewRecordBatch(nil, nil, 0),
			Err:     nil,
			HasNext: false,
		}
	}
	batch := cte.records[s.index]
	s.index++
	return NextResponse{
		Batch:   batch,
		Err:     nil,
		HasNext: s.index < len(cte.records),
	}
}

func (s *cteScan) Schema() *catalog.Schema {
	return &s.output
}
