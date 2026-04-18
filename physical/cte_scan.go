package physical

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical"
)

type cteScan struct {
	allocator      memory.Allocator
	node           *logical.Scan
	executionState *ExecutionState
	index          int
}

func newScan(node *logical.Scan, executionState *ExecutionState, allocator memory.Allocator) *cteScan {
	return &cteScan{
		allocator:      allocator,
		node:           node,
		executionState: executionState,
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
	cteName := s.node.CTE.Name
	cte, ok := s.executionState.GetCTE(cteName)
	if !ok {
		return NextResponse{
			Err: fmt.Errorf("unable to find CTE %q", cteName),
		}
	}
	arrowSchema := s.arrowSchema()
	if s.index >= len(cte.records) {
		return NextResponse{
			Batch:   array.NewRecordBatch(arrowSchema, nil, 0),
			Err:     nil,
			HasNext: false,
		}
	}
	b := cte.records[s.index]
	if len(arrowSchema.Fields()) != len(b.Schema().Fields()) {
		panic(fmt.Errorf("expected %d columns, got %d", len(arrowSchema.Fields()), len(b.Schema().Fields())))
	}

	batch := array.NewRecordBatch(arrowSchema, b.Columns(), b.NumRows())
	s.index++

	return NextResponse{
		Batch:   batch,
		Err:     nil,
		HasNext: s.index < len(cte.records),
	}
}

func (s *cteScan) arrowSchema() *arrow.Schema {
	columns := s.Schema().Columns
	fields := make([]arrow.Field, len(columns))
	prefix := s.node.RelationID + "."
	for i, col := range columns {

		fields[i] = arrow.Field{
			Name:     prefix + col.Name,
			Type:     toDataType(col.Type),
			Nullable: true,
		}
	}
	return arrow.NewSchema(fields, nil)
}

func (s *cteScan) Schema() *catalog.Schema {
	schema := s.node.Schema()
	return &schema
}
