package physical

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical"
)

type project struct {
	allocator   memory.Allocator
	input       Iterator
	schema      catalog.Schema
	selectExprs []logical.SelectExpr
}

func newProject(input Iterator, node *logical.Project, allocator memory.Allocator) *project {
	return &project{
		allocator:   allocator,
		input:       input,
		schema:      node.Schema(),
		selectExprs: node.SelectExprs,
	}
}

func (p *project) Open() error {
	return p.input.Open()
}

func (p *project) Close() error {
	return p.input.Close()
}

func toDataType(columnType catalog.ColumnType) arrow.DataType {
	switch columnType {
	case catalog.ColumnTypeInt:
		return arrow.PrimitiveTypes.Int64
	case catalog.ColumnTypeDouble:
		return arrow.PrimitiveTypes.Float64
	case catalog.ColumnTypeString:
		return arrow.BinaryTypes.String
	case catalog.ColumnTypeBool:
		return arrow.FixedWidthTypes.Boolean
	default:
		panic("unhandled column type")
	}
}

func (p *project) Next(ctx context.Context) NextResponse {
	innerRes := p.input.Next(ctx)
	if innerRes.Err != nil {
		return NextResponse{Err: innerRes.Err}
	}
	defer innerRes.Batch.Release()

	fields := make([]arrow.Field, 0, len(p.selectExprs))
	cols := make([]arrow.Array, 0, len(p.selectExprs))
	eval := newEvaluator(p.allocator)
	for i, expr := range p.selectExprs {
		fields = append(fields, arrow.Field{
			Name:     p.schema.Columns[i].Name,
			Type:     toDataType(p.schema.Columns[i].Type),
			Nullable: true,
		})
		exprRes := eval.evaluateExpr(expr.Expr, innerRes.Batch)
		if exprRes.err != nil {
			return NextResponse{Err: exprRes.err}
		}
		cols = append(cols, exprRes.array)
	}

	schema := arrow.NewSchema(fields, nil)
	batch := array.NewRecordBatch(schema, cols, innerRes.Batch.NumRows())

	return NextResponse{
		Batch:   batch,
		Err:     nil,
		HasNext: innerRes.HasNext,
	}
}

func (p *project) Schema() *catalog.Schema {
	return &p.schema
}
