package physical

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/catalog"
)

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

func mergeBatches(batches []arrow.RecordBatch, allocator memory.Allocator) (arrow.RecordBatch, error) {
	if len(batches) == 0 {
		return nil, fmt.Errorf("no batches")
	}

	schema := batches[0].Schema()
	numCols := int(batches[0].NumCols())
	cols := make([]arrow.Array, numCols)
	totalRows := int64(0)
	defer func() {
		for _, col := range cols {
			if col != nil {
				col.Release()
			}
		}
	}()

	for _, batch := range batches {
		totalRows += batch.NumRows()
	}
	for i := 0; i < numCols; i++ {
		pieces := make([]arrow.Array, len(batches))
		for j, batch := range batches {
			pieces[j] = batch.Column(i)
		}
		merged, err := array.Concatenate(pieces, allocator)
		if err != nil {
			return nil, err
		}
		cols[i] = merged
	}

	out := array.NewRecordBatch(schema, cols, totalRows)
	return out, nil
}

func emptyBatch(schema *arrow.Schema, allocator memory.Allocator) arrow.RecordBatch {
	cols := make([]arrow.Array, 0, len(schema.Fields()))
	defer func() {
		for _, col := range cols {
			col.Release()
		}
	}()
	for _, field := range schema.Fields() {
		builder := array.NewBuilder(allocator, field.Type)
		cols = append(cols, builder.NewArray())
		builder.Release()
	}

	res := array.NewRecordBatch(schema, cols, 0)
	return res
}

func toArrowSchema(schema *catalog.Schema) *arrow.Schema {
	fields := make([]arrow.Field, len(schema.Columns))
	for i, col := range schema.Columns {
		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     toDataType(col.Type),
			Nullable: true,
		}
	}
	return arrow.NewSchema(fields, nil)
}
