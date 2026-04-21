package physical

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/ast"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical"
)

type loopJoin struct {
	allocator memory.Allocator
	left      Iterator
	right     Iterator
	node      *logical.Join
	joined    arrow.RecordBatch
	current   int64
	opened    bool
}

func newLoopJoin(left Iterator, right Iterator, node *logical.Join, allocator memory.Allocator) *loopJoin {
	return &loopJoin{
		allocator: allocator,
		left:      left,
		right:     right,
		node:      node,
		current:   0,
		opened:    false,
	}
}

func (j *loopJoin) Open() error {
	if j.opened {
		panic("loopJoin already open")
	}

	j.opened = true
	if err := j.left.Open(); err != nil {
		return err
	}
	leftBatch, err := drain(j.left, j.allocator)
	if err != nil {
		return err
	}
	defer leftBatch.Release()

	if err := j.right.Open(); err != nil {
		return err
	}
	rightBatch, err := drain(j.right, j.allocator)
	if err != nil {
		return err
	}
	defer rightBatch.Release()

	batches := make([]arrow.RecordBatch, 0, int(leftBatch.NumRows()))
	defer func() {
		for _, batch := range batches {
			if batch != nil {
				batch.Release()
			}
		}
	}()

	rightNumRows := int(rightBatch.NumRows())
	joinedFields := make([]arrow.Field, 0, leftBatch.NumCols()+rightBatch.NumCols())
	joinedFields = append(joinedFields, leftBatch.Schema().Fields()...)
	joinedFields = append(joinedFields, rightBatch.Schema().Fields()...)
	joinedSchema := arrow.NewSchema(joinedFields, nil)

	// fow now we only support inner join and left join, therefore joined become empty result if leftBatch is empty,
	if leftBatch.NumRows() == 0 {
		j.joined = emptyBatch(joinedSchema, j.allocator)
		return nil
	}

	eval := newEvaluator(j.allocator)

	for leftIndex := 0; leftIndex < int(leftBatch.NumRows()); leftIndex++ {
		err = func() error {
			b, err := times(leftBatch, leftIndex, rightNumRows, j.allocator)
			if err != nil {
				return err
			}
			defer b.Release()

			batch := array.NewRecordBatch(joinedSchema, append(b.Columns(), rightBatch.Columns()...), int64(rightNumRows))
			defer batch.Release()

			mask, err := eval.evaluateSearchCondition(j.node.On, batch)
			if err != nil {
				return err
			}
			defer mask.Release()

			filtered, err := compute.FilterRecordBatch(
				context.Background(),
				batch,
				mask,
				compute.DefaultFilterOptions(),
			)
			if err != nil {
				return err
			}
			defer filtered.Release()

			switch j.node.Type {
			case ast.JoinTypeInnerJoin:
				filtered.Retain()
				batches = append(batches, filtered)
			case ast.JoinTypeLeftJoin:
				if filtered.NumRows() == 0 {
					l, err := times(leftBatch, leftIndex, 1, j.allocator)
					if err != nil {
						return err
					}
					defer l.Release()

					r := newSingleNullBatch(rightBatch, j.allocator)
					defer r.Release()
					batches = append(batches, array.NewRecordBatch(joinedSchema, append(l.Columns(), r.Columns()...), 1))

				} else {
					filtered.Retain()
					batches = append(batches, filtered)
				}
			default:
				panic("unknown joinType")
			}
			return nil
		}()

		if err != nil {
			return err
		}
	}

	joined, err := mergeBatches(batches, j.allocator)
	if err != nil {
		return err
	}
	j.joined = joined

	return nil
}

func newSingleNullBatch(source arrow.RecordBatch, allocator memory.Allocator) arrow.RecordBatch {
	schema := source.Schema()
	columns := make([]arrow.Array, schema.NumFields())
	defer func() {
		for _, column := range columns {
			if column != nil {
				column.Release()
			}
		}
	}()

	for i, f := range schema.Fields() {
		builder := array.NewBuilder(allocator, f.Type)
		builder.AppendNull()
		columns[i] = builder.NewArray()
		builder.Release()
	}

	return array.NewRecordBatch(schema, columns, 1)
}

func times(source arrow.RecordBatch, rowIndex int, targetNumRows int, allocator memory.Allocator) (arrow.RecordBatch, error) {
	indexBuilder := array.NewUint64Builder(allocator)
	defer indexBuilder.Release()

	for i := 0; i < targetNumRows; i++ {
		indexBuilder.Append(uint64(rowIndex))
	}
	indices := indexBuilder.NewUint64Array()
	defer indices.Release()

	taken, err := compute.Take(
		context.Background(),
		compute.TakeOptions{},
		&compute.RecordDatum{Value: source},
		&compute.ArrayDatum{Value: indices.Data()},
	)

	if err != nil {
		return nil, err
	}

	b := taken.(*compute.RecordDatum).Value
	return b, nil
}

func (j *loopJoin) Next(ctx context.Context) NextResponse {
	n := min(j.joined.NumRows(), j.current+batchSize)
	slice := j.joined.NewSlice(j.current, n)
	j.current = n

	return NextResponse{
		Batch:   slice,
		Err:     nil,
		HasNext: j.current < j.joined.NumRows(),
	}
}

func (j *loopJoin) Schema() *catalog.Schema {
	schema := j.node.Schema()
	return &schema
}

func (j *loopJoin) Close() error {
	leftErr := j.left.Close()
	rightErr := j.right.Close()
	if j.joined != nil {
		j.joined.Release()
	}

	if leftErr != nil {
		return leftErr
	}
	return rightErr
}
