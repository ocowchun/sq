package queryexec

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical"
	"github.com/ocowchun/sq/physical"
)

type QueryExec struct {
	catalog   *catalog.Catalog
	allocator memory.Allocator
}

func New() *QueryExec {
	return &QueryExec{
		catalog:   catalog.New(),
		allocator: memory.NewGoAllocator(),
	}
}

func (e *QueryExec) Query(ctx context.Context, sql string) (*Iterator, error) {
	logicalPlan, err := logical.BuildLogicalOptimizedPlan(e.catalog, sql)
	if err != nil {
		return nil, err
	}

	physicalPlan, err := physical.BuildPlan(logicalPlan, e.allocator)
	if err != nil {
		return nil, err
	}

	state := physicalPlan.ExecutionState
	for _, task := range physicalPlan.CTESetupTasks {
		err = e.runCTESetupTasks(ctx, task, state)
		if err != nil {
			return nil, err
		}
	}

	iter := &Iterator{
		inner:   physicalPlan.Iterator,
		hasNext: true,
		state:   state,
	}
	return iter, nil
}

type Iterator struct {
	inner   physical.Iterator
	hasNext bool
	state   *physical.ExecutionState
}

func (i *Iterator) HasNext() bool {
	return i.hasNext
}

func (i *Iterator) Next(ctx context.Context) (arrow.RecordBatch, error) {
	if !i.hasNext {
		return nil, fmt.Errorf("no more records")
	}
	innerRes := i.inner.Next(ctx)
	if innerRes.Err != nil {
		i.hasNext = false
		return nil, innerRes.Err
	}
	i.hasNext = innerRes.HasNext
	return innerRes.Batch, nil
}

func (i *Iterator) Close() error {

	return nil
}

func (e *QueryExec) runCTESetupTasks(ctx context.Context, task *physical.CTESetupTask, state *physical.ExecutionState) error {
	// TODO: clean batch when error
	batches := make([]arrow.RecordBatch, 0)
	for {
		nextRes := task.Iterator.Next(ctx)
		if nextRes.Err != nil {
			return nextRes.Err
		}
		if nextRes.Batch.NumRows() > 0 {
			batches = append(batches, nextRes.Batch)
		}
		if !nextRes.HasNext {
			break
		}
	}
	err := state.RegisterCTE(task.Name, task.Schema, batches)
	if err != nil {
		return err
	}
	return nil
}
