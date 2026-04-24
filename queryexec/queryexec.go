package queryexec

import (
	"context"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical"
	"github.com/ocowchun/sq/physical"
)

type QueryExec struct {
	catalog   *catalog.Catalog
	allocator memory.Allocator
	awsConfig aws.Config
}

func New(awsConfig aws.Config) *QueryExec {
	return &QueryExec{
		catalog:   catalog.New(),
		allocator: memory.NewGoAllocator(),
		awsConfig: awsConfig,
	}
}

func (e *QueryExec) Query(ctx context.Context, sql string) (*Iterator, error) {
	logicalPlan, err := logical.BuildLogicalOptimizedPlan(e.catalog, sql)
	if err != nil {
		return nil, err
	}

	physicalPlan, err := physical.BuildPlan(logicalPlan, e.allocator, e.awsConfig)
	if err != nil {
		return nil, err
	}

	state := physicalPlan.ExecutionState
	iter := &Iterator{
		inner:   physicalPlan.Iterator,
		hasNext: true,
		state:   state,
	}

	for _, task := range physicalPlan.CTESetupTasks {
		err = e.runCTESetupTask(ctx, task, state)
		if err != nil {
			iter.Close()
			return nil, err
		}
	}

	err = iter.inner.Open()
	if err != nil {
		iter.Close()
		return nil, err
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

// Close releases the physical iterator tree and query execution state.
// It may be called during Query setup cleanup if the physical Open path fails
// after partially opening resources.
func (i *Iterator) Close() error {
	err := i.inner.Close()
	i.state.Close()
	return err
}

func (e *QueryExec) runCTESetupTask(ctx context.Context, task *physical.CTESetupTask, state *physical.ExecutionState) error {
	batches := make([]arrow.RecordBatch, 0)
	defer func() {
		for _, batch := range batches {
			batch.Release()
		}
	}()

	err := task.Iterator.Open()
	if err != nil {
		return err
	}
	defer task.Iterator.Close()

	for {
		nextRes := task.Iterator.Next(ctx)
		if nextRes.Err != nil {
			return nextRes.Err
		}
		if nextRes.Batch.NumRows() == 0 {
			nextRes.Batch.Release()
		} else {
			batches = append(batches, nextRes.Batch)
		}

		if !nextRes.HasNext {
			break
		}
	}
	err = state.RegisterCTE(task.Name, task.Schema, batches)
	if err != nil {
		return err
	}
	return nil
}
