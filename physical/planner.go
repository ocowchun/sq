package physical

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical"
)

type Plan struct {
	CTESetupTasks  []*CTESetupTask
	Iterator       Iterator
	ExecutionState *ExecutionState
}

type CTESetupTask struct {
	Name     string
	Iterator Iterator
	Schema   catalog.Schema
}

func BuildPlan(logicalPlan logical.Node, allocator memory.Allocator) (*Plan, error) {
	state := newExecutionState()

	return buildIterator(logicalPlan, state, allocator)
}

func buildIterator(logicalPlan logical.Node, state *ExecutionState, allocator memory.Allocator) (*Plan, error) {
	switch node := logicalPlan.(type) {
	case *logical.Statement:
		tasks := make([]*CTESetupTask, 0, len(node.CTEs))
		for _, cte := range node.CTEs {
			subPlan, err := buildIterator(cte.Query, state, allocator)
			if err != nil {
				return nil, err
			}
			if len(subPlan.CTESetupTasks) > 0 {
				tasks = append(tasks, subPlan.CTESetupTasks...)
			}
			task := &CTESetupTask{
				Name:     cte.Name,
				Iterator: subPlan.Iterator,
				Schema:   cte.Schema,
			}
			tasks = append(tasks, task)
		}
		subPlan, err := buildIterator(node.Root, state, allocator)
		if err != nil {
			return nil, err
		}
		if len(subPlan.CTESetupTasks) > 0 {
			tasks = append(tasks, subPlan.CTESetupTasks...)
		}

		return &Plan{
			CTESetupTasks:  tasks,
			Iterator:       subPlan.Iterator,
			ExecutionState: state,
		}, nil
	case *logical.Scan:
		return &Plan{
			CTESetupTasks:  make([]*CTESetupTask, 0),
			Iterator:       newScan(node, state, allocator),
			ExecutionState: state,
		}, nil
	case *logical.S3ObjectScan:
		return &Plan{
			CTESetupTasks:  make([]*CTESetupTask, 0),
			Iterator:       newS3ObjectScan(node, allocator),
			ExecutionState: state,
		}, nil
	case *logical.Filter:
		subPlan, err := buildIterator(node.Input, state, allocator)
		if err != nil {
			return nil, err
		}

		return &Plan{
			CTESetupTasks:  subPlan.CTESetupTasks,
			Iterator:       newFilter(subPlan.Iterator, node, allocator),
			ExecutionState: state,
		}, nil
	case *logical.Project:
		subPlan, err := buildIterator(node.Input, state, allocator)
		if err != nil {
			return nil, err
		}

		return &Plan{
			CTESetupTasks:  subPlan.CTESetupTasks,
			Iterator:       newProject(subPlan.Iterator, node, allocator),
			ExecutionState: state,
		}, nil
	case *logical.Join:
		left, err := buildIterator(node.Left, state, allocator)
		if err != nil {
			return nil, err
		}
		right, err := buildIterator(node.Right, state, allocator)
		if err != nil {
			return nil, err
		}
		cteSetupTasks := left.CTESetupTasks
		cteSetupTasks = append(cteSetupTasks, right.CTESetupTasks...)
		return &Plan{
			CTESetupTasks: cteSetupTasks,
			Iterator: newLoopJoin(
				left.Iterator,
				right.Iterator,
				node,
				allocator,
			),
			ExecutionState: state,
		}, nil
	case *logical.Limit:
		subPlan, err := buildIterator(node.Input, state, allocator)
		if err != nil {
			return nil, err
		}
		return &Plan{
			CTESetupTasks:  subPlan.CTESetupTasks,
			Iterator:       newLimit(subPlan.Iterator, node.Count, allocator),
			ExecutionState: state,
		}, nil
	case *logical.OrderBy:
		subPlan, err := buildIterator(node.Input, state, allocator)
		if err != nil {
			return nil, err
		}
		return &Plan{
			CTESetupTasks:  subPlan.CTESetupTasks,
			Iterator:       newOrderBy(subPlan.Iterator, node.Orderings, allocator),
			ExecutionState: state,
		}, nil
	default:
		return nil, fmt.Errorf("unsupported plan type: %T", node)
	}
}
