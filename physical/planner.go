package physical

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/logical"
)

func BuildPlan(logicalPlan logical.Node, allocator memory.Allocator) (Iterator, error) {
	return buildIterator(logicalPlan, allocator)
}

func buildIterator(plan logical.Node, allocator memory.Allocator) (Iterator, error) {
	switch node := plan.(type) {
	case *logical.Scan:
		panic("implement me")
	case *logical.S3ObjectScan:
		return newS3ObjectScan(node, allocator), nil
	case *logical.Filter:
		input, err := buildIterator(node.Input, allocator)
		if err != nil {
			return nil, err
		}

		return newFilter(input, node, allocator), nil
	case *logical.Project:
		input, err := buildIterator(node.Input, allocator)
		if err != nil {
			return nil, err
		}

		return newProject(input, node, allocator), nil
	case *logical.Join:
		panic("implement me")
	default:
		return nil, fmt.Errorf("unsupported plan type: %T", plan)
	}
}
