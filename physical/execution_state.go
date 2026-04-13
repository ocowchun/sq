package physical

import (
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/ocowchun/sq/catalog"
)

type ExecutionState struct {
	ctes     map[string]*CTE
	isClosed bool
}

func newExecutionState() *ExecutionState {
	return &ExecutionState{ctes: make(map[string]*CTE), isClosed: false}
}

type CTE struct {
	schema  catalog.Schema
	records []arrow.RecordBatch
}

func (s *ExecutionState) GetCTE(name string) (*CTE, bool) {
	if s.isClosed {
		panic("execution state is closed")
	}

	cte, ok := s.ctes[name]
	return cte, ok
}

func (s *ExecutionState) RegisterCTE(name string, schema catalog.Schema, records []arrow.RecordBatch) error {
	if s.isClosed {
		panic("execution state is closed")
	}

	if _, ok := s.GetCTE(name); ok {
		return fmt.Errorf("CTE '%s' already registered", name)
	}
	cte := &CTE{schema: schema, records: records}
	s.ctes[name] = cte
	return nil
}
func (s *ExecutionState) Close() error {
	s.isClosed = true

	for _, cte := range s.ctes {
		for _, batch := range cte.records {
			batch.Release()
		}
	}
	return nil
}
