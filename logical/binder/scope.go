package binder

import (
	"fmt"

	"github.com/ocowchun/sq/catalog"
)

type scope struct {
	// key is visibleName
	tables map[string]*scopeTable
	// key is columnName
	columns       map[string][]*scopeColumn
	tablesInOrder []*scopeTable
}

type scopeTable struct {
	name        string
	alias       string
	visibleName string
	schema      catalog.Schema
	source      TableSource
	columns     []*scopeColumn
	accessKind  catalog.AccessKind
}
type TableSource uint8

const (
	TableSourceCatalog TableSource = iota
	TableSourceCTE
)

type scopeColumn struct {
	table      *scopeTable
	name       string
	columnType catalog.ColumnType
	index      int
}

func (column *scopeColumn) ColumnRef() *ColumnRef {
	return &ColumnRef{
		TableName:   column.table.name,
		TableAlias:  column.table.alias,
		ColumnName:  column.name,
		ColumnIndex: column.index,
		ColumnType:  column.columnType,
	}
}

func newScope() *scope {
	return &scope{
		tables:  make(map[string]*scopeTable),
		columns: make(map[string][]*scopeColumn),
	}
}

func (s *scope) addRelation(
	name string,
	schema catalog.Schema,
	alias string,
	source TableSource,
	accessKind catalog.AccessKind,
) (*scopeTable, error) {
	visibleName := name
	if alias != "" {
		visibleName = alias
	}
	if _, ok := s.tables[visibleName]; ok {
		return nil, fmt.Errorf("table %s already exists", visibleName)
	}

	boundTable := &scopeTable{
		name:        name,
		alias:       alias,
		visibleName: visibleName,
		schema:      schema,
		source:      source,
		accessKind:  accessKind,
	}
	s.tables[visibleName] = boundTable
	s.tablesInOrder = append(s.tablesInOrder, boundTable)

	for i, col := range schema.Columns {
		boundColumn := &scopeColumn{
			table:      boundTable,
			name:       col.Name,
			columnType: col.Type,
			index:      i,
		}
		boundTable.columns = append(boundTable.columns, boundColumn)
		s.columns[boundColumn.name] = append(s.columns[boundColumn.name], boundColumn)
	}

	return boundTable, nil
}

func (s *scope) resolveColumn(name string) (*scopeColumn, error) {
	candidate := s.columns[name]
	if len(candidate) == 0 {
		return nil, fmt.Errorf("column %s not found", name)
	} else if len(candidate) > 1 {
		return nil, fmt.Errorf("ambiguous column %s", name)
	}
	return candidate[0], nil
}

func (s *scope) resolveQualified(tableName string, name string) (*scopeColumn, error) {
	table, ok := s.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", name)
	}

	for i, col := range table.schema.Columns {
		if col.Name == name {
			return &scopeColumn{
				table:      table,
				name:       name,
				columnType: col.Type,
				index:      i,
			}, nil
		}
	}

	return nil, fmt.Errorf("unknown column %s on %s", tableName, name)
}

func (s *scope) visibleColumns() []*scopeColumn {
	columns := make([]*scopeColumn, 0, len(s.columns))
	for _, table := range s.tablesInOrder {
		columns = append(columns, table.columns...)
	}
	return columns
}

func (s *scope) tableColumns(tableName string) ([]*scopeColumn, error) {
	table, ok := s.tables[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s not found", tableName)
	}

	return table.columns, nil
}
