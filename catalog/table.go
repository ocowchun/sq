package catalog

import (
	"slices"
)

type ColumnType uint8

const (
	ColumnTypeString ColumnType = iota
	ColumnTypeBool
	ColumnTypeInt
	ColumnTypeDouble
	ColumnTypeDatetime
	ColumnTypeNull
)

func (c ColumnType) String() string {
	switch c {
	case ColumnTypeString:
		return "string"
	case ColumnTypeBool:
		return "bool"
	case ColumnTypeInt:
		return "int"
	case ColumnTypeDouble:
		return "double"
	case ColumnTypeDatetime:
		return "datetime"
	case ColumnTypeNull:
		return "null"
	default:
		return "unknown"
	}
}

func (c ColumnType) IsIn(types ...ColumnType) bool {
	return slices.Contains(types, c)
}

type Column struct {
	Name string
	Type ColumnType
}

type Schema struct {
	Columns []Column
}

type Table struct {
	Name       string
	Schema     Schema
	AccessKind AccessKind
}

type AccessKind uint8

const (
	AccessKindDefault AccessKind = iota
	AccessKindS3Sdk
)
