package function

import (
	"fmt"

	"github.com/ocowchun/sq/catalog"
)

type Value struct {
	Value     any
	ValueType catalog.ColumnType
	IsNull    bool
}

func NewValue(value any, valueType catalog.ColumnType, isNull bool) *Value {
	return &Value{
		Value:     value,
		ValueType: valueType,
		IsNull:    isNull,
	}
}

type Function interface {
	Input() []catalog.ColumnType
	Output() catalog.ColumnType
	Run(args []*Value) (*Value, error)
}

func extractString(args []*Value, index int) (string, error) {
	if index >= len(args) {
		return "", fmt.Errorf("index %d out of range", index)
	}

	str, ok := args[index].Value.(string)
	if !ok {
		return "", fmt.Errorf("expected a string for argument[%d]", index)
	}
	return str, nil
}

func extractInt(args []*Value, index int) (int64, error) {
	if index >= len(args) {
		return 0, fmt.Errorf("index %d out of range", index)
	}

	num, ok := args[index].Value.(int64)
	if !ok {
		return 0, fmt.Errorf("expected a int for argument[%d]", index)
	}
	return num, nil
}

type env struct {
	functions map[string]Function
}

var _env = &env{
	functions: make(map[string]Function),
}

func GetFunction(name string) (Function, bool) {
	fun, ok := _env.functions[name]
	return fun, ok
}

func init() {
	_env.functions["split_part"] = &splitPart{}
	_env.functions["replace"] = &replace{}
	_env.functions["length"] = &length{}
	_env.functions["lower"] = &lower{}
	_env.functions["upper"] = &upper{}
}
