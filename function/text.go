package function

import (
	"fmt"
	"strings"

	"github.com/ocowchun/sq/catalog"
)

// split_part(string, separator, index)
type splitPart struct {
}

func (f *splitPart) Input() []catalog.ColumnType {
	return []catalog.ColumnType{
		catalog.ColumnTypeString,
		catalog.ColumnTypeString,
		catalog.ColumnTypeInt,
	}
}
func (f *splitPart) Output() catalog.ColumnType {
	return catalog.ColumnTypeString
}
func (f *splitPart) Run(args []*Value) (*Value, error) {
	str, err := extractString(args, 0)
	if err != nil {
		return nil, err
	}

	separator, err := extractString(args, 1)
	if err != nil {
		return nil, err
	}
	index, err := extractInt(args, 2)
	if err != nil {
		return nil, err
	}
	if index < 0 {
		return nil, fmt.Errorf("index must greater than or equal 0, but get %d", index)
	}

	subStrings := strings.Split(str, separator)
	if int(index) >= len(subStrings) {
		return nil, fmt.Errorf("index out of range")
	}

	ret := &Value{
		Value:     subStrings[int(index)],
		ValueType: catalog.ColumnTypeString,
	}
	return ret, nil
}

// replace(string, source, target)
type replace struct {
}

func (f *replace) Input() []catalog.ColumnType {
	return []catalog.ColumnType{
		catalog.ColumnTypeString,
		catalog.ColumnTypeString,
		catalog.ColumnTypeString,
	}
}

func (f *replace) Output() catalog.ColumnType {
	return catalog.ColumnTypeString
}

func (f *replace) Run(args []*Value) (*Value, error) {
	str, err := extractString(args, 0)
	if err != nil {
		return nil, err
	}

	source, err := extractString(args, 1)
	if err != nil {
		return nil, err
	}

	target, err := extractString(args, 2)
	if err != nil {
		return nil, err
	}

	val := strings.Replace(str, source, target, -1)
	ret := &Value{
		Value:     val,
		ValueType: catalog.ColumnTypeString,
	}
	return ret, nil
}

// length(string)
type length struct {
}

func (f *length) Input() []catalog.ColumnType {
	return []catalog.ColumnType{
		catalog.ColumnTypeString,
	}
}

func (f *length) Output() catalog.ColumnType {
	return catalog.ColumnTypeInt
}

func (f *length) Run(args []*Value) (*Value, error) {
	str, err := extractString(args, 0)
	if err != nil {
		return nil, err
	}

	ret := &Value{
		Value:     int64(len(str)),
		ValueType: catalog.ColumnTypeInt,
	}
	return ret, nil
}

type lower struct {
}

func (f *lower) Input() []catalog.ColumnType {
	return []catalog.ColumnType{
		catalog.ColumnTypeString,
	}
}

func (f *lower) Output() catalog.ColumnType {
	return catalog.ColumnTypeString
}

func (f *lower) Run(args []*Value) (*Value, error) {
	str, err := extractString(args, 0)
	if err != nil {
		return nil, err
	}

	ret := &Value{
		Value:     strings.ToLower(str),
		ValueType: catalog.ColumnTypeString,
	}
	return ret, nil
}

type upper struct {
}

func (f *upper) Input() []catalog.ColumnType {
	return []catalog.ColumnType{
		catalog.ColumnTypeString,
	}
}

func (f *upper) Output() catalog.ColumnType {
	return catalog.ColumnTypeString
}

func (f *upper) Run(args []*Value) (*Value, error) {
	str, err := extractString(args, 0)
	if err != nil {
		return nil, err
	}

	ret := &Value{
		Value:     strings.ToUpper(str),
		ValueType: catalog.ColumnTypeString,
	}
	return ret, nil
}
