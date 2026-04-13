package function

import (
	"testing"

	"github.com/ocowchun/sq/catalog"
)

func Test_SplitPart(t *testing.T) {
	fun, _ := GetFunction("split_part")

	res, err := fun.Run([]*Value{
		{
			Value:     "hello world",
			ValueType: catalog.ColumnTypeString,
			IsNull:    false,
		},
		{
			Value:     " ",
			ValueType: catalog.ColumnTypeString,
			IsNull:    false,
		},
		{
			Value:     int64(1),
			ValueType: catalog.ColumnTypeInt,
			IsNull:    false,
		},
	})

	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	if res.ValueType != fun.Output() {
		t.Errorf("Unexpected ValueType %s, expected %s", res.ValueType, fun.Output())
	}
	val, ok := res.Value.(string)
	if !ok {
		t.Errorf("Unexpected result %v", res)
	}
	if val != "world" {
		t.Errorf("Unexpected result %v", val)
	}
}

func Test_Replace(t *testing.T) {
	fun, _ := GetFunction("replace")

	res, err := fun.Run([]*Value{
		{
			Value:     "hello",
			ValueType: catalog.ColumnTypeString,
			IsNull:    false,
		},
		{
			Value:     "l",
			ValueType: catalog.ColumnTypeString,
			IsNull:    false,
		},
		{
			Value:     "-",
			ValueType: catalog.ColumnTypeString,
			IsNull:    false,
		},
	})

	if err != nil {
		t.Errorf("Unexpected error %v", err)
	}
	if res.ValueType != fun.Output() {
		t.Errorf("Unexpected ValueType %s, expected %s", res.ValueType, fun.Output())
	}
	val, ok := res.Value.(string)
	if !ok {
		t.Errorf("Unexpected result %v, expected: string", res)
	}
	if val != "he--o" {
		t.Errorf("Unexpected result %s, expected: %s", val, "he--o")
	}
}
