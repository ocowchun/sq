package physical

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/compute"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/ocowchun/sq/ast"
	"github.com/ocowchun/sq/catalog"
	"github.com/ocowchun/sq/logical"
)

type evaluator struct {
	allocator memory.Allocator
}

func newEvaluator(allocator memory.Allocator) *evaluator {
	return &evaluator{
		allocator: allocator,
	}
}

type EvaluatedResponse struct {
	array        arrow.Array
	responseType catalog.ColumnType
	err          error
}

// Returns a boolean array where false means the row should be filtered out, and true means the row should be kept.
func (e *evaluator) evaluateSearchCondition(condition logical.SearchCondition, batch arrow.RecordBatch) (*array.Boolean, error) {
	switch cond := condition.(type) {
	case *logical.OrSearchCondition:
		left, err := e.evaluateSearchCondition(cond.Left, batch)
		if err != nil {
			return nil, err
		}
		right, err := e.evaluateSearchCondition(cond.Right, batch)
		if err != nil {
			return nil, err
		}
		leftDatum := compute.NewDatum(left)
		defer leftDatum.Release()

		rightDatum := compute.NewDatum(right)
		defer rightDatum.Release()
		out, err := compute.CallFunction(
			context.Background(),
			"or_kleene",
			nil,
			leftDatum,
			rightDatum,
		)
		if err != nil {
			return nil, err
		}
		defer out.Release()

		result := out.(*compute.ArrayDatum).MakeArray()

		return result.(*array.Boolean), nil
	case *logical.AndSearchCondition:
		left, err := e.evaluateSearchCondition(cond.Left, batch)
		if err != nil {
			return nil, err
		}
		right, err := e.evaluateSearchCondition(cond.Right, batch)
		if err != nil {
			return nil, err
		}
		leftDatum := compute.NewDatum(left)
		defer leftDatum.Release()

		rightDatum := compute.NewDatum(right)
		defer rightDatum.Release()

		out, err := compute.CallFunction(
			context.Background(),
			"and_kleene",
			nil,
			leftDatum,
			rightDatum,
		)
		if err != nil {
			return nil, err
		}
		defer out.Release()

		result := out.(*compute.ArrayDatum).MakeArray()

		return result.(*array.Boolean), nil
	case *logical.LikePredicate:
		return e.evalLikePredicate(cond, batch)
	case *logical.IsNullPredicate:
		innerRes := e.evaluateExpr(cond.Expression, batch)
		if innerRes.err != nil {
			return nil, innerRes.err
		}

		datum := compute.NewDatum(innerRes.array)
		defer datum.Release()

		funcName := "is_null"
		if cond.Not {
			funcName = "is_not_null"
		}

		out, err := compute.CallFunction(
			context.Background(),
			funcName,
			nil,
			datum,
		)
		if err != nil {
			return nil, err
		}
		defer out.Release()

		result := out.(*compute.ArrayDatum).MakeArray()

		return result.(*array.Boolean), nil
	case *logical.InPredicate:
		return e.evalInPredicate(cond, batch)
	case *logical.ExprPredicate:
		res := e.evaluateExpr(cond.Expr, batch)
		if res.err != nil {
			return nil, res.err
		}
		return res.array.(*array.Boolean), nil
	default:
		panic("unknown condition type")
	}
}

func (e *evaluator) evalInPredicate(predicate *logical.InPredicate, batch arrow.RecordBatch) (*array.Boolean, error) {
	innerRes := e.evaluateExpr(predicate.Left, batch)
	if innerRes.err != nil {
		return nil, innerRes.err
	}

	//slices.Contains()

	switch predicate.Left.Type() {
	case catalog.ColumnTypeInt:
		ints := make([]int64, 0, len(predicate.Exprs))
		for _, expr := range predicate.Exprs {
			ints = append(ints, expr.Value.(int64))
		}

		ary := innerRes.array.(*array.Int64)
		b1 := array.NewBooleanBuilder(e.allocator)
		defer b1.Release()
		isNotLike := predicate.Not
		for i := 0; i < innerRes.array.Len(); i++ {
			if ary.IsNull(i) {
				b1.AppendNull()
				continue
			}

			b1.Append(slices.Contains(ints, ary.Value(i)) != isNotLike)
		}

		a1 := b1.NewArray().(*array.Boolean)
		return a1, nil
	case catalog.ColumnTypeBool:
		bs := make([]bool, 0, len(predicate.Exprs))
		for _, expr := range predicate.Exprs {
			bs = append(bs, expr.Value.(bool))
		}

		ary := innerRes.array.(*array.Boolean)
		b1 := array.NewBooleanBuilder(e.allocator)
		defer b1.Release()
		isNotLike := predicate.Not
		for i := 0; i < innerRes.array.Len(); i++ {
			if ary.IsNull(i) {
				b1.AppendNull()
				continue
			}

			b1.Append(slices.Contains(bs, ary.Value(i)) != isNotLike)
		}

		a1 := b1.NewArray().(*array.Boolean)
		return a1, nil
	case catalog.ColumnTypeString:
		strs := make([]string, 0, len(predicate.Exprs))
		for _, expr := range predicate.Exprs {
			strs = append(strs, expr.Value.(string))
		}

		ary := innerRes.array.(*array.String)
		b1 := array.NewBooleanBuilder(e.allocator)
		defer b1.Release()
		isNotLike := predicate.Not
		for i := 0; i < innerRes.array.Len(); i++ {
			if ary.IsNull(i) {
				b1.AppendNull()
				continue
			}

			b1.Append(slices.Contains(strs, ary.Value(i)) != isNotLike)
		}

		a1 := b1.NewArray().(*array.Boolean)
		return a1, nil

	default:
		return nil, fmt.Errorf("in predicate doesn't support column type: %v", predicate.Left.Type())
	}
}

func (e *evaluator) evalLikePredicate(predicate *logical.LikePredicate, batch arrow.RecordBatch) (*array.Boolean, error) {
	innerRes := e.evaluateExpr(predicate.Left, batch)
	if innerRes.err != nil {
		return nil, innerRes.err
	}

	strs := innerRes.array.(*array.String)
	b1 := array.NewBooleanBuilder(e.allocator)
	defer b1.Release()
	isNotLike := predicate.Not
	for i := 0; i < strs.Len(); i++ {
		// TODO: how to handle not like + null case
		if strs.IsNull(i) {
			b1.AppendNull()
			continue
		}

		matched := matchPattern(strs.Value(i), predicate.Pattern)
		b1.Append(matched != isNotLike)
	}

	a1 := b1.NewArray().(*array.Boolean)
	return a1, nil
}

func matchPattern(source string, pattern string) bool {
	// TODO: only support %xxx, %xxx%, xxx% now
	if pattern[0] == '%' {
		if pattern[len(pattern)-1] == '%' {
			return strings.Contains(source, pattern[1:len(pattern)-1])
		}
		return strings.HasSuffix(source, pattern[1:])
	} else if pattern[len(pattern)-1] == '%' {
		return strings.HasPrefix(source, pattern[:len(pattern)-1])
	}
	return source == pattern
}

func (e *evaluator) evaluateExpr(expr logical.Expr, batch arrow.RecordBatch) EvaluatedResponse {
	switch exp := expr.(type) {
	case *logical.LiteralExpr:
		return e.evaluateLiteralExpr(exp, batch)
	case *logical.ColumnRef:
		return e.evaluateColumnRef(exp, batch)
	case *logical.UnaryExpr:
		return e.evaluateUnaryExpr(exp, batch)
	case *logical.BinaryExpr:
		return e.evaluateBinaryExpr(exp, batch)
	default:
		panic("unreachable")
	}
}

func (e *evaluator) evaluateBinaryExpr(expr *logical.BinaryExpr, batch arrow.RecordBatch) EvaluatedResponse {
	leftRes := e.evaluateExpr(expr.Left, batch)
	if leftRes.err != nil {
		return EvaluatedResponse{
			err: leftRes.err,
		}
	}
	rightRes := e.evaluateExpr(expr.Right, batch)
	if rightRes.err != nil {
		return EvaluatedResponse{
			err: rightRes.err,
		}
	}

	if expr.Left.Type().IsIn(catalog.ColumnTypeString) && expr.Right.Type().IsIn(catalog.ColumnTypeString) && expr.Op.IsIn(ast.BinaryOpAdd) {
		return e.evaluateAddString(leftRes.array.(*array.String), rightRes.array.(*array.String))
	}

	left := compute.NewDatum(leftRes.array)
	defer left.Release()

	right := compute.NewDatum(rightRes.array)
	defer right.Release()

	if expr.Left.Type().IsIn(catalog.ColumnTypeInt, catalog.ColumnTypeDouble) && expr.Right.Type().IsIn(catalog.ColumnTypeInt, catalog.ColumnTypeDouble) {
		return e.evaluateNumericBinary(expr, left, right, expr.Op)
	} else if expr.Left.Type() == expr.Right.Type() {
		switch expr.Left.Type() {
		case catalog.ColumnTypeBool:
			return e.evaluateEqualBinary(left, right, expr.Op)
		case catalog.ColumnTypeString:
			return e.evaluateEqualBinary(left, right, expr.Op)
		case catalog.ColumnTypeDatetime:
			panic("not implemented")
		default:
			panic("unreachable")
		}

	} else {
		//// TODO: check binder reject invalid BinaryExpr
		panic("unreachable")

	}
}

func (e *evaluator) evaluateAddString(left *array.String, right *array.String) EvaluatedResponse {
	builder := array.NewStringBuilder(e.allocator)
	defer builder.Release()
	for i := 0; i < left.Len(); i++ {
		if left.IsNull(i) || right.IsNull(i) {
			builder.AppendNull()
		} else {
			val := left.Value(i) + right.Value(i)
			builder.Append(val)
		}
	}

	return EvaluatedResponse{
		array:        builder.NewArray(),
		responseType: catalog.ColumnTypeString,
		err:          nil,
	}
}

func (e *evaluator) evaluateEqualBinary(left compute.Datum, right compute.Datum, op ast.BinaryOp) EvaluatedResponse {
	ctx := context.Background()
	switch op {
	case ast.BinaryOpEqual:
		out, err := compute.CallFunction(ctx, "equal", nil, left, right)
		if err != nil {
			return EvaluatedResponse{
				err: err,
			}
		}
		defer out.Release()

		arr := out.(*compute.ArrayDatum).MakeArray()
		return EvaluatedResponse{
			array:        arr,
			responseType: catalog.ColumnTypeBool,
			err:          nil,
		}
	case ast.BinaryOpNotEqual:
		out, err := compute.CallFunction(ctx, "not_equal", nil, left, right)
		if err != nil {
			return EvaluatedResponse{
				err: err,
			}
		}
		defer out.Release()

		arr := out.(*compute.ArrayDatum).MakeArray()
		return EvaluatedResponse{
			array:        arr,
			responseType: catalog.ColumnTypeBool,
			err:          nil,
		}
	default:
		panic("unreachable")
	}
}

func (e *evaluator) evaluateNumericBinary(expr *logical.BinaryExpr, left compute.Datum, right compute.Datum, op ast.BinaryOp) EvaluatedResponse {
	ctx := context.Background()
	responseType := expr.Left.Type()
	if expr.Op.IsIn(ast.BinaryOpEqual, ast.BinaryOpNotEqual, ast.BinaryOpGreater, ast.BinaryOpGreaterEqual, ast.BinaryOpLess, ast.BinaryOpLessEqual) {
		responseType = catalog.ColumnTypeBool
	} else if responseType != expr.Right.Type() {
		responseType = catalog.ColumnTypeDouble
	}

	switch expr.Op {
	case ast.BinaryOpAdd:
		out, err := compute.Add(ctx, compute.ArithmeticOptions{}, left, right)
		if err != nil {
			return EvaluatedResponse{
				err: err,
			}
		}
		defer out.Release()

		arr := out.(*compute.ArrayDatum).MakeArray()
		return EvaluatedResponse{
			array:        arr,
			responseType: responseType,
			err:          nil,
		}
	case ast.BinaryOpSub:
		out, err := compute.Subtract(ctx, compute.ArithmeticOptions{}, left, right)
		if err != nil {
			return EvaluatedResponse{
				err: err,
			}
		}
		defer out.Release()

		arr := out.(*compute.ArrayDatum).MakeArray()
		return EvaluatedResponse{
			array:        arr,
			responseType: responseType,
			err:          nil,
		}
	case ast.BinaryOpMul:
		out, err := compute.Multiply(ctx, compute.ArithmeticOptions{}, left, right)
		if err != nil {
			return EvaluatedResponse{
				err: err,
			}
		}
		defer out.Release()

		arr := out.(*compute.ArrayDatum).MakeArray()
		return EvaluatedResponse{
			array:        arr,
			responseType: responseType,
			err:          nil,
		}
	case ast.BinaryOpDiv:
		out, err := compute.Divide(ctx, compute.ArithmeticOptions{}, left, right)
		if err != nil {
			return EvaluatedResponse{
				err: err,
			}
		}
		defer out.Release()

		arr := out.(*compute.ArrayDatum).MakeArray()
		return EvaluatedResponse{
			array:        arr,
			responseType: responseType,
			err:          nil,
		}
	case ast.BinaryOpEqual:
		out, err := compute.CallFunction(ctx, "equal", nil, left, right)
		if err != nil {
			return EvaluatedResponse{
				err: err,
			}
		}
		defer out.Release()

		arr := out.(*compute.ArrayDatum).MakeArray()
		return EvaluatedResponse{
			array:        arr,
			responseType: responseType,
			err:          nil,
		}
	case ast.BinaryOpNotEqual:
		out, err := compute.CallFunction(ctx, "not_equal", nil, left, right)
		if err != nil {
			return EvaluatedResponse{
				err: err,
			}
		}
		defer out.Release()

		arr := out.(*compute.ArrayDatum).MakeArray()
		return EvaluatedResponse{
			array:        arr,
			responseType: responseType,
			err:          nil,
		}
	case ast.BinaryOpGreater:
		out, err := compute.CallFunction(ctx, "greater", nil, left, right)
		if err != nil {
			return EvaluatedResponse{
				err: err,
			}
		}
		defer out.Release()

		arr := out.(*compute.ArrayDatum).MakeArray()
		return EvaluatedResponse{
			array:        arr,
			responseType: responseType,
			err:          nil,
		}
	case ast.BinaryOpGreaterEqual:
		out, err := compute.CallFunction(ctx, "greater_equal", nil, left, right)
		if err != nil {
			return EvaluatedResponse{
				err: err,
			}
		}
		defer out.Release()

		arr := out.(*compute.ArrayDatum).MakeArray()
		return EvaluatedResponse{
			array:        arr,
			responseType: responseType,
			err:          nil,
		}
	case ast.BinaryOpLess:
		out, err := compute.CallFunction(ctx, "less", nil, left, right)
		if err != nil {
			return EvaluatedResponse{
				err: err,
			}
		}
		defer out.Release()

		arr := out.(*compute.ArrayDatum).MakeArray()
		return EvaluatedResponse{
			array:        arr,
			responseType: responseType,
			err:          nil,
		}
	case ast.BinaryOpLessEqual:
		out, err := compute.CallFunction(ctx, "less_equal", nil, left, right)
		if err != nil {
			return EvaluatedResponse{
				err: err,
			}
		}
		defer out.Release()

		arr := out.(*compute.ArrayDatum).MakeArray()
		return EvaluatedResponse{
			array:        arr,
			responseType: responseType,
			err:          nil,
		}
	default:
		panic("unreachable")
	}

}

func (e *evaluator) evaluateAddOperator(
	leftArray arrow.Array,
	rightArray arrow.Array,
	lefColumnType catalog.ColumnType,
	rightColumnType catalog.ColumnType,
) EvaluatedResponse {
	switch {
	case lefColumnType == catalog.ColumnTypeInt && rightColumnType == catalog.ColumnTypeInt:
		builder := array.NewInt64Builder(e.allocator)
		defer builder.Release()
		lefts := leftArray.(*array.Int64)
		rights := rightArray.(*array.Int64)

		for i := 0; i < lefts.Len(); i++ {
			if lefts.IsNull(i) || rights.IsNull(i) {
				builder.AppendNull()
			} else {
				val := lefts.Value(i) + rights.Value(i)
				builder.Append(val)
			}
		}
		return EvaluatedResponse{
			array:        builder.NewArray(),
			responseType: catalog.ColumnTypeInt,
			err:          nil,
		}
	case lefColumnType == catalog.ColumnTypeInt && rightColumnType == catalog.ColumnTypeDouble:
		builder := array.NewFloat64Builder(e.allocator)
		defer builder.Release()
		lefts := leftArray.(*array.Int64)
		rights := rightArray.(*array.Float64)

		for i := 0; i < lefts.Len(); i++ {
			if lefts.IsNull(i) || rights.IsNull(i) {
				builder.AppendNull()
			} else {
				val := float64(lefts.Value(i)) + rights.Value(i)
				builder.Append(val)
			}
		}
		return EvaluatedResponse{
			array:        builder.NewArray(),
			responseType: catalog.ColumnTypeDouble,
			err:          nil,
		}
	case lefColumnType == catalog.ColumnTypeDouble && rightColumnType == catalog.ColumnTypeInt:
		builder := array.NewFloat64Builder(e.allocator)
		defer builder.Release()
		lefts := leftArray.(*array.Float64)
		rights := rightArray.(*array.Int64)

		for i := 0; i < lefts.Len(); i++ {
			if lefts.IsNull(i) || rights.IsNull(i) {
				builder.AppendNull()
			} else {
				val := lefts.Value(i) + float64(rights.Value(i))
				builder.Append(val)
			}
		}
		return EvaluatedResponse{
			array:        builder.NewArray(),
			responseType: catalog.ColumnTypeDouble,
			err:          nil,
		}
	case lefColumnType == catalog.ColumnTypeDouble && rightColumnType == catalog.ColumnTypeDouble:
		builder := array.NewFloat64Builder(e.allocator)
		defer builder.Release()
		lefts := leftArray.(*array.Float64)
		rights := rightArray.(*array.Float64)

		for i := 0; i < lefts.Len(); i++ {
			if lefts.IsNull(i) || rights.IsNull(i) {
				builder.AppendNull()
			} else {
				val := lefts.Value(i) + rights.Value(i)
				builder.Append(val)
			}
		}
		return EvaluatedResponse{
			array:        builder.NewArray(),
			responseType: catalog.ColumnTypeDouble,
			err:          nil,
		}
	case lefColumnType == catalog.ColumnTypeString && rightColumnType == catalog.ColumnTypeString:
		builder := array.NewStringBuilder(e.allocator)
		defer builder.Release()
		lefts := leftArray.(*array.String)
		rights := rightArray.(*array.String)

		for i := 0; i < lefts.Len(); i++ {
			if lefts.IsNull(i) || rights.IsNull(i) {
				builder.AppendNull()
			} else {
				val := lefts.Value(i) + rights.Value(i)
				builder.Append(val)
			}
		}
		return EvaluatedResponse{
			array:        builder.NewArray(),
			responseType: catalog.ColumnTypeString,
			err:          nil,
		}
	default:
		return EvaluatedResponse{
			err: fmt.Errorf("unsupported types for + operator: %s and %s", lefColumnType, rightColumnType),
		}
	}
}

func (e *evaluator) evaluateUnaryExpr(expr *logical.UnaryExpr, batch arrow.RecordBatch) EvaluatedResponse {
	// TODO: check binder reject invalid UnaryExpr
	if expr.Op != ast.UnaryOpNegate {
		panic(fmt.Sprintf("unary operator not supported: %v", expr.Op))
	}

	res := e.evaluateExpr(expr.Expr, batch)
	if res.err != nil {
		return EvaluatedResponse{
			err: res.err,
		}
	}
	innerArray := res.array

	switch expr.ColumnType {
	case catalog.ColumnTypeInt:
		builder := array.NewInt64Builder(e.allocator)
		defer builder.Release()
		ints := innerArray.(*array.Int64)

		for i := 0; i < ints.Len(); i++ {
			if ints.IsNull(i) {
				builder.AppendNull()
			} else {
				val := ints.Value(i)
				builder.Append(-val)
			}
		}
		return EvaluatedResponse{
			array:        builder.NewArray(),
			responseType: catalog.ColumnTypeInt,
			err:          nil,
		}
	case catalog.ColumnTypeDouble:
		builder := array.NewFloat64Builder(e.allocator)
		defer builder.Release()
		floats := innerArray.(*array.Float64)

		for i := 0; i < floats.Len(); i++ {
			if floats.IsNull(i) {
				builder.AppendNull()
			} else {
				val := floats.Value(i)
				builder.Append(-val)
			}
		}

		return EvaluatedResponse{
			array:        builder.NewArray(),
			responseType: catalog.ColumnTypeDouble,
			err:          nil,
		}

	default:
		panic("unreachable")
	}
}

func (e *evaluator) evaluateColumnRef(expr *logical.ColumnRef, batch arrow.RecordBatch) EvaluatedResponse {
	switch expr.ColumnType {
	case catalog.ColumnTypeString:
		return EvaluatedResponse{
			array:        batch.Column(expr.ColumnIndex).(*array.String),
			responseType: catalog.ColumnTypeString,
			err:          nil,
		}
	case catalog.ColumnTypeInt:
		return EvaluatedResponse{
			array:        batch.Column(expr.ColumnIndex).(*array.Int64),
			responseType: catalog.ColumnTypeInt,
			err:          nil,
		}
	case catalog.ColumnTypeDouble:
		return EvaluatedResponse{
			array:        batch.Column(expr.ColumnIndex).(*array.Float64),
			responseType: catalog.ColumnTypeDouble,
			err:          nil,
		}
	case catalog.ColumnTypeBool:
		return EvaluatedResponse{
			array:        batch.Column(expr.ColumnIndex).(*array.Boolean),
			responseType: catalog.ColumnTypeBool,
			err:          nil,
		}
	case catalog.ColumnTypeDatetime:
		panic("not implemented")
	case catalog.ColumnTypeNull:
		panic("unreachable")
	default:
		panic("unreachable")
	}
}

func (e *evaluator) evaluateLiteralExpr(expr *logical.LiteralExpr, batch arrow.RecordBatch) EvaluatedResponse {
	length := int(batch.NumRows())

	switch expr.LiteralType {
	case catalog.ColumnTypeString:
		val := expr.Value.(string)

		builder := array.NewStringBuilder(e.allocator)
		defer builder.Release()
		for i := 0; i < length; i++ {
			builder.Append(val)
		}
		return EvaluatedResponse{
			array:        builder.NewArray(),
			responseType: catalog.ColumnTypeString,
			err:          nil,
		}

	case catalog.ColumnTypeInt:
		val := expr.Value.(int64)

		builder := array.NewInt64Builder(e.allocator)
		defer builder.Release()
		for i := 0; i < length; i++ {
			builder.Append(val)
		}

		return EvaluatedResponse{
			array:        builder.NewArray(),
			responseType: catalog.ColumnTypeInt,
			err:          nil,
		}
	case catalog.ColumnTypeDouble:
		val := expr.Value.(float64)

		builder := array.NewFloat64Builder(e.allocator)
		defer builder.Release()
		for i := 0; i < length; i++ {
			builder.Append(val)
		}
		return EvaluatedResponse{
			array:        builder.NewArray(),
			responseType: catalog.ColumnTypeDouble,
			err:          nil,
		}
	case catalog.ColumnTypeBool:
		val := expr.Value.(bool)

		builder := array.NewBooleanBuilder(e.allocator)
		defer builder.Release()
		for i := 0; i < length; i++ {
			builder.Append(val)
		}
		return EvaluatedResponse{
			array:        builder.NewArray(),
			responseType: catalog.ColumnTypeBool,
			err:          nil,
		}
	case catalog.ColumnTypeDatetime:
		panic("not implemented")
	case catalog.ColumnTypeNull:
		builder := array.NewBooleanBuilder(e.allocator)
		defer builder.Release()
		builder.AppendNulls(length)

		return EvaluatedResponse{
			array:        builder.NewArray(),
			responseType: catalog.ColumnTypeNull,
			err:          nil,
		}
	default:
		panic("unreachable")
	}
}
