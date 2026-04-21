package logical

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/ocowchun/sq/ast"
	"github.com/ocowchun/sq/catalog"
)

func Format(node Node) string {
	if node == nil {
		return "<nil>"
	}

	var b strings.Builder
	formatNode(&b, node, 0)
	return b.String()
}

func formatNode(b *strings.Builder, node Node, depth int) {
	if b.Len() > 0 {
		b.WriteByte('\n')
	}
	b.WriteString(strings.Repeat("  ", depth))

	switch n := node.(type) {
	case *Statement:
		fmt.Fprintf(b, "Statement")
		for _, cte := range n.CTEs {
			formatSection(b, depth+1, fmt.Sprintf("CTE(name=%s, schema=%s)", cte.Name, formatSchema(cte.Schema)), cte.Query)
		}
		formatSection(b, depth+1, "Root", n.Root)
	case *Scan:
		if n.CTE != nil {
			fmt.Fprintf(b, "CTEScan(name=%s%s, relation=%s, schema=%s)", n.CTE.Name, formatAliasSuffix(n.Table.Alias), formatRelationName(n.RelationName), formatSchema(n.Schema()))
			return
		}
		fmt.Fprintf(b, "Scan(table=%s, relation=%s, schema=%s)", formatTable(n.Table), formatRelationName(n.RelationName), formatSchema(n.Schema()))
	case *Filter:
		fmt.Fprintf(b, "Filter(predicate=%s)", formatSearchCondition(n.Predicate))
		formatNode(b, n.Input, depth+1)
	case *Project:
		fmt.Fprintf(b, "Project(select=%s, schema=%s)", formatSelectExprs(n.SelectExprs), formatSchema(n.Schema()))
		formatNode(b, n.Input, depth+1)
	case *Join:
		fmt.Fprintf(b, "Join(type=%s, on=%s, schema=%s)", formatJoinType(n.Type), formatSearchCondition(n.On), formatSchema(n.Schema()))
		formatNode(b, n.Left, depth+1)
		formatNode(b, n.Right, depth+1)
	default:
		fmt.Fprintf(b, "%T", node)
	}
}

func formatSection(b *strings.Builder, depth int, label string, node Node) {
	if b.Len() > 0 {
		b.WriteByte('\n')
	}
	b.WriteString(strings.Repeat("  ", depth))
	b.WriteString(label)
	if node != nil {
		formatNode(b, node, depth+1)
	}
}

func formatTable(table Table) string {
	if table.Alias == "" {
		return table.Name
	}
	return table.Name + " AS " + table.Alias
}

func formatAliasSuffix(alias string) string {
	if alias == "" {
		return ""
	}
	return " AS " + alias
}

func formatRelationName(relationName string) string {
	if relationName == "" {
		return "<unknown>"
	}
	return relationName
}

func formatSchema(schema catalog.Schema) string {
	parts := make([]string, 0, len(schema.Columns))
	for _, column := range schema.Columns {
		parts = append(parts, column.Name+":"+column.Type.String())
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func formatSelectExprs(exprs []SelectExpr) string {
	parts := make([]string, 0, len(exprs))
	for _, item := range exprs {
		part := formatExpr(item.Expr)
		if item.Alias != "" {
			part += " AS " + item.Alias
		}
		parts = append(parts, part)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func formatExprList(exprs []Expr) string {
	parts := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		parts = append(parts, formatExpr(expr))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func formatLiteralList(exprs []*LiteralExpr) string {
	parts := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		parts = append(parts, formatExpr(expr))
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func formatSearchCondition(condition SearchCondition) string {
	switch cond := condition.(type) {
	case *OrSearchCondition:
		return "(" + formatSearchCondition(cond.Left) + " OR " + formatSearchCondition(cond.Right) + ")"
	case *AndSearchCondition:
		return "(" + formatSearchCondition(cond.Left) + " AND " + formatSearchCondition(cond.Right) + ")"
	case *LikePredicate:
		if cond.Not {
			return "(" + formatExpr(cond.Left) + " not like " + cond.Pattern + ")"
		}
		return "(" + formatExpr(cond.Left) + " like " + cond.Pattern + ")"
	case *InPredicate:
		if cond.Not {
			return "(" + formatExpr(cond.Left) + " NOT IN " + formatLiteralList(cond.Exprs) + ")"
		}
		return "(" + formatExpr(cond.Left) + " IN " + formatLiteralList(cond.Exprs) + ")"
	case *IsNullPredicate:
		if cond.Not {
			return "(" + formatExpr(cond.Expression) + " IS NOT NULL)"
		}
		return "(" + formatExpr(cond.Expression) + " IS NULL)"
	case *ExprPredicate:
		return formatExpr(cond.Expr)
	default:
		return fmt.Sprintf("%T", condition)
	}
}

func formatExpr(expr Expr) string {
	if expr == nil {
		return "<nil>"
	}

	switch e := expr.(type) {
	case *LiteralExpr:
		return formatLiteral(e)
	case *ColumnRef:
		if e.TableAlias != "" {
			return e.TableAlias + "." + e.ColumnName
		}
		if e.TableName != "" {
			return e.TableName + "." + e.ColumnName
		}
		return e.ColumnName
	case *UnaryExpr:
		return "(" + formatUnaryOp(e.Op) + " " + formatExpr(e.Expr) + ")"
	case *BinaryExpr:
		return "(" + formatExpr(e.Left) + " " + formatBinaryOp(e.Op) + " " + formatExpr(e.Right) + ")"
	default:
		return fmt.Sprintf("%T", expr)
	}
}

func formatCallArgs(exprs []Expr) string {
	parts := make([]string, 0, len(exprs))
	for _, expr := range exprs {
		parts = append(parts, formatExpr(expr))
	}
	return strings.Join(parts, ", ")
}

func formatLiteral(expr *LiteralExpr) string {
	if expr.LiteralType == catalog.ColumnTypeNull {
		return "NULL"
	}

	switch expr.LiteralType {
	case catalog.ColumnTypeString:
		return strconv.Quote(expr.Value.(string))
	case catalog.ColumnTypeBool:
		if expr.Value == true {
			return "true"
		}
		return "false"
	case catalog.ColumnTypeInt:
		return fmt.Sprintf("%d", expr.Value.(int64))
	case catalog.ColumnTypeDouble:
		return strconv.FormatFloat(expr.Value.(float64), 'f', -1, 64)
	default:
		return fmt.Sprintf("%v", expr.Value)
	}
}

func formatJoinType(joinType ast.JoinType) string {
	switch joinType {
	case ast.JoinTypeInnerJoin:
		return "INNER"
	case ast.JoinTypeLeftJoin:
		return "LEFT"
	default:
		return fmt.Sprintf("JoinType(%d)", joinType)
	}
}

func formatUnaryOp(op ast.UnaryOp) string {
	switch op {
	case ast.UnaryOpNegate:
		return "-"
	default:
		return fmt.Sprintf("UnaryOp(%d)", op)
	}
}

func formatBinaryOp(op ast.BinaryOp) string {
	switch op {
	case ast.BinaryOpAdd:
		return "+"
	case ast.BinaryOpSub:
		return "-"
	case ast.BinaryOpMul:
		return "*"
	case ast.BinaryOpDiv:
		return "/"
	case ast.BinaryOpEqual:
		return "="
	case ast.BinaryOpNotEqual:
		return "!="
	case ast.BinaryOpLess:
		return "<"
	case ast.BinaryOpLessEqual:
		return "<="
	case ast.BinaryOpGreater:
		return ">"
	case ast.BinaryOpGreaterEqual:
		return ">="
	default:
		return fmt.Sprintf("BinaryOp(%d)", op)
	}
}
