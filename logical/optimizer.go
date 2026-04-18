package logical

import (
	"fmt"
	"slices"
	"strings"

	"github.com/ocowchun/sq/ast"
	"github.com/ocowchun/sq/catalog"
)

func OptimizeLogical(plan Node) (Node, error) {
	optimized, err := pushDownFilters(plan)
	if err != nil {
		return nil, err
	}

	optimized, err = rewriteObjectAccess(optimized)
	if err != nil {
		return nil, err
	}

	err = rejectBareS3ObjectScan(optimized)
	if err != nil {
		return nil, err
	}

	return optimized, nil
}

func rejectBareS3ObjectScan(plan Node) error {
	switch node := plan.(type) {
	case *Statement:
		for _, cte := range node.CTEs {
			if err := rejectBareS3ObjectScan(cte.Query); err != nil {
				return err
			}
		}
		return rejectBareS3ObjectScan(node.Root)
	case *Filter:
		return rejectBareS3ObjectScan(node.Input)
	case *Project:
		return rejectBareS3ObjectScan(node.Input)
	case *Join:
		if err := rejectBareS3ObjectScan(node.Left); err != nil {
			return err
		}
		return rejectBareS3ObjectScan(node.Right)
	case *Scan:
		if isObjectTable(node) {
			return fmt.Errorf(`table %q requires bucket_name = <string literal>`, node.Table.Name)
		}
		return nil
	default:
		return nil
	}
}

func rewriteObjectAccess(plan Node) (Node, error) {
	switch node := plan.(type) {
	case *Statement:
		for i := range node.CTEs {
			query, err := rewriteObjectAccess(node.CTEs[i].Query)
			if err != nil {
				return nil, err
			}
			node.CTEs[i].Query = query
		}
		root, err := rewriteObjectAccess(node.Root)
		if err != nil {
			return nil, err
		}
		node.Root = root
		return node, nil
	case *Filter:
		input, err := rewriteObjectAccess(node.Input)
		if err != nil {
			return nil, err
		}
		return rewriteObjectFilter(input, node.Predicate)
	case *Project:
		input, err := rewriteObjectAccess(node.Input)
		if err != nil {
			return nil, err
		}
		node.Input = input
		return node, nil
	case *Join:
		left, err := rewriteObjectAccess(node.Left)
		if err != nil {
			return nil, err
		}
		right, err := rewriteObjectAccess(node.Right)
		if err != nil {
			return nil, err
		}
		node.Left = left
		node.Right = right
		return node, nil
	case *Sort:
		input, err := rewriteObjectAccess(node.Input)
		if err != nil {
			return nil, err
		}
		node.Input = input
		return node, nil
	case *Limit:
		input, err := rewriteObjectAccess(node.Input)
		if err != nil {
			return nil, err
		}
		node.Input = input
		return node, nil
	default:
		return plan, nil
	}
}

func rewriteObjectFilter(input Node, searchCondition SearchCondition) (Node, error) {
	if searchCondition == nil {
		return input, nil
	}

	if nested, ok := input.(*Filter); ok {
		conditions := append(splitConjunctions(nested.Predicate), splitConjunctions(searchCondition)...)
		return rewriteObjectFilter(nested.Input, combineConjunctions(conditions))
	}

	if scan, ok := input.(*Scan); ok && isObjectTable(scan) {
		return rewriteObjectScan(scan, searchCondition)
	}

	return &Filter{
		Input:     input,
		Predicate: searchCondition,
	}, nil
}

func rewriteObjectScan(scan *Scan, searchCondition SearchCondition) (Node, error) {
	conditions := splitConjunctions(searchCondition)

	var (
		bucketValue     string
		prefixCondition SearchCondition
		bucketCondition SearchCondition
		prefixValue     string
		residuals       []SearchCondition
	)

	for _, condition := range conditions {
		if hasSearchConditionReferencesColumn(condition, scan.RelationID, "bucket_name") {
			if bucketCondition != nil {
				return nil, fmt.Errorf(`table %q requires exactly one bucket_name predicate`, scan.Table.Name)
			}
			value, err := bucketNameValue(scan.RelationID, condition, scan.Table.Name)
			if err != nil {
				return nil, err
			}
			bucketCondition = condition
			bucketValue = value
			continue
		}

		if value, ok := keyPrefixValue(scan.RelationID, condition); ok {
			if prefixCondition != nil {
				return nil, fmt.Errorf(`table %q supports only one key prefix predicate`, scan.Table.Name)
			}
			prefixCondition = condition
			prefixValue = value
			continue

		}
		residuals = append(residuals, condition)
	}

	if bucketCondition == nil {
		return nil, fmt.Errorf(`table %q requires bucket_name = <string literal>`, scan.Table.Name)
	}
	s3ObjectScan := &S3ObjectScan{
		RelationID:   scan.RelationID,
		RelationName: scan.RelationName,
		Table:        scan.Table,
		BucketName:   bucketValue,
	}
	if prefixValue != "" {
		s3ObjectScan.KeyPrefix = &prefixValue
	}

	if len(residuals) == 0 {
		return s3ObjectScan, nil
	}

	return &Filter{
		Input:     s3ObjectScan,
		Predicate: combineConjunctions(residuals),
	}, nil
}

func keyPrefixValue(relationID string, condition SearchCondition) (string, bool) {
	likePredicate, ok := condition.(*LikePredicate)
	if !ok || likePredicate.Not {
		return "", false
	}

	keyRef, ok := likePredicate.Left.(*ColumnRef)
	if !ok || keyRef.RelationID != relationID || keyRef.ColumnName != "key" {
		return "", false
	}
	if !strings.HasSuffix(likePredicate.Pattern, "%") {
		return "", false
	}

	elements := strings.Split(likePredicate.Pattern, "%")
	return elements[0], true
}

func bucketNameValue(relationID string, condition SearchCondition, tableName string) (string, error) {
	exprPredicate, ok := condition.(*ExprPredicate)
	if !ok {
		return "", fmt.Errorf(`table %q requires bucket_name = <string literal>`, tableName)
	}
	binaryExpr, ok := exprPredicate.Expr.(*BinaryExpr)
	if !ok {
		return "", fmt.Errorf(`table %q requires bucket_name = <string literal>`, tableName)
	}

	left, ok := binaryExpr.Left.(*ColumnRef)
	if !ok || left.RelationID != relationID || left.ColumnName != "bucket_name" {
		return "", fmt.Errorf(`table %q requires bucket_name = <string literal>`, tableName)
	}

	if binaryExpr.Op != ast.BinaryOpEqual {
		return "", fmt.Errorf("table %q requires bucket_name = <string literal>", tableName)
	}

	right, ok := binaryExpr.Right.(*LiteralExpr)
	if !ok || right.LiteralType != catalog.ColumnTypeString {
		return "", fmt.Errorf(`table %q requires bucket_name = <string literal>`, tableName)
	}

	return right.Value.(string), nil
}

func hasSearchConditionReferencesColumn(searchCondition SearchCondition, relationID string, columnName string) bool {
	switch condition := searchCondition.(type) {
	case *OrSearchCondition:
		return hasSearchConditionReferencesColumn(condition.Left, relationID, columnName) || hasSearchConditionReferencesColumn(condition.Right, relationID, columnName)
	case *AndSearchCondition:
		return hasSearchConditionReferencesColumn(condition.Left, relationID, columnName) || hasSearchConditionReferencesColumn(condition.Right, relationID, columnName)
	case *LikePredicate:
		return hasExprReferencesColumn(condition.Left, relationID, columnName)
	case *InPredicate:
		return hasExprReferencesColumn(condition.Left, relationID, columnName)
	case *IsNullPredicate:
		return hasExprReferencesColumn(condition.Expression, relationID, columnName)
	case *ExprPredicate:
		return hasExprReferencesColumn(condition.Expr, relationID, columnName)
	default:
		return false
	}
}

func hasExprReferencesColumn(expr Expr, relationID string, columnName string) bool {
	switch e := expr.(type) {
	case *ColumnRef:
		return e.RelationID == relationID && e.ColumnName == columnName
	case *UnaryExpr:
		return hasExprReferencesColumn(e.Expr, relationID, columnName)
	case *BinaryExpr:
		return hasExprReferencesColumn(e.Left, relationID, columnName) || hasExprReferencesColumn(e.Right, relationID, columnName)
	default:
		return false
	}
}

func isObjectTable(scan *Scan) bool {
	return scan.Table.AccessKind == catalog.AccessKindS3Sdk
}

func pushDownFilters(plan Node) (Node, error) {
	switch node := plan.(type) {
	case *Statement:
		for i := range node.CTEs {
			query, err := pushDownFilters(node.CTEs[i].Query)
			if err != nil {
				return nil, err
			}
			node.CTEs[i].Query = query
		}

		root, err := pushDownFilters(node.Root)
		if err != nil {
			return nil, err
		}
		node.Root = root
		return node, nil
	case *Filter:
		input, err := pushDownFilters(node.Input)
		if err != nil {
			return nil, err
		}
		return pushDownFilterInNode(input, node.Predicate)
	case *Project:
		input, err := pushDownFilters(node.Input)
		if err != nil {
			return nil, err
		}
		node.Input = input
		return node, nil
	case *Join:
		left, err := pushDownFilters(node.Left)
		if err != nil {
			return nil, err
		}
		node.Left = left
		right, err := pushDownFilters(node.Right)
		if err != nil {
			return nil, err
		}
		node.Right = right
		return node, nil
	case *Sort:
		input, err := pushDownFilters(node.Input)
		if err != nil {
			return nil, err
		}
		node.Input = input
		return node, nil
	case *Limit:
		input, err := pushDownFilters(node.Input)
		if err != nil {
			return nil, err
		}
		node.Input = input
		return node, nil
	default:
		return plan, nil
	}
}

func pushDownFilterInNode(input Node, searchCondition SearchCondition) (Node, error) {
	join, ok := input.(*Join)
	if !ok {
		return &Filter{
			Input:     input,
			Predicate: searchCondition,
		}, nil
	}
	leftPredicates, rightPredicates, residualPredicates := classifyJoinPredicates(join, splitConjunctions(searchCondition))

	if len(leftPredicates) > 0 {
		left, err := pushDownFilters(attachFilter(join.Left, leftPredicates))
		if err != nil {
			return nil, err
		}
		join.Left = left
	}
	if len(rightPredicates) > 0 {
		right, err := pushDownFilters(attachFilter(join.Right, leftPredicates))
		if err != nil {
			return nil, err
		}
		join.Right = right
	}
	if len(residualPredicates) == 0 {
		return join, nil
	}

	return &Filter{
		Input:     join,
		Predicate: combineConjunctions(residualPredicates),
	}, nil
}

func attachFilter(node Node, conditions []SearchCondition) Node {
	if len(conditions) == 0 {
		return node
	}

	if filter, ok := node.(*Filter); ok {
		existing := splitConjunctions(filter.Predicate)
		conditions = append(existing, conditions...)
		node = filter.Input
	}

	return &Filter{
		Input:     node,
		Predicate: combineConjunctions(conditions),
	}
}

func combineConjunctions(conditions []SearchCondition) SearchCondition {
	if len(conditions) == 0 {
		return nil
	}

	combined := conditions[0]
	for _, condition := range conditions[1:] {
		combined = &AndSearchCondition{
			Left:  combined,
			Right: condition,
		}
	}
	return combined
}

func classifyJoinPredicates(join *Join, conditions []SearchCondition) (left []SearchCondition, right []SearchCondition, residual []SearchCondition) {
	leftRelationIDs := NodeRelationIDs(join.Left)
	rightRelationIDs := NodeRelationIDs(join.Right)

	for _, condition := range conditions {
		relationIDs := SearchConditionRelationIds(condition)
		switch {
		case len(relationIDs) == 0:
			residual = append(residual, condition)
		case relationIDsSubset(relationIDs, leftRelationIDs):
			left = append(left, condition)
		case join.Type == ast.JoinTypeInnerJoin && relationIDsSubset(relationIDs, rightRelationIDs):
			right = append(right, condition)
		default:
			residual = append(residual, condition)
		}
	}
	return left, right, residual
}

func relationIDsSubset(ids []string, candidates []string) bool {
	for _, id := range ids {
		if !slices.Contains(candidates, id) {
			return false
		}
	}

	return len(ids) > 0
}

func splitConjunctions(searchCondition SearchCondition) []SearchCondition {
	andSc, ok := searchCondition.(*AndSearchCondition)
	if !ok {
		return []SearchCondition{searchCondition}
	}

	conjunctions := splitConjunctions(andSc.Left)
	return append(conjunctions, splitConjunctions(andSc.Right)...)
}

func NodeRelationIDs(node Node) []string {
	switch n := node.(type) {
	case *Statement:
		return NodeRelationIDs(n.Root)
	case *Scan:
		if n.RelationID == "" {
			return nil
		}
		return []string{n.RelationID}
	case *Filter:
		return NodeRelationIDs(n.Input)
	case *Project:
		return NodeRelationIDs(n.Input)
	case *Join:
		return mergeRelationIDs(NodeRelationIDs(n.Left), NodeRelationIDs(n.Right))
	default:
		return nil
	}
}
