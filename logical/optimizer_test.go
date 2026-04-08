package logical

import (
	"fmt"
	"testing"
)

func Test_OptimizePushesInnerJoinLeftPredicate(t *testing.T) {
	sql := "SELECT u.name, t.name FROM users as u INNER JOIN teams as t ON u.team_id = t.id WHERE u.score > 10"
	plan, err := buildLogicalPlan(testCatalog(), sql)
	if err != nil {
		t.Fatal(err)
	}

	fmt.Println(Format(plan))

	optimized, err := OptimizeLogical(plan)
	if err != nil {
		t.Fatal(err)
	}

	// assert filter is push down to the left side of the join
	project, ok := optimized.(*Project)
	if !ok {
		t.Fatalf("optimized should be a Project")
	}
	join, ok := project.Input.(*Join)
	if !ok {
		t.Fatalf("project.Input should be a Join")
	}
	_, ok = join.Left.(*Filter)
	if !ok {
		t.Fatalf("join.Left should be a Filter")
	}
}

func Test_OptimizeRewriteS3ObjectScan(t *testing.T) {
	sql := "select * from objects where bucket_name = 'my-bucket' and key like 'foo/%'"
	plan, err := buildLogicalPlan(testCatalog(), sql)
	if err != nil {
		t.Fatal(err)
	}
	optimized, err := OptimizeLogical(plan)
	if err != nil {
		t.Fatal(err)
	}

	project, ok := optimized.(*Project)
	if !ok {
		t.Fatal("optimized should be a Project")
	}

	scan, ok := project.Input.(*S3ObjectScan)
	if !ok {
		t.Fatal("project.Input. should be a S3ObjectScan")
	}
	if scan.BucketName != "my-bucket" {
		t.Fatal("project.BucketName should be my-bucket")
	}
	if *scan.KeyPrefix != "foo/" {
		t.Fatal("project.KeyPrefix should be foo/")
	}
}

func Test_OptimizeRejectInvalidCases(t *testing.T) {

	tests := []struct {
		caseName string
		sql      string
	}{
		{
			"objects table query without bucket_name predicate",
			"select * from objects",
		},
		{
			"objects table query without bucket_name = xxx",
			"select * from objects where bucket_name != ''",
		},
		{
			"objects table query with bucket_name predicate but not a string literal",
			"select * from objects where bucket_name = 'foo' and bucket_name != ''",
		},
	}
	for _, test := range tests {
		plan, err := buildLogicalPlan(testCatalog(), test.sql)
		if err != nil {
			t.Fatal(err)
		}
		_, err = OptimizeLogical(plan)
		if err == nil {
			t.Fatalf("expected error for case: %s", test.caseName)
		}
	}
}
