package parser

import "testing"

func Test_Parser(t *testing.T) {
	input := "select * from object where name = \"my-bucket\" and key like \"my-key\""
	//" and key like 'my-prefix/%'"

	stmt, err := Parse(input)

	if err != nil {
		t.Fatal(err)
	}
	if stmt == nil {
		t.Fatal("expected statement")
	}
}

func Test_ParserJoin(t *testing.T) {
	input := "select * from a inner join b on a.id = b.a_id"
	//" and key like 'my-prefix/%'"

	stmt, err := Parse(input)

	if err != nil {
		t.Fatal(err)
	}
	if stmt == nil {
		t.Fatal("expected statement")
	}
}

func Test_ParseInvalidSQL(t *testing.T) {
	inputs := []string{
		"select * where",
	}
	for _, input := range inputs {
		_, err := Parse(input)

		if err == nil {
			t.Fatal(err)
		}
	}

}

func Test_ParserWithCTE(t *testing.T) {
	input := `
with myBucket as (select * from object where name = "my-bucket")
select key
from myBucket
where key like "%.csv"
`

	stmt, err := Parse(input)

	if err != nil {
		t.Fatal(err)
	}
	if stmt == nil {
		t.Fatal("expected statement")
	}
}

func Test_ParserWithFunction(t *testing.T) {
	input := `
select replace(key, "my-prefix/", "") as key
from object
`

	stmt, err := Parse(input)

	if err != nil {
		t.Fatal(err)
	}
	if stmt == nil {
		t.Fatal("expected statement")
	}
}
