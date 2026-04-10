package catalog

type Catalog struct {
	tables map[string]Table
}

var s3ObjectSchema = Schema{
	Columns: []Column{
		{Name: "key", Type: ColumnTypeString},
		{Name: "bucket_name", Type: ColumnTypeString},
		{Name: "size", Type: ColumnTypeInt},
	},
}

func New() *Catalog {
	tables := make(map[string]Table)
	c := &Catalog{
		tables: tables,
	}
	c.RegisterTable(Table{
		Name:       "objects",
		Schema:     s3ObjectSchema,
		AccessKind: AccessKindS3Sdk,
	})
	return c
}

func (c *Catalog) RegisterTable(table Table) {
	c.tables[table.Name] = table
}

func (c *Catalog) GetTable(name string) (Table, bool) {
	table, ok := c.tables[name]
	return table, ok
}
