package catalog

type Catalog struct {
	tables map[string]Table
}

func New() *Catalog {
	return &Catalog{
		tables: make(map[string]Table),
	}
}

func (c *Catalog) RegisterTable(table Table) {
	c.tables[table.Name] = table
}

func (c *Catalog) GetTable(name string) (Table, bool) {
	table, ok := c.tables[name]
	return table, ok
}
