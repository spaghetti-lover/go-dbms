package db

// Index definition
type IndexDef struct {
	Name   string
	Cols   []string
	Prefix uint8
}

// Table definition (schema)
type TableDef struct {
	Name    string
	Cols    []string
	Types   []ValueType
	PKeyN   int   // number of primary key columns
	Prefix  uint8 // table prefix for key encoding
	Indexes []IndexDef
}

// MetaTable stores metadata of database (db_version, next_table_prefix, schema_version)
var MetaTable = &TableDef{
	Name:   "@meta",
	Cols:   []string{"key", "value"},
	Types:  []ValueType{ValueBytes, ValueBytes},
	PKeyN:  2,
	Prefix: 1,
}

// TableCatalog stores schema definition of all user tables. It map table_name -> TableDef
var TableCatalog = &TableDef{
	Name:   "@table",
	Cols:   []string{"name", "def"},
	Types:  []ValueType{ValueBytes, ValueBytes},
	PKeyN:  2,
	Prefix: 2,
}
