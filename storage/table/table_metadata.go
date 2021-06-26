package table

type TableMetadata struct {
	schema *Schema
	name   string
	table  *TableHeap
	oid    uint32
}

func (t *TableMetadata) Schema() *Schema {
	return t.schema
}

func (t *TableMetadata) OID() uint32 {
	return t.oid
}

func (t *TableMetadata) Table() *TableHeap {
	return t.table
}
