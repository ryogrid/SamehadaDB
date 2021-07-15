package catalog

import (
	"github.com/brunocalza/go-bustub/storage/access"
	"github.com/brunocalza/go-bustub/storage/table"
)

type TableMetadata struct {
	schema *table.Schema
	name   string
	table  *access.TableHeap
	oid    uint32
}

func (t *TableMetadata) Schema() *table.Schema {
	return t.schema
}

func (t *TableMetadata) OID() uint32 {
	return t.oid
}

func (t *TableMetadata) Table() *access.TableHeap {
	return t.table
}
