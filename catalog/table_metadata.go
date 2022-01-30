// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package catalog

import (
	"github.com/ryogrid/SamehadaDB/interfaces"
	"github.com/ryogrid/SamehadaDB/storage/table"
)

type TableMetadata struct {
	schema *table.Schema
	name   string
	table  *interfaces.ITableHeap
	oid    uint32
}

func (t *TableMetadata) Schema() *table.Schema {
	return t.schema
}

func (t *TableMetadata) OID() uint32 {
	return t.oid
}

func (t *TableMetadata) Table() *interfaces.ITableHeap {
	return t.table
}
