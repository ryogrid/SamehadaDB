// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package catalog

import (
	"github.com/ryogrid/SaitomDB/storage/access"
	"github.com/ryogrid/SaitomDB/storage/table"
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
