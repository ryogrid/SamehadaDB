// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package catalog

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/index"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
)

type TableMetadata struct {
	schema *schema.Schema
	name   string
	table  *access.TableHeap
	// index data class obj of each column
	// if column has no index, respond element is nil
	indexes []index.Index
	oid     uint32
}

func NewTableMetadata(schema *schema.Schema, name string, table *access.TableHeap, oid uint32) *TableMetadata {
	ret := new(TableMetadata)
	ret.schema = schema
	ret.name = name
	ret.table = table
	ret.oid = oid

	indexes := make([]index.Index, 0)
	for idx, column_ := range schema.GetColumns() {
		if column_.HasIndex() {
			im := index.NewIndexMetadata(column_.GetColumnName()+"_index", name, schema, []uint32{uint32(idx)})
			// TODO: (SDB) index bucket size is 50 (auto size extending is needed...)
			indexes = append(indexes, index.NewLinearProbeHashTableIndex(im, table.GetBufferPoolManager(), uint32(idx), common.BucketSize))
		} else {
			indexes = append(indexes, nil)
		}
	}

	ret.indexes = indexes

	return ret
}

func (t *TableMetadata) Schema() *schema.Schema {
	return t.schema
}

func (t *TableMetadata) OID() uint32 {
	return t.oid
}

func (t *TableMetadata) Table() *access.TableHeap {
	return t.table
}

func (t *TableMetadata) GetIndex(colIndex int) *index.Index {
	ret := t.indexes[colIndex]
	if ret == nil {
		return nil
	} else {
		return &t.indexes[colIndex]
	}
}

func (t *TableMetadata) GetColumnNum() uint32 {
	return t.schema.GetColumnCount()
}
