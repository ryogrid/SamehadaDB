// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package catalog

import (
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/index"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
)

type TableMetadata struct {
	schema *schema.Schema
	name   string
	table  *access.TableHeap
	// index data class obj of each column
	// if column has no index, respond element is nil
	indexes []index.Index
	// locking is needed when accessing statiscs.colStats[x]
	statiscs *TableStatistics
	oid      uint32
}

func NewTableMetadata(schema *schema.Schema, name string, table *access.TableHeap, oid uint32, logManager *recovery.LogManager, isGracefulShutdown bool) *TableMetadata {
	ret := new(TableMetadata)
	ret.schema = schema
	ret.name = name
	ret.table = table
	ret.statiscs = NewTableStatistics(schema)
	ret.oid = oid

	indexes := make([]index.Index, 0)
	for idx, col := range schema.GetColumns() {
		if col.HasIndex() {
			switch col.IndexKind() {
			case index_constants.IndexKindHash:
				// index bucket size is common.BucketSizeOfHashIndex (auto size extending is needed...)
				//       note: one bucket is used pages for storing index key/value pairs for a column.
				//             one page can store 512 key/value pair
				im := index.NewIndexMetadata(col.GetColumnName()+"_index", name, schema, []uint32{uint32(idx)})
				hIdx := index.NewLinearProbeHashTableIndex(im, table.GetBufferPoolManager(), uint32(idx), common.BucketSizeOfHashIndex, col.IndexHeaderPageID())

				indexes = append(indexes, hIdx)
				// at first allocation of pages for index, column's indexHeaderPageID is -1 at above code (col.IndexHeaderPageID() == -1)
				// because first allocation occurs when table creation is processed (not launched DB instace from existing db file which has difinition of this table)
				// so, for first allocation case, allocated page GetPageID of header page need to be set to column info here
				col.SetIndexHeaderPageID(hIdx.GetHeaderPageID())
			case index_constants.IndexKindUniqSkipList:
				// currently, SkipList Index always use new pages even if relaunch
				im := index.NewIndexMetadata(col.GetColumnName()+"_index", name, schema, []uint32{uint32(idx)})
				// TODO: (SDB) need to add index headae ID argument like HashIndex (NewTableMetadata)
				slIdx := index.NewUniqSkipListIndex(im, table.GetBufferPoolManager(), uint32(idx))

				indexes = append(indexes, slIdx)
				//col.SetIndexHeaderPageID(slIdx.GetHeaderPageID())
			case index_constants.IndexKindSkipList:
				// currently, SkipList Index always use new pages even if relaunch
				im := index.NewIndexMetadata(col.GetColumnName()+"_index", name, schema, []uint32{uint32(idx)})
				// TODO: (SDB) need to add index headae ID argument like HashIndex (NewTableMetadata)
				slIdx := index.NewSkipListIndex(im, table.GetBufferPoolManager(), uint32(idx), logManager)

				indexes = append(indexes, slIdx)
				//col.SetIndexHeaderPageID(slIdx.GetHeaderPageID())
			case index_constants.IndexKindBtree:
				im := index.NewIndexMetadata(col.GetColumnName()+"_index", name, schema, []uint32{uint32(idx)})
				var pageZeroID *int32 = nil
				if col.IndexHeaderPageID() != -1 && isGracefulShutdown {
					pageZeroID = new(int32)
					*pageZeroID = int32(col.IndexHeaderPageID())
				}

				btrIdx := index.NewBTreeIndex(im, table.GetBufferPoolManager(), uint32(idx), logManager, pageZeroID)

				indexes = append(indexes, btrIdx)
				col.SetIndexHeaderPageID(btrIdx.GetHeaderPageID())
			default:
				panic("illegal index kind!")
			}
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

func (t *TableMetadata) GetIndex(colIndex int) index.Index {
	ret := t.indexes[colIndex]
	if ret == nil {
		return nil
	} else {
		return t.indexes[colIndex]
	}
}

func (t *TableMetadata) GetColumnNum() uint32 {
	return t.schema.GetColumnCount()
}

// return list of indexes
// ATTENTION: returned list's length is same with column num of table.
//
//	value of elements corresponding to columns which doesn't have index is nil
func (t *TableMetadata) Indexes() []index.Index {
	return t.indexes
}

func (t *TableMetadata) GetStatistics() *TableStatistics {
	return t.statiscs
}

func (t *TableMetadata) GetTableName() *string {
	return &t.name
}
