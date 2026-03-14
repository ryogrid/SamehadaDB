package index

import (
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/lib/types"

	"github.com/ryogrid/SamehadaDB/lib/container/hash"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
)

type LinearProbeHashTableIndex struct {
	// container
	container hash.LinearProbeHashTable
	metadata  *IndexMetadata
	// idx of target column on table
	colIdx uint32
}

func NewLinearProbeHashTableIndex(metadata *IndexMetadata, bufferPoolManager *buffer.BufferPoolManager, colIdx uint32,
	numBuckets int, headerPageID types.PageID) *LinearProbeHashTableIndex {
	ret := new(LinearProbeHashTableIndex)
	ret.metadata = metadata
	ret.container = *hash.NewLinearProbeHashTable(bufferPoolManager, numBuckets, headerPageID)
	ret.colIdx = colIdx
	return ret
}

// Return the metadata object associated with the index
func (htidx *LinearProbeHashTableIndex) GetMetadata() *IndexMetadata { return htidx.metadata }

func (htidx *LinearProbeHashTableIndex) GetIndexColumnCount() uint32 {
	return htidx.metadata.GetIndexColumnCount()
}
func (htidx *LinearProbeHashTableIndex) GetName() *string { return htidx.metadata.GetName() }
func (htidx *LinearProbeHashTableIndex) GetTupleSchema() *schema.Schema {
	return htidx.metadata.GetTupleSchema()
}
func (htidx *LinearProbeHashTableIndex) GetKeyAttrs() []uint32 { return htidx.metadata.GetKeyAttrs() }

func (htidx *LinearProbeHashTableIndex) InsertEntry(key *tuple.Tuple, rid page.RID, transaction interface{}) {
	tupleSchema := htidx.GetTupleSchema()
	keyDataInBytes := key.GetValueInBytes(tupleSchema, htidx.colIdx)

	htidx.container.Insert(keyDataInBytes, samehada_util.PackRIDtoUint64(&rid))
}

func (htidx *LinearProbeHashTableIndex) DeleteEntry(key *tuple.Tuple, rid page.RID, transaction interface{}) {
	tupleSchema := htidx.GetTupleSchema()
	keyDataInBytes := key.GetValueInBytes(tupleSchema, htidx.colIdx)

	htidx.container.Remove(keyDataInBytes, samehada_util.PackRIDtoUint64(&rid))
}

func (htidx *LinearProbeHashTableIndex) ScanKey(key *tuple.Tuple, transaction interface{}) []page.RID {
	tupleSchema := htidx.GetTupleSchema()
	keyDataInBytes := key.GetValueInBytes(tupleSchema, htidx.colIdx)

	packedValues := htidx.container.GetValue(keyDataInBytes)
	var retArr []page.RID
	for _, packedVal := range packedValues {
		retArr = append(retArr, samehada_util.UnpackUint64toRID(packedVal))
	}
	return retArr
}

func (htidx *LinearProbeHashTableIndex) UpdateEntry(oldKey *tuple.Tuple, oldRID page.RID, newKey *tuple.Tuple, newRID page.RID, transaction interface{}) {
	panic("not implemented yet")
}

func (htidx *LinearProbeHashTableIndex) GetRangeScanIterator(startkey *tuple.Tuple, endKey *tuple.Tuple, txn interface{}) IndexRangeScanIterator {
	return nil
}

func (htidx *LinearProbeHashTableIndex) GetHeaderPageID() types.PageID {
	return htidx.container.GetHeaderPageID()
}
