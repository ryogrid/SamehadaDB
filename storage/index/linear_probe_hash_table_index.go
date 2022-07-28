package index

import (
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"

	hash "github.com/ryogrid/SamehadaDB/container/hash"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
)

type LinearProbeHashTableIndex struct {
	// comparator for key
	//KeyComparator comparator_;
	// container
	container hash.LinearProbeHashTable
	metadata  *IndexMetadata
	// idx of target column on table
	col_idx uint32
}

// TODO: (SDB) need to add index header page ID argument (NewLinearProbeHashTableIndex)
func NewLinearProbeHashTableIndex(metadata *IndexMetadata, buffer_pool_manager *buffer.BufferPoolManager, col_idx uint32,
	num_buckets int) *LinearProbeHashTableIndex {
	ret := new(LinearProbeHashTableIndex)
	ret.metadata = metadata
	ret.container = *hash.NewLinearProbeHashTable(buffer_pool_manager, num_buckets)
	ret.col_idx = col_idx
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

func (htidx *LinearProbeHashTableIndex) InsertEntry(key *tuple.Tuple, rid page.RID, transaction *access.Transaction) {
	tupleSchema_ := htidx.GetTupleSchema()
	keyDataInBytes := key.GetValueInBytes(tupleSchema_, htidx.col_idx)

	htidx.container.Insert(keyDataInBytes, samehada_util.PackRIDtoUint32(&rid))
}

func (htidx *LinearProbeHashTableIndex) DeleteEntry(key *tuple.Tuple, rid page.RID, transaction *access.Transaction) {
	tupleSchema_ := htidx.GetTupleSchema()
	keyDataInBytes := key.GetValueInBytes(tupleSchema_, htidx.col_idx)

	htidx.container.Remove(keyDataInBytes, samehada_util.PackRIDtoUint32(&rid))
}

func (htidx *LinearProbeHashTableIndex) ScanKey(key *tuple.Tuple, transaction *access.Transaction) []page.RID {
	tupleSchema_ := htidx.GetTupleSchema()
	keyDataInBytes := key.GetValueInBytes(tupleSchema_, htidx.col_idx)

	packed_values := htidx.container.GetValue(keyDataInBytes)
	var ret_arr []page.RID
	for _, packed_val := range packed_values {
		ret_arr = append(ret_arr, samehada_util.UnpackUint32toRID(packed_val))
	}
	return ret_arr
}
