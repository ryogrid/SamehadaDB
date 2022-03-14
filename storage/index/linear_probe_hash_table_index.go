package index

import (
	hash "github.com/ryogrid/SamehadaDB/container/hash"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
)

// TODO: (SDB) need port LinearProbeHashTableIndex class

//#define HASH_TABLE_INDEX_TYPE LinearProbeHashTableIndex<KeyType, ValueType, KeyComparator>

type LinearProbeHashTableIndex struct {
	// comparator for key
	//KeyComparator comparator_;
	// container
	container hash.LinearProbeHashTable
	metadata  *IndexMetadata
}

func NewLinearProbeHashTableIndex(metadata *IndexMetadata, buffer_pool_manager *BufferPoolManager,
	num_buckets int) *LinearProbeHashTableIndex {
	ret := new(LinearProbeHashTableIndex)
	ret.metadata = metadata
	ret.container = *hash.NewHashTable(buffer_pool_manager, num_buckets)
	return ret
}

// Return the metadata object associated with the index
func (htidx *LinearProbeHashTableIndex) GetMetadata() *IndexMetadata { return htidx.metadata }

func (htidx *LinearProbeHashTableIndex) GetIndexColumnCount() uint32 {
	return htidx.metadata.GetIndexColumnCount()
}
func (htidx *LinearProbeHashTableIndex) GetName() *string { return htidx.metadata.GetName() }
func (htidx *LinearProbeHashTableIndex) GetKeySchema() *schema.Schema {
	return htidx.metadata.GetKeySchema()
}
func (htidx *LinearProbeHashTableIndex) GetKeyAttrs() []uint32 { return htidx.metadata.GetKeyAttrs() }

func (htidx *LinearProbeHashTableIndex) InsertEntry(key *tuple.Tuple, rid page.RID, transaction *access.Transaction) {
	// // construct insert index key
	// KeyType index_key;
	// index_key.SetFromKey(key);

	htidx.container.Insert(index_key, rid)
}

func (htidx *LinearProbeHashTableIndex) DeleteEntry(key *tuple.Tuple, rid page.RID, transaction *access.Transaction) {
	// // construct delete index key
	// KeyType index_key;
	// index_key.SetFromKey(key);

	htidx.container.Remove(index_key, rid)
}

func (htidx *LinearProbeHashTableIndex) ScanKey(key *tuple.Tuple, result *[]page.RID, transaction *access.Transaction) {
	// // construct scan index key
	// KeyType index_key;
	// index_key.SetFromKey(key);

	htidx.container.GetValue(index_key, result)
}
