package index

// TODO: (SDB) not implemented yet skip_list_index.go

import (
	"github.com/ryogrid/SamehadaDB/container/skip_list"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
)

type SkipListIndexOnMem struct {
	container skip_list.SkipListOnMem
	metadata  *IndexMetadata
	// idx of target column on table
	col_idx uint32
}

type SkipListIndex struct {
	container skip_list.SkipList
	metadata  *IndexMetadata
	// idx of target column on table
	col_idx uint32
}

func NewSkiplistIndexOnMem(metadata *IndexMetadata, col_idx uint32) *SkipListIndexOnMem {
	ret := new(SkipListIndexOnMem)
	ret.metadata = metadata
	val := types.NewInteger(0)
	ret.container = *skip_list.NewSkipListOnMem(1, &val, math.MaxUint32, true)
	ret.col_idx = col_idx
	return ret
}

func NewSkiplistIndex(metadata *IndexMetadata, buffer_pool_manager *buffer.BufferPoolManager, col_idx uint32,
	num_buckets int) *SkipListIndex {
	ret := new(SkipListIndex)
	ret.metadata = metadata
	ret.container = *skip_list.NewSkipList(buffer_pool_manager, num_buckets)
	ret.col_idx = col_idx
	return ret
}

// Return the metadata object associated with the index
func (slidx *SkipListIndexOnMem) GetMetadata() *IndexMetadata { return nil }

func (slidx *SkipListIndexOnMem) GetIndexColumnCount() uint32 {
	return math.MaxUint32
}

func (slidx *SkipListIndexOnMem) GetName() *string { return slidx.metadata.GetName() }

func (slidx *SkipListIndexOnMem) GetTupleSchema() *schema.Schema {
	return nil
}

func (slidx *SkipListIndexOnMem) GetKeyAttrs() []uint32 { return nil }

// Return the metadata object associated with the index
func (slidx *SkipListIndex) GetMetadata() *IndexMetadata { return slidx.metadata }

func (slidx *SkipListIndex) GetIndexColumnCount() uint32 {
	return slidx.metadata.GetIndexColumnCount()
}

func (slidx *SkipListIndex) GetName() *string { return slidx.metadata.GetName() }

func (slidx *SkipListIndex) GetTupleSchema() *schema.Schema {
	return slidx.metadata.GetTupleSchema()
}

func (slidx *SkipListIndex) GetKeyAttrs() []uint32 { return slidx.metadata.GetKeyAttrs() }

func (slidx *SkipListIndexOnMem) InsertEntry(key *tuple.Tuple, rid page.RID, transaction *access.Transaction) {
	tupleSchema_ := slidx.GetTupleSchema()
	keyVal := key.GetValue(tupleSchema_, slidx.col_idx)

	slidx.container.InsertOnMem(&keyVal, samehada_util.PackRIDtoUint32(&rid))
}

func (slidx *SkipListIndex) InsertEntry(key *tuple.Tuple, rid page.RID, transaction *access.Transaction) {
	tupleSchema_ := slidx.GetTupleSchema()
	keyDataInBytes := key.GetValueInBytes(tupleSchema_, slidx.col_idx)

	slidx.container.Insert(keyDataInBytes, samehada_util.PackRIDtoUint32(&rid))
}

func (slidx *SkipListIndexOnMem) DeleteEntry(key *tuple.Tuple, rid page.RID, transaction *access.Transaction) {
	tupleSchema_ := slidx.GetTupleSchema()
	keyVal := key.GetValue(tupleSchema_, slidx.col_idx)

	slidx.container.RemoveOnMem(&keyVal, samehada_util.PackRIDtoUint32(&rid))
}

func (slidx *SkipListIndex) DeleteEntry(key *tuple.Tuple, rid page.RID, transaction *access.Transaction) {
	tupleSchema_ := slidx.GetTupleSchema()
	keyDataInBytes := key.GetValueInBytes(tupleSchema_, slidx.col_idx)

	slidx.container.Remove(keyDataInBytes, samehada_util.PackRIDtoUint32(&rid))
}

func (slidx *SkipListIndexOnMem) ScanKey(key *tuple.Tuple, transaction *access.Transaction) []page.RID {
	tupleSchema_ := slidx.GetTupleSchema()
	keyVal := key.GetValue(tupleSchema_, slidx.col_idx)

	packed_value := slidx.container.GetValueOnMem(&keyVal)
	var ret_arr []page.RID
	ret_arr = append(ret_arr, samehada_util.UnpackUint32toRID(packed_value))
	return ret_arr
}

func (slidx *SkipListIndex) ScanKey(key *tuple.Tuple, transaction *access.Transaction) []page.RID {
	tupleSchema_ := slidx.GetTupleSchema()
	keyDataInBytes := key.GetValueInBytes(tupleSchema_, slidx.col_idx)

	packed_values := slidx.container.GetValue(keyDataInBytes)
	var ret_arr []page.RID
	for _, packed_val := range packed_values {
		ret_arr = append(ret_arr, samehada_util.UnpackUint32toRID(packed_val))
	}
	return ret_arr
}

// get iterator which iterates entry in key sorted order
// and iterates specified key range.
// when start_key arg is nil , start point is head of entry list. when end_key, end point is tail of the list
func (slidx *SkipListIndexOnMem) Iterator(start_key *tuple.Tuple, end_key *tuple.Tuple, transaction *access.Transaction) *skip_list.SkipListIteratorOnMem {
	tupleSchema_ := slidx.GetTupleSchema()
	var start_val *types.Value = nil
	if start_key != nil {
		start_val = samehada_util.GetPonterOfValue(start_key.GetValue(tupleSchema_, slidx.col_idx))
	}

	var end_val *types.Value = nil
	if end_key != nil {
		end_val = samehada_util.GetPonterOfValue(end_key.GetValue(tupleSchema_, slidx.col_idx))
	}

	return slidx.container.IteratorOnMem(start_val, end_val)
}
