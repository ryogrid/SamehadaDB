package index

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/container/skip_list"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
)

type UniqSkipListIndex struct {
	container skip_list.SkipList
	metadata  *IndexMetadata
	// idx of target column on table
	col_idx uint32
}

func NewUniqSkipListIndex(metadata *IndexMetadata, buffer_pool_manager *buffer.BufferPoolManager, col_idx uint32) *UniqSkipListIndex {
	ret := new(UniqSkipListIndex)
	ret.metadata = metadata
	ret.container = *skip_list.NewSkipList(buffer_pool_manager, ret.metadata.GetTupleSchema().GetColumn(col_idx).GetType())
	ret.col_idx = col_idx
	return ret
}

func (slidx *UniqSkipListIndex) InsertEntry(key *tuple.Tuple, rid page.RID, transaction interface{}) {
	tupleSchema_ := slidx.GetTupleSchema()
	keyVal := key.GetValue(tupleSchema_, slidx.col_idx)

	slidx.container.Insert(&keyVal, samehada_util.PackRIDtoUint64(&rid))
}

func (slidx *UniqSkipListIndex) DeleteEntry(key *tuple.Tuple, rid page.RID, transaction interface{}) {
	tupleSchema_ := slidx.GetTupleSchema()
	keyVal := key.GetValue(tupleSchema_, slidx.col_idx)

	isSuccess := slidx.container.Remove(&keyVal, samehada_util.PackRIDtoUint64(&rid))
	if isSuccess == false {
		panic(fmt.Sprintf("UniqSkipListIndex::DeleteEntry: %v %v\n", keyVal.ToIFValue(), rid))
	}
}

func (slidx *UniqSkipListIndex) ScanKey(key *tuple.Tuple, transaction interface{}) []page.RID {
	tupleSchema_ := slidx.GetTupleSchema()
	keyVal := key.GetValue(tupleSchema_, slidx.col_idx)

	ret_arr := make([]page.RID, 0)
	packed_value := slidx.container.GetValue(&keyVal)
	if packed_value != math.MaxUint64 {
		// when packed_vale == math.MaxUint32 => true, keyVal is not found on index
		ret_arr = append(ret_arr, samehada_util.UnpackUint64toRID(packed_value))
	}
	return ret_arr
}

func (slidx *UniqSkipListIndex) UpdateEntry(oldKey *tuple.Tuple, oldRID page.RID, newKey *tuple.Tuple, newRID page.RID, transaction interface{}) {
	slidx.DeleteEntry(oldKey, oldRID, transaction)
	slidx.InsertEntry(newKey, newRID, transaction)
}

// get iterator which iterates entry in key sorted order
// and iterates specified key range.
// when start_key arg is nil , start point is head of entry list. when end_key, end point is tail of the list
func (slidx *UniqSkipListIndex) GetRangeScanIterator(start_key *tuple.Tuple, end_key *tuple.Tuple, transaction interface{}) IndexRangeScanIterator {
	tupleSchema_ := slidx.GetTupleSchema()
	var start_val *types.Value = nil
	if start_key != nil {
		start_val = samehada_util.GetPonterOfValue(start_key.GetValue(tupleSchema_, slidx.col_idx))
	}

	var end_val *types.Value = nil
	if end_key != nil {
		end_val = samehada_util.GetPonterOfValue(end_key.GetValue(tupleSchema_, slidx.col_idx))
	}

	return slidx.container.Iterator(start_val, end_val)
}

// Return the metadata object associated with the index
func (slidx *UniqSkipListIndex) GetMetadata() *IndexMetadata { return slidx.metadata }

func (slidx *UniqSkipListIndex) GetIndexColumnCount() uint32 {
	return slidx.metadata.GetIndexColumnCount()
}

func (slidx *UniqSkipListIndex) GetName() *string { return slidx.metadata.GetName() }

func (slidx *UniqSkipListIndex) GetTupleSchema() *schema.Schema {
	return slidx.metadata.GetTupleSchema()
}

func (slidx *UniqSkipListIndex) GetKeyAttrs() []uint32 { return slidx.metadata.GetKeyAttrs() }

func (slidx *UniqSkipListIndex) GetHeaderPageId() types.PageID {
	return slidx.container.GetHeaderPageId()
}
