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

type SkipListIndex struct {
	container skip_list.SkipList
	metadata  *IndexMetadata
	// idx of target column on table
	col_idx uint32
	//rwlatch common.ReaderWriterLatch
}

// TODO: (SDB) all function and methods should be modified appropriately to support key duplication

func NewSkipListIndex(metadata *IndexMetadata, buffer_pool_manager *buffer.BufferPoolManager, col_idx uint32) *SkipListIndex {
	ret := new(SkipListIndex)
	ret.metadata = metadata
	ret.container = *skip_list.NewSkipList(buffer_pool_manager, ret.metadata.GetTupleSchema().GetColumn(col_idx).GetType())
	ret.col_idx = col_idx
	//ret.rwlatch = common.NewRWLatch()
	return ret
}

func (slidx *SkipListIndex) InsertEntry(key *tuple.Tuple, rid page.RID, txn interface{}) {
	//slidx.rwlatch.WLock()
	//defer slidx.rwlatch.WUnlock()

	tupleSchema_ := slidx.GetTupleSchema()
	orgKeyVal := key.GetValue(tupleSchema_, slidx.col_idx)
	convedKeyValBytes := samehada_util.ConvValueAndRIDToDicOrderComparableBytes(&orgKeyVal, &rid)

	convedKeyVal := types.NewValueFromBytes(convedKeyValBytes, types.Varchar)

	slidx.container.Insert(convedKeyVal, samehada_util.PackRIDtoUint64(&rid))
}

func (slidx *SkipListIndex) DeleteEntry(key *tuple.Tuple, rid page.RID, txn interface{}) {
	//slidx.rwlatch.WLock()
	//defer slidx.rwlatch.WUnlock()

	tupleSchema_ := slidx.GetTupleSchema()
	orgKeyVal := key.GetValue(tupleSchema_, slidx.col_idx)
	convedKeyValBytes := samehada_util.ConvValueAndRIDToDicOrderComparableBytes(&orgKeyVal, &rid)

	convedKeyVal := types.NewValueFromBytes(convedKeyValBytes, types.Varchar)

	isSuccess := slidx.container.Remove(convedKeyVal, 0)
	if isSuccess == false {
		panic(fmt.Sprintf("SkipListIndex::DeleteEntry: %v %v\n", convedKeyVal.ToIFValue(), rid))
	}
}

func (slidx *SkipListIndex) ScanKey(key *tuple.Tuple, txn interface{}) []page.RID {
	//slidx.rwlatch.RLock()
	//defer slidx.rwlatch.RUnlock()

	tupleSchema_ := slidx.GetTupleSchema()
	orgKeyVal := key.GetValue(tupleSchema_, slidx.col_idx)
	smallestKeyValBytes := samehada_util.ConvValueAndRIDToDicOrderComparableBytes(&orgKeyVal, &page.RID{-1, 0})
	smallestKeyVal := types.NewValueFromBytes(smallestKeyValBytes, types.Varchar)
	biggestKeyValBytes := samehada_util.ConvValueAndRIDToDicOrderComparableBytes(&orgKeyVal, &page.RID{math.MaxInt32, math.MaxUint32})
	biggestKeyVal := types.NewValueFromBytes(biggestKeyValBytes, types.Varchar)

	rangeItr := slidx.container.Iterator(smallestKeyVal, biggestKeyVal)

	// TODO: (SDB) impl of this file under this line is not done yet

	ret_arr := make([]page.RID, 0)
	packed_value := slidx.container.GetValue(&keyVal)
	if packed_value != math.MaxUint64 {
		// when packed_vale == math.MaxUint32 => true, keyVal is not found on index
		ret_arr = append(ret_arr, samehada_util.UnpackUint64toRID(packed_value))
	}
	return ret_arr
}

func (slidx *SkipListIndex) UpdateEntry(oldKey *tuple.Tuple, oldRID page.RID, newKey *tuple.Tuple, newRID page.RID, txn interface{}) {
	//slidx.rwlatch.WLock()
	//defer slidx.rwlatch.WUnlock()

	slidx.DeleteEntry(oldKey, oldRID, txn)
	slidx.InsertEntry(newKey, newRID, txn)
}

// get iterator which iterates entry in key sorted order
// and iterates specified key range.
// when start_key arg is nil , start point is head of entry list. when end_key, end point is tail of the list
func (slidx *SkipListIndex) GetRangeScanIterator(start_key *tuple.Tuple, end_key *tuple.Tuple, transaction interface{}) IndexRangeScanIterator {
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
func (slidx *SkipListIndex) GetMetadata() *IndexMetadata { return slidx.metadata }

func (slidx *SkipListIndex) GetIndexColumnCount() uint32 {
	return slidx.metadata.GetIndexColumnCount()
}

func (slidx *SkipListIndex) GetName() *string { return slidx.metadata.GetName() }

func (slidx *SkipListIndex) GetTupleSchema() *schema.Schema {
	return slidx.metadata.GetTupleSchema()
}

func (slidx *SkipListIndex) GetKeyAttrs() []uint32 { return slidx.metadata.GetKeyAttrs() }

func (slidx *SkipListIndex) GetHeaderPageId() types.PageID {
	return slidx.container.GetHeaderPageId()
}
