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

func NewSkipListIndex(metadata *IndexMetadata, buffer_pool_manager *buffer.BufferPoolManager, col_idx uint32) *SkipListIndex {
	ret := new(SkipListIndex)
	ret.metadata = metadata
	//ret.container = *skip_list.NewSkipList(buffer_pool_manager, ret.metadata.GetTupleSchema().GetColumn(col_idx).GetType())

	// SkipListIndex uses special technique to support key duplication with SkipList supporting unique key only
	// for the thechnique, key type is fixed to Varchar (comparison is done on dict order as byte array)
	ret.container = *skip_list.NewSkipList(buffer_pool_manager, types.Varchar)
	ret.col_idx = col_idx
	//ret.rwlatch = common.NewRWLatch()
	return ret
}

func (slidx *SkipListIndex) InsertEntry(key *tuple.Tuple, rid page.RID, txn interface{}) {
	//slidx.rwlatch.WLock()
	//defer slidx.rwlatch.WUnlock()

	tupleSchema_ := slidx.GetTupleSchema()
	orgKeyVal := key.GetValue(tupleSchema_, slidx.col_idx)

	convedKeyVal := samehada_util.EncodeValueAndRIDToDicOrderComparableBytes(&orgKeyVal, &rid)

	slidx.container.Insert(convedKeyVal, samehada_util.PackRIDtoUint64(&rid))
}

func (slidx *SkipListIndex) DeleteEntry(key *tuple.Tuple, rid page.RID, txn interface{}) {
	//slidx.rwlatch.WLock()
	//defer slidx.rwlatch.WUnlock()

	tupleSchema_ := slidx.GetTupleSchema()
	orgKeyVal := key.GetValue(tupleSchema_, slidx.col_idx)

	convedKeyVal := samehada_util.EncodeValueAndRIDToDicOrderComparableBytes(&orgKeyVal, &rid)

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
	smallestKeyVal := samehada_util.EncodeValueAndRIDToDicOrderComparableBytes(&orgKeyVal, &page.RID{0, 0})
	biggestKeyVal := samehada_util.EncodeValueAndRIDToDicOrderComparableBytes(&orgKeyVal, &page.RID{math.MaxInt32, math.MaxUint32})

	// Attention: returned itr's containing keys are string type Value which is constructed with byte arr of concatenated  original key and value
	rangeItr := slidx.container.Iterator(smallestKeyVal, biggestKeyVal)

	retArr := make([]page.RID, 0)
	for done, _, _, rid := rangeItr.Next(); !done; done, _, _, rid = rangeItr.Next() {
		retArr = append(retArr, *rid)
	}

	return retArr
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
// Attention: returned itr's containing keys are string type Value which is constructed with byte arr of concatenated original key and value
func (slidx *SkipListIndex) GetRangeScanIterator(start_key *tuple.Tuple, end_key *tuple.Tuple, transaction interface{}) IndexRangeScanIterator {
	tupleSchema_ := slidx.GetTupleSchema()
	var smallestKeyVal *types.Value = nil
	if start_key != nil {
		orgStartKeyVal := start_key.GetValue(tupleSchema_, slidx.col_idx)
		smallestKeyVal = samehada_util.EncodeValueAndRIDToDicOrderComparableBytes(&orgStartKeyVal, &page.RID{0, 0})
	}

	var biggestKeyVal *types.Value = nil
	if end_key != nil {
		orgEndKeyVal := end_key.GetValue(tupleSchema_, slidx.col_idx)
		biggestKeyVal = samehada_util.EncodeValueAndRIDToDicOrderComparableBytes(&orgEndKeyVal, &page.RID{math.MaxInt32, math.MaxUint32})
	}

	return slidx.container.Iterator(smallestKeyVal, biggestKeyVal)
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
