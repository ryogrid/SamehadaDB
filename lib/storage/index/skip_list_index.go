package index

import (
	"github.com/ryogrid/SamehadaDB/lib/container/skip_list"
	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"math"
	"sync"
)

type SkipListIndex struct {
	container skip_list.SkipList
	metadata  *IndexMetadata
	// idx of target column on table
	col_idx     uint32
	log_manager *recovery.LogManager
	// UpdateEntry only get Write lock
	updateMtx sync.RWMutex
}

func NewSkipListIndex(metadata *IndexMetadata, buffer_pool_manager *buffer.BufferPoolManager, col_idx uint32, log_manager *recovery.LogManager) *SkipListIndex {
	ret := new(SkipListIndex)
	ret.metadata = metadata

	// SkipListIndex uses special technique to support key duplication with SkipList supporting unique key only
	// for the thechnique, key type is fixed to Varchar (comparison is done on dict order as byte array)
	ret.container = *skip_list.NewSkipList(buffer_pool_manager, types.Varchar, log_manager)
	ret.col_idx = col_idx
	ret.updateMtx = sync.RWMutex{}
	ret.log_manager = log_manager
	return ret
}

func (slidx *SkipListIndex) insertEntryInner(key *tuple.Tuple, rid page.RID, txn interface{}, isNoLock bool) {
	tupleSchema_ := slidx.GetTupleSchema()
	orgKeyVal := key.GetValue(tupleSchema_, slidx.col_idx)

	convedKeyVal := samehada_util.EncodeValueAndRIDToDicOrderComparableVarchar(&orgKeyVal, &rid)

	if isNoLock == false {
		slidx.updateMtx.RLock()
		defer slidx.updateMtx.RUnlock()
	}
	slidx.container.Insert(convedKeyVal, samehada_util.PackRIDtoUint64(&rid))
}

func (slidx *SkipListIndex) InsertEntry(key *tuple.Tuple, rid page.RID, txn interface{}) {
	slidx.insertEntryInner(key, rid, txn, false)
}

func (slidx *SkipListIndex) deleteEntryInner(key *tuple.Tuple, rid page.RID, txn interface{}, isNoLock bool) {
	tupleSchema_ := slidx.GetTupleSchema()
	orgKeyVal := key.GetValue(tupleSchema_, slidx.col_idx)

	convedKeyVal := samehada_util.EncodeValueAndRIDToDicOrderComparableVarchar(&orgKeyVal, &rid)

	//revertedOrgKey := samehada_util.ExtractOrgKeyFromDicOrderComparableEncodedVarchar(convedKeyVal, orgKeyVal.ValueType())
	//if !revertedOrgKey.CompareEquals(orgKeyVal) {
	//	panic("key conversion may fail!")
	//}

	if isNoLock == false {
		slidx.updateMtx.RLock()
		defer slidx.updateMtx.RUnlock()
	}
	isSuccess := slidx.container.Remove(convedKeyVal, 0)
	if isSuccess == false {
		//panic(fmt.Sprintf("SkipListIndex::deleteEntryInner: %v %v\n", convedKeyVal.ToIFValue(), rid))
	}
}

func (slidx *SkipListIndex) DeleteEntry(key *tuple.Tuple, rid page.RID, txn interface{}) {
	slidx.deleteEntryInner(key, rid, txn, false)
}

func (slidx *SkipListIndex) ScanKey(key *tuple.Tuple, txn interface{}) []page.RID {
	tupleSchema_ := slidx.GetTupleSchema()
	orgKeyVal := key.GetValue(tupleSchema_, slidx.col_idx)
	smallestKeyVal := samehada_util.EncodeValueAndRIDToDicOrderComparableVarchar(&orgKeyVal, &page.RID{0, 0})
	biggestKeyVal := samehada_util.EncodeValueAndRIDToDicOrderComparableVarchar(&orgKeyVal, &page.RID{math.MaxInt32, math.MaxUint32})

	slidx.updateMtx.RLock()
	// Attention: returned itr's containing keys are string type Value which is constructed with byte arr of concatenated  original key and value
	rangeItr := slidx.container.Iterator(smallestKeyVal, biggestKeyVal)

	retArr := make([]page.RID, 0)
	for done, _, _, rid := rangeItr.Next(); !done; done, _, _, rid = rangeItr.Next() {
		retArr = append(retArr, *rid)
	}
	slidx.updateMtx.RUnlock()

	return retArr
}

func (slidx *SkipListIndex) UpdateEntry(oldKey *tuple.Tuple, oldRID page.RID, newKey *tuple.Tuple, newRID page.RID, txn interface{}) {
	slidx.updateMtx.Lock()
	defer slidx.updateMtx.Unlock()
	slidx.deleteEntryInner(oldKey, oldRID, txn, true)
	slidx.insertEntryInner(newKey, newRID, txn, true)
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
		smallestKeyVal = samehada_util.EncodeValueAndRIDToDicOrderComparableVarchar(&orgStartKeyVal, &page.RID{0, 0})
	}

	var biggestKeyVal *types.Value = nil
	if end_key != nil {
		orgEndKeyVal := end_key.GetValue(tupleSchema_, slidx.col_idx)
		biggestKeyVal = samehada_util.EncodeValueAndRIDToDicOrderComparableVarchar(&orgEndKeyVal, &page.RID{math.MaxInt32, math.MaxUint32})
	}

	slidx.updateMtx.RLock()
	defer slidx.updateMtx.RUnlock()
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
