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
	colIdx     uint32
	logManager *recovery.LogManager
	// UpdateEntry only get Write lock
	updateMtx sync.RWMutex
}

func NewSkipListIndex(metadata *IndexMetadata, bufferPoolManager *buffer.BufferPoolManager, colIdx uint32, logManager *recovery.LogManager) *SkipListIndex {
	ret := new(SkipListIndex)
	ret.metadata = metadata

	// SkipListIndex uses special technique to support key duplication with SkipList supporting unique key only
	// for the thechnique, key type is fixed to Varchar (comparison is done on dict order as byte array)
	ret.container = *skip_list.NewSkipList(bufferPoolManager, types.Varchar, logManager)
	ret.colIdx = colIdx
	ret.updateMtx = sync.RWMutex{}
	ret.logManager = logManager
	return ret
}

func (slidx *SkipListIndex) insertEntryInner(key *tuple.Tuple, rid page.RID, txn interface{}, isNoLock bool) {
	tupleSchema := slidx.GetTupleSchema()
	orgKeyVal := key.GetValue(tupleSchema, slidx.colIdx)

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
	tupleSchema := slidx.GetTupleSchema()
	orgKeyVal := key.GetValue(tupleSchema, slidx.colIdx)

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
	tupleSchema := slidx.GetTupleSchema()
	orgKeyVal := key.GetValue(tupleSchema, slidx.colIdx)
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
// when startKey arg is nil , start point is head of entry list. when endKey, end point is tail of the list
// Attention: returned itr's containing keys are string type Value which is constructed with byte arr of concatenated original key and value
func (slidx *SkipListIndex) GetRangeScanIterator(startKey *tuple.Tuple, endKey *tuple.Tuple, transaction interface{}) IndexRangeScanIterator {
	tupleSchema := slidx.GetTupleSchema()
	var smallestKeyVal *types.Value = nil
	if startKey != nil {
		orgStartKeyVal := startKey.GetValue(tupleSchema, slidx.colIdx)
		smallestKeyVal = samehada_util.EncodeValueAndRIDToDicOrderComparableVarchar(&orgStartKeyVal, &page.RID{0, 0})
	}

	var biggestKeyVal *types.Value = nil
	if endKey != nil {
		orgEndKeyVal := endKey.GetValue(tupleSchema, slidx.colIdx)
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

func (slidx *SkipListIndex) GetHeaderPageID() types.PageID {
	return slidx.container.GetHeaderPageID()
}
