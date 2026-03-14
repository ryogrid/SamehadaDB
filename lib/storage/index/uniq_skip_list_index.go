package index

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/container/skip_list"
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"math"
	"sync"
)

type UniqSkipListIndex struct {
	container skip_list.SkipList
	metadata  *IndexMetadata
	// idx of target column on table
	colIdx uint32
	// UpdateEntry only get Write lock
	updateMtx sync.RWMutex
}

func NewUniqSkipListIndex(metadata *IndexMetadata, bufferPoolManager *buffer.BufferPoolManager, colIdx uint32) *UniqSkipListIndex {
	ret := new(UniqSkipListIndex)
	ret.metadata = metadata
	ret.container = *skip_list.NewSkipList(bufferPoolManager, ret.metadata.GetTupleSchema().GetColumn(colIdx).GetType(), nil)
	ret.colIdx = colIdx
	ret.updateMtx = sync.RWMutex{}
	return ret
}

func (slidx *UniqSkipListIndex) insertEntryInner(key *tuple.Tuple, rid page.RID, txn interface{}, isNoLock bool) {
	tupleSchema := slidx.GetTupleSchema()
	keyVal := key.GetValue(tupleSchema, slidx.colIdx)

	if isNoLock == false {
		slidx.updateMtx.RLock()
		defer slidx.updateMtx.RUnlock()
	}
	slidx.container.Insert(&keyVal, samehada_util.PackRIDtoUint64(&rid))
}

func (slidx *UniqSkipListIndex) InsertEntry(key *tuple.Tuple, rid page.RID, transaction interface{}) {
	slidx.insertEntryInner(key, rid, transaction, false)
}

func (slidx *UniqSkipListIndex) deleteEntryInner(key *tuple.Tuple, rid page.RID, txn interface{}, isNoLock bool) {
	tupleSchema := slidx.GetTupleSchema()
	keyVal := key.GetValue(tupleSchema, slidx.colIdx)

	if isNoLock == false {
		slidx.updateMtx.RLock()
		defer slidx.updateMtx.RUnlock()
	}
	isSuccess := slidx.container.Remove(&keyVal, samehada_util.PackRIDtoUint64(&rid))
	if isSuccess == false {
		panic(fmt.Sprintf("UniqSkipListIndex::deleteEntryInner: %v %v\n", keyVal.ToIFValue(), rid))
	}
}

func (slidx *UniqSkipListIndex) DeleteEntry(key *tuple.Tuple, rid page.RID, transaction interface{}) {
	slidx.deleteEntryInner(key, rid, transaction, false)
}

func (slidx *UniqSkipListIndex) ScanKey(key *tuple.Tuple, transaction interface{}) []page.RID {
	tupleSchema := slidx.GetTupleSchema()
	keyVal := key.GetValue(tupleSchema, slidx.colIdx)

	retArr := make([]page.RID, 0)
	slidx.updateMtx.RLock()
	packedValue := slidx.container.GetValue(&keyVal)
	slidx.updateMtx.RUnlock()
	if packedValue != math.MaxUint64 {
		// when packed_vale == math.MaxUint32 => true, keyVal is not found on index
		retArr = append(retArr, samehada_util.UnpackUint64toRID(packedValue))
	}
	return retArr
}

func (slidx *UniqSkipListIndex) UpdateEntry(oldKey *tuple.Tuple, oldRID page.RID, newKey *tuple.Tuple, newRID page.RID, transaction interface{}) {
	slidx.updateMtx.Lock()
	defer slidx.updateMtx.Unlock()
	slidx.deleteEntryInner(oldKey, oldRID, transaction, true)
	slidx.insertEntryInner(newKey, newRID, transaction, true)
}

// get iterator which iterates entry in key sorted order
// and iterates specified key range.
// when startKey arg is nil , start point is head of entry list. when endKey, end point is tail of the list
func (slidx *UniqSkipListIndex) GetRangeScanIterator(startKey *tuple.Tuple, endKey *tuple.Tuple, transaction interface{}) IndexRangeScanIterator {
	tupleSchema := slidx.GetTupleSchema()
	var startVal *types.Value = nil
	if startKey != nil {
		startVal = samehada_util.GetPonterOfValue(startKey.GetValue(tupleSchema, slidx.colIdx))
	}

	var endVal *types.Value = nil
	if endKey != nil {
		endVal = samehada_util.GetPonterOfValue(endKey.GetValue(tupleSchema, slidx.colIdx))
	}

	slidx.updateMtx.RLock()
	defer slidx.updateMtx.RUnlock()
	return slidx.container.Iterator(startVal, endVal)
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

func (slidx *UniqSkipListIndex) GetHeaderPageID() types.PageID {
	return slidx.container.GetHeaderPageID()
}
