package index

import (
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/container/btree"
	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
	blink_tree "github.com/ryogrid/bltree-go-for-embedding"
	"math"
	"sync"
)

type BTreeIndex struct {
	container *btree.BLTreeWrapper
	metadata  *IndexMetadata
	// idx of target column on table
	col_idx     uint32
	log_manager *recovery.LogManager
	// UpdateEntry only get Write lock
	updateMtx sync.RWMutex
}

func NewBTreeIndex(metadata *IndexMetadata, buffer_pool_manager *buffer.BufferPoolManager, col_idx uint32, log_manager *recovery.LogManager) *BTreeIndex {
	ret := new(BTreeIndex)
	ret.metadata = metadata

	// BTreeIndex uses special technique to support key duplication with SkipList supporting unique key only
	// for the thechnique, key type is fixed to Varchar (comparison is done on dict order as byte array)
	//ret.container = *skip_list.NewSkipList(buffer_pool_manager, types.Varchar, log_manager)
	ret.container = btree.NewBLTreeWrapper(blink_tree.NewBLTree(blink_tree.NewBufMgr(12, blink_tree.HASH_TABLE_ENTRY_CHAIN_LEN*common.MaxTxnThreadNum*2, btree.NewParentBufMgrImpl(buffer_pool_manager), nil)))
	ret.col_idx = col_idx
	ret.updateMtx = sync.RWMutex{}
	ret.log_manager = log_manager
	return ret
}

func (btidx *BTreeIndex) insertEntryInner(key *tuple.Tuple, rid page.RID, txn interface{}, isNoLock bool) {
	tupleSchema_ := btidx.GetTupleSchema()
	orgKeyVal := key.GetValue(tupleSchema_, btidx.col_idx)

	convedKeyVal := samehada_util.EncodeValueAndRIDToDicOrderComparableVarchar(&orgKeyVal, &rid)

	if isNoLock == false {
		btidx.updateMtx.RLock()
		defer btidx.updateMtx.RUnlock()
	}
	btidx.container.Insert(convedKeyVal, samehada_util.PackRIDtoUint64(&rid))
}

func (btidx *BTreeIndex) InsertEntry(key *tuple.Tuple, rid page.RID, txn interface{}) {
	btidx.insertEntryInner(key, rid, txn, false)
}

func (btidx *BTreeIndex) deleteEntryInner(key *tuple.Tuple, rid page.RID, txn interface{}, isNoLock bool) {
	tupleSchema_ := btidx.GetTupleSchema()
	orgKeyVal := key.GetValue(tupleSchema_, btidx.col_idx)

	convedKeyVal := samehada_util.EncodeValueAndRIDToDicOrderComparableVarchar(&orgKeyVal, &rid)

	//revertedOrgKey := samehada_util.ExtractOrgKeyFromDicOrderComparableEncodedVarchar(convedKeyVal, orgKeyVal.ValueType())
	//if !revertedOrgKey.CompareEquals(orgKeyVal) {
	//	panic("key conversion may fail!")
	//}

	if isNoLock == false {
		btidx.updateMtx.RLock()
		defer btidx.updateMtx.RUnlock()
	}
	isSuccess := btidx.container.Remove(convedKeyVal, 0)
	if isSuccess == false {
		//panic(fmt.Sprintf("BTreeIndex::deleteEntryInner: %v %v\n", convedKeyVal.ToIFValue(), rid))
	}
}

func (btidx *BTreeIndex) DeleteEntry(key *tuple.Tuple, rid page.RID, txn interface{}) {
	btidx.deleteEntryInner(key, rid, txn, false)
}

func (btidx *BTreeIndex) ScanKey(key *tuple.Tuple, txn interface{}) []page.RID {
	tupleSchema_ := btidx.GetTupleSchema()
	orgKeyVal := key.GetValue(tupleSchema_, btidx.col_idx)
	smallestKeyVal := samehada_util.EncodeValueAndRIDToDicOrderComparableVarchar(&orgKeyVal, &page.RID{0, 0})
	biggestKeyVal := samehada_util.EncodeValueAndRIDToDicOrderComparableVarchar(&orgKeyVal, &page.RID{math.MaxInt32, math.MaxUint32})

	btidx.updateMtx.RLock()
	// Attention: returned itr's containing keys are string type Value which is constructed with byte arr of concatenated  original key and value
	rangeItr := btidx.container.Iterator(smallestKeyVal, biggestKeyVal)

	retArr := make([]page.RID, 0)
	for done, _, _, rid := rangeItr.Next(); !done; done, _, _, rid = rangeItr.Next() {
		retArr = append(retArr, *rid)
	}
	btidx.updateMtx.RUnlock()

	return retArr
}

func (btidx *BTreeIndex) UpdateEntry(oldKey *tuple.Tuple, oldRID page.RID, newKey *tuple.Tuple, newRID page.RID, txn interface{}) {
	btidx.updateMtx.Lock()
	defer btidx.updateMtx.Unlock()
	btidx.deleteEntryInner(oldKey, oldRID, txn, true)
	btidx.insertEntryInner(newKey, newRID, txn, true)
}

// get iterator which iterates entry in key sorted order
// and iterates specified key range.
// when start_key arg is nil , start point is head of entry list. when end_key, end point is tail of the list
// Attention: returned itr's containing keys are string type Value which is constructed with byte arr of concatenated original key and value
func (btidx *BTreeIndex) GetRangeScanIterator(start_key *tuple.Tuple, end_key *tuple.Tuple, transaction interface{}) IndexRangeScanIterator {
	tupleSchema_ := btidx.GetTupleSchema()
	var smallestKeyVal *types.Value = nil
	if start_key != nil {
		orgStartKeyVal := start_key.GetValue(tupleSchema_, btidx.col_idx)
		smallestKeyVal = samehada_util.EncodeValueAndRIDToDicOrderComparableVarchar(&orgStartKeyVal, &page.RID{0, 0})
	}

	var biggestKeyVal *types.Value = nil
	if end_key != nil {
		orgEndKeyVal := end_key.GetValue(tupleSchema_, btidx.col_idx)
		biggestKeyVal = samehada_util.EncodeValueAndRIDToDicOrderComparableVarchar(&orgEndKeyVal, &page.RID{math.MaxInt32, math.MaxUint32})
	}

	btidx.updateMtx.RLock()
	defer btidx.updateMtx.RUnlock()
	return btidx.container.Iterator(smallestKeyVal, biggestKeyVal)
}

// Return the metadata object associated with the index
func (btidx *BTreeIndex) GetMetadata() *IndexMetadata { return btidx.metadata }

func (btidx *BTreeIndex) GetIndexColumnCount() uint32 {
	return btidx.metadata.GetIndexColumnCount()
}

func (btidx *BTreeIndex) GetName() *string { return btidx.metadata.GetName() }

func (btidx *BTreeIndex) GetTupleSchema() *schema.Schema {
	return btidx.metadata.GetTupleSchema()
}

func (btidx *BTreeIndex) GetKeyAttrs() []uint32 { return btidx.metadata.GetKeyAttrs() }

//func (slidx *BTreeIndex) GetHeaderPageId() types.PageID {
//	return slidx.container.GetHeaderPageId()
//}
