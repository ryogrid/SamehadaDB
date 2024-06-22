package skip_list

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/page/skip_list_page"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"math"
	"math/rand"
)

type SkipListOpType int32
type LatchOpCase int32

const (
	SKIP_LIST_OP_GET SkipListOpType = iota
	SKIP_LIST_OP_REMOVE
	SKIP_LIST_OP_INSERT
)

const (
	SKIP_LIST_UTIL_GET_LATCH LatchOpCase = iota
	SKIP_LIST_UTIL_UNLATCH
)

/**
 * Implementation of skip list that is backed by a buffer pool
 * manager. Non-unique keys are not supported yet. Supports insert, delete and GetRangeScanIterator.
 */

type SkipList struct {
	headerPage      *skip_list_page.SkipListHeaderPage
	startNode       *skip_list_page.SkipListBlockPage
	SentinelNodeID  types.PageID
	bpm             *buffer.BufferPoolManager
	headerPageLatch common.ReaderWriterLatch
	log_manager     *recovery.LogManager
}

func NewSkipList(bpm *buffer.BufferPoolManager, keyType types.TypeID, log_manager *recovery.LogManager) *SkipList {
	ret := new(SkipList)
	ret.bpm = bpm
	var sentinelNode *skip_list_page.SkipListBlockPage
	ret.headerPage, ret.startNode, sentinelNode = skip_list_page.NewSkipListHeaderPage(bpm, keyType)
	ret.SentinelNodeID = sentinelNode.GetPageId()
	ret.log_manager = log_manager

	return ret
}

func (sl *SkipList) getHeaderPage() *skip_list_page.SkipListHeaderPage {
	return sl.headerPage
}

func (sl *SkipList) getStartNode() *skip_list_page.SkipListBlockPage {
	return sl.startNode
}

func latchOpWithOpType(node *skip_list_page.SkipListBlockPage, getOrUnlatch LatchOpCase, opType SkipListOpType) {
	switch opType {
	case SKIP_LIST_OP_GET:
		if getOrUnlatch == SKIP_LIST_UTIL_GET_LATCH {
			node.RLatch()
			node.AddRLatchRecord(int32(-1 * opType * 1000))
		} else if getOrUnlatch == SKIP_LIST_UTIL_UNLATCH {
			node.RemoveRLatchRecord(int32(-1 * opType * 1000))
			node.RUnlatch()
		} else {
			panic("unknown latch operaton")
		}
	default:
		if getOrUnlatch == SKIP_LIST_UTIL_GET_LATCH {
			node.WLatch()
			node.AddWLatchRecord(int32(-1 * opType * 1000))
		} else if getOrUnlatch == SKIP_LIST_UTIL_UNLATCH {
			node.RemoveWLatchRecord(int32(-1 * opType * 1000))
			node.WUnlatch()
		} else {
			panic("unknown latch operaton")
		}
	}
}

// ATTENTION:
// this method returns with keep having RLatch or WLatch of corners_[0] and one pin of corners_[0]
// (when corners_[0] is startNode, having pin count is two, but caller does not have to consider the difference)
func (sl *SkipList) FindNode(key *types.Value, opType SkipListOpType) (isSuccess bool, foundNode *skip_list_page.SkipListBlockPage, predOfCorners_ []skip_list_page.SkipListCornerInfo, corners_ []skip_list_page.SkipListCornerInfo) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.DEBUG_INFO > 0 {
			common.ShPrintf(common.DEBUG_INFO, "FindNode: start. key=%v opType=%d\n", key.ToIFValue(), opType)
		}
		if common.ActiveLogKindSetting&common.BUFFER_INTERNAL_STATE > 0 {
			sl.bpm.PrintBufferUsageState("SkipList::FindNode start. ")
			defer func() {
				sl.bpm.PrintBufferUsageState("SkipList::FindNode end. ")
			}()
		}
	}

	pred := sl.getStartNode()
	latchOpWithOpType(pred, SKIP_LIST_UTIL_GET_LATCH, opType)
	sl.bpm.IncPinOfPage(pred)

	predOfPredId := types.InvalidPageID
	predOfPredLSN := types.LSN(-1)
	predOfCorners := make([]skip_list_page.SkipListCornerInfo, skip_list_page.MAX_FOWARD_LIST_LEN)
	// entry of corners is corner node or target node
	corners := make([]skip_list_page.SkipListCornerInfo, skip_list_page.MAX_FOWARD_LIST_LEN)
	var curr *skip_list_page.SkipListBlockPage = nil
	for ii := skip_list_page.MAX_FOWARD_LIST_LEN - 1; ii >= 0; ii-- {
		//fmt.Printf("level %d\n", i)
		for {
			if pred == sl.startNode && pred.GetForwardEntry(ii) == sl.SentinelNodeID {
				break
			}
			curr = skip_list_page.FetchAndCastToBlockPage(sl.bpm, pred.GetForwardEntry(int(ii)))
			if curr == nil {
				common.ShPrintf(common.FATAL, "PageID to passed FetchAndCastToBlockPage is %d\n", pred.GetForwardEntry(int(ii)))
				panic("SkipList::FindNode: FetchAndCastToBlockPage returned nil!")
			}
			latchOpWithOpType(curr, SKIP_LIST_UTIL_GET_LATCH, opType)
			if !curr.GetSmallestKey(key.ValueType()).CompareLessThanOrEqual(*key) {
				//  (ii + 1) level's corner node or target node has been identified (= pred)
				break
			} else {
				// keep moving foward
				predOfPredId = pred.GetPageId()
				predOfPredLSN = pred.GetLSN()
				if pred.GetPageId() != sl.getStartNode().GetPageId() {
					sl.bpm.UnpinPage(pred.GetPageId(), false)
				}
				latchOpWithOpType(pred, SKIP_LIST_UTIL_UNLATCH, opType)
				pred = curr
			}
		}
		if opType == SKIP_LIST_OP_REMOVE && ii != 0 && pred.GetEntryCnt() == 1 && key.CompareEquals(pred.GetSmallestKey(key.ValueType())) {
			// pred is already reached goal, so change pred to appropriate node
			common.ShPrintf(common.DEBUG_INFO, "SkipList::FindNode: node should be removed found!\n")
			predOfCorners[ii] = skip_list_page.SkipListCornerInfo{types.InvalidPageID, -1}
			corners[ii] = skip_list_page.SkipListCornerInfo{predOfPredId, predOfPredLSN}

			sl.bpm.UnpinPage(curr.GetPageId(), false)
			latchOpWithOpType(curr, SKIP_LIST_UTIL_UNLATCH, opType)

			if pred.GetPageId() != sl.getStartNode().GetPageId() {
				sl.bpm.UnpinPage(pred.GetPageId(), false)
			}
			latchOpWithOpType(pred, SKIP_LIST_UTIL_UNLATCH, opType)

			// go backward for gathering appropriate corner nodes info
			pred = skip_list_page.FetchAndCastToBlockPage(sl.bpm, predOfPredId)
			if pred == nil {
				// pred has been deallocated
				return false, nil, nil, nil
			}
			latchOpWithOpType(pred, SKIP_LIST_UTIL_GET_LATCH, opType)

			// check updating occurred or not
			afterLSN := pred.GetLSN()
			if predOfPredLSN != afterLSN {
				// updating exists
				sl.bpm.UnpinPage(pred.GetPageId(), false)
				latchOpWithOpType(pred, SKIP_LIST_UTIL_UNLATCH, opType)
				if common.EnableDebug {
					common.ShPrintf(common.DEBUG_INFO, "FindNode: finished with rety. key=%v opType=%d\n", key.ToIFValue(), opType)
					common.ShPrintf(common.DEBUG_INFO, "pred: ")
					pred.PrintMutexDebugInfo()
					common.ShPrintf(common.DEBUG_INFO, "curr: ")
					curr.PrintMutexDebugInfo()
				}
				return false, nil, nil, nil
			}
		} else {
			if curr != nil {
				sl.bpm.UnpinPage(curr.GetPageId(), false)
				latchOpWithOpType(curr, SKIP_LIST_UTIL_UNLATCH, opType)
			}
			predOfCorners[ii] = skip_list_page.SkipListCornerInfo{predOfPredId, predOfPredLSN}
			corners[ii] = skip_list_page.SkipListCornerInfo{pred.GetPageId(), pred.GetLSN()}
		}
	}

	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO, "FindNode: finished without rety. key=%v opType=%d\n", key.ToIFValue(), opType)
		common.ShPrintf(common.DEBUG_INFO, "pred: ")
		pred.PrintMutexDebugInfo()
		pred.PrintPinCount()
		common.ShPrintf(common.DEBUG_INFO, "curr: ")
		if curr != nil {
			curr.PrintMutexDebugInfo()
		}
		pred.PrintPinCount()
	}

	return true, pred, predOfCorners, corners
}

// ATTENTION:
// this method returns with keep having RLatch of corners_[0] and pinned corners_[0]
func (sl *SkipList) FindNodeWithEntryIdxForItr(key *types.Value) (found_ bool, node_ *skip_list_page.SkipListBlockPage, idx_ int32) {
	// get idx of target entry or one of nearest smaller entry
	_, node, _, _ := sl.FindNode(key, SKIP_LIST_OP_GET)

	// locking is not needed because already have lock with FindNode method call
	found, _, idx := node.FindEntryByKey(key)
	return found, node, idx
}

func (sl *SkipList) GetValue(key *types.Value) uint64 {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			common.ShPrintf(common.DEBUG_INFO, "SkipList::GetValue: start. key=%v\n", key.ToIFValue())
		}

		if common.ActiveLogKindSetting&common.BUFFER_INTERNAL_STATE > 0 {
			sl.bpm.PrintBufferUsageState("SkipList::GetValue start. ")
			defer func() {
				sl.bpm.PrintBufferUsageState("SkipList::GetValue end. ")
			}()
		}
	}
	_, node, _, _ := sl.FindNode(key, SKIP_LIST_OP_GET)
	// locking is not needed because already have lock with FindNode method call
	found, entry, _ := node.FindEntryByKey(key)
	sl.bpm.UnpinPage(node.GetPageId(), false)
	node.RemoveRLatchRecord(key.ToInteger())
	node.RUnlatch()

	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO, "SkipList::GetValue: finish. key=%v\n", key.ToIFValue())
	}
	if found {
		return entry.Value
	} else {
		return math.MaxUint64
	}
}

func (sl *SkipList) Insert(key *types.Value, value uint64) (err error) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("SkipList::Insert: start. key=%v\n", key.ToIFValue())
		}
		if common.ActiveLogKindSetting&common.BUFFER_INTERNAL_STATE > 0 {
			sl.bpm.PrintBufferUsageState("SkipList::Insert start. ")
			defer func() {
				sl.bpm.PrintBufferUsageState("SkipList::Insert end. ")
			}()
		}
	}
	isNeedRetry := true

	for isNeedRetry {
		isSuccess, node, _, corners := sl.FindNode(key, SKIP_LIST_OP_INSERT)
		if !isSuccess {
			// when isSuccess == false, all latch and pin is released already
			common.ShPrintf(common.DEBUG_INFO, "SkipList::Insert: retry. key=%v\n", key.ToIFValue())
			continue
		}
		levelWhenNodeSplitOccur := sl.GetNodeLevel()

		// locking is not needed because already have lock with FindNode method call
		isNeedRetry = node.Insert(key, value, sl.bpm, corners, levelWhenNodeSplitOccur)
		// lock and pin of node is already released on Insert method call
	}

	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO, "SkipList::Insert: finish. key=%v\n", key.ToIFValue())
	}
	return nil
}

func (sl *SkipList) Remove(key *types.Value, value uint64) (isDeleted_ bool) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("SkipList::Remove: start. key=%v\n", key.ToIFValue())
		}
		if common.ActiveLogKindSetting&common.BUFFER_INTERNAL_STATE > 0 {
			sl.bpm.PrintBufferUsageState("SkipList::Remove start. ")
			defer func() {
				sl.bpm.PrintBufferUsageState("SkipList::Remove end. ")
			}()
		}
	}
	isNodeShouldBeDeleted := false
	isDeleted := false
	isNeedRetry := true

	for isNeedRetry {
		isSuccess, node, predOfCorners, corners := sl.FindNode(key, SKIP_LIST_OP_REMOVE)
		if !isSuccess {
			// having no latch and pin here
			continue
		}

		pageId := node.GetPageId()

		// locking is not needed because already have lock with FindNode method call
		isNodeShouldBeDeleted, isDeleted, isNeedRetry = node.Remove(sl.bpm, key, predOfCorners, corners)
		// lock and pin which is got FindNode are released on Remove method

		if isNodeShouldBeDeleted {
			sl.bpm.DeallocatePage(pageId)
		}
	}

	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO, "SkipList::Remove: finish. key=%v\n", key.ToIFValue())
	}
	return isDeleted
}

func (sl *SkipList) Iterator(rangeStartKey *types.Value, rangeEndKey *types.Value) *SkipListIterator {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("SkipList::Remove: start.\n")
		}
		if common.ActiveLogKindSetting&common.BUFFER_INTERNAL_STATE > 0 {
			sl.bpm.PrintBufferUsageState("SkipList::Iterator start. ")
			defer func() {
				sl.bpm.PrintBufferUsageState("SkipList::Iterator end. ")
			}()
		}
	}
	return NewSkipListIterator(sl, rangeStartKey, rangeEndKey)
}

func (sl *SkipList) GetNodeLevel() int32 {
	var retLevel int32 = 1
	for rand.Float32() < common.SkipListProb { // no MaxLevel check
		retLevel++
	}

	ret := int32(math.Min(float64(retLevel), float64(skip_list_page.MAX_FOWARD_LIST_LEN)))

	return ret
}

func (sl *SkipList) GetHeaderPageId() types.PageID {
	return sl.headerPage.GetPageId()
}
