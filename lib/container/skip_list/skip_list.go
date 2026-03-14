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
	SkipListOpGet SkipListOpType = iota
	SkipListOpRemove
	SkipListOpInsert
)

const (
	SkipListUtilGetLatch LatchOpCase = iota
	SkipListUtilUnlatch
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
	logManager     *recovery.LogManager
}

func NewSkipList(bpm *buffer.BufferPoolManager, keyType types.TypeID, logManager *recovery.LogManager) *SkipList {
	ret := new(SkipList)
	ret.bpm = bpm
	var sentinelNode *skip_list_page.SkipListBlockPage
	ret.headerPage, ret.startNode, sentinelNode = skip_list_page.NewSkipListHeaderPage(bpm, keyType)
	ret.SentinelNodeID = sentinelNode.GetPageID()
	ret.logManager = logManager

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
	case SkipListOpGet:
		if getOrUnlatch == SkipListUtilGetLatch {
			node.RLatch()
			node.AddRLatchRecord(int32(-1 * opType * 1000))
		} else if getOrUnlatch == SkipListUtilUnlatch {
			node.RemoveRLatchRecord(int32(-1 * opType * 1000))
			node.RUnlatch()
		} else {
			panic("unknown latch operaton")
		}
	default:
		if getOrUnlatch == SkipListUtilGetLatch {
			node.WLatch()
			node.AddWLatchRecord(int32(-1 * opType * 1000))
		} else if getOrUnlatch == SkipListUtilUnlatch {
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
func (sl *SkipList) FindNode(key *types.Value, opType SkipListOpType) (isSuccess bool, foundNode *skip_list_page.SkipListBlockPage, predOfCornersVal []skip_list_page.SkipListCornerInfo, cornersVal []skip_list_page.SkipListCornerInfo) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.DebugInfo > 0 {
			common.ShPrintf(common.DebugInfo, "FindNode: start. key=%v opType=%d\n", key.ToIFValue(), opType)
		}
		if common.ActiveLogKindSetting&common.BufferInternalState > 0 {
			sl.bpm.PrintBufferUsageState("SkipList::FindNode start. ")
			defer func() {
				sl.bpm.PrintBufferUsageState("SkipList::FindNode end. ")
			}()
		}
	}

	pred := sl.getStartNode()
	latchOpWithOpType(pred, SkipListUtilGetLatch, opType)
	sl.bpm.IncPinOfPage(pred)

	predOfPredId := types.InvalidPageID
	predOfPredLSN := types.LSN(-1)
	predOfCorners := make([]skip_list_page.SkipListCornerInfo, skip_list_page.MaxForwardListLen)
	// entry of corners is corner node or target node
	corners := make([]skip_list_page.SkipListCornerInfo, skip_list_page.MaxForwardListLen)
	var curr *skip_list_page.SkipListBlockPage = nil
	for ii := skip_list_page.MaxForwardListLen - 1; ii >= 0; ii-- {
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
			latchOpWithOpType(curr, SkipListUtilGetLatch, opType)
			if !curr.GetSmallestKey(key.ValueType()).CompareLessThanOrEqual(*key) {
				//  (ii + 1) level's corner node or target node has been identified (= pred)
				break
			} else {
				// keep moving foward
				predOfPredId = pred.GetPageID()
				predOfPredLSN = pred.GetLSN()
				if pred.GetPageID() != sl.getStartNode().GetPageID() {
					sl.bpm.UnpinPage(pred.GetPageID(), false)
				}
				latchOpWithOpType(pred, SkipListUtilUnlatch, opType)
				pred = curr
			}
		}
		if opType == SkipListOpRemove && ii != 0 && pred.GetEntryCnt() == 1 && key.CompareEquals(pred.GetSmallestKey(key.ValueType())) {
			// pred is already reached goal, so change pred to appropriate node
			common.ShPrintf(common.DebugInfo, "SkipList::FindNode: node should be removed found!\n")
			predOfCorners[ii] = skip_list_page.SkipListCornerInfo{types.InvalidPageID, -1}
			corners[ii] = skip_list_page.SkipListCornerInfo{predOfPredId, predOfPredLSN}

			sl.bpm.UnpinPage(curr.GetPageID(), false)
			latchOpWithOpType(curr, SkipListUtilUnlatch, opType)

			if pred.GetPageID() != sl.getStartNode().GetPageID() {
				sl.bpm.UnpinPage(pred.GetPageID(), false)
			}
			latchOpWithOpType(pred, SkipListUtilUnlatch, opType)

			// go backward for gathering appropriate corner nodes info
			pred = skip_list_page.FetchAndCastToBlockPage(sl.bpm, predOfPredId)
			if pred == nil {
				// pred has been deallocated
				return false, nil, nil, nil
			}
			latchOpWithOpType(pred, SkipListUtilGetLatch, opType)

			// check updating occurred or not
			afterLSN := pred.GetLSN()
			if predOfPredLSN != afterLSN {
				// updating exists
				sl.bpm.UnpinPage(pred.GetPageID(), false)
				latchOpWithOpType(pred, SkipListUtilUnlatch, opType)
				if common.EnableDebug {
					common.ShPrintf(common.DebugInfo, "FindNode: finished with rety. key=%v opType=%d\n", key.ToIFValue(), opType)
					common.ShPrintf(common.DebugInfo, "pred: ")
					pred.PrintMutexDebugInfo()
					common.ShPrintf(common.DebugInfo, "curr: ")
					curr.PrintMutexDebugInfo()
				}
				return false, nil, nil, nil
			}
		} else {
			if curr != nil {
				sl.bpm.UnpinPage(curr.GetPageID(), false)
				latchOpWithOpType(curr, SkipListUtilUnlatch, opType)
			}
			predOfCorners[ii] = skip_list_page.SkipListCornerInfo{predOfPredId, predOfPredLSN}
			corners[ii] = skip_list_page.SkipListCornerInfo{pred.GetPageID(), pred.GetLSN()}
		}
	}

	if common.EnableDebug {
		common.ShPrintf(common.DebugInfo, "FindNode: finished without rety. key=%v opType=%d\n", key.ToIFValue(), opType)
		common.ShPrintf(common.DebugInfo, "pred: ")
		pred.PrintMutexDebugInfo()
		pred.PrintPinCount()
		common.ShPrintf(common.DebugInfo, "curr: ")
		if curr != nil {
			curr.PrintMutexDebugInfo()
		}
		pred.PrintPinCount()
	}

	return true, pred, predOfCorners, corners
}

// ATTENTION:
// this method returns with keep having RLatch of corners_[0] and pinned corners_[0]
func (sl *SkipList) FindNodeWithEntryIdxForItr(key *types.Value) (foundVal bool, nodeVal *skip_list_page.SkipListBlockPage, idxVal int32) {
	// get idx of target entry or one of nearest smaller entry
	_, node, _, _ := sl.FindNode(key, SkipListOpGet)

	// locking is not needed because already have lock with FindNode method call
	found, _, idx := node.FindEntryByKey(key)
	return found, node, idx
}

func (sl *SkipList) GetValue(key *types.Value) uint64 {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			common.ShPrintf(common.DebugInfo, "SkipList::GetValue: start. key=%v\n", key.ToIFValue())
		}

		if common.ActiveLogKindSetting&common.BufferInternalState > 0 {
			sl.bpm.PrintBufferUsageState("SkipList::GetValue start. ")
			defer func() {
				sl.bpm.PrintBufferUsageState("SkipList::GetValue end. ")
			}()
		}
	}
	_, node, _, _ := sl.FindNode(key, SkipListOpGet)
	// locking is not needed because already have lock with FindNode method call
	found, entry, _ := node.FindEntryByKey(key)
	sl.bpm.UnpinPage(node.GetPageID(), false)
	node.RemoveRLatchRecord(key.ToInteger())
	node.RUnlatch()

	if common.EnableDebug {
		common.ShPrintf(common.DebugInfo, "SkipList::GetValue: finish. key=%v\n", key.ToIFValue())
	}
	if found {
		return entry.Value
	} else {
		return math.MaxUint64
	}
}

func (sl *SkipList) Insert(key *types.Value, value uint64) (err error) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("SkipList::Insert: start. key=%v\n", key.ToIFValue())
		}
		if common.ActiveLogKindSetting&common.BufferInternalState > 0 {
			sl.bpm.PrintBufferUsageState("SkipList::Insert start. ")
			defer func() {
				sl.bpm.PrintBufferUsageState("SkipList::Insert end. ")
			}()
		}
	}
	isNeedRetry := true

	for isNeedRetry {
		isSuccess, node, _, corners := sl.FindNode(key, SkipListOpInsert)
		if !isSuccess {
			// when isSuccess == false, all latch and pin is released already
			common.ShPrintf(common.DebugInfo, "SkipList::Insert: retry. key=%v\n", key.ToIFValue())
			continue
		}
		levelWhenNodeSplitOccur := sl.GetNodeLevel()

		// locking is not needed because already have lock with FindNode method call
		isNeedRetry = node.Insert(key, value, sl.bpm, corners, levelWhenNodeSplitOccur)
		// lock and pin of node is already released on Insert method call
	}

	if common.EnableDebug {
		common.ShPrintf(common.DebugInfo, "SkipList::Insert: finish. key=%v\n", key.ToIFValue())
	}
	return nil
}

func (sl *SkipList) Remove(key *types.Value, value uint64) (isDeletedVal bool) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("SkipList::Remove: start. key=%v\n", key.ToIFValue())
		}
		if common.ActiveLogKindSetting&common.BufferInternalState > 0 {
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
		isSuccess, node, predOfCorners, corners := sl.FindNode(key, SkipListOpRemove)
		if !isSuccess {
			// having no latch and pin here
			continue
		}

		pageID := node.GetPageID()

		// locking is not needed because already have lock with FindNode method call
		isNodeShouldBeDeleted, isDeleted, isNeedRetry = node.Remove(sl.bpm, key, predOfCorners, corners)
		// lock and pin which is got FindNode are released on Remove method

		if isNodeShouldBeDeleted {
			sl.bpm.DeallocatePage(pageID, false)
		}
	}

	if common.EnableDebug {
		common.ShPrintf(common.DebugInfo, "SkipList::Remove: finish. key=%v\n", key.ToIFValue())
	}
	return isDeleted
}

func (sl *SkipList) Iterator(rangeStartKey *types.Value, rangeEndKey *types.Value) *SkipListIterator {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("SkipList::Remove: start.\n")
		}
		if common.ActiveLogKindSetting&common.BufferInternalState > 0 {
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

	ret := int32(math.Min(float64(retLevel), float64(skip_list_page.MaxForwardListLen)))

	return ret
}

func (sl *SkipList) GetHeaderPageID() types.PageID {
	return sl.headerPage.GetPageID()
}
