package skip_list

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	"github.com/ryogrid/SamehadaDB/types"
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
 * manager. Non-unique keys are not supported yet. Supports insert, delete and Iterator.
 */

type SkipList struct {
	//headerPageID_   types.PageID //*skip_list_page.SkipListHeaderPage
	headerPage      *skip_list_page.SkipListHeaderPage
	startNode       *skip_list_page.SkipListBlockPage
	SentinelNodeID  types.PageID
	bpm             *buffer.BufferPoolManager
	headerPageLatch common.ReaderWriterLatch
}

func NewSkipList(bpm *buffer.BufferPoolManager, keyType types.TypeID) *SkipList {
	//rand.Seed(time.Now().UnixNano())
	//rand.Seed(777)

	ret := new(SkipList)
	ret.bpm = bpm
	var sentinelNode *skip_list_page.SkipListBlockPage
	ret.headerPage, ret.startNode, sentinelNode = skip_list_page.NewSkipListHeaderPage(bpm, keyType) //header.ID()
	ret.SentinelNodeID = sentinelNode.GetPageId()

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
		} else if getOrUnlatch == SKIP_LIST_UTIL_UNLATCH {
			node.RUnlatch()
		} else {
			panic("unknown latch operaton")
		}
	default:
		if getOrUnlatch == SKIP_LIST_UTIL_GET_LATCH {
			node.RLatch()
		} else if getOrUnlatch == SKIP_LIST_UTIL_UNLATCH {
			node.RUnlatch()
		} else {
			panic("unknown latch operaton")
		}
	}
}

// ATTENTION:
// this method returns with keep having RLatch or WLatch of corners_[0] and not Unping corners_[0]
func (sl *SkipList) FindNode(key *types.Value, opType SkipListOpType) (isSuccess bool, foundNode *skip_list_page.SkipListBlockPage, predOfCorners_ []skip_list_page.SkipListCornerInfo, corners_ []skip_list_page.SkipListCornerInfo) {
	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO, "FindNode: start. key=%v opType=%d\n", key.ToIFValue(), opType)
	}

	//headerPage := skip_list_page.FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)
	//headerPage := sl.getHeaderPage()
	//startPageId := headerPage.GetListStartPageId()
	// lock of headerPage is not needed becaus its content is not changed
	//sl.bpm.UnpinPage(headerPage.GetPageId(), false)

	//pred := skip_list_page.FetchAndCastToBlockPage(sl.bpm, sl.headerPage.GetListStartPageId())
	pred := sl.getStartNode()
	latchOpWithOpType(pred, SKIP_LIST_UTIL_GET_LATCH, opType)
	//pred.IncPinCount()
	sl.bpm.IncPinOfPage(pred)

	// loop invariant: pred.key < searchKey
	//fmt.Println("---")
	//fmt.Println(key.ToInteger())
	//moveCnt := 0
	predOfPredId := types.InvalidPageID
	predOfPredLSN := types.LSN(-1)
	predOfCorners := make([]skip_list_page.SkipListCornerInfo, skip_list_page.MAX_FOWARD_LIST_LEN)
	// entry of corners is corner node or target node
	corners := make([]skip_list_page.SkipListCornerInfo, skip_list_page.MAX_FOWARD_LIST_LEN)
	var curr *skip_list_page.SkipListBlockPage = nil
	for ii := (skip_list_page.MAX_FOWARD_LIST_LEN - 1); ii >= 0; ii-- {
		//fmt.Printf("level %d\n", i)
		for {
			//moveCnt++
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

			// memory for checking update existence while no latch having period
			beforeLSN := pred.GetLSN()
			beforePredId := pred.GetPageId()
			if pred.GetPageId() != sl.getStartNode().GetPageId() {
				sl.bpm.UnpinPage(pred.GetPageId(), false)
			}
			latchOpWithOpType(pred, SKIP_LIST_UTIL_UNLATCH, opType)

			// go backward for gathering appropriate corner nodes info
			pred = skip_list_page.FetchAndCastToBlockPage(sl.bpm, predOfPredId)
			latchOpWithOpType(pred, SKIP_LIST_UTIL_GET_LATCH, opType)

			// check updating occurred or not
			beforePred := skip_list_page.FetchAndCastToBlockPage(sl.bpm, beforePredId)
			latchOpWithOpType(beforePred, SKIP_LIST_UTIL_GET_LATCH, opType)
			afterLSN := beforePred.GetLSN()
			// check update state of beforePred (pred which was pred before sliding)
			if beforeLSN != afterLSN {
				// updating exists
				sl.bpm.UnpinPage(pred.GetPageId(), false)
				latchOpWithOpType(pred, SKIP_LIST_UTIL_UNLATCH, opType)
				sl.bpm.UnpinPage(beforePred.GetPageId(), false)
				latchOpWithOpType(beforePred, SKIP_LIST_UTIL_UNLATCH, opType)
				if common.EnableDebug {
					common.ShPrintf(common.DEBUG_INFO, "FindNode: finished with rety. key=%v opType=%d\n", key.ToIFValue(), opType)
					common.ShPrintf(common.DEBUG_INFO, "pred: ")
					pred.PrintMutexDebugInfo()
					common.ShPrintf(common.DEBUG_INFO, "curr: ")
					curr.PrintMutexDebugInfo()
				}
				return false, nil, nil, nil
			}
			sl.bpm.UnpinPage(beforePred.GetPageId(), false)
			latchOpWithOpType(beforePred, SKIP_LIST_UTIL_UNLATCH, opType)
		} else {
			if curr != nil {
				sl.bpm.UnpinPage(curr.GetPageId(), false)
				latchOpWithOpType(curr, SKIP_LIST_UTIL_UNLATCH, opType)
			}
			if ii == 0 {
				if opType != SKIP_LIST_OP_GET {
					// try upgrade lock from RLock to WRlock

					origLSN := pred.GetLSN()
					predPageId := pred.GetPageId()
					pred.RUnlatch()

					pred := skip_list_page.FetchAndCastToBlockPage(sl.bpm, predPageId)
					pred.WLatch()
					// check update
					if pred.GetLSN() != origLSN {
						// pred node is updated, so need retry

						// originaly having pin
						//sl.bpm.UnpinPage(pred.GetPageId(), false)
						//pred.DecPinCount()
						sl.bpm.DecPinOfPage(pred)
						// additionaly got pin at Fetch
						sl.bpm.UnpinPage(pred.GetPageId(), false)
						pred.WUnlatch()
						return false, nil, nil, nil
					}
					// additionaly got pin at Fetch
					//sl.bpm.UnpinPage(pred.GetPageId(), false)
					//pred.DecPinCount()
					sl.bpm.DecPinOfPage(pred)
				}

				//// when reaching target on level-1, if operation is insert or remove, upgrade lock of node from RLock to WLock at
				//upgraded := latchOpWithOpType(pred, SKIP_LIST_UTIL_UPGRADE, opType)
				//if !upgraded {
				//	//// release RLock (when upgrade fails, RLock is keeped)
				//	//latchOpWithOpType(pred, SKIP_LIST_UTIL_UNLATCH, opType)
				//	// if pred is start node, unpin is ok here
				//	sl.bpm.UnpinPage(pred.GetPageId(), false)
				//	latchOpWithOpType(pred, SKIP_LIST_UTIL_UNLATCH, opType)
				//
				//	fmt.Println("upgrade fail")
				//	// retry operation from start...
				//	return false, nil, nil, nil
				//}
			}
			predOfCorners[ii] = skip_list_page.SkipListCornerInfo{predOfPredId, predOfPredLSN}
			corners[ii] = skip_list_page.SkipListCornerInfo{pred.GetPageId(), pred.GetLSN()}
			//if curr != nil {
			//	latchOpWithOpType(curr, SKIP_LIST_UTIL_UNLATCH, opType)
			//	sl.bpm.UnpinPage(curr.GetPageId(), false)
			//}
		}
	}
	/*
		pred.RUnlatch()
		sl.bpm.UnpinPage(pred.GetPageId(), false)
	*/

	//common.ShPrintf(common.DEBUG_INFO, "SkipList::FindNode: moveCnt=%d\n", moveCnt)

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
func (sl *SkipList) FindNodeWithEntryIdxForItr(key *types.Value) (isSuccess_ bool, idx_ int32, predOfCorners_ []skip_list_page.SkipListCornerInfo, corners_ []skip_list_page.SkipListCornerInfo) {
	// get idx of target entry or one of nearest smaller entry
	_, node, predOfCorners, corners := sl.FindNode(key, SKIP_LIST_OP_GET)

	//node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0].PageId)
	// locking is not needed because already have lock with FindNode method call
	_, _, idx := node.FindEntryByKey(key)
	//// this unpin is needed because node is already pinned at FindNode
	//sl.bpm.UnpinPage(node.GetPageId(), false)
	return true, idx, predOfCorners, corners
}

func (sl *SkipList) GetValue(key *types.Value) uint32 {
	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO, "SkipList::GetValue: start. key=%v\n", key.ToIFValue())
	}
	_, node, _, _ := sl.FindNode(key, SKIP_LIST_OP_GET)
	//node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0].PageId)
	// locking is not needed because already have lock with FindNode method call
	found, entry, _ := node.FindEntryByKey(key)
	sl.bpm.UnpinPage(node.GetPageId(), false)
	node.RUnlatch()
	//sl.bpm.UnpinPage(node.GetPageId(), false)

	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO, "SkipList::GetValue: finish. key=%v\n", key.ToIFValue())
	}
	if found {
		return entry.Value
	} else {
		return math.MaxUint32
	}
}

func (sl *SkipList) Insert(key *types.Value, value uint32) (err error) {
	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO, "SkipList::Insert: start. key=%v\n", key.ToIFValue())
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

		//node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0].PageId)
		//// locking is not needed because already have lock with FindNode method call
		isNeedRetry = node.Insert(key, value, sl.bpm, corners, levelWhenNodeSplitOccur)
		//node.WUnlatch()
		//sl.bpm.UnpinPage(node.GetPageId(), true)
	}

	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO, "SkipList::Insert: finish. key=%v\n", key.ToIFValue())
	}
	return nil
}

func (sl *SkipList) Remove(key *types.Value, value uint32) (isDeleted_ bool) {
	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO, "SkipList::Remove: start. key=%v\n", key.ToIFValue())
	}
	isNodeShouldBeDeleted := false
	isDeleted := false
	isNeedRetry := true

	for isNeedRetry {
		isSuccess, node, predOfCorners, corners := sl.FindNode(key, SKIP_LIST_OP_REMOVE)
		if !isSuccess {
			// any latch and pin have here
			continue
		}
		//node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0].PageId)
		// locking is not needed because already have lock with FindNode method call
		isNodeShouldBeDeleted, isDeleted, isNeedRetry = node.Remove(sl.bpm, key, predOfCorners, corners)
		// lock and pin which is got FindNode is released on Remove method
		// except isNodeShouldBeDeleted == true case

		//sl.bpm.UnpinPage(node.GetPageId(), true)
		if isNodeShouldBeDeleted {
			//sl.bpm.DeletePage(corners[0].PageId)
		}
	}

	if common.EnableDebug {
		common.ShPrintf(common.DEBUG_INFO, "SkipList::Remove: finish. key=%v\n", key.ToIFValue())
	}
	return isDeleted
}

// TODO: (SDB) cuncurrent iterator need RID list when iterator is created

func (sl *SkipList) Iterator(rangeStartKey *types.Value, rangeEndKey *types.Value) *SkipListIterator {
	ret := new(SkipListIterator)

	//headerPage := skip_list_page.FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)
	headerPage := sl.getHeaderPage()

	ret.sl = sl
	ret.bpm = sl.bpm
	//ret.curNode = skip_list_page.FetchAndCastToBlockPage(sl.bpm, headerPage.GetListStartPageId())
	ret.curNode = sl.getStartNode()
	ret.curIdx = 0
	ret.rangeStartKey = rangeStartKey
	ret.rangeEndKey = rangeEndKey
	ret.keyType = headerPage.GetKeyType()

	if rangeStartKey != nil {
		//sl.bpm.UnpinPage(headerPage.GetListStartPageId(), false)
		ret.curNode = nil
	}

	//sl.bpm.UnpinPage(sl.headerPageID, false)

	return ret
}

func (sl *SkipList) GetNodeLevel() int32 {
	rand.Float32() //returns a random value in [0..1)
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
