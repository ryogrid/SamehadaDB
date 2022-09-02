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

const (
	SKIP_LIST_OP_GET = iota
	SKIP_LIST_OP_REMOVE
	SKIP_LIST_OP_INSERT
)

/**
 * Implementation of skip list that is backed by a buffer pool
 * manager. Non-unique keys are not supported yet. Supports insert, delete and Iterator.
 */

type SkipList struct {
	headerPageID    types.PageID //*skip_list_page.SkipListHeaderPage
	bpm             *buffer.BufferPoolManager
	headerPageLatch common.ReaderWriterLatch
}

func NewSkipList(bpm *buffer.BufferPoolManager, keyType types.TypeID) *SkipList {
	//rand.Seed(time.Now().UnixNano())
	//rand.Seed(777)

	ret := new(SkipList)
	ret.bpm = bpm
	ret.headerPageID = skip_list_page.NewSkipListHeaderPage(bpm, keyType) //header.ID()

	return ret
}

// TODO: (SDB) in concurrent impl, locking in this method is needed. and caller must do unlock (SkipList::FindNode)

// ATTENTION:
// this method returns with keep having RLatch or WLatch of corners_[0] and not Unping corners_[0]
func (sl *SkipList) FindNode(key *types.Value, opType SkipListOpType) (predOfCorners_ []skip_list_page.SkipListCornerInfo, corners_ []skip_list_page.SkipListCornerInfo) {
	headerPage := skip_list_page.FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)
	startPageId := headerPage.GetListStartPageId()
	// lock of headerPage is not needed becaus its content is not changed
	sl.bpm.UnpinPage(headerPage.GetPageId(), false)

	pred := skip_list_page.FetchAndCastToBlockPage(sl.bpm, startPageId)
	if opType != SKIP_LIST_OP_GET {
		pred.WLatch()
	} else {
		pred.RLatch()
	}

	// loop invariant: pred.key < searchKey
	//fmt.Println("---")
	//fmt.Println(key.ToInteger())
	//moveCnt := 0
	predOfPredId := types.InvalidPageID
	predOfPredLSN := types.LSN(-1)
	predOfCorners := make([]skip_list_page.SkipListCornerInfo, skip_list_page.MAX_FOWARD_LIST_LEN)
	// entry of corners is corner node or target node
	corners := make([]skip_list_page.SkipListCornerInfo, skip_list_page.MAX_FOWARD_LIST_LEN)
	var curr *skip_list_page.SkipListBlockPage
	for ii := (skip_list_page.MAX_FOWARD_LIST_LEN - 1); ii >= 0; ii-- {
		//fmt.Printf("level %d\n", i)
		for {
			//moveCnt++
			curr = skip_list_page.FetchAndCastToBlockPage(sl.bpm, pred.GetForwardEntry(int(ii)))
			if curr == nil {
				common.ShPrintf(common.FATAL, "PageID to passed FetchAndCastToBlockPage is %d\n", pred.GetForwardEntry(int(ii)))
				panic("SkipList::FindNode: FetchAndCastToBlockPage returned nil!")
			}
			if opType != SKIP_LIST_OP_GET {
				// level-1 and operation is remove or insert, need WLatch when reached target node
				curr.WLatch()
			} else {
				curr.RLatch()
			}
			if !curr.GetSmallestKey(key.ValueType()).CompareLessThanOrEqual(*key) {
				//  (ii + 1) level's corner node or target node has been identified (= pred)
				break
			} else {
				// keep moving foward
				predOfPredId = pred.GetPageId()
				predOfPredLSN = pred.GetLSN()
				if opType != SKIP_LIST_OP_GET {
					pred.WUnlatch()
				} else {
					pred.RUnlatch()
				}
				sl.bpm.UnpinPage(pred.GetPageId(), false)
				pred = curr
			}
		}
		if opType == SKIP_LIST_OP_REMOVE && ii != 0 && pred.GetEntryCnt() == 1 && key.CompareEquals(pred.GetSmallestKey(key.ValueType())) {
			// pred is already reached goal, so change pred to appropriate node
			common.ShPrintf(common.DEBUG, "SkipList::FindNode: node should be removed found!\n")
			predOfCorners[ii] = skip_list_page.SkipListCornerInfo{types.InvalidPageID, -1}
			corners[ii] = skip_list_page.SkipListCornerInfo{predOfPredId, predOfPredLSN}
			if opType != SKIP_LIST_OP_GET {
				pred.WUnlatch()
			} else {
				pred.RUnlatch()
			}
			sl.bpm.UnpinPage(pred.GetPageId(), false)
			// go backward for gathering appropriate corner nodes info
			pred = skip_list_page.FetchAndCastToBlockPage(sl.bpm, predOfPredId)
			if opType != SKIP_LIST_OP_GET {
				pred.WLatch()
			} else {
				pred.RLatch()
			}
			if opType != SKIP_LIST_OP_GET {
				curr.WUnlatch()
			} else {
				curr.RUnlatch()
			}
			sl.bpm.UnpinPage(curr.GetPageId(), false)
		} else {
			predOfCorners[ii] = skip_list_page.SkipListCornerInfo{predOfPredId, predOfPredLSN}
			corners[ii] = skip_list_page.SkipListCornerInfo{pred.GetPageId(), pred.GetLSN()}
			if opType != SKIP_LIST_OP_GET {
				curr.WUnlatch()
			} else {
				curr.RUnlatch()
			}
			sl.bpm.UnpinPage(curr.GetPageId(), false)
		}
	}
	/*
		pred.RUnlatch()
		sl.bpm.UnpinPage(pred.GetPageId(), false)
	*/

	//common.ShPrintf(common.DEBUG, "SkipList::FindNode: moveCnt=%d\n", moveCnt)

	return predOfCorners, corners
}

// ATTENTION:
// this method returns with keep having RLatch or WLatch of corners_[0] and not Unping corners_[0]
func (sl *SkipList) FindNodeWithEntryIdxForItr(key *types.Value) (idx_ int32, predOfCorners_ []skip_list_page.SkipListCornerInfo, corners_ []skip_list_page.SkipListCornerInfo) {
	// get idx of target entry or one of nearest smaller entry
	predOfCorners, corners := sl.FindNode(key, SKIP_LIST_OP_GET)

	node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0].PageId)
	// locking is not needed because already have lock with FindNode method call
	_, _, idx := node.FindEntryByKey(key)
	sl.bpm.UnpinPage(node.GetPageId(), false)
	return idx, predOfCorners, corners
}

func (sl *SkipList) GetValue(key *types.Value) uint32 {
	_, corners := sl.FindNode(key, SKIP_LIST_OP_GET)
	node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0].PageId)
	// locking is not needed because already have lock with FindNode method call
	found, entry, _ := node.FindEntryByKey(key)
	node.RUnlatch()
	sl.bpm.UnpinPage(node.GetPageId(), false)
	if found {
		return entry.Value
	} else {
		return math.MaxUint32
	}
}

func (sl *SkipList) Insert(key *types.Value, value uint32) (err error) {
	isNeedRetry := true

	for isNeedRetry {
		_, corners := sl.FindNode(key, SKIP_LIST_OP_INSERT)
		levelWhenNodeSplitOccur := sl.GetNodeLevel()

		node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0].PageId)
		// locking is not needed because already have lock with FindNode method call
		isNeedRetry = node.Insert(key, value, sl.bpm, corners, levelWhenNodeSplitOccur)
		//node.WUnlatch()
		sl.bpm.UnpinPage(node.GetPageId(), true)
	}

	return nil
}

func (sl *SkipList) Remove(key *types.Value, value uint32) (isDeleted_ bool) {
	isNodeShouldBeDeleted := false
	isDeleted := false
	isNeedRetry := true

	for isNeedRetry {
		predOfCorners, corners := sl.FindNode(key, SKIP_LIST_OP_REMOVE)
		node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0].PageId)
		// locking is not needed because already have lock with FindNode method call
		isNodeShouldBeDeleted, isDeleted, isNeedRetry = node.Remove(sl.bpm, key, predOfCorners, corners)
		// lock of node is released on Remove method
		sl.bpm.UnpinPage(node.GetPageId(), true)
		if isNodeShouldBeDeleted {
			sl.bpm.DeletePage(corners[0].PageId)
		}
	}

	return isDeleted
}

// TODO: (SDB) cuncurrent iterator need RID list when iterator is created

func (sl *SkipList) Iterator(rangeStartKey *types.Value, rangeEndKey *types.Value) *SkipListIterator {
	ret := new(SkipListIterator)

	headerPage := skip_list_page.FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)

	ret.sl = sl
	ret.bpm = sl.bpm
	ret.curNode = skip_list_page.FetchAndCastToBlockPage(sl.bpm, headerPage.GetListStartPageId())
	ret.curIdx = 0
	ret.rangeStartKey = rangeStartKey
	ret.rangeEndKey = rangeEndKey
	ret.keyType = headerPage.GetKeyType()

	if rangeStartKey != nil {
		sl.bpm.UnpinPage(headerPage.GetListStartPageId(), false)
		ret.curNode = nil
	}

	sl.bpm.UnpinPage(sl.headerPageID, false)

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
	return sl.headerPageID
}
