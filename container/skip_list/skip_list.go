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
	rand.Seed(777)

	ret := new(SkipList)
	ret.bpm = bpm
	ret.headerPageID = skip_list_page.NewSkipListHeaderPage(bpm, keyType) //header.ID()

	return ret
}

//func (sl *SkipList) handleDelMarkedNode(delMarkedNode *skip_list_page.SkipListBlockPage, curNode *skip_list_page.SkipListBlockPage, curLevelIdx int32) (isCanDelete bool) {
//	curNode.SetForwardEntry(int(curLevelIdx), delMarkedNode.GetForwardEntry(int(curLevelIdx)))
//
//	// marked entry's connectivity is collectly modified on curLevelIdx
//	delMarkedNode.SetForwardEntry(int(curLevelIdx), common.InvalidPageID)
//
//	// check whether all connectivity is removed.
//	// and if result is true, marked node can be removed
//	level := int(delMarkedNode.GetLevel())
//	isAllRemoved := true
//	for ii := 0; ii < level; ii++ {
//		fwdEntry := delMarkedNode.GetForwardEntry(ii)
//		if fwdEntry != common.InvalidPageID {
//			isAllRemoved = false
//			break
//		}
//	}
//	if isAllRemoved {
//		return true
//	} else {
//		return false
//	}
//}

// TODO: (SDB) in concurrent impl, locking in this method is needed. and caller must do unlock (SkipList::FindNode)

func (sl *SkipList) FindNode(key *types.Value, opType SkipListOpType) (predOfCorners_ []types.PageID, corners_ []types.PageID, succOfCorners_ []types.PageID) {
	headerPage := skip_list_page.FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)

	startPageId := headerPage.GetListStartPageId()
	// TODO: (SDB) need to consider pred == startPage is OK?

	pred := skip_list_page.FetchAndCastToBlockPage(sl.bpm, startPageId)
	// loop invariant: pred.key < searchKey
	//fmt.Println("---")
	//fmt.Println(key.ToInteger())
	//moveCnt := 0
	//corners := make([]types.PageID, headerPage.GetCurMaxLevel()+1)
	predOfPredId := types.InvalidPageID
	predOfCorners := make([]types.PageID, skip_list_page.MAX_FOWARD_LIST_LEN)
	// entry of corners is corner node or target node
	corners := make([]types.PageID, skip_list_page.MAX_FOWARD_LIST_LEN)
	succOfCorners := make([]types.PageID, skip_list_page.MAX_FOWARD_LIST_LEN)
	var curr *skip_list_page.SkipListBlockPage
	for ii := (skip_list_page.MAX_FOWARD_LIST_LEN - 1); ii >= 0; ii-- {
		//fmt.Printf("level %d\n", i)
		for {
			curr = skip_list_page.FetchAndCastToBlockPage(sl.bpm, pred.GetForwardEntry(int(ii)))
			if curr == nil {
				common.ShPrintf(common.FATAL, "PageID to passed FetchAndCastToBlockPage is %d\n", pred.GetForwardEntry(int(ii)))
				panic("SkipList::FindNode: FetchAndCastToBlockPage returned nil!")
			}
			if !curr.GetSmallestKey(key.ValueType()).CompareLessThanOrEqual(*key) && !curr.GetIsNeedDeleted() {
				//  (ii + 1) level's corner node or target node has been identified (= pred)
				break
			} else {
				// keep moving foward
				predOfPredId = pred.GetPageId()
				sl.bpm.UnpinPage(pred.GetPageId(), false)
				pred = curr
			}
		}
		if opType == SKIP_LIST_OP_REMOVE && pred.GetEntryCnt() == 1 && key.CompareEquals(pred.GetSmallestKey(key.ValueType())) {
			// pred is already reached goal
			predOfCorners[ii] = types.InvalidPageID
			corners[ii] = predOfPredId
			succOfCorners[ii] = pred.GetPageId()
			sl.bpm.UnpinPage(curr.GetPageId(), false)
			sl.bpm.UnpinPage(pred.GetPageId(), false)
			// go backward for gathering appropriate corner nodes info
			pred = skip_list_page.FetchAndCastToBlockPage(sl.bpm, predOfPredId)
		} else {
			predOfCorners[ii] = predOfPredId
			corners[ii] = pred.GetPageId()
			succOfCorners[ii] = curr.GetPageId()
			sl.bpm.UnpinPage(curr.GetPageId(), false)
		}
	}
	sl.bpm.UnpinPage(pred.GetPageId(), false)
	sl.bpm.UnpinPage(headerPage.GetPageId(), false)

	return predOfCorners, corners, succOfCorners
}

// TODO: (SDB) in concurrent impl, locking in this method is needed. and caller must do unlock (SkipList::FindNodeWithEntryIdxForItr)

func (sl *SkipList) FindNodeWithEntryIdxForItr(key *types.Value) (idx_ int32, predOfCorners_ []types.PageID, corners_ []types.PageID, succOfCorners_ []types.PageID) {
	predOfCorners, corners, succOfCorners := sl.FindNode(key, SKIP_LIST_OP_GET)
	// get idx of target entry or one of nearest smaller entry

	node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0])
	_, _, idx := node.FindEntryByKey(key)
	sl.bpm.UnpinPage(node.GetPageId(), false)
	return idx, predOfCorners, corners, succOfCorners
}

func (sl *SkipList) GetValue(key *types.Value) uint32 {
	_, corners, _ := sl.FindNode(key, SKIP_LIST_OP_GET)
	node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0])
	found, entry, _ := node.FindEntryByKey(key)
	sl.bpm.UnpinPage(node.GetPageId(), false)
	if found {
		return entry.Value
	} else {
		return math.MaxUint32
	}
}

func (sl *SkipList) Insert(key *types.Value, value uint32) (err error) {
	// Utilise skipPathList which is a (vertical) array
	// of pointers to the elements which will be
	// predecessors of the new element.

	//headerPage := skip_list_page.FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)

	_, corners, _ := sl.FindNode(key, SKIP_LIST_OP_INSERT)
	levelWhenNodeSplitOccur := sl.GetNodeLevel()

	//startPage := skip_list_page.FetchAndCastToBlockPage(sl.bpm, headerPage.GetListStartPageId())
	node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0])
	node.Insert(key, value, sl.bpm, corners, levelWhenNodeSplitOccur)

	//sl.bpm.UnpinPage(startPage.GetPageId(), true)
	//sl.bpm.UnpinPage(headerPage.GetPageId(), true)
	sl.bpm.UnpinPage(node.GetPageId(), true)

	return nil
}

func (sl *SkipList) Remove(key *types.Value, value uint32) (isDeleted bool) {
	predOfCorners, corners, _ := sl.FindNode(key, SKIP_LIST_OP_REMOVE)
	node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0])
	isNodeShouldBeDeleted, isDeleted_, _ := node.Remove(sl.bpm, key, predOfCorners, corners)
	sl.bpm.UnpinPage(node.GetPageId(), true)
	if isNodeShouldBeDeleted {
		sl.bpm.DeletePage(corners[0])
	}

	return isDeleted_
}

func (sl *SkipList) Iterator(rangeStartKey *types.Value, rangeEndKey *types.Value) *SkipListIterator {
	ret := new(SkipListIterator)

	headerPage := skip_list_page.FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)

	ret.bpm = sl.bpm
	ret.curNode = skip_list_page.FetchAndCastToBlockPage(sl.bpm, headerPage.GetListStartPageId())
	ret.curIdx = 0
	ret.rangeStartKey = rangeStartKey
	ret.rangeEndKey = rangeEndKey
	ret.keyType = headerPage.GetKeyType()

	sl.bpm.UnpinPage(sl.headerPageID, false)

	if rangeStartKey != nil {
		sl.bpm.UnpinPage(ret.curNode.GetPageId(), false)
		var corners []types.PageID
		ret.curIdx, _, corners, _ = sl.FindNodeWithEntryIdxForItr(rangeStartKey)
		ret.curNode = skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0])
	}

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
