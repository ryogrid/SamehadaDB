package skip_list

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"math/rand"
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

func (sl *SkipList) handleDelMarkedNode(delMarkedNode *skip_list_page.SkipListBlockPage, curNode *skip_list_page.SkipListBlockPage, curLevelIdx int32) (isCanDelete bool) {
	curNode.SetForwardEntry(int(curLevelIdx), delMarkedNode.GetForwardEntry(int(curLevelIdx)))

	// marked entry's connectivity is collectly modified on curLevelIdx
	delMarkedNode.SetForwardEntry(int(curLevelIdx), common.InvalidPageID)

	// check whether all connectivity is removed.
	// and if result is true, marked node can be removed
	level := int(delMarkedNode.GetLevel())
	isAllRemoved := true
	for ii := 0; ii < level; ii++ {
		fwdEntry := delMarkedNode.GetForwardEntry(ii)
		if fwdEntry != common.InvalidPageID {
			isAllRemoved = false
			break
		}
	}
	if isAllRemoved {
		return true
	} else {
		return false
	}
}

// TODO: (SDB) in concurrent impl, locking in this method is needed. and caller must do unlock (SkipList::FindNode)

func (sl *SkipList) FindNode(key *types.Value) (lfound_ int32, preds_ []types.PageID, succs_ []types.PageID) {
	headerPage := skip_list_page.FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)

	startPageId := headerPage.GetListStartPageId()
	// TODO: (SDB) need to consider pred == startPage is OK?
	pred := skip_list_page.FetchAndCastToBlockPage(sl.bpm, startPageId)
	// loop invariant: pred.key < searchKey
	//fmt.Println("---")
	//fmt.Println(key.ToInteger())
	//moveCnt := 0
	//succs := make([]types.PageID, headerPage.GetCurMaxLevel()+1)
	lfound := -1
	preds := make([]types.PageID, skip_list_page.MAX_FOWARD_LIST_LEN)
	succs := make([]types.PageID, skip_list_page.MAX_FOWARD_LIST_LEN)
	var curr *skip_list_page.SkipListBlockPage
	for ii := (skip_list_page.MAX_FOWARD_LIST_LEN - 1); ii >= 0; ii-- {
		//fmt.Printf("level %d\n", i)
		for {
			curr = skip_list_page.FetchAndCastToBlockPage(sl.bpm, pred.GetForwardEntry(int(ii)))
			if curr == nil {
				common.ShPrintf(common.FATAL, "PageID to passed FetchAndCastToBlockPage is %d\n", pred.GetForwardEntry(int(ii)))
				panic("SkipList::FindNode: FetchAndCastToBlockPage returned nil!")
			}
			if !curr.GetBiggestKey(key.ValueType()).CompareLessThanOrEqual(*key) && !curr.GetIsNeedDeleted() {
				// reached (ii + 1) level's nearest pred
				break
			} else {
				// keep moving foward
				sl.bpm.UnpinPage(pred.GetPageId(), false)
				pred = curr
			}
		}

		if lfound == -1 {
			lfound = ii
		}
		preds[ii] = pred.GetPageId()
		succs[ii] = curr.GetPageId()
		sl.bpm.UnpinPage(curr.GetPageId(), false)
	}
	sl.bpm.UnpinPage(pred.GetPageId(), false)
	sl.bpm.UnpinPage(headerPage.GetPageId(), false)

	return int32(lfound), preds, succs
}

// TODO: (SDB) in concurrent impl, locking in this method is needed. and caller must do unlock (SkipList::FindNodeWithEntryIdxForItr)

func (sl *SkipList) FindNodeWithEntryIdxForItr(key *types.Value) (lfound_ int32, idx_ int32, preds_ []types.PageID, nexts_ []types.PageID) {
	lfound, preds, succs := sl.FindNode(key)
	// get idx of target entry or one of nearest smaller entry

	// TODO: (SDB) need to consider lfound == -1 case (SkipList::FindNodeWithEntryIdxForItr)
	node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, succs[lfound])
	_, _, idx := node.FindEntryByKey(key)
	sl.bpm.UnpinPage(node.GetPageId(), false)
	return lfound, idx, preds, succs
}

func (sl *SkipList) GetValue(key *types.Value) uint32 {
	lfound, _, succs := sl.FindNode(key)
	// TODO: (SDB) need to consider lfound == -1 case (SkipList::GetValue)
	node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, succs[lfound])
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

	headerPage := skip_list_page.FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)

	node, skipPathList, _ := sl.FindNode(key)
	levelWhenNodeSplitOccur := sl.GetNodeLevel()

	startPage := skip_list_page.FetchAndCastToBlockPage(sl.bpm, headerPage.GetListStartPageId())
	node.Insert(key, value, sl.bpm, skipPathList, levelWhenNodeSplitOccur, skip_list_page.MAX_FOWARD_LIST_LEN, startPage)

	sl.bpm.UnpinPage(startPage.GetPageId(), true)
	sl.bpm.UnpinPage(headerPage.GetPageId(), true)
	sl.bpm.UnpinPage(node.GetPageId(), true)

	return nil
}

func (sl *SkipList) Remove(key *types.Value, value uint32) (isDeleted bool) {
	node, _, skipPathListPrev := sl.FindNode(key)
	isDeleted_, _ := node.Remove(key, skipPathListPrev)
	sl.bpm.UnpinPage(node.GetPageId(), true)

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
		ret.curNode, ret.curIdx = sl.FindNodeWithEntryIdxForItr(rangeStartKey)
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
