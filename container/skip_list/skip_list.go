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

// TODO: (SDB) in concurrent impl, locking in this method is needed. and caller must do unlock (SkipList::FindNode)

func (sl *SkipList) FindNode(key *types.Value, opType SkipListOpType) (predOfCorners_ []types.PageID, corners_ []types.PageID, succOfCorners_ []types.PageID) {
	headerPage := skip_list_page.FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)

	startPageId := headerPage.GetListStartPageId()

	pred := skip_list_page.FetchAndCastToBlockPage(sl.bpm, startPageId)
	// loop invariant: pred.key < searchKey
	//fmt.Println("---")
	//fmt.Println(key.ToInteger())
	//moveCnt := 0
	predOfPredId := types.InvalidPageID
	predOfCorners := make([]types.PageID, skip_list_page.MAX_FOWARD_LIST_LEN)
	// entry of corners is corner node or target node
	corners := make([]types.PageID, skip_list_page.MAX_FOWARD_LIST_LEN)
	succOfCorners := make([]types.PageID, skip_list_page.MAX_FOWARD_LIST_LEN)
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
			if !curr.GetSmallestKey(key.ValueType()).CompareLessThanOrEqual(*key) {
				//  (ii + 1) level's corner node or target node has been identified (= pred)
				break
			} else {
				// keep moving foward
				predOfPredId = pred.GetPageId()
				sl.bpm.UnpinPage(pred.GetPageId(), false)
				pred = curr
			}
		}
		if opType == SKIP_LIST_OP_REMOVE && ii != 0 && pred.GetEntryCnt() == 1 && key.CompareEquals(pred.GetSmallestKey(key.ValueType())) {
			// pred is already reached goal
			common.ShPrintf(common.DEBUG, "SkipList::FindNode: node should be removed found!\n")
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
	//common.ShPrintf(common.DEBUG, "SkipList::FindNode: moveCnt=%d\n", moveCnt)

	return predOfCorners, corners, succOfCorners
}

func (sl *SkipList) FindNodeWithEntryIdxForItr(key *types.Value) (idx_ int32, predOfCorners_ []types.PageID, corners_ []types.PageID, succOfCorners_ []types.PageID) {
	// get idx of target entry or one of nearest smaller entry
	predOfCorners, corners, succOfCorners := sl.FindNode(key, SKIP_LIST_OP_GET)

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
	_, corners, _ := sl.FindNode(key, SKIP_LIST_OP_INSERT)
	levelWhenNodeSplitOccur := sl.GetNodeLevel()

	node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0])
	node.Insert(key, value, sl.bpm, corners, levelWhenNodeSplitOccur)

	sl.bpm.UnpinPage(node.GetPageId(), true)

	return nil
}

func (sl *SkipList) Remove(key *types.Value, value uint32) (isDeleted_ bool) {
	predOfCorners, corners, _ := sl.FindNode(key, SKIP_LIST_OP_REMOVE)
	node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, corners[0])
	isNodeShouldBeDeleted, isDeleted := node.Remove(sl.bpm, key, predOfCorners, corners)
	sl.bpm.UnpinPage(node.GetPageId(), true)
	if isNodeShouldBeDeleted {
		sl.bpm.DeletePage(corners[0])
	}

	return isDeleted
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
