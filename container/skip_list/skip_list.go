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

// TODO: (SDB) when on-disk impl, checking whether all connectivity is removed and node deallocation should be done if needed
func (sl *SkipList) handleDelMarkedNode(delMarkedNode *skip_list_page.SkipListBlockPage, curNode *skip_list_page.SkipListBlockPage, curLevelIdx int32) {
	curNode.SetForwardEntry(int(curLevelIdx), delMarkedNode.GetForwardEntry(int(curLevelIdx)))

	// marked connectivity is collectly modified on curLevelIdx
	delMarkedNode.SetForwardEntry(int(curLevelIdx), common.InvalidPageID)
}

// TODO: (SDB) in concurrent impl, locking in this method is needed. and caller must do unlock (SkipList::FindNode)

// Attention:
//   caller must call UnpinPage with appropriate diaty page to the got page when page using ends
func (sl *SkipList) FindNode(key *types.Value) (found_node *skip_list_page.SkipListBlockPage, skipPath []types.PageID, skipPathPrev []types.PageID) {
	headerPage := skip_list_page.FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)

	startPageId := headerPage.GetListStartPageId()
	//page_ := sl.bpm.FetchPage(startPageId)
	//node := (*skip_list_page.SkipListBlockPage)(unsafe.Pointer(page_))
	node := skip_list_page.FetchAndCastToBlockPage(sl.bpm, startPageId)
	// loop invariant: node.key < searchKey
	//fmt.Println("---")
	//fmt.Println(key.ToInteger())
	//moveCnt := 0
	skipPathList := make([]types.PageID, headerPage.GetCurMaxLevel()+1)
	skipPathListPrev := make([]types.PageID, headerPage.GetCurMaxLevel())
	for ii := (headerPage.GetCurMaxLevel() - 1); ii >= 0; ii-- {
		//fmt.Printf("level %d\n", i)
		for {
			tmpNode := skip_list_page.FetchAndCastToBlockPage(sl.bpm, node.GetForwardEntry(int(ii)))
			if tmpNode == nil {
				common.ShPrintf(common.FATAL, "PageID to passed FetchAndCastToBlockPage is %d\n", node.GetForwardEntry(int(ii)))
				panic("SkipList::FindNode: FetchAndCastToBlockPage returned nil!")
			}
			if tmpNode.GetSmallestKey(key.ValueType()).CompareLessThanOrEqual(*key) {
				// do nothing
			} else {
				sl.bpm.UnpinPage(tmpNode.GetPageId(), false)
				break
			}

			//if node.GetForwardEntry(int(ii)).GetIsNeedDeleted() {
			if tmpNode.GetIsNeedDeleted() {
				// when next node is isNeedDeleted marked node
				// stop at current node and handle next node

				// handle node (isNeedDeleted marked) and returns appropriate node (prev node at ii + 1 level)
				//sl.handleDelMarkedNode(node.GetForwardEntry(int(ii)), node, ii)
				sl.handleDelMarkedNode(tmpNode, node, ii)
				sl.bpm.UnpinPage(tmpNode.GetPageId(), true)
				if node.GetIsNeedDeleted() {
					panic("return value of handleDelMarkedNode is invalid!")
				}
			} else {
				// move to next node
				prevPageId := node.GetPageId()
				skipPathListPrev[ii] = node.GetPageId()
				//node = node.GetForwardEntry(int(ii))
				node = tmpNode
				sl.bpm.UnpinPage(prevPageId, false)
			}
		}
		skipPathList[ii] = node.GetPageId()
	}
	sl.bpm.UnpinPage(headerPage.GetPageId(), false)

	return node, skipPathList, skipPathListPrev
}

// TODO: (SDB) in concurrent impl, locking in this method is needed. and caller must do unlock (SkipList::FindNodeWithEntryIdxForIterator)

// Attention:
//   caller must call UnpinPage with appropriate diaty page to the got page when page using ends
func (sl *SkipList) FindNodeWithEntryIdxForIterator(key *types.Value) (*skip_list_page.SkipListBlockPage, int32) {
	node, _, _ := sl.FindNode(key)
	// get idx of target entry or one of nearest smaller entry
	_, _, idx := node.FindEntryByKey(key)
	return node, idx
}

func (sl *SkipList) GetValue(key *types.Value) uint32 {
	node, _, _ := sl.FindNode(key)
	found, entry, _ := node.FindEntryByKey(key)
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
	if levelWhenNodeSplitOccur == headerPage.GetCurMaxLevel() {
		levelWhenNodeSplitOccur++
	}

	startPage := skip_list_page.FetchAndCastToBlockPage(sl.bpm, headerPage.GetListStartPageId())
	isNewNodeCreated := node.Insert(key, value, sl.bpm, skipPathList, levelWhenNodeSplitOccur, headerPage.GetCurMaxLevel(), startPage)
	if isNewNodeCreated && levelWhenNodeSplitOccur > headerPage.GetCurMaxLevel() {
		headerPage.SetCurMaxLevel(levelWhenNodeSplitOccur)
	}
	//node.Insert(key, value, sl.bpm, skipPathList, levelWhenNodeSplitOccur, sl.headerPageID.curMaxLevel, sl.headerPageID.listStartPageId)

	sl.bpm.UnpinPage(startPage.GetPageId(), true)
	sl.bpm.UnpinPage(headerPage.GetPageId(), true)
	sl.bpm.UnpinPage(node.GetPageId(), true)

	return nil
}

func (sl *SkipList) Remove(key *types.Value, value uint32) (isDeleted bool) {
	node, _, skipPathListPrev := sl.FindNode(key)
	isDeleted_, _ := node.Remove(key, skipPathListPrev)
	sl.bpm.UnpinPage(node.GetPageId(), true)

	headerPage := skip_list_page.FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)

	// if there are no node at *level* except start and end node due to node delete
	// curMaxLevel should be down to the level
	if isDeleted_ {
		// if isNeedDeleted marked node exists, check logic below has no problem
		newMaxLevel := int32(0)
		startNode := skip_list_page.FetchAndCastToBlockPage(sl.bpm, headerPage.GetListStartPageId())
		for ii := int32(0); ii < headerPage.GetCurMaxLevel(); ii++ {
			tmpNode := skip_list_page.FetchAndCastToBlockPage(sl.bpm, startNode.GetForwardEntry(int(ii)))
			if tmpNode.GetSmallestKey(key.ValueType()).IsInfMax() {
				sl.bpm.UnpinPage(tmpNode.GetPageId(), false)
				break
			}
			sl.bpm.UnpinPage(tmpNode.GetPageId(), false)
			newMaxLevel++
		}
		headerPage.SetCurMaxLevel(newMaxLevel)
		sl.bpm.UnpinPage(startNode.GetPageId(), false)
	}
	sl.bpm.UnpinPage(sl.headerPageID, true)

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
		ret.curNode, ret.curIdx = sl.FindNodeWithEntryIdxForIterator(rangeStartKey)
	}

	return ret
}

func (sl *SkipList) GetNodeLevel() int32 {
	rand.Float32() //returns a random value in [0..1)
	var retLevel int32 = 1
	for rand.Float32() < common.SkipListProb { // no MaxLevel check
		retLevel++
	}

	headerPage := skip_list_page.FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)
	ret := int32(math.Min(float64(retLevel), float64(headerPage.GetCurMaxLevel())))
	sl.bpm.UnpinPage(sl.headerPageID, false)

	return ret
}

func (sl *SkipList) GetHeaderPageId() types.PageID {
	return sl.headerPageID
}
