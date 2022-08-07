package skip_list

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"math/rand"
	"unsafe"
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

// handleDelMarked: isNeedDeleted marked node is found on node traverse, do link modification for complete deletion
func (sl *SkipList) FindNode(key *types.Value, handleDelMarked bool) (found_node *skip_list_page.SkipListBlockPage, skipPath []*skip_list_page.SkipListBlockPage, skipPathPrev []*skip_list_page.SkipListBlockPage) {
	headerPage := FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)

	startPageId := headerPage.GetListStartPageId()
	page_ := sl.bpm.FetchPage(startPageId)
	node := (*skip_list_page.SkipListBlockPage)(unsafe.Pointer(page_))
	// loop invariant: node.key < searchKey
	//fmt.Println("---")
	//fmt.Println(key.ToInteger())
	//moveCnt := 0
	skipPathList := make([]*skip_list_page.SkipListBlockPage, headerPage.GetCurMaxLevel()+1)
	skipPathListPrev := make([]*skip_list_page.SkipListBlockPage, headerPage.GetCurMaxLevel())
	for ii := (headerPage.GetCurMaxLevel() - 1); ii >= 0; ii-- {
		//fmt.Printf("level %d\n", i)
		for node.GetForwardEntry(int(ii)).GetSmallestKey().CompareLessThanOrEqual(*key) {

			if node.GetForwardEntry(int(ii)).GetIsNeedDeleted() {
				// when next node is isNeedDeleted marked node
				// stop at current node and handle next node

				// handle node (isNeedDeleted marked) and returns appropriate node (prev node at ii + 1 level)
				sl.handleDelMarkedNode(node.GetForwardEntry(int(ii)), node, ii)
				if node.GetIsNeedDeleted() {
					panic("return value of handleDelMarkedNode is invalid!")
				}
			} else {
				// move to next node
				skipPathListPrev[ii] = node
				node = node.GetForwardEntry(int(ii))
			}
		}
		skipPathList[ii] = node
	}
	sl.bpm.UnpinPage(sl.headerPageID, false)

	return node, skipPathList, skipPathListPrev
}

func (sl *SkipList) FindNodeWithEntryIdxForIterator(key *types.Value) (*skip_list_page.SkipListBlockPage, int32) {
	node, _, _ := sl.FindNode(key, true)
	// get idx of target entry or one of nearest smaller entry
	_, _, idx := node.FindEntryByKey(key)
	return node, idx
}

func (sl *SkipList) GetValue(key *types.Value) uint32 {
	node, _, _ := sl.FindNode(key, true)
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

	headerPage := FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)

	node, skipPathList, _ := sl.FindNode(key, false)
	levelWhenNodeSplitOccur := sl.GetNodeLevel()
	if levelWhenNodeSplitOccur == headerPage.GetCurMaxLevel() {
		levelWhenNodeSplitOccur++
	}
	isNewNodeCreated := node.Insert(key, value, sl.bpm, skipPathList, levelWhenNodeSplitOccur, headerPage.GetCurMaxLevel(), headerPage.GetListStartPageId())
	if isNewNodeCreated && levelWhenNodeSplitOccur > headerPage.GetCurMaxLevel() {
		headerPage.SetCurMaxLevel(levelWhenNodeSplitOccur)
	}
	//node.Insert(key, value, sl.bpm, skipPathList, levelWhenNodeSplitOccur, sl.headerPageID.curMaxLevel, sl.headerPageID.listStartPageId)

	sl.bpm.UnpinPage(sl.headerPageID, true)

	return nil
}

func (sl *SkipList) Remove(key *types.Value, value uint32) (isDeleted bool) {
	node, _, skipPathListPrev := sl.FindNode(key, false)
	isDeleted_, _ := node.Remove(key, skipPathListPrev)

	headerPage := FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)

	// if there are no node at *level* except start and end node due to node delete
	// curMaxLevel should be down to the level
	if isDeleted_ {
		// if isNeedDeleted marked node exists, check logic below has no problem
		newMaxLevel := int32(0)
		for ii := int32(0); ii < headerPage.GetCurMaxLevel(); ii++ {
			if headerPage.GetListStartPageId().GetForwardEntry(ii).GetSmallestKey().IsInfMax() {
				break
			}
			newMaxLevel++
		}
		headerPage.SetCurMaxLevel(newMaxLevel)
	}
	sl.bpm.UnpinPage(sl.headerPageID, true)

	return isDeleted_
}

func (sl *SkipList) Iterator(rangeStartKey *types.Value, rangeEndKey *types.Value) *SkipListIterator {
	ret := new(SkipListIterator)

	headerPage := FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)

	ret.bpm = sl.bpm
	ret.curNode = FetchAndCastToBlockPage(sl.bpm, headerPage.GetListStartPageId())
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

	headerPage := FetchAndCastToHeaderPage(sl.bpm, sl.headerPageID)
	ret := int32(math.Min(float64(retLevel), float64(headerPage.GetCurMaxLevel())))
	sl.bpm.UnpinPage(sl.headerPageID, false)

	return ret
}

func (sl *SkipList) GetHeaderPageId() types.PageID {
	return sl.headerPageID
}

// TODO: (SDB) in concurrent impl, locking in this method is needed. and caller must do unlock (FectchAndCastToBlockPage)

// Attention:
//   caller must call UnpinPage with appropriate diaty page to the got page when page using ends
func FetchAndCastToBlockPage(bpm *buffer.BufferPoolManager, pageId types.PageID) *skip_list_page.SkipListBlockPage {
	bPage := bpm.FetchPage(pageId)
	return (*skip_list_page.SkipListBlockPage)(unsafe.Pointer(bPage))
}

// TODO: (SDB) in concurrent impl, locking in this method is needed. and caller must do unlock (FectchAndCastToBlockPage)

// Attention:
//   caller must call UnpinPage with appropriate diaty page to the got page when page using ends
func FetchAndCastToHeaderPage(bpm *buffer.BufferPoolManager, pageId types.PageID) *skip_list_page.SkipListHeaderPage {
	hPageData := bpm.FetchPage(pageId).Data()
	return (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(hPageData))
}
