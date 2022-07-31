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
	headerPage *skip_list_page.SkipListHeaderPage
	bpm        *buffer.BufferPoolManager
	//list_latch common.ReaderWriterLatch
}

func NewSkipList(bpm *buffer.BufferPoolManager, keyType types.TypeID) *SkipList {
	//rand.Seed(time.Now().UnixNano())
	rand.Seed(777)

	ret := new(SkipList)
	ret.bpm = bpm
	ret.headerPage = skip_list_page.NewSkipListHeaderPage(bpm, keyType) //header.ID()

	return ret
}

// TODO: (SDB) when on-disk impl, checking whether all connectivity is removed and node deallocation should be done if needed
func (sl *SkipList) handleDelMarkedNode(delMarkedNode *skip_list_page.SkipListBlockPage, curNode *skip_list_page.SkipListBlockPage, curLevelIdx int32) {
	curNode.SetForward(curLevelIdx, delMarkedNode.GetForward(curLevelIdx))

	// marked connectivity is collectly modified on curLevelIdx
	delMarkedNode.SetForward(curLevelIdx, nil)
}

// handleDelMarked: IsNeedDeleted marked node is found on node traverse, do link modification for complete deletion
func (sl *SkipList) FindNode(key *types.Value, handleDelMarked bool) (found_node *skip_list_page.SkipListBlockPage, skipPath []*skip_list_page.SkipListBlockPage, skipPathPrev []*skip_list_page.SkipListBlockPage) {
	node := sl.headerPage.GetListStartPage()
	// loop invariant: node.key < searchKey
	//fmt.Println("---")
	//fmt.Println(key.ToInteger())
	//moveCnt := 0
	skipPathList := make([]*skip_list_page.SkipListBlockPage, sl.headerPage.GetCurMaxLevel()+1)
	skipPathListPrev := make([]*skip_list_page.SkipListBlockPage, sl.headerPage.GetCurMaxLevel())
	for ii := (sl.headerPage.GetCurMaxLevel() - 1); ii >= 0; ii-- {
		//fmt.Printf("level %d\n", i)
		for node.GetForward(ii).GetSmallestKey().CompareLessThanOrEqual(*key) {

			if node.GetForward(ii).GetIsNeedDeleted() {
				// when next node is IsNeedDeleted marked node
				// stop at current node and handle next node

				// handle node (IsNeedDeleted marked) and returns appropriate node (prev node at ii + 1 level)
				sl.handleDelMarkedNode(node.GetForward(ii), node, ii)
				if node.GetIsNeedDeleted() {
					panic("return value of handleDelMarkedNode is invalid!")
				}
			} else {
				// move to next node
				skipPathListPrev[ii] = node
				node = node.GetForward(ii)
			}
		}
		skipPathList[ii] = node
	}
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

	node, skipPathList, _ := sl.FindNode(key, false)
	levelWhenNodeSplitOccur := sl.GetNodeLevel()
	if levelWhenNodeSplitOccur == sl.headerPage.GetCurMaxLevel() {
		levelWhenNodeSplitOccur++
	}
	isNewNodeCreated := node.Insert(key, value, sl.bpm, skipPathList, levelWhenNodeSplitOccur, sl.headerPage.GetCurMaxLevel(), sl.headerPage.GetListStartPage())
	if isNewNodeCreated && levelWhenNodeSplitOccur > sl.headerPage.GetCurMaxLevel() {
		sl.headerPage.SetCurMaxLevel(levelWhenNodeSplitOccur)
	}
	//node.Insert(key, value, sl.bpm, skipPathList, levelWhenNodeSplitOccur, sl.headerPage.CurMaxLevel, sl.headerPage.ListStartPage)

	return nil
}

func (sl *SkipList) Remove(key *types.Value, value uint32) (isDeleted bool) {
	node, _, skipPathListPrev := sl.FindNode(key, false)
	isDeleted_, _ := node.Remove(key, skipPathListPrev)

	// if there are no node at *level* except start and end node due to node delete
	// CurMaxLevel should be down to the level
	if isDeleted_ {
		// if IsNeedDeleted marked node exists, check logic below has no problem
		newMaxLevel := int32(0)
		for ii := int32(0); ii < sl.headerPage.GetCurMaxLevel(); ii++ {
			if sl.headerPage.GetListStartPage().GetForward(ii).GetSmallestKey().IsInfMax() {
				break
			}
			newMaxLevel++
		}
		sl.headerPage.SetCurMaxLevel(newMaxLevel)
	}

	return isDeleted_
}

func (sl *SkipList) Iterator(rangeStartKey *types.Value, rangeEndKey *types.Value) *SkipListIterator {
	ret := new(SkipListIterator)
	ret.curNode = sl.headerPage.GetListStartPage()
	ret.curIdx = 0
	ret.rangeStartKey = rangeStartKey
	ret.rangeEndKey = rangeEndKey

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
	return int32(math.Min(float64(retLevel), float64(sl.headerPage.GetCurMaxLevel())))
}
