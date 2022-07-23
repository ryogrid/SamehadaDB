package skip_list

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"math/rand"
	"time"
)

/**
 * Implementation of linear probing hash table that is backed by a buffer pool
 * manager. Non-unique keys are supported. Supports insert and delete. The
 * table dynamically grows once full.
 */

type SkipListOnMem struct {
	CurMaxLevel int32
	Level       int32
	Key         *types.Value
	Val         uint32
	Forward     []*SkipListOnMem
}

type SkipList struct {
	headerPageId *skip_list_page.SkipListHeaderPage //types.PageID
	bpm          *buffer.BufferPoolManager
	list_latch   common.ReaderWriterLatch
}

func NewSkipListOnMem(level int32, key *types.Value, value uint32, isHeader bool) *SkipListOnMem {
	rand.Seed(time.Now().UnixNano())

	ret := new(SkipListOnMem)
	ret.Level = level
	ret.CurMaxLevel = level
	ret.Forward = make([]*SkipListOnMem, 20)
	ret.Val = value
	ret.Key = key
	if isHeader {
		// chain sentinel node
		sentinel := NewSkipListOnMem(level, nil, math.MaxUint32, false)
		switch key.ValueType() {
		case types.Integer:
			infVal := types.NewInteger(0)
			infVal.SetInfMax()
			sentinel.Key = &infVal
		case types.Float:
			infVal := types.NewFloat(0)
			infVal.SetInfMax()
			sentinel.Key = &infVal
		case types.Varchar:
			infVal := types.NewVarchar("")
			infVal.SetInfMax()
			sentinel.Key = &infVal
		}
		// set sentinel node at (meybe) all level
		for ii := 0; ii < 20; ii++ {
			ret.Forward[ii] = sentinel
		}
	}

	return ret
}

func NewSkipList(bpm *buffer.BufferPoolManager, keyType types.TypeID) *SkipList {
	//rand.Seed(time.Now().UnixNano())
	rand.Seed(777)

	ret := new(SkipList)
	ret.bpm = bpm
	ret.headerPageId = skip_list_page.NewSkipListHeaderPage(bpm, keyType) //header.ID()

	return ret
}

// TODO: (SDB) when on-disk impl, checking whether all connectivity is removed and node deallocation should be done if needed
func (sl *SkipList) handleDelMarkedNode(delMarkedNode *skip_list_page.SkipListBlockPage, curLevel int32, skipPathListPrev []*skip_list_page.SkipListBlockPage) *skip_list_page.SkipListBlockPage {
	skipPathListPrev[curLevel].Forward[curLevel] = delMarkedNode.Forward[curLevel]
	// marked connectivity is collectly modified on curLevel
	delMarkedNode.Forward[curLevel] = nil

	return skipPathListPrev[curLevel]
}

// handleDelMarked: IsNeedDeleted marked node is found on node traverse, do link modification for complete deletion
func (sl *SkipList) FindNode(key *types.Value, handleDelMarked bool) (found_node *skip_list_page.SkipListBlockPage, skipPath []*skip_list_page.SkipListBlockPage, skipPathPrev []*skip_list_page.SkipListBlockPage) {
	node := sl.headerPageId.ListStartPage
	// loop invariant: node.key < searchKey
	//fmt.Println("---")
	//fmt.Println(key.ToInteger())
	//moveCnt := 0
	skipPathList := make([]*skip_list_page.SkipListBlockPage, sl.headerPageId.CurMaxLevel+1)
	skipPathListPrev := make([]*skip_list_page.SkipListBlockPage, sl.headerPageId.CurMaxLevel)
	//handleDelMarkedList := make([]bool, sl.headerPageId.CurMaxLevel)
	for ii := (sl.headerPageId.CurMaxLevel - 1); ii >= 0; ii-- {
		//fmt.Printf("level %d\n", i)
		for node.Forward[ii].SmallestKey.CompareLessThanOrEqual(*key) {
			skipPathListPrev[ii] = node
			node = node.Forward[ii]
			//fmt.Printf("%d ", node.Key.ToInteger())
			//moveCnt++
			//if node.IsNeedDeleted && handleDelMarkedList[ii] == false && node.Forward[ii] != nil {
			if handleDelMarked && node.IsNeedDeleted && node.Forward[ii] != nil {
				// handle node (IsNeedDeleted marked) and returns appropriate node (prev node at ii + 1 level)
				node = sl.handleDelMarkedNode(node, ii, skipPathListPrev)
				//handleDelMarkedList[ii] = true
			}
		}
		skipPathList[ii] = node
		//fmt.Println("")
	}
	//fmt.Println(moveCnt)
	return node, skipPathList, skipPathListPrev
}

func (sl *SkipList) FindNodeWithEntryIdxForIterator(key *types.Value) (*skip_list_page.SkipListBlockPage, int32) {
	node, _, _ := sl.FindNode(key, true)
	// get idx of target entry or one of nearest smaller entry
	_, _, idx := node.FindEntryByKey(key)
	return node, idx
}

func (sl *SkipListOnMem) getValueOnMemInner(key *types.Value) *SkipListOnMem {
	x := sl
	for i := (x.CurMaxLevel - 1); i >= 0; i-- {
		for x.Forward[i].Key.CompareLessThan(*key) {
			x = x.Forward[i]
		}
	}
	return x
}

func (sl *SkipListOnMem) GetValueOnMem(key *types.Value) uint32 {
	x := sl.getValueOnMemInner(key)
	xf := x.Forward[0]
	// x.key < searchKey <= x.forward[0].key
	if xf.Key.CompareEquals(*key) {
		return xf.Val
	} else {
		return math.MaxUint32
	}
}

func (sl *SkipListOnMem) GetEqualOrNearestSmallerNodeOnMem(key *types.Value) *SkipListOnMem {
	x := sl.getValueOnMemInner(key)
	return x
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

func (sl *SkipListOnMem) InsertOnMem(key *types.Value, value uint32) (err error) {
	// Utilise update which is a (vertical) array
	// of pointers to the elements which will be
	// predecessors of the new element.
	var update []*SkipListOnMem = make([]*SkipListOnMem, sl.CurMaxLevel+1)
	x := sl
	for ii := (sl.CurMaxLevel - 1); ii >= 0; ii-- {
		for x.Forward[ii].Key.CompareLessThan(*key) {
			x = x.Forward[ii]
		}
		//note: x.key < searchKey <= x.forward[ii].key
		update[ii] = x
	}
	x = x.Forward[0]
	if x.Key.CompareEquals(*key) {
		x.Val = value
		return nil
	} else {
		// key not found, do insertion here:
		newLevel := sl.GetNodeLevel()
		/* If the newLevel is greater than the current level
		   of the list, knock newLevel down so that it is only
		   one level more than the current level of the list.
		   In other words, we will increase the level of the
		   list by at most one on each insertion. */
		if newLevel >= sl.CurMaxLevel {
			newLevel = sl.CurMaxLevel + 1
			sl.CurMaxLevel = newLevel
			update[newLevel-1] = sl
		}
		x := NewSkipListOnMem(newLevel, key, value, false)
		for ii := int32(0); ii < newLevel; ii++ {
			x.Forward[ii] = update[ii].Forward[ii]
			update[ii].Forward[ii] = x
		}
		return nil
	}
}

// for Debug
func (sl *SkipListOnMem) CheckElemListOnMem() {
	x := sl
	for x = x.Forward[0]; !x.Key.IsInfMax(); x = x.Forward[0] {
		fmt.Println(x.Key.ToInteger())
	}
}

func (sl *SkipList) Insert(key *types.Value, value uint32) (err error) {
	// Utilise skipPathList which is a (vertical) array
	// of pointers to the elements which will be
	// predecessors of the new element.

	////fmt.Println("Insert of SkipList called!")
	//var skipPathList []*skip_list_page.SkipListBlockPage = make([]*skip_list_page.SkipListBlockPage, sl.headerPageId.CurMaxLevel+1)
	//node := sl.headerPageId.ListStartPage
	//for ii := (sl.headerPageId.CurMaxLevel - 1); ii >= 0; ii-- {
	//	//fmt.Printf("At Insert of SkipList: ii = %d, node = %v\n", ii, *node)
	//	for node.Forward[ii].SmallestKey.CompareLessThanOrEqual(*key) {
	//		node = node.Forward[ii]
	//	}
	//	//note: node.SmallestKey <= searchKey < node.forward[ii].SmallestKey
	//	skipPathList[ii] = node
	//}
	node, skipPathList, _ := sl.FindNode(key, false)
	levelWhenNodeSplitOccur := sl.GetNodeLevel()
	if levelWhenNodeSplitOccur == sl.headerPageId.CurMaxLevel {
		levelWhenNodeSplitOccur++
	}
	isNewNodeCreated := node.Insert(key, value, sl.bpm, skipPathList, levelWhenNodeSplitOccur, sl.headerPageId.CurMaxLevel, sl.headerPageId.ListStartPage)
	if isNewNodeCreated && levelWhenNodeSplitOccur > sl.headerPageId.CurMaxLevel {
		sl.headerPageId.CurMaxLevel = levelWhenNodeSplitOccur
	}
	//node.Insert(key, value, sl.bpm, skipPathList, levelWhenNodeSplitOccur, sl.headerPageId.CurMaxLevel, sl.headerPageId.ListStartPage)

	return nil
}

func (sl *SkipListOnMem) RemoveOnMem(key *types.Value, value uint32) {
	// update is an array of pointers to the
	// predecessors of the element to be deleted.
	var update []*SkipListOnMem = make([]*SkipListOnMem, sl.CurMaxLevel)
	x := sl
	for ii := (sl.CurMaxLevel - 1); ii >= 0; ii-- {
		for x.Forward[ii].Key.CompareLessThan(*key) {
			x = x.Forward[ii]
		}
		update[ii] = x
	}
	x = x.Forward[0]
	if x.Key.CompareEquals(*key) {
		// go delete ...
		for ii := int32(0); ii < sl.CurMaxLevel; ii++ {
			if update[ii].Forward[ii] != x {
				break //(**)
			}
			update[ii].Forward[ii] = x.Forward[ii]
		}
		/* if deleting the element causes some of the
		   highest level list to become empty, decrease the
		   list level until a non-empty list is encountered.*/
		for (sl.CurMaxLevel > 1) && (sl.Forward[sl.CurMaxLevel-1] == sl) {
			sl.CurMaxLevel--
		}
	}
}

func (sl *SkipList) Remove(key *types.Value, value uint32) (isDeleted bool) {
	//node := sl.headerPageId.ListStartPage
	//
	//var skipPathListPrev []*skip_list_page.SkipListBlockPage = make([]*skip_list_page.SkipListBlockPage, sl.headerPageId.CurMaxLevel)
	//
	//for ii := (sl.headerPageId.CurMaxLevel - 1); ii >= 0; ii-- {
	//	for node.Forward[ii].SmallestKey.CompareLessThanOrEqual(*key) {
	//		skipPathListPrev[ii] = node
	//		node = node.Forward[ii]
	//	}
	//}
	node, _, skipPathListPrev := sl.FindNode(key, false)

	//for ii := (sl.headerPageId.CurMaxLevel - 1); ii >= 0; ii-- {
	//	for node.Forward[ii].SmallestKey.CompareLessThanOrEqual(*key) {
	//		node = node.Forward[ii]
	//	}
	//}

	// remove specified entry from found node

	isDeleted_, _ := node.Remove(key, skipPathListPrev)

	// if there are no node at *level* except start and end node due to node delete
	// CurMaxLevel should be down to the level
	if isDeleted_ {
		// if IsNeedDeleted marked node exists, check logic below has no problem
		newMaxLevel := int32(0)
		for ii := int32(0); ii < sl.headerPageId.CurMaxLevel; ii++ {
			if sl.headerPageId.ListStartPage.Forward[ii].SmallestKey.IsInfMax() {
				break
			}
			newMaxLevel++
		}
		sl.headerPageId.CurMaxLevel = newMaxLevel
	}

	return isDeleted_
}

func (sl *SkipListOnMem) IteratorOnMem(rangeStartKey *types.Value, rangeEndKey *types.Value) *SkipListIteratorOnMem {
	ret := new(SkipListIteratorOnMem)
	ret.curNode = sl
	ret.rangeStartKey = rangeStartKey
	ret.rangeEndKey = rangeEndKey

	if rangeStartKey != nil {
		ret.curNode = ret.curNode.GetEqualOrNearestSmallerNodeOnMem(rangeStartKey)
	}

	return ret
}

func (sl *SkipList) Iterator(rangeStartKey *types.Value, rangeEndKey *types.Value) *SkipListIterator {
	ret := new(SkipListIterator)
	ret.curNode = sl.headerPageId.ListStartPage
	ret.curIdx = 0
	ret.rangeStartKey = rangeStartKey
	ret.rangeEndKey = rangeEndKey

	if rangeStartKey != nil {
		ret.curNode, ret.curIdx = sl.FindNodeWithEntryIdxForIterator(rangeStartKey)
	}

	return ret
}

func (sl *SkipListOnMem) GetNodeLevel() int32 {
	//rand.Float32() returns a random value in [0..1)
	var retLevel int32 = 1
	for rand.Float32() < common.SkipListProb { // no MaxLevel check
		retLevel++
	}
	return int32(math.Min(float64(retLevel), float64(sl.CurMaxLevel)))
}

func (sl *SkipList) GetNodeLevel() int32 {
	rand.Float32() //returns a random value in [0..1)
	var retLevel int32 = 1
	for rand.Float32() < common.SkipListProb { // no MaxLevel check
		retLevel++
	}
	return int32(math.Min(float64(retLevel), float64(sl.headerPageId.CurMaxLevel)))
}
