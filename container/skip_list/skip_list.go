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
	rand.Seed(time.Now().UnixNano())

	ret := new(SkipList)
	ret.bpm = bpm
	ret.headerPageId = skip_list_page.NewSkipListHeaderPage(bpm, keyType) //header.ID()

	return ret
}

func (sl *SkipList) FindNode(key *types.Value) *skip_list_page.SkipListBlockPage {
	node := sl.headerPageId.ListStartPage
	// loop invariant: node.key < searchKey
	//fmt.Println("---")
	//fmt.Println(key.ToInteger())
	//moveCnt := 0
	for i := (sl.headerPageId.CurMaxLevel - 1); i >= 0; i-- {
		//fmt.Printf("level %d\n", i)
		for node.Forward[i].SmallestKey.CompareLessThanOrEqual(*key) {
			node = node.Forward[i]
			//fmt.Printf("%d ", node.Key.ToInteger())
			//moveCnt++
		}
		//fmt.Println("")
	}
	//fmt.Println(moveCnt)
	return node
}

func (sl *SkipList) FindNodeWithEntryIdxForIterator(key *types.Value) (*skip_list_page.SkipListBlockPage, int32) {
	node := sl.FindNode(key)
	// get idx of target entry or one of nearest smaller entry
	_, _, idx := node.FindEntryByKey(key)
	return node, idx
}

func (sl *SkipListOnMem) getValueOnMemInner(key *types.Value) *SkipListOnMem {
	x := sl
	// loop invariant: x.key < searchKey
	//fmt.Println("---")
	//fmt.Println(key.ToInteger())
	//moveCnt := 0
	for i := (x.CurMaxLevel - 1); i >= 0; i-- {
		//fmt.Printf("level %d\n", i)
		for x.Forward[i].Key.CompareLessThan(*key) {
			x = x.Forward[i]
			//fmt.Printf("%d ", x.Key.ToInteger())
			//moveCnt++
		}
		//fmt.Println("")
	}
	//fmt.Println(moveCnt)
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
	//sl.list_latch.RLock()
	//defer sl.list_latch.RUnlock()
	//hPageData := sl.bpm.FetchPage(sl.headerPageId).Data()
	//headerPage := (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(hPageData))
	//
	//hash := hash(key)
	//
	//originalBucketIndex := hash % headerPage.NumBlocks()
	//originalBucketOffset := hash % skip_list_page.BlockArraySize
	//
	//iterator := newSkipListIterator(sl.bpm, headerPage, originalBucketIndex, originalBucketOffset)
	//
	//result := []uint32{}
	//blockPage, offset := iterator.blockPage, iterator.offset
	//var bucket uint32
	//for blockPage.IsOccupied(offset) { // stop the search and we find an empty spot
	//	if blockPage.IsReadable(offset) && blockPage.KeyAt(offset).CompareEquals(*types.NewValueFromBytes(key, types.Integer)) {
	//		result = append(result, blockPage.ValueAt(offset))
	//	}
	//
	//	iterator.next()
	//	blockPage, bucket, offset = iterator.blockPage, iterator.bucket, iterator.offset
	//	if bucket == originalBucketIndex && offset == originalBucketOffset {
	//		break
	//	}
	//}
	//
	//sl.bpm.UnpinPage(iterator.blockId, true)
	//sl.bpm.UnpinPage(sl.headerPageId, false)
	//
	//return result

	node := sl.FindNode(key)
	//xf := node.Forward[0]
	//// node.SmallestKey < searchKey <= node.forward[0].SmallestKey
	//if xf.SmallestKey.CompareEquals(*key) {
	//	return xf.Val
	//} else {
	//	return math.MaxUint32
	//}
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
	//sl.list_latch.WLock()
	//defer sl.list_latch.WUnlock()
	//hPageData := sl.bpm.FetchPage(sl.headerPageId).Data()
	//headerPage := (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(hPageData))
	//
	//hash := hash(key)
	//
	//originalBucketIndex := hash % headerPage.NumBlocks()
	//originalBucketOffset := hash % skip_list_page.BlockArraySize
	//
	//iterator := newSkipListIterator(sl.bpm, headerPage, originalBucketIndex, originalBucketOffset)
	//
	//blockPage, offset := iterator.blockPage, iterator.offset
	//var bucket uint32
	//for {
	//	if blockPage.IsOccupied(offset) && blockPage.ValueAt(offset) == value {
	//		err = errors.New("duplicated values on the same key are not allowed")
	//		break
	//	}
	//
	//	if !blockPage.IsOccupied(offset) {
	//		blockPage.Insert(offset, hash, value)
	//		err = nil
	//		break
	//	}
	//	iterator.next()
	//
	//	blockPage, bucket, offset = iterator.blockPage, iterator.bucket, iterator.offset
	//	if bucket == originalBucketIndex && offset == originalBucketOffset {
	//		break
	//	}
	//}
	//
	//sl.bpm.UnpinPage(iterator.blockId, true)
	//sl.bpm.UnpinPage(sl.headerPageId, false)
	//
	//return

	// Utilise skipPathList which is a (vertical) array
	// of pointers to the elements which will be
	// predecessors of the new element.
	var skipPathList []*skip_list_page.SkipListBlockPage = make([]*skip_list_page.SkipListBlockPage, sl.headerPageId.CurMaxLevel+1)
	node := sl.headerPageId.ListStartPage
	for ii := (sl.headerPageId.CurMaxLevel - 1); ii >= 0; ii-- {
		for node.Forward[ii].SmallestKey.CompareLessThanOrEqual(*key) {
			node = node.Forward[ii]
		}
		//note: node.SmallestKey <= searchKey < node.forward[ii].SmallestKey
		skipPathList[ii] = node
	}
	//node = node.Forward[0]
	//if node.Key.CompareEquals(*key) {
	//	node.Val = value
	//	return nil
	//} else {
	//	// key not found, do insertion here:
	//	newLevel := sl.GetNodeLevel()
	//	// If the newLevel is greater than the current levelWhenNodeSplitOccur
	//	// of the list, knock newLevel down so that it is only
	//	// one levelWhenNodeSplitOccur more than the current levelWhenNodeSplitOccur of the list.
	//	// In other words, we will increase the levelWhenNodeSplitOccur of the
	//	// list by at most one on each insertion.
	//	if newLevel >= sl.CurMaxLevel {
	//		newLevel = sl.CurMaxLevel + 1
	//		sl.CurMaxLevel = newLevel
	//		skipPathList[newLevel-1] = sl
	//	}
	//	node := NewSkipListOnMem(newLevel, key, value, false)
	//	for ii := int32(0); ii < newLevel; ii++ {
	//		node.Forward[ii] = skipPathList[ii].Forward[ii]
	//		skipPathList[ii].Forward[ii] = node
	//	}
	//	return nil
	//}
	levelWhenNodeSplitOccur := sl.GetNodeLevel()
	if levelWhenNodeSplitOccur == sl.headerPageId.CurMaxLevel {
		levelWhenNodeSplitOccur++
	}
	isNewNodeCreated := node.Insert(key, value, sl.bpm, skipPathList, levelWhenNodeSplitOccur, sl.headerPageId.ListStartPage)
	if isNewNodeCreated && levelWhenNodeSplitOccur > sl.headerPageId.CurMaxLevel {
		sl.headerPageId.CurMaxLevel = levelWhenNodeSplitOccur
	}

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

func (sl *SkipList) Remove(key *types.Value, value uint32) {
	//sl.list_latch.WLock()
	//defer sl.list_latch.WUnlock()
	//hPageData := sl.bpm.FetchPage(sl.headerPageId).Data()
	//headerPage := (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(hPageData))
	//
	//hash := hash(key)
	//
	//originalBucketIndex := hash % headerPage.NumBlocks()
	//originalBucketOffset := hash % skip_list_page.BlockArraySize
	//
	//iterator := newSkipListIterator(sl.bpm, headerPage, originalBucketIndex, originalBucketOffset)
	//
	//blockPage, offset := iterator.blockPage, iterator.offset
	//var bucket uint32
	//for blockPage.IsOccupied(offset) { // stop the search and we find an empty spot
	//	if blockPage.IsOccupied(offset) && blockPage.KeyAt(offset).CompareEquals(*types.NewValueFromBytes(key, types.Integer)) && blockPage.ValueAt(offset) == value {
	//		blockPage.Remove(offset)
	//	}
	//
	//	iterator.next()
	//	blockPage, bucket, offset = iterator.blockPage, iterator.bucket, iterator.offset
	//	if bucket == originalBucketIndex && offset == originalBucketOffset {
	//		break
	//	}
	//}
	//
	//sl.bpm.UnpinPage(iterator.blockId, true)
	//sl.bpm.UnpinPage(sl.headerPageId, false)

	// update is an array of pointers to the
	// predecessors of the element to be deleted.
	node := sl.headerPageId.ListStartPage
	for ii := (sl.headerPageId.CurMaxLevel - 1); ii >= 0; ii-- {
		for node.Forward[ii].SmallestKey.CompareLessThanOrEqual(*key) {
			node = node.Forward[ii]
		}
	}
	//node = node.Forward[0]

	//if node.Key.CompareEquals(*key) {
	//	// go delete ...
	//	for ii := int32(0); ii < sl.CurMaxLevel; ii++ {
	//		if update[ii].Forward[ii] != node {
	//			break //(**)
	//		}
	//		update[ii].Forward[ii] = node.Forward[ii]
	//	}
	//	// if deleting the element causes some of the
	//	// highest level list to become empty, decrease the
	//	// list level until a non-empty list is encountered.
	//	for (sl.CurMaxLevel > 1) && (sl.Forward[sl.CurMaxLevel-1] == sl) {
	//		sl.CurMaxLevel--
	//	}
	//}

	// remove specified entry from found node
	node.Remove(key)
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

// TODO: (SDB) not implemented yet (Iterator)
func (sl *SkipList) Iterator(rangeStartKey *types.Value, rangeEndKey *types.Value) *SkipListIterator {
	//ret := new(SkipListIteratorOnMem)
	//ret.curNode = sl
	//ret.rangeStartKey = rangeStartKey
	//ret.rangeEndKey = rangeEndKey
	//
	//if rangeStartKey != nil {
	//	ret.curNode = ret.curNode.GetEqualOrNearestSmallerNodeOnMem(rangeStartKey)
	//}
	//
	//return ret

	ret := new(SkipListIterator)
	if len(sl.headerPageId.ListStartPage.Entries) == 1 {
		// there are no entry corresponding user record

	}
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
	//rand.Float32() returns a random value in [0..1)
	var retLevel int32 = 1
	for rand.Float32() < common.SkipListProb { // no MaxLevel check
		retLevel++
	}
	return int32(math.Min(float64(retLevel), float64(sl.headerPageId.CurMaxLevel)))
}
