// TODO: (SDB) not implemented yet skip_list.go

package skip_list

import (
	"encoding/binary"
	"errors"
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	"math"
	"math/rand"
	"time"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/types"
	"github.com/spaolacci/murmur3"
)

/**
 * Implementation of linear probing hash table that is backed by a buffer pool
 * manager. Non-unique keys are supported. Supports insert and delete. The
 * table dynamically grows once full.
 */

type SkipListOnMem struct {
	//headerPageId types.PageID
	//bpm          *buffer.BufferPoolManager
	//list_latch   common.ReaderWriterLatch
	IsHeader    bool
	Key         *types.Value
	Val         uint32
	Level       int32
	CurMaxLevel int32
	Forward     []*SkipListOnMem
}

type SkipList struct {
	headerPageId types.PageID
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
			infVal.SetInf()
			sentinel.Key = &infVal
		case types.Float:
			infVal := types.NewFloat(0)
			infVal.SetInf()
			sentinel.Key = &infVal
		case types.Varchar:
			infVal := types.NewVarchar("")
			infVal.SetInf()
			sentinel.Key = &infVal
		}
		// set sentinel node at (meybe) all level
		for ii := 0; ii < 20; ii++ {
			ret.Forward[ii] = sentinel
		}
	}

	return ret
	//header := bpm.NewPage()
	//headerData := header.Data()
	//headerPage := (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(headerData))
	//
	//headerPage.SetPageId(header.ID())
	//headerPage.SetSize(numBuckets * skip_list_page.BlockArraySize)
	//
	//for i := 0; i < numBuckets; i++ {
	//	np := bpm.NewPage()
	//	headerPage.AddBlockPageId(np.ID())
	//	bpm.UnpinPage(np.ID(), true)
	//}
	//bpm.UnpinPage(header.ID(), true)
	//return &SkipList{}
}

func NewSkipList(bpm *buffer.BufferPoolManager, numBuckets int) *SkipList {
	header := bpm.NewPage()
	headerData := header.Data()
	headerPage := (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(headerData))

	headerPage.SetPageId(header.ID())
	headerPage.SetSize(numBuckets * skip_list_page.BlockArraySize)

	for i := 0; i < numBuckets; i++ {
		np := bpm.NewPage()
		headerPage.AddBlockPageId(np.ID())
		bpm.UnpinPage(np.ID(), true)
	}
	bpm.UnpinPage(header.ID(), true)

	return &SkipList{header.ID(), bpm, common.NewRWLatch()}
}

func (sl *SkipListOnMem) GetValueOnMem(key *types.Value) uint32 {
	//sl.list_latch.RLock()
	//defer sl.list_latch.RUnlock()
	//hPageData := sl.bpm.FetchPage(sl.headerPageId).Data()
	//headerPage := (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(hPageData))
	//
	//hash := hash(nil)
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
	//	if blockPage.IsReadable(offset) && blockPage.KeyAt(offset).CompareEquals(*key) {
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

	x := sl
	// loop invariant: x.key < searchKey
	for i := (x.CurMaxLevel - 1); i >= 0; i-- {
		for x.Forward[i].Key.CompareLessThan(*key) {
			x = x.Forward[i]
		}
	}
	// x.key < searchKey <= x.forward[0].key
	x = x.Forward[0]
	if x.Key.CompareEquals(*key) {
		return x.Val
	} else {
		return math.MaxUint32
	}
}

func (sl *SkipList) GetValue(key []byte) []uint32 {
	sl.list_latch.RLock()
	defer sl.list_latch.RUnlock()
	hPageData := sl.bpm.FetchPage(sl.headerPageId).Data()
	headerPage := (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(hPageData))

	hash := hash(key)

	originalBucketIndex := hash % headerPage.NumBlocks()
	originalBucketOffset := hash % skip_list_page.BlockArraySize

	iterator := newSkipListIterator(sl.bpm, headerPage, originalBucketIndex, originalBucketOffset)

	result := []uint32{}
	blockPage, offset := iterator.blockPage, iterator.offset
	var bucket uint32
	for blockPage.IsOccupied(offset) { // stop the search and we find an empty spot
		if blockPage.IsReadable(offset) && blockPage.KeyAt(offset).CompareEquals(*types.NewValueFromBytes(key, types.Integer)) {
			result = append(result, blockPage.ValueAt(offset))
		}

		iterator.next()
		blockPage, bucket, offset = iterator.blockPage, iterator.bucket, iterator.offset
		if bucket == originalBucketIndex && offset == originalBucketOffset {
			break
		}
	}

	sl.bpm.UnpinPage(iterator.blockId, true)
	sl.bpm.UnpinPage(sl.headerPageId, false)

	return result
}

func (sl *SkipListOnMem) InsertOnMem(key *types.Value, value uint32) (err error) {
	//sl.list_latch.WLock()
	//defer sl.list_latch.WUnlock()
	//hPageData := sl.bpm.FetchPage(sl.headerPageId).Data()
	//headerPage := (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(hPageData))
	//
	//hash := hash(nil)
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
		if newLevel > sl.CurMaxLevel {
			newLevel = sl.CurMaxLevel + 1
			sl.CurMaxLevel = newLevel
			update[newLevel] = sl
		}
		x := NewSkipListOnMem(newLevel, key, value, false)
		for ii := int32(0); ii < newLevel; ii++ {
			x.Forward[ii] = update[ii].Forward[ii]
			update[ii].Forward[ii] = x
		}
		return nil
	}
}

func (sl *SkipList) Insert(key []byte, value uint32) (err error) {
	sl.list_latch.WLock()
	defer sl.list_latch.WUnlock()
	hPageData := sl.bpm.FetchPage(sl.headerPageId).Data()
	headerPage := (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(hPageData))

	hash := hash(key)

	originalBucketIndex := hash % headerPage.NumBlocks()
	originalBucketOffset := hash % skip_list_page.BlockArraySize

	iterator := newSkipListIterator(sl.bpm, headerPage, originalBucketIndex, originalBucketOffset)

	blockPage, offset := iterator.blockPage, iterator.offset
	var bucket uint32
	for {
		if blockPage.IsOccupied(offset) && blockPage.ValueAt(offset) == value {
			err = errors.New("duplicated values on the same key are not allowed")
			break
		}

		if !blockPage.IsOccupied(offset) {
			blockPage.Insert(offset, hash, value)
			err = nil
			break
		}
		iterator.next()

		blockPage, bucket, offset = iterator.blockPage, iterator.bucket, iterator.offset
		if bucket == originalBucketIndex && offset == originalBucketOffset {
			break
		}
	}

	sl.bpm.UnpinPage(iterator.blockId, true)
	sl.bpm.UnpinPage(sl.headerPageId, false)

	return
}

func (sl *SkipListOnMem) RemoveOnMem(key *types.Value, value uint32) {
	//sl.list_latch.WLock()
	//defer sl.list_latch.WUnlock()
	//hPageData := sl.bpm.FetchPage(sl.headerPageId).Data()
	//headerPage := (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(hPageData))
	//
	//hash := hash(nil)
	//
	//originalBucketIndex := hash % headerPage.NumBlocks()
	//originalBucketOffset := hash % skip_list_page.BlockArraySize
	//
	//iterator := newSkipListIterator(sl.bpm, headerPage, originalBucketIndex, originalBucketOffset)
	//
	//blockPage, offset := iterator.blockPage, iterator.offset
	//var bucket uint32
	//for blockPage.IsOccupied(offset) { // stop the search and we find an empty spot
	//	if blockPage.IsOccupied(offset) && blockPage.KeyAt(offset).CompareEquals(*key) && blockPage.ValueAt(offset) == value {
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
		//destroy_remove(x)
		/* if deleting the element causes some of the
		   highest level list to become empty, decrease the
		   list level until a non-empty list is encountered.*/
		for (sl.CurMaxLevel > 1) && (sl.Forward[sl.CurMaxLevel-1] == sl) {
			sl.CurMaxLevel--
		}
	}
}

func (sl *SkipList) Remove(key []byte, value uint32) {
	sl.list_latch.WLock()
	defer sl.list_latch.WUnlock()
	hPageData := sl.bpm.FetchPage(sl.headerPageId).Data()
	headerPage := (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(hPageData))

	hash := hash(key)

	originalBucketIndex := hash % headerPage.NumBlocks()
	originalBucketOffset := hash % skip_list_page.BlockArraySize

	iterator := newSkipListIterator(sl.bpm, headerPage, originalBucketIndex, originalBucketOffset)

	blockPage, offset := iterator.blockPage, iterator.offset
	var bucket uint32
	for blockPage.IsOccupied(offset) { // stop the search and we find an empty spot
		if blockPage.IsOccupied(offset) && blockPage.KeyAt(offset).CompareEquals(*types.NewValueFromBytes(key, types.Integer)) && blockPage.ValueAt(offset) == value {
			blockPage.Remove(offset)
		}

		iterator.next()
		blockPage, bucket, offset = iterator.blockPage, iterator.bucket, iterator.offset
		if bucket == originalBucketIndex && offset == originalBucketOffset {
			break
		}
	}

	sl.bpm.UnpinPage(iterator.blockId, true)
	sl.bpm.UnpinPage(sl.headerPageId, false)
}

func hash(key []byte) uint32 {
	h := murmur3.New128()

	h.Write(key)
	hash := h.Sum(nil)

	return binary.LittleEndian.Uint32(hash)
}

func (sl *SkipListOnMem) GetNodeLevel() int32 {
	//random() returns a random value in [0..1)
	var retLevel int32 = 1
	for rand.Float32() < common.SkipListProb { // no MaxLevel check
		retLevel++
	}
	return int32(math.Min(float64(retLevel), float64(sl.CurMaxLevel)))
}
