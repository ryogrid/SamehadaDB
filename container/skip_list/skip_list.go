// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

// TODO: (SDB) not implemented yet skip_list.go

package skip_list

import (
	"encoding/binary"
	"errors"
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
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
type SkipList struct {
	headerPageId types.PageID
	bpm          *buffer.BufferPoolManager
	table_latch  common.ReaderWriterLatch
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

func (sl *SkipList) GetValue(key []byte) []uint32 {
	sl.table_latch.RLock()
	defer sl.table_latch.RUnlock()
	hPageData := sl.bpm.FetchPage(sl.headerPageId).Data()
	headerPage := (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(hPageData))

	hash := sl.hash(key)

	originalBucketIndex := hash % headerPage.NumBlocks()
	originalBucketOffset := hash % skip_list_page.BlockArraySize

	iterator := newSkipListIterator(sl.bpm, headerPage, originalBucketIndex, originalBucketOffset)

	result := []uint32{}
	blockPage, offset := iterator.blockPage, iterator.offset
	var bucket uint32
	for blockPage.IsOccupied(offset) { // stop the search and we find an empty spot
		if blockPage.IsReadable(offset) && blockPage.KeyAt(offset) == hash {
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

func (sl *SkipList) Insert(key []byte, value uint32) (err error) {
	sl.table_latch.WLock()
	defer sl.table_latch.WUnlock()
	hPageData := sl.bpm.FetchPage(sl.headerPageId).Data()
	headerPage := (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(hPageData))

	hash := sl.hash(key)

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

func (sl *SkipList) Remove(key []byte, value uint32) {
	sl.table_latch.WLock()
	defer sl.table_latch.WUnlock()
	hPageData := sl.bpm.FetchPage(sl.headerPageId).Data()
	headerPage := (*skip_list_page.SkipListHeaderPage)(unsafe.Pointer(hPageData))

	hash := sl.hash(key)

	originalBucketIndex := hash % headerPage.NumBlocks()
	originalBucketOffset := hash % skip_list_page.BlockArraySize

	iterator := newSkipListIterator(sl.bpm, headerPage, originalBucketIndex, originalBucketOffset)

	blockPage, offset := iterator.blockPage, iterator.offset
	var bucket uint32
	for blockPage.IsOccupied(offset) { // stop the search and we find an empty spot
		if blockPage.IsOccupied(offset) && blockPage.KeyAt(offset) == hash && blockPage.ValueAt(offset) == value {
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

func (sl *SkipList) hash(key []byte) uint32 {
	h := murmur3.New128()
	//bs := make([]byte, 4)
	//binary.LittleEndian.PutUint32(bs, uint32(key))

	//h.Write(bs)
	h.Write(key)

	hash := h.Sum(nil)

	//return int(binary.LittleEndian.Uint32(hash))
	return binary.LittleEndian.Uint32(hash)
}
