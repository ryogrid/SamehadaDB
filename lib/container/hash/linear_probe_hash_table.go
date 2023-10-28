// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package hash

import (
	"encoding/binary"
	"errors"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"github.com/spaolacci/murmur3"
)

/**
 * Implementation of linear probing hash table that is backed by a buffer pool
 * manager. Non-unique keys are supported. Supports insert and delete.
 */

// Limitation: current implementation contain BlockArraySize(252) * 1020 = 257,040 record info at most
// TODO: (SDB) LinearProbeHashTable does not dynamically grows...
type LinearProbeHashTable struct {
	headerPageId types.PageID
	bpm          *buffer.BufferPoolManager
	table_latch  common.ReaderWriterLatch
}

// numBuckets should be less than 1020
func NewLinearProbeHashTable(bpm *buffer.BufferPoolManager, numBuckets int, headerPageId types.PageID) *LinearProbeHashTable {
	if numBuckets > 1020 {
		panic("numBuckets should be less than 1020")
	}

	if headerPageId == types.InvalidPageID {
		header := bpm.NewPage()
		headerData := header.Data()
		headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(headerData))

		headerPage.SetPageId(header.GetPageId())
		headerPage.SetSize(numBuckets * page.BlockArraySize)

		for i := 0; i < numBuckets; i++ {
			np := bpm.NewPage()
			headerPage.AddBlockPageId(np.GetPageId())
			bpm.UnpinPage(np.GetPageId(), true)
		}
		bpm.UnpinPage(header.GetPageId(), true)

		// on current not expandable HashTable impl
		// flush of header page is needed only creating time
		// because, content of header page is changed only creatting time
		bpm.FlushPage(header.GetPageId())

		return &LinearProbeHashTable{header.GetPageId(), bpm, common.NewRWLatch()}
	} else {
		header := bpm.FetchPage(headerPageId)
		bpm.UnpinPage(header.GetPageId(), true)

		return &LinearProbeHashTable{header.GetPageId(), bpm, common.NewRWLatch()}
	}
}

func (ht *LinearProbeHashTable) GetValue(key []byte) []uint64 {
	ht.table_latch.RLock()
	defer ht.table_latch.RUnlock()
	hPageData := ht.bpm.FetchPage(ht.headerPageId).Data()
	headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(hPageData))

	hash := ht.hash(key)

	originalBucketIndex := hash % headerPage.NumBlocks()
	originalBucketOffset := hash % page.BlockArraySize

	iterator := newHashTableIterator(ht.bpm, headerPage, originalBucketIndex, originalBucketOffset)

	result := []uint64{}
	blockPage, offset := iterator.blockPage, iterator.offset
	var bucket uint64
	for blockPage.IsOccupied(offset) { // stop the search and we find an empty spot
		if blockPage.IsReadable(offset) && blockPage.KeyAt(offset) == uint64(hash) {
			result = append(result, blockPage.ValueAt(offset))
		}

		iterator.next()
		blockPage, bucket, offset = iterator.blockPage, iterator.bucket, iterator.offset
		if bucket == originalBucketIndex && offset == originalBucketOffset {
			break
		}
	}

	ht.bpm.UnpinPage(iterator.blockId, true)
	ht.bpm.UnpinPage(ht.headerPageId, false)

	return result
}

func (ht *LinearProbeHashTable) Insert(key []byte, value uint64) (err error) {
	ht.table_latch.WLock()
	defer ht.table_latch.WUnlock()
	hPageData := ht.bpm.FetchPage(ht.headerPageId).Data()
	headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(hPageData))

	hash := ht.hash(key)

	originalBucketIndex := hash % headerPage.NumBlocks()
	originalBucketOffset := hash % page.BlockArraySize

	iterator := newHashTableIterator(ht.bpm, headerPage, originalBucketIndex, originalBucketOffset)

	blockPage, offset := iterator.blockPage, iterator.offset
	var bucket uint64
	for {
		if blockPage.IsOccupied(offset) && blockPage.IsReadable(offset) && blockPage.ValueAt(offset) == value {
			err = errors.New("duplicated values on the same key are not allowed")
			break
		}

		// insert to deleted marked slot
		if blockPage.IsOccupied(offset) && !blockPage.IsReadable(offset) {
			blockPage.Insert(offset, uint64(hash), value)
			err = nil
			break
		}

		if !blockPage.IsOccupied(offset) {
			blockPage.Insert(offset, uint64(hash), value)
			err = nil
			break
		}
		iterator.next()

		blockPage, bucket, offset = iterator.blockPage, iterator.bucket, iterator.offset
		if bucket == originalBucketIndex && offset == originalBucketOffset {
			break
		}
	}

	ht.bpm.UnpinPage(iterator.blockId, true)
	ht.bpm.UnpinPage(ht.headerPageId, false)

	return
}

func (ht *LinearProbeHashTable) Remove(key []byte, value uint64) {
	ht.table_latch.WLock()
	defer ht.table_latch.WUnlock()
	hPageData := ht.bpm.FetchPage(ht.headerPageId).Data()
	headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(hPageData))

	hash := ht.hash(key)

	originalBucketIndex := hash % uint64(headerPage.NumBlocks())
	originalBucketOffset := hash % page.BlockArraySize

	iterator := newHashTableIterator(ht.bpm, headerPage, originalBucketIndex, originalBucketOffset)

	blockPage, offset := iterator.blockPage, iterator.offset
	var bucket uint64
	for blockPage.IsOccupied(offset) { // stop the search and we find an empty spot
		if blockPage.IsOccupied(offset) && blockPage.KeyAt(offset) == uint64(hash) && blockPage.ValueAt(offset) == value {
			blockPage.Remove(offset)
		}

		iterator.next()
		blockPage, bucket, offset = iterator.blockPage, iterator.bucket, iterator.offset
		if bucket == originalBucketIndex && offset == originalBucketOffset {
			break
		}
	}

	ht.bpm.UnpinPage(iterator.blockId, true)
	ht.bpm.UnpinPage(ht.headerPageId, false)
}

func (ht *LinearProbeHashTable) hash(key []byte) uint64 {
	h := murmur3.New128()

	h.Write(key)

	hash := h.Sum(nil)

	return binary.LittleEndian.Uint64(hash)
}

func (ht *LinearProbeHashTable) GetHeaderPageId() types.PageID {
	return ht.headerPageId
}
