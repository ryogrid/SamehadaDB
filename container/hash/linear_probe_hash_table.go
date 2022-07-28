// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package hash

import (
	"encoding/binary"
	"errors"
	"github.com/ryogrid/SamehadaDB/storage/page"
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
// TODO: (SDB) LinearProbeHashTable does not dynamically grows...
type LinearProbeHashTable struct {
	headerPageId types.PageID
	bpm          *buffer.BufferPoolManager
	table_latch  common.ReaderWriterLatch
}

// TODO: (SDB) need to add index header page ID argument (NewLinearProbeHashTable)
func NewLinearProbeHashTable(bpm *buffer.BufferPoolManager, numBuckets int) *LinearProbeHashTable {
	// TODO: when valid index header page ID argument, header page should be fetched
	//       nad adding block pages are not needed
	header := bpm.NewPage()
	headerData := header.Data()
	headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(headerData))

	headerPage.SetPageId(header.ID())
	headerPage.SetSize(numBuckets * page.BlockArraySize)

	for i := 0; i < numBuckets; i++ {
		np := bpm.NewPage()
		headerPage.AddBlockPageId(np.ID())
		bpm.UnpinPage(np.ID(), true)
	}
	bpm.UnpinPage(header.ID(), true)

	return &LinearProbeHashTable{header.ID(), bpm, common.NewRWLatch()}
}

func (ht *LinearProbeHashTable) GetValue(key []byte) []uint32 {
	ht.table_latch.RLock()
	defer ht.table_latch.RUnlock()
	hPageData := ht.bpm.FetchPage(ht.headerPageId).Data()
	headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(hPageData))

	hash := ht.hash(key)

	originalBucketIndex := hash % headerPage.NumBlocks()
	originalBucketOffset := hash % page.BlockArraySize

	iterator := newHashTableIterator(ht.bpm, headerPage, originalBucketIndex, originalBucketOffset)

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

	ht.bpm.UnpinPage(iterator.blockId, true)
	ht.bpm.UnpinPage(ht.headerPageId, false)

	return result
}

func (ht *LinearProbeHashTable) Insert(key []byte, value uint32) (err error) {
	ht.table_latch.WLock()
	defer ht.table_latch.WUnlock()
	hPageData := ht.bpm.FetchPage(ht.headerPageId).Data()
	headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(hPageData))

	hash := ht.hash(key)

	originalBucketIndex := hash % headerPage.NumBlocks()
	originalBucketOffset := hash % page.BlockArraySize

	iterator := newHashTableIterator(ht.bpm, headerPage, originalBucketIndex, originalBucketOffset)

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

	ht.bpm.UnpinPage(iterator.blockId, true)
	ht.bpm.UnpinPage(ht.headerPageId, false)

	return
}

func (ht *LinearProbeHashTable) Remove(key []byte, value uint32) {
	ht.table_latch.WLock()
	defer ht.table_latch.WUnlock()
	hPageData := ht.bpm.FetchPage(ht.headerPageId).Data()
	headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(hPageData))

	hash := ht.hash(key)

	originalBucketIndex := hash % headerPage.NumBlocks()
	originalBucketOffset := hash % page.BlockArraySize

	iterator := newHashTableIterator(ht.bpm, headerPage, originalBucketIndex, originalBucketOffset)

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

	ht.bpm.UnpinPage(iterator.blockId, true)
	ht.bpm.UnpinPage(ht.headerPageId, false)
}

//func (ht *LinearProbeHashTable) hash(key int) int {
func (ht *LinearProbeHashTable) hash(key []byte) uint32 {
	h := murmur3.New128()
	//bs := make([]byte, 4)
	//binary.LittleEndian.PutUint32(bs, uint32(key))

	//h.Write(bs)
	h.Write(key)

	hash := h.Sum(nil)

	//return int(binary.LittleEndian.Uint32(hash))
	return binary.LittleEndian.Uint32(hash)
}
