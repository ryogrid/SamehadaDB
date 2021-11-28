// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in license/go-bustub dir

package hash

import (
	"encoding/binary"
	"errors"
	"unsafe"

	"github.com/ryogrid/SaitomDB/storage/buffer"
	"github.com/ryogrid/SaitomDB/storage/page"
	"github.com/ryogrid/SaitomDB/types"
	"github.com/spaolacci/murmur3"
)

type LinearProbeHashTable struct {
	headerPageId types.PageID
	bpm          *buffer.BufferPoolManager
}

func NewHashTable(bpm *buffer.BufferPoolManager, numBuckets int) *LinearProbeHashTable {
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

	return &LinearProbeHashTable{header.ID(), bpm}
}

func (ht *LinearProbeHashTable) GetValue(key int) []int {
	hPageData := ht.bpm.FetchPage(ht.headerPageId).Data()
	headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(hPageData))

	hash := ht.hash(key)

	originalBucketIndex := hash % headerPage.NumBlocks()
	originalBucketOffset := hash % page.BlockArraySize

	iterator := newHashTableIterator(ht.bpm, headerPage, originalBucketIndex, originalBucketOffset)

	result := []int{}
	blockPage, offset := iterator.blockPage, iterator.offset
	var bucket int
	for blockPage.IsOccupied(offset) { // stop the search and we find an empty spot
		if blockPage.IsReadable(offset) && blockPage.KeyAt(offset) == key {
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

func (ht *LinearProbeHashTable) Insert(key int, value int) (err error) {
	hPageData := ht.bpm.FetchPage(ht.headerPageId).Data()
	headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(hPageData))

	hash := ht.hash(key)

	originalBucketIndex := hash % headerPage.NumBlocks()
	originalBucketOffset := hash % page.BlockArraySize

	iterator := newHashTableIterator(ht.bpm, headerPage, originalBucketIndex, originalBucketOffset)

	blockPage, offset := iterator.blockPage, iterator.offset
	var bucket int
	for {
		if blockPage.IsOccupied(offset) && blockPage.ValueAt(offset) == value {
			err = errors.New("duplicated values on the same key are not allowed")
			break
		}

		if !blockPage.IsOccupied(offset) {
			blockPage.Insert(offset, key, value)
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

func (ht *LinearProbeHashTable) Remove(key int, value int) {
	hPageData := ht.bpm.FetchPage(ht.headerPageId).Data()
	headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(hPageData))

	hash := ht.hash(key)

	originalBucketIndex := hash % headerPage.NumBlocks()
	originalBucketOffset := hash % page.BlockArraySize

	iterator := newHashTableIterator(ht.bpm, headerPage, originalBucketIndex, originalBucketOffset)

	blockPage, offset := iterator.blockPage, iterator.offset
	var bucket int
	for blockPage.IsOccupied(offset) { // stop the search and we find an empty spot
		if blockPage.IsOccupied(offset) && blockPage.KeyAt(offset) == key && blockPage.ValueAt(offset) == value {
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

func (ht *LinearProbeHashTable) hash(key int) int {
	h := murmur3.New128()
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(key))

	h.Write(bs)

	hash := h.Sum(nil)

	return int(binary.LittleEndian.Uint32(hash))
}
