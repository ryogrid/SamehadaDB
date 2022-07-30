// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package hash

import (
	"github.com/ryogrid/SamehadaDB/storage/page"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/types"
)

type hashTableIterator struct {
	bpm        *buffer.BufferPoolManager
	headerPage *page.HashTableHeaderPage
	bucket     uint32
	offset     uint32
	blockId    types.PageID
	blockPage  *page.HashTableBlockPage
}

func newHashTableIterator(bpm *buffer.BufferPoolManager, header *page.HashTableHeaderPage, bucket uint32, offset uint32) *hashTableIterator {
	blockPageId := header.GetBlockPageId(bucket)

	bPageData := bpm.FetchPage(blockPageId).Data()
	blockPage := (*page.HashTableBlockPage)(unsafe.Pointer(bPageData))

	return &hashTableIterator{bpm, header, bucket, offset, blockPageId, blockPage}
}

func (itr *hashTableIterator) next() {
	itr.offset++
	// reached end of the current block page, we need to go to the next one
	if itr.offset >= page.BlockArraySize {
		itr.bucket += 1
		itr.offset = 0

		// we need to go to the first block
		if itr.bucket >= itr.headerPage.NumBlocks() {
			itr.bucket = 0
		}

		itr.bpm.UnpinPage(itr.blockId, true)
		itr.blockId = itr.headerPage.GetBlockPageId(itr.bucket)

		bPageData := itr.bpm.FetchPage(itr.blockId).Data()
		itr.blockPage = (*page.HashTableBlockPage)(unsafe.Pointer(bPageData))
	}
}
