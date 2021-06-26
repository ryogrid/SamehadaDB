package hash

import (
	"unsafe"

	"github.com/brunocalza/go-bustub/buffer"
	"github.com/brunocalza/go-bustub/storage/page"
)

type hashTableIterator struct {
	bpm        *buffer.BufferPoolManager
	headerPage *page.HashTableHeaderPage
	bucket     int
	offset     int
	blockId    page.PageID
	blockPage  *page.HashTableBlockPage
}

func newHashTableIterator(bpm *buffer.BufferPoolManager, header *page.HashTableHeaderPage, bucket int, offset int) *hashTableIterator {
	blockPageId := header.GetBlockPageId(bucket)

	bPageData := bpm.FetchPage(blockPageId).Data()
	blockPage := (*page.HashTableBlockPage)(unsafe.Pointer(bPageData))

	return &hashTableIterator{bpm, header, bucket, offset, blockPageId, blockPage}
}

func (itr *hashTableIterator) next() {
	itr.offset++
	// the current block page is full, we need to go to the next one
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
