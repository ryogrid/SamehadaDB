// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package page

import "github.com/ryogrid/SamehadaDB/types"

/**
 *
 * Header format (size in byte, 16 bytes in total):
 * -------------------------------------------------------------
 * | LSN (4) | Size (4) | PageId(4) | NextBlockIndex(4)
 * -------------------------------------------------------------
 */
type HashTableHeaderPage struct {
	pageId       types.PageID
	lsn          int // log sequence number
	nextIndex    int // the next index to add a new entry to blockPageIds
	size         int // the number of key/value pairs the hash table can hold
	blockPageIds [1020]types.PageID
}

func (page *HashTableHeaderPage) GetBlockPageId(index int) types.PageID {
	return page.blockPageIds[index]
}

func (page *HashTableHeaderPage) GetPageId() types.PageID {
	return page.pageId
}

func (page *HashTableHeaderPage) SetPageId(pageId types.PageID) {
	page.pageId = pageId
}

func (page *HashTableHeaderPage) GetLSN() int {
	return page.lsn
}

func (page *HashTableHeaderPage) SetLSN(lsn int) {
	page.lsn = lsn
}

func (page *HashTableHeaderPage) AddBlockPageId(pageId types.PageID) {
	page.blockPageIds[page.nextIndex] = pageId
	page.nextIndex++
}

func (page *HashTableHeaderPage) NumBlocks() int {
	return page.nextIndex
}

func (page *HashTableHeaderPage) SetSize(size int) {
	page.size = size
}

func (page *HashTableHeaderPage) GetSize() int {
	return page.size
}
