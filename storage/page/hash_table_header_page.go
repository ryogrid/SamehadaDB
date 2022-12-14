// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package page

import "github.com/ryogrid/SamehadaDB/types"

/**
 *
 * Header Page for linear probing hash table.
 *
 * Header format (size in byte, 12 bytes in total):
 * ----------------------------------------------------------------------
 * |  PageId(4) | NextBlockIndex(4) | Size (4) | BlockPageIds (4) x 1020
 * ----------------------------------------------------------------------
 * all Page content size: 12 + 4 * 1020 = 4092
 */
type HashTableHeaderPage struct {
	pageId types.PageID
	//lsn          int    // log sequence number
	nextIndex    uint32 // the next index to add a new entry to blockPageIds
	size         int    // the number of key/value pairs the hash table can hold
	blockPageIds [1020]types.PageID
}

func (page *HashTableHeaderPage) GetBlockPageId(index uint32) types.PageID {
	return page.blockPageIds[index]
}

func (page *HashTableHeaderPage) GetPageId() types.PageID {
	return page.pageId
}

func (page *HashTableHeaderPage) SetPageId(pageId types.PageID) {
	page.pageId = pageId
}

//func (page *HashTableHeaderPage) GetLSN() int {
//	return page.lsn
//}
//
//func (page *HashTableHeaderPage) SetLSN(lsn int) {
//	page.lsn = lsn
//}

func (page *HashTableHeaderPage) AddBlockPageId(pageId types.PageID) {
	page.blockPageIds[page.nextIndex] = pageId
	page.nextIndex++
}

func (page *HashTableHeaderPage) NumBlocks() uint32 {
	return page.nextIndex
}

func (page *HashTableHeaderPage) SetSize(size int) {
	page.size = size
}

func (page *HashTableHeaderPage) GetSize() int {
	return page.size
}
