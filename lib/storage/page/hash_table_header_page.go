// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package page

import "github.com/ryogrid/SamehadaDB/lib/types"

/**
 *
 * Header Page for linear probing hash table.
 *
 * Header format (size in byte, 12 bytes in total):
 * ----------------------------------------------------------------------
 * |  PageID(4) | NextBlockIndex(4) | Size (4) | BlockPageIDs (4) x 1020
 * ----------------------------------------------------------------------
 * all Page content size: 12 + 8 * 1020 = 4096
 */
type HashTableHeaderPage struct {
	pageID types.PageID
	//lsn          int    // log sequence number
	nextIndex    uint64 // the next index to add a new entry to blockPageIDs
	size         int    // the number of key/value pairs the hash table can hold
	blockPageIDs [1020]types.PageID
}

func (page *HashTableHeaderPage) GetBlockPageID(index uint64) types.PageID {
	return page.blockPageIDs[index]
}

func (page *HashTableHeaderPage) GetPageID() types.PageID {
	return page.pageID
}

func (page *HashTableHeaderPage) SetPageID(pageID types.PageID) {
	page.pageID = pageID
}

func (page *HashTableHeaderPage) AddBlockPageID(pageID types.PageID) {
	page.blockPageIDs[page.nextIndex] = pageID
	page.nextIndex++
}

func (page *HashTableHeaderPage) NumBlocks() uint64 {
	return page.nextIndex
}

func (page *HashTableHeaderPage) SetSize(size int) {
	page.size = size
}

func (page *HashTableHeaderPage) GetSize() int {
	return page.size
}
