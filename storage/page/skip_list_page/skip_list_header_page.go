// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package skip_list_page

import "github.com/ryogrid/SamehadaDB/types"

// TODO: (SDB) not implemented yet skip_list_header_page.go

/**
 *
 * Header Page for linear probing hash table.
 *
 * Header format (size in byte, 16 bytes in total):
 * -------------------------------------------------------------
 * | LSN (4) | Size (4) | PageId(4) | NextBlockIndex(4)
 * -------------------------------------------------------------
 */
type SkipListHeaderPage struct {
	pageId       types.PageID
	lsn          int    // log sequence number
	nextIndex    uint32 // the next index to add a new entry to blockPageIds
	size         int    // the number of key/value pairs the hash table can hold
	blockPageIds [1020]types.PageID
}

func (page_ *SkipListHeaderPage) GetBlockPageId(index uint32) types.PageID {
	return page_.blockPageIds[index]
}

func (page_ *SkipListHeaderPage) GetPageId() types.PageID {
	return page_.pageId
}

func (page_ *SkipListHeaderPage) SetPageId(pageId types.PageID) {
	page_.pageId = pageId
}

func (page_ *SkipListHeaderPage) GetLSN() int {
	return page_.lsn
}

func (page_ *SkipListHeaderPage) SetLSN(lsn int) {
	page_.lsn = lsn
}

func (page_ *SkipListHeaderPage) AddBlockPageId(pageId types.PageID) {
	page_.blockPageIds[page_.nextIndex] = pageId
	page_.nextIndex++
}

func (page_ *SkipListHeaderPage) NumBlocks() uint32 {
	return page_.nextIndex
}

func (page_ *SkipListHeaderPage) SetSize(size int) {
	page_.size = size
}

func (page_ *SkipListHeaderPage) GetSize() int {
	return page_.size
}
