// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package access

import (
	"github.com/ryogrid/SaitomDB/storage/buffer"
	"github.com/ryogrid/SaitomDB/storage/page"
	"github.com/ryogrid/SaitomDB/storage/table"
	"github.com/ryogrid/SaitomDB/types"
)

// TableHeap represents a physical table on disk.
// It contains the id of the first table table. The table page is a doubly-linked to other table pages.
type TableHeap struct {
	bpm         *buffer.BufferPoolManager
	firstPageId types.PageID
}

// NewTableHeap creates a table heap without a transaction. (open table)
func NewTableHeap(bpm *buffer.BufferPoolManager) *TableHeap {
	p := bpm.NewPage()
	firstPage := table.CastPageAsTablePage(p)
	firstPage.Init(p.ID(), types.InvalidPageID)
	bpm.UnpinPage(p.ID(), true)
	return &TableHeap{bpm, p.ID()}
}

// InitTableHeap ...
func InitTableHeap(bpm *buffer.BufferPoolManager, pageId types.PageID) *TableHeap {
	return &TableHeap{bpm, pageId}
}

// GetFirstPageId returns firstPageId
func (t *TableHeap) GetFirstPageId() types.PageID {
	return t.firstPageId
}

// InsertTuple inserts a tuple into the table
//
// It fetches the first page and tries to insert the tuple there.
// If the tuple is too large (>= page_size):
// 1. It tries to insert in the next page
// 2. If there is no next page, it creates a new page and insert in it
func (t *TableHeap) InsertTuple(tuple *table.Tuple) (rid *page.RID, err error) {
	currentPage := table.CastPageAsTablePage(t.bpm.FetchPage(t.firstPageId))

	for {
		rid, err = currentPage.InsertTuple(tuple)
		if err == nil || err == table.ErrEmptyTuple {
			break
		}

		nextPageId := currentPage.GetNextPageId()
		if nextPageId.IsValid() {
			t.bpm.UnpinPage(currentPage.GetTablePageId(), false)
			currentPage = table.CastPageAsTablePage(t.bpm.FetchPage(nextPageId))
		} else {
			p := t.bpm.NewPage()
			newPage := table.CastPageAsTablePage(p)
			currentPage.SetNextPageId(p.ID())
			newPage.Init(p.ID(), currentPage.GetTablePageId())
			t.bpm.UnpinPage(currentPage.GetTablePageId(), true)
			currentPage = newPage
		}
	}

	t.bpm.UnpinPage(currentPage.GetTablePageId(), true)
	return rid, nil
}

// GetTuple reads a tuple from the table
func (t *TableHeap) GetTuple(rid *page.RID) *table.Tuple {
	page := table.CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	defer t.bpm.UnpinPage(page.ID(), false)
	return page.GetTuple(rid)
}

// GetFirstTuple reads the first tuple from the table
func (t *TableHeap) GetFirstTuple() *table.Tuple {
	var rid *page.RID
	pageId := t.firstPageId
	for pageId.IsValid() {
		page := table.CastPageAsTablePage(t.bpm.FetchPage(pageId))
		rid = page.GetTupleFirstRID()
		t.bpm.UnpinPage(pageId, false)
		if rid != nil {
			break
		}
		pageId = page.GetNextPageId()
	}
	return t.GetTuple(rid)
}

// Iterator returns a iterator for this table heap
func (t *TableHeap) Iterator() *TableHeapIterator {
	return NewTableHeapIterator(t)
}
