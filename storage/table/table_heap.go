package table

import (
	"unsafe"

	"github.com/brunocalza/go-bustub/storage/buffer"
	"github.com/brunocalza/go-bustub/storage/page"
)

// TableHeap represents a physical table on disk.
// It contains the id of the first table page. The table page is a doubly-linked to other table pages.
type TableHeap struct {
	bpm         *buffer.BufferPoolManager
	firstPageId page.PageID
}

// NewTableHeap creates a table heap without a transaction. (open table)
func NewTableHeap(bpm *buffer.BufferPoolManager) *TableHeap {
	p := bpm.NewPage()
	firstPage := (*TablePage)(unsafe.Pointer(p))
	firstPage.init(p.ID(), page.InvalidID)
	bpm.UnpinPage(p.ID(), true)
	return &TableHeap{bpm, p.ID()}
}

// InsertTuple inserts a tuple into the table
//
// It fetches the first page and tries to insert the tuple there.
// If the tuple is too large (>= page_size):
// 1. It tries to insert in the next page
// 2. If there is no next page, it creates a new page and insert in it
func (t *TableHeap) InsertTuple(tuple *Tuple) (rid *page.RID, err error) {
	currentPage := (*TablePage)(unsafe.Pointer(t.bpm.FetchPage(t.firstPageId)))

	for {
		rid, err = currentPage.InsertTuple(tuple)
		if err == nil || err == ErrEmptyTuple {
			break
		}

		nextPageId := currentPage.getNextPageId()
		if nextPageId.IsValid() {
			t.bpm.UnpinPage(currentPage.getTablePageId(), false)
			currentPage = (*TablePage)(unsafe.Pointer(t.bpm.FetchPage(nextPageId)))
		} else {
			p := t.bpm.NewPage()
			newPage := (*TablePage)(unsafe.Pointer(p))
			currentPage.setNextPageId(p.ID())
			newPage.init(p.ID(), currentPage.getTablePageId())
			t.bpm.UnpinPage(currentPage.getTablePageId(), true)
			currentPage = newPage
		}
	}

	t.bpm.UnpinPage(currentPage.getTablePageId(), true)
	return rid, nil
}

// GetTuple reads a tuple from the table
func (t *TableHeap) GetTuple(rid *page.RID) *Tuple {
	page := (*TablePage)(unsafe.Pointer(t.bpm.FetchPage(rid.GetPageId())))
	defer t.bpm.UnpinPage(page.ID(), false)
	return page.getTuple(rid)
}

// GetFirstTuple reads the first tuple from the table
func (t *TableHeap) GetFirstTuple() *Tuple {
	var rid *page.RID
	pageId := t.firstPageId
	for pageId.IsValid() {
		page := (*TablePage)(unsafe.Pointer(t.bpm.FetchPage(pageId)))
		rid = page.getTupleFirstRID()
		t.bpm.UnpinPage(pageId, false)
		if rid != nil {
			break
		}
		pageId = page.getNextPageId()
	}
	return t.GetTuple(rid)
}

// Iterator returns a iterator for this table heap
func (t *TableHeap) Iterator() *TableHeapIterator {
	return NewTableHeapIterator(t)
}
