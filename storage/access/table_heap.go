// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package access

import (
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

// TableHeap represents a physical table on disk.
// It contains the id of the first table table. The table page is a doubly-linked to other table pages.
type TableHeap struct {
	bpm          *buffer.BufferPoolManager
	firstPageId  types.PageID
	log_manager  *recovery.LogManager
	lock_manager *LockManager
}

// NewTableHeap creates a table heap without a  (open table)
func NewTableHeap(bpm *buffer.BufferPoolManager, log_manager *recovery.LogManager, lock_manager *LockManager, transaction *Transaction) *TableHeap {
	p := bpm.NewPage()
	firstPage := CastPageAsTablePage(p)
	firstPage.Init(p.ID(), types.InvalidPageID, log_manager, lock_manager, transaction)
	bpm.UnpinPage(p.ID(), true)
	return &TableHeap{bpm, p.ID(), log_manager, lock_manager}
}

// InitTableHeap ...
func InitTableHeap(bpm *buffer.BufferPoolManager, pageId types.PageID, log_manager *recovery.LogManager, lock_manager *LockManager) *TableHeap {
	return &TableHeap{bpm, pageId, log_manager, lock_manager}
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
func (t *TableHeap) InsertTuple(tuple *tuple.Tuple, txn *Transaction) (rid *page.RID, err error) {
	currentPage := CastPageAsTablePage(t.bpm.FetchPage(t.firstPageId))

	for {
		rid, err = currentPage.InsertTuple(tuple, t.log_manager, t.lock_manager, txn)
		if err == nil || err == ErrEmptyTuple {
			break
		}

		nextPageId := currentPage.GetNextPageId()
		if nextPageId.IsValid() {
			t.bpm.UnpinPage(currentPage.GetTablePageId(), false)
			currentPage = CastPageAsTablePage(t.bpm.FetchPage(nextPageId))
		} else {
			p := t.bpm.NewPage()
			newPage := CastPageAsTablePage(p)
			currentPage.SetNextPageId(p.ID())
			newPage.Init(p.ID(), currentPage.GetTablePageId(), t.log_manager, t.lock_manager, txn)
			t.bpm.UnpinPage(currentPage.GetTablePageId(), true)
			currentPage = newPage
		}
	}

	t.bpm.UnpinPage(currentPage.GetTablePageId(), true)
	return rid, nil
}

// GetTuple reads a tuple from the table
func (t *TableHeap) GetTuple(rid *page.RID, transaction *Transaction) *tuple.Tuple {
	page := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	defer t.bpm.UnpinPage(page.ID(), false)
	return page.GetTuple(rid, t.log_manager, t.lock_manager, transaction)
}

// GetFirstTuple reads the first tuple from the table
func (t *TableHeap) GetFirstTuple() *tuple.Tuple {
	var rid *page.RID
	pageId := t.firstPageId
	for pageId.IsValid() {
		page := CastPageAsTablePage(t.bpm.FetchPage(pageId))
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
