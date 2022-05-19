// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package access

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

// TableHeap represents a physical table on disk.
// It contains the id of the first table page. The table page is a doubly-linked to other table pages.
type TableHeap struct {
	bpm          *buffer.BufferPoolManager
	firstPageId  types.PageID
	log_manager  *recovery.LogManager
	lock_manager *LockManager
}

// NewTableHeap creates a table heap without a  (open table)
func NewTableHeap(bpm *buffer.BufferPoolManager, log_manager *recovery.LogManager, lock_manager *LockManager, txn *Transaction) *TableHeap {
	p := bpm.NewPage()

	firstPage := CastPageAsTablePage(p)
	firstPage.WLatch()
	firstPage.Init(p.ID(), types.InvalidPageID, log_manager, lock_manager, txn)
	firstPage.WUnlatch()
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
// PAY ATTENTION: index entry is not inserted
//
// It fetches the first page and tries to insert the tuple there.
// If the tuple is too large (>= page_size):
// 1. It tries to insert in the next page
// 2. If there is no next page, it creates a new page and insert in it
func (t *TableHeap) InsertTuple(tuple_ *tuple.Tuple, txn *Transaction) (rid *page.RID, err error) {
	currentPage := CastPageAsTablePage(t.bpm.FetchPage(t.firstPageId))

	// Insert into the first page with enough space. If no such page exists, create a new page and insert into that.
	// INVARIANT: currentPage is WLatched if you leave the loop normally.

	for {
		currentPage.WLatch()
		rid, err = currentPage.InsertTuple(tuple_, t.log_manager, t.lock_manager, txn)
		if err == nil || err == ErrEmptyTuple {
			currentPage.WUnlatch()
			break
		}
		if rid == nil && err != nil && err != ErrEmptyTuple && err != ErrNotEnoughSpace {
			currentPage.WUnlatch()
			return nil, err
		}

		nextPageId := currentPage.GetNextPageId()
		if nextPageId.IsValid() {
			t.bpm.UnpinPage(currentPage.GetTablePageId(), false)
			currentPage.WUnlatch()
			currentPage = CastPageAsTablePage(t.bpm.FetchPage(nextPageId))
			//currentPage.WLatch()
		} else {
			p := t.bpm.NewPage()
			currentPage.SetNextPageId(p.ID())
			currentPage.WUnlatch()
			newPage := CastPageAsTablePage(p)
			//newPage.WLatch()
			//currentPage.SetNextPageId(p.ID())
			currentPage.RLatch()
			newPage.Init(p.ID(), currentPage.GetTablePageId(), t.log_manager, t.lock_manager, txn)
			t.bpm.UnpinPage(currentPage.GetTablePageId(), true)
			currentPage.RUnlatch()
			currentPage = newPage
		}
	}
	//currentPage.WUnlatch()

	t.bpm.UnpinPage(currentPage.GetTablePageId(), true)
	// Update the transaction's write set.
	txn.AddIntoWriteSet(NewWriteRecord(*rid, INSERT, new(tuple.Tuple), t))
	return rid, nil
}

// TODO: (SDB) need to update selected column only (UpdateTuple of TableHeap)
// update_ranges_xxxx contains update data ranges x1_new <= data < x2_new
// spesified nil, range is ignored and data buffer of new tuple replace existed data on Page
func (t *TableHeap) UpdateTuple(tuple_ *tuple.Tuple, update_ranges_new [][2]int, update_col_idxs []int, schema_ *schema.Schema, rid page.RID, txn *Transaction) bool {
	// Find the page which contains the tuple.
	page_ := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	// If the page could not be found, then abort the transaction.
	if page_ == nil {
		txn.SetState(ABORTED)
		return false
	}
	// Update the tuple; but first save the old value for rollbacks.
	old_tuple := new(tuple.Tuple)
	old_tuple.SetRID(new(page.RID))
	page_.WLatch()

	is_updated := page_.UpdateTuple(tuple_, update_ranges_new, update_col_idxs, schema_, old_tuple, &rid, txn, t.lock_manager, t.log_manager)
	page_.WUnlatch()
	t.bpm.UnpinPage(page_.GetTablePageId(), is_updated)
	// Update the transaction's write set.
	if is_updated && txn.GetState() != ABORTED {
		txn.AddIntoWriteSet(NewWriteRecord(rid, UPDATE, old_tuple, t))
	}
	return is_updated
}

func (t *TableHeap) MarkDelete(rid *page.RID, txn *Transaction) bool {
	// TODO(Amadou): remove empty page
	// Find the page which contains the tuple.
	page_ := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	// If the page could not be found, then abort the transaction.
	if page_ == nil {
		txn.SetState(ABORTED)
		return false
	}
	// Otherwise, mark the tuple as deleted.
	page_.WLatch()
	is_marked := page_.MarkDelete(rid, txn, t.lock_manager, t.log_manager)
	page_.WUnlatch()
	t.bpm.UnpinPage(page_.GetTablePageId(), true)
	if is_marked {
		// Update the transaction's write set.
		txn.AddIntoWriteSet(NewWriteRecord(*rid, DELETE, new(tuple.Tuple), t))
	}

	return is_marked
}

func (t *TableHeap) ApplyDelete(rid *page.RID, txn *Transaction) {
	// Find the page which contains the tuple.
	page_ := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	common.SH_Assert(page_ != nil, "Couldn't find a page containing that RID.")
	// Delete the tuple from the page.
	page_.WLatch()
	page_.ApplyDelete(rid, txn, t.log_manager)
	//t.lock_manager.Unlock(txn, []page.RID{*rid})
	page_.WUnlatch()
	t.bpm.UnpinPage(page_.GetTablePageId(), true)
}

func (t *TableHeap) RollbackDelete(rid *page.RID, txn *Transaction) {
	// Find the page which contains the tuple.
	page_ := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	common.SH_Assert(page_ != nil, "Couldn't find a page containing that RID.")
	// Rollback the delete.
	page_.WLatch()
	page_.RollbackDelete(rid, txn, t.log_manager)
	page_.WUnlatch()
	t.bpm.UnpinPage(page_.GetTablePageId(), true)
}

// GetTuple reads a tuple from the table
func (t *TableHeap) GetTuple(rid *page.RID, txn *Transaction) *tuple.Tuple {
	if !txn.IsSharedLocked(rid) && !txn.IsExclusiveLocked(rid) && !t.lock_manager.LockShared(txn, rid) {
		txn.SetState(ABORTED)
		return nil
	}
	page := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	defer t.bpm.UnpinPage(page.ID(), false)
	page.RLatch()
	ret := page.GetTuple(rid, t.log_manager, t.lock_manager, txn)
	page.RUnlatch()
	return ret
}

// GetFirstTuple reads the first tuple from the table
func (t *TableHeap) GetFirstTuple(txn *Transaction) *tuple.Tuple {
	var rid *page.RID = nil
	pageId := t.firstPageId
	for pageId.IsValid() {
		page := CastPageAsTablePage(t.bpm.FetchPage(pageId))
		page.RLatch()
		rid = page.GetTupleFirstRID()
		t.bpm.UnpinPage(pageId, false)
		if rid != nil {
			page.RUnlatch()
			break
		}
		pageId = page.GetNextPageId()
		page.RUnlatch()
	}
	if rid == nil {
		return nil
	}
	return t.GetTuple(rid, txn)
}

// Iterator returns a iterator for this table heap
func (t *TableHeap) Iterator(txn *Transaction) *TableHeapIterator {
	return NewTableHeapIterator(t, t.lock_manager, txn)
}

func (t *TableHeap) GetBufferPoolManager() *buffer.BufferPoolManager {
	return t.bpm
}
