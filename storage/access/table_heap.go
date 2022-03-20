// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package access

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page"
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
	firstPage.Init(p.ID(), types.InvalidPageID, log_manager, lock_manager, txn)
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
func (t *TableHeap) InsertTuple(tuple_ *tuple.Tuple, txn *Transaction, idxKeyColNum int, table_metadata *catalog.TableMetadata) (rid *page.RID, err error) {
	currentPage := CastPageAsTablePage(t.bpm.FetchPage(t.firstPageId))

	for {
		rid, err = currentPage.InsertTuple(tuple_, t.log_manager, t.lock_manager, txn)
		if err == nil || err == ErrEmptyTuple {
			break
		}
		// TODO: (SDB) rid setting is SemehadaDB original code. Pay attention.
		tuple_.SetRID(rid)

		// TODO: (SDB) insert index entry if needed

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
	// Update the transaction's write set.
	txn.AddIntoWriteSet(NewWriteRecord(*rid, INSERT, new(tuple.Tuple), t))
	return rid, nil
}

/* TODO: (SDB) not ported yet
/*
bool TableHeap::MarkDelete(const RID &rid, Transaction *txn) {
	// TODO(Amadou): remove empty page
	// Find the page which contains the tuple.
	auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
	// If the page could not be found, then abort the transaction.
	if (page == nullptr) {
	  txn->SetState(TransactionState::ABORTED);
	  return false;
	}
	// Otherwise, mark the tuple as deleted.
	page->WLatch();
	page->MarkDelete(rid, txn, lock_manager_, log_manager_);
	page->WUnlatch();
	buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
	// Update the transaction's write set.
	txn->GetWriteSet()->emplace_back(rid, WType::DELETE, Tuple{}, this);
	return true;
  }

  bool TableHeap::UpdateTuple(const Tuple &tuple, const RID &rid, Transaction *txn) {
	// Find the page which contains the tuple.
	auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
	// If the page could not be found, then abort the transaction.
	if (page == nullptr) {
	  txn->SetState(TransactionState::ABORTED);
	  return false;
	}
	// Update the tuple; but first save the old value for rollbacks.
	Tuple old_tuple;
	page->WLatch();
	bool is_updated = page->UpdateTuple(tuple, &old_tuple, rid, txn, lock_manager_, log_manager_);
	page->WUnlatch();
	buffer_pool_manager_->UnpinPage(page->GetTablePageId(), is_updated);
	// Update the transaction's write set.
	if (is_updated && txn->GetState() != TransactionState::ABORTED) {
	  txn->GetWriteSet()->emplace_back(rid, WType::UPDATE, old_tuple, this);
	}
	return is_updated;
  }

  void TableHeap::ApplyDelete(const RID &rid, Transaction *txn) {
	// Find the page which contains the tuple.
	auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
	BUSTUB_ASSERT(page != nullptr, "Couldn't find a page containing that RID.");
	// Delete the tuple from the page.
	page->WLatch();
	page->ApplyDelete(rid, txn, log_manager_);
	lock_manager_->Unlock(txn, rid);
	page->WUnlatch();
	buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  }

  void TableHeap::RollbackDelete(const RID &rid, Transaction *txn) {
	// Find the page which contains the tuple.
	auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
	BUSTUB_ASSERT(page != nullptr, "Couldn't find a page containing that RID.");
	// Rollback the delete.
	page->WLatch();
	page->RollbackDelete(rid, txn, log_manager_);
	page->WUnlatch();
	buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  }
*/
// GetTuple reads a tuple from the table
func (t *TableHeap) GetTuple(rid *page.RID, txn *Transaction) *tuple.Tuple {
	page := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	defer t.bpm.UnpinPage(page.ID(), false)
	return page.GetTuple(rid, t.log_manager, t.lock_manager, txn)
}

/*
// GetTuple reads a tuple from the table
func (t *TableHeap) GetTuplesWithIndexKey(key []byte, table_metadata *catalog.TableMetadata, txn *Transaction) *tuple.Tuple {
	// TODO: (SDB) need implment GetTuplesWithIndexKey
	panic("not implmented yet")
	// get Index class from table_metadata and get RIDs with it. then call GetTuple.
	page := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	defer t.bpm.UnpinPage(page.ID(), false)
	return page.GetTuple(rid, t.log_manager, t.lock_manager, txn)
}
*/

// GetFirstTuple reads the first tuple from the table
func (t *TableHeap) GetFirstTuple(txn *Transaction) *tuple.Tuple {
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
	return t.GetTuple(rid, txn)
}

// Iterator returns a iterator for this table heap
func (t *TableHeap) Iterator(txn *Transaction) *TableHeapIterator {
	return NewTableHeapIterator(t, txn)
}
