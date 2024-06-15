// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package access

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

// TableHeap represents a physical table on disk.
// It contains the id of the first table page. The table page is a doubly-linked to other table pages.
type TableHeap struct {
	bpm          *buffer.BufferPoolManager
	firstPageId  types.PageID
	log_manager  *recovery.LogManager
	lock_manager *LockManager
	// for make faster to insert tuple, memorize last page id (not persisted)
	// but, seek from last means waste unused space of former pages
	// when delete or update occurs it is initialized to firstPageId
	lastPageId types.PageID
}

// NewTableHeap creates a table heap without a  (open table)
func NewTableHeap(bpm *buffer.BufferPoolManager, log_manager *recovery.LogManager, lock_manager *LockManager, txn *Transaction) *TableHeap {
	p := bpm.NewPage()

	firstPage := CastPageAsTablePage(p)
	firstPage.WLatch()
	firstPage.AddWLatchRecord(int32(txn.txn_id))
	firstPage.Init(p.GetPageId(), types.InvalidPageID, log_manager, lock_manager, txn)
	// flush page for recovery process works...
	bpm.FlushPage(p.GetPageId())
	bpm.UnpinPage(p.GetPageId(), true)
	firstPage.RemoveWLatchRecord(int32(txn.txn_id))
	firstPage.WUnlatch()
	return &TableHeap{bpm, p.GetPageId(), log_manager, lock_manager, p.GetPageId()}
}

// InitTableHeap ...
func InitTableHeap(bpm *buffer.BufferPoolManager, pageId types.PageID, log_manager *recovery.LogManager, lock_manager *LockManager) *TableHeap {
	return &TableHeap{bpm, pageId, log_manager, lock_manager, pageId}
}

// GetFirstPageId returns firstPageId
func (t *TableHeap) GetFirstPageId() types.PageID {
	return t.firstPageId
}

// InsertTuple inserts a tuple1 into the table
// PAY ATTENTION: index entry is not inserted
//
// It fetches the first page and tries to insert the tuple1 there.
// If the tuple1 is too large (>= page_size):
// 1. It tries to insert in the next page
// 2. If there is no next page, it creates a new page and insert in it
func (t *TableHeap) InsertTuple(tuple_ *tuple.Tuple, txn *Transaction, oid uint32) (rid *page.RID, err error) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("TableHeap::InsertTuple called. txn.txn_id:%v dbgInfo:%s tuple_:%v\n", txn.txn_id, txn.dbgInfo, *tuple_)
		}
		if common.ActiveLogKindSetting&common.BUFFER_INTERNAL_STATE > 0 {
			t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::InsertTuple start. txn.txn_id: %d dbgInfo:%s", txn.txn_id, txn.dbgInfo))
			defer func() {
				t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::InsertTuple end. txn.txn_id: %d dbgInfo:%s", txn.txn_id, txn.dbgInfo))
			}()
		}
	}

	// seek from last (almost case)
	currentPage := CastPageAsTablePage(t.bpm.FetchPage(t.lastPageId))

	currentPage.WLatch()
	currentPage.AddWLatchRecord(int32(txn.txn_id))
	// Insert into the first page with enough space. If no such page exists, create a new page and insert into that.
	// INVARIANT: currentPage is WLatched if you leave the loop normally.

	for {
		rid, err = currentPage.InsertTuple(tuple_, t.log_manager, t.lock_manager, txn)
		if err == nil || err == ErrEmptyTuple {
			break
		}

		nextPageId := currentPage.GetNextPageId()
		if nextPageId.IsValid() {
			nextPage := CastPageAsTablePage(t.bpm.FetchPage(nextPageId))
			nextPage.WLatch()
			nextPage.AddWLatchRecord(int32(txn.txn_id))
			t.bpm.UnpinPage(currentPage.GetPageId(), false)
			if common.EnableDebug && common.ActiveLogKindSetting&common.PIN_COUNT_ASSERT > 0 {
				common.SH_Assert(currentPage.PinCount() == 0, "PinCount is not zero at TableHeap::InsertTuple!!!")
			}
			currentPage.RemoveWLatchRecord(int32(txn.txn_id))
			currentPage.WUnlatch()
			currentPage = nextPage
			// holding WLatch of currentPage here
		} else {
			p := t.bpm.NewPage()
			newPage := CastPageAsTablePage(p)
			newPage.WLatch()
			newPage.AddWLatchRecord(int32(txn.txn_id))
			currentPage.SetNextPageId(p.GetPageId())
			currentPageId := currentPage.GetPageId()
			newPage.Init(p.GetPageId(), currentPageId, t.log_manager, t.lock_manager, txn)
			t.bpm.UnpinPage(currentPage.GetPageId(), true)
			if common.EnableDebug && common.ActiveLogKindSetting&common.PIN_COUNT_ASSERT > 0 {
				common.SH_Assert(currentPage.PinCount() == 0, "PinCount is not zero when finish TablePage::UpdateTuple!!!")
			}
			currentPage.RemoveWLatchRecord(int32(txn.txn_id))
			currentPage.WUnlatch()
			currentPage = newPage
			// holding WLatch of currentPage here
		}
	}

	currentPageId := currentPage.GetPageId()
	// memorize last page id
	t.lastPageId = currentPageId
	t.bpm.UnpinPage(currentPageId, true)
	if common.EnableDebug && common.ActiveLogKindSetting&common.PIN_COUNT_ASSERT > 0 {
		common.SH_Assert(currentPage.PinCount() == 0, "PinCount is not zero when finish TablePage::InsertTuple!!!")
	}
	currentPage.RemoveWLatchRecord(int32(txn.txn_id))
	currentPage.WUnlatch()
	// Update the transaction's write set.
	txn.AddIntoWriteSet(NewWriteRecord(rid, INSERT, tuple_, nil, t, oid))
	return rid, nil
}

// if specified nil to update_col_idxs and schema_, all data of existed tuple1 is replaced one of new_tuple
// if specified not nil, new_tuple also should have all columns defined in schema. but not update target value can be dummy value
func (t *TableHeap) UpdateTuple(tuple_ *tuple.Tuple, update_col_idxs []int, schema_ *schema.Schema, oid uint32, rid page.RID, txn *Transaction, isRollback bool) (is_success bool, new_rid_ *page.RID, err_ error, update_tuple_ *tuple.Tuple, old_tuple_ *tuple.Tuple) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("TableHeap::UpadteTuple called. txn.txn_id:%v dbgInfo:%s update_col_idxs:%v rid:%v\n", txn.txn_id, txn.dbgInfo, update_col_idxs, rid)
		}
		if common.ActiveLogKindSetting&common.BUFFER_INTERNAL_STATE > 0 {
			t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::UpdateTuple start.  txn.txn_id: %d dbgInfo:%s", txn.txn_id, txn.dbgInfo))
			defer func() {
				t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::UpdateTuple end.  txn.txn_id: %d dbgInfo:%s", txn.txn_id, txn.dbgInfo))
			}()
		}
	}
	// Find the page which contains the tuple1.
	page_ := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	// If the page could not be found, then abort the transaction.
	if page_ == nil {
		txn.SetState(ABORTED)
		return false, nil, ErrGeneral, nil, nil
	}
	// Update the tuple1; but first save the old value for rollbacks.
	old_tuple := new(tuple.Tuple)
	old_tuple.SetRID(new(page.RID))

	page_.WLatch()
	page_.AddWLatchRecord(int32(txn.txn_id))
	is_updated, err, need_follow_tuple := page_.UpdateTuple(tuple_, update_col_idxs, schema_, old_tuple, &rid, txn, t.lock_manager, t.log_manager)
	t.bpm.UnpinPage(page_.GetPageId(), is_updated)
	if common.EnableDebug && common.ActiveLogKindSetting&common.PIN_COUNT_ASSERT > 0 {
		common.SH_Assert(page_.PinCount() == 0, "PinCount is not zero when finish TablePage::UpdateTuple!!!")
	}
	page_.RemoveWLatchRecord(int32(txn.txn_id))
	page_.WUnlatch()

	if !is_updated {
		fmt.Println("TableHeap::UpdateTuple(): is_updated:", is_updated, " err:", err)
	}

	var new_rid *page.RID
	var isUpdateWithDelInsert = false
	if is_updated == false && err == ErrNotEnoughSpace {
		// delete old_tuple(rid)
		// and insert need_follow_tuple(new_rid)
		// as updating

		// first, delete target tuple1 (old data)
		var is_deleted bool
		if isRollback {
			// when this method is used on TransactinManager::Abort,
			// ApplyDelete should be used because there is no occasion to commit deleted mark
			t.ApplyDelete(&rid, txn)
			// above ApplyDelete does not fail
			is_deleted = true
		} else {
			is_deleted = t.MarkDelete(&rid, oid, txn)
		}

		if !is_deleted {
			//fmt.Println("TableHeap::UpdateTuple(): MarkDelete failed")
			txn.SetState(ABORTED)
			return false, nil, ErrGeneral, nil, nil
		}

		var err2 error = nil
		_, err2 = t.InsertTuple(need_follow_tuple, txn, oid)
		if err2 != nil {
			fmt.Println("TableHeap::UpdateTuple(): InsertTuple failed")
			txn.SetState(ABORTED)
			//txn.AddIntoWriteSet(NewWriteRecord(&rid, nil, DELETE, old_tuple, nil, t, oid))
			return false, nil, ErrPartialUpdate, nil, old_tuple
		}

		// change return values to success
		err = nil
		is_updated = true
		isUpdateWithDelInsert = true
	}

	// add appropriate transaction's write set of Update.
	// when txn is ABORTED state case, data is not updated. so adding a write set entry is not needed
	// when err == ErrNotEnoughSpace route and old tuple delete is only succeeded, DELETE write set entry is added above (no come here)
	if is_updated && txn.GetState() != ABORTED {
		if isUpdateWithDelInsert {
			// adding write record of UPDATE is not needed
		} else {
			// reset seek start point of Insert to first page
			t.lastPageId = t.firstPageId
			txn.AddIntoWriteSet(NewWriteRecord(&rid, UPDATE, old_tuple, need_follow_tuple, t, oid))
		}
	}

	return is_updated, new_rid, err, need_follow_tuple, old_tuple
}

// when isForUpdate arg is true, write record is not created
func (t *TableHeap) MarkDelete(rid *page.RID, oid uint32, txn *Transaction) bool {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("TableHeap::MarkDelete called. txn.txn_id:%v rid:%v  dbgInfo:%s\n", txn.txn_id, *rid, txn.dbgInfo)
		}
		if common.ActiveLogKindSetting&common.BUFFER_INTERNAL_STATE > 0 {
			t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::MarkDelete start.  txn.txn_id: %d dbgInfo:%s", txn.txn_id, txn.dbgInfo))
			defer func() {
				t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::MarkDelete end.  txn.txn_id: %d dbgInfo:%s", txn.txn_id, txn.dbgInfo))
			}()
		}
	}
	// Find the page which contains the tuple1.
	page_ := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	// If the page could not be found, then abort the transaction.
	if page_ == nil {
		txn.SetState(ABORTED)
		return false
	}
	// Otherwise, mark the tuple1 as deleted.
	page_.WLatch()
	page_.AddWLatchRecord(int32(txn.txn_id))
	is_marked, markedTuple := page_.MarkDelete(rid, txn, t.lock_manager, t.log_manager)
	t.bpm.UnpinPage(page_.GetPageId(), true)
	if common.EnableDebug && common.ActiveLogKindSetting&common.PIN_COUNT_ASSERT > 0 {
		common.SH_Assert(page_.PinCount() == 0, "PinCount is not zero when finish TablePage::MarkDelete!!!")
	}
	page_.RemoveWLatchRecord(int32(txn.txn_id))
	page_.WUnlatch()
	if is_marked {
		// Update the transaction's write set.
		txn.AddIntoWriteSet(NewWriteRecord(rid, DELETE, markedTuple, nil, t, oid))
	}

	return is_marked
}

func (t *TableHeap) ApplyDelete(rid *page.RID, txn *Transaction) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("TableHeap::ApplyDelete called. txn.txn_id:%v rid:%v dbgInfo:%s\n", txn.txn_id, *rid, txn.dbgInfo)
		}
		if common.ActiveLogKindSetting&common.BUFFER_INTERNAL_STATE > 0 {
			t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::ApplyDelete start. txn.txn_id: %d dbgInfo:%s", txn.txn_id, txn.dbgInfo))
			defer func() {
				t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::ApplyDelete end. txn.txn_id: %d dbgInfo:%s", txn.txn_id, txn.dbgInfo))
			}()
		}
	}
	// Find the page which contains the tuple1.
	page_ := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	common.SH_Assert(page_ != nil, "Couldn't find a page containing that RID.")
	// Delete the tuple1 from the page.
	page_.WLatch()
	page_.AddWLatchRecord(int32(txn.txn_id))
	page_.ApplyDelete(rid, txn, t.log_manager)

	// reset seek start point of Insert to first page
	t.lastPageId = t.firstPageId

	t.bpm.UnpinPage(page_.GetPageId(), true)
	if common.EnableDebug && common.ActiveLogKindSetting&common.PIN_COUNT_ASSERT > 0 {
		common.SH_Assert(page_.PinCount() == 0, "PinCount is not zero when finish TablePage::ApplyDelete!!!")
	}
	page_.RemoveWLatchRecord(int32(txn.txn_id))
	page_.WUnlatch()
}

func (t *TableHeap) RollbackDelete(rid *page.RID, txn *Transaction) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("TableHeap::RollBackDelete called. txn.txn_id:%v  dbgInfo:%s rid:%v\n", txn.txn_id, txn.dbgInfo, *rid)
		}
		if common.ActiveLogKindSetting&common.BUFFER_INTERNAL_STATE > 0 {
			t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::RollBackDelete start. txn.txn_id: %d dbgInfo:%s", txn.txn_id, txn.dbgInfo))
			defer func() {
				t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::RollBackDelete end. txn.txn_id: %d dbgInfo:%s", txn.txn_id, txn.dbgInfo))
			}()
		}
	}
	// Find the page which contains the tuple1.
	page_ := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	common.SH_Assert(page_ != nil, "Couldn't find a page containing that RID.")
	// Rollback the delete.
	page_.WLatch()
	page_.AddWLatchRecord(int32(txn.txn_id))
	page_.RollbackDelete(rid, txn, t.log_manager)
	t.bpm.UnpinPage(page_.GetPageId(), true)
	if common.EnableDebug && common.ActiveLogKindSetting&common.PIN_COUNT_ASSERT > 0 {
		common.SH_Assert(page_.PinCount() == 0, "PinCount is not zero when finish TablePage::RollbackDelete!!!")
	}
	page_.RemoveWLatchRecord(int32(txn.txn_id))
	page_.WUnlatch()
}

// GetTuple reads a tuple1 from the table
func (t *TableHeap) GetTuple(rid *page.RID, txn *Transaction) (*tuple.Tuple, error) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("TableHeap::GetTuple called. txn.txn_id:%v rid:%v dbgInfo:%s\n", txn.txn_id, *rid, txn.dbgInfo)
		}
		if common.ActiveLogKindSetting&common.BUFFER_INTERNAL_STATE > 0 {
			t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::GetTuple start. txn.txn_id: %d dbgInfo:%s", txn.txn_id, txn.dbgInfo))
			defer func() {
				t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::GetTuple end. txn.txn_id: %d dbgInfo:%s", txn.txn_id, txn.dbgInfo))
			}()
		}
	}
	if !txn.IsRecoveryPhase() {
		if !txn.IsSharedLocked(rid) && !txn.IsExclusiveLocked(rid) && !t.lock_manager.LockShared(txn, rid) {
			txn.SetState(ABORTED)
			return nil, ErrGeneral
		}
	}
	page := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	page.RLatch()
	ret, err := page.GetTuple(rid, t.log_manager, t.lock_manager, txn)
	page.RUnlatch()
	t.bpm.UnpinPage(page.GetPageId(), false)

	return ret, err
}

// GetFirstTuple reads the first tuple1 from the table
func (t *TableHeap) GetFirstTuple(txn *Transaction) *tuple.Tuple {
	var rid *page.RID = nil
	pageId := t.firstPageId
	for pageId.IsValid() {
		page := CastPageAsTablePage(t.bpm.FetchPage(pageId))
		page.WLatch()
		rid = page.GetTupleFirstRID()
		t.bpm.UnpinPage(pageId, false)
		if rid != nil {
			page.WUnlatch()
			break
		}
		pageId = page.GetNextPageId()
		page.WUnlatch()
	}
	if rid == nil {
		return nil
	}

	// here thread has no pin and latch of page which contains got tuple1
	retTuple, _ := t.GetTuple(rid, txn)
	return retTuple
}

// Iterator returns a iterator for this table heap
func (t *TableHeap) Iterator(txn *Transaction) *TableHeapIterator {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("TableHeap::Iterator called. txn.txn_id:%v  dbgInfo:%s\n", txn.txn_id, txn.dbgInfo)
		}
		if common.ActiveLogKindSetting&common.BUFFER_INTERNAL_STATE > 0 {
			t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::Iterator start. txn.txn_id: %d dbgInfo:%s", txn.txn_id, txn.dbgInfo))
			defer func() {
				t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::Iterator end. txn.txn_id: %d dbgInfo:%s", txn.txn_id, txn.dbgInfo))
			}()
		}
	}
	return NewTableHeapIterator(t, t.lock_manager, txn)
}

func (t *TableHeap) GetBufferPoolManager() *buffer.BufferPoolManager {
	return t.bpm
}
