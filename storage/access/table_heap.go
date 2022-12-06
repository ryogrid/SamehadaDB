// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package access

import (
	"fmt"
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
	firstPage.AddWLatchRecord(int32(txn.txn_id))
	firstPage.Init(p.GetPageId(), types.InvalidPageID, log_manager, lock_manager, txn)
	// flush page for recovery process works...
	bpm.FlushPage(p.GetPageId())
	bpm.UnpinPage(p.GetPageId(), true)
	firstPage.RemoveWLatchRecord(int32(txn.txn_id))
	firstPage.WUnlatch()
	return &TableHeap{bpm, p.GetPageId(), log_manager, lock_manager}
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
	currentPage := CastPageAsTablePage(t.bpm.FetchPage(t.firstPageId))
	currentPage.WLatch()
	currentPage.AddWLatchRecord(int32(txn.txn_id))
	// Insert into the first page with enough space. If no such page exists, create a new page and insert into that.
	// INVARIANT: currentPage is WLatched if you leave the loop normally.

	for {
		rid, err = currentPage.InsertTuple(tuple_, t.log_manager, t.lock_manager, txn)
		if err == nil || err == ErrEmptyTuple {
			//currentPage.WUnlatch()
			break
		}
		if rid == nil && err != nil && err != ErrEmptyTuple && err != ErrNotEnoughSpace {
			t.bpm.UnpinPage(currentPage.GetPageId(), false)
			if common.EnableDebug && common.ActiveLogKindSetting&common.PIN_COUNT_ASSERT > 0 {
				common.SH_Assert(currentPage.PinCount() == 0, "PinCount is not zero at TableHeap::InsertTuple!!!")
			}
			currentPage.RemoveWLatchRecord(int32(txn.txn_id))
			currentPage.WUnlatch()
			return nil, err
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
			currentPage.SetNextPageId(p.GetPageId())
			currentPageId := currentPage.GetPageId()
			newPage := CastPageAsTablePage(p)
			newPage.WLatch()
			newPage.AddWLatchRecord(int32(txn.txn_id))
			t.bpm.UnpinPage(currentPage.GetPageId(), true)
			if common.EnableDebug && common.ActiveLogKindSetting&common.PIN_COUNT_ASSERT > 0 {
				common.SH_Assert(currentPage.PinCount() == 0, "PinCount is not zero when finish TablePage::UpdateTuple!!!")
			}
			currentPage.RemoveWLatchRecord(int32(txn.txn_id))
			currentPage.WUnlatch()
			newPage.Init(p.GetPageId(), currentPageId, t.log_manager, t.lock_manager, txn)
			//t.bpm.FlushPage(newPage.GetPageId())
			//newPage.WUnlatch()
			currentPage = newPage
			// holding WLatch of currentPage here
		}
	}

	t.bpm.UnpinPage(currentPage.GetPageId(), true)
	if common.EnableDebug && common.ActiveLogKindSetting&common.PIN_COUNT_ASSERT > 0 {
		common.SH_Assert(currentPage.PinCount() == 0, "PinCount is not zero when finish TablePage::InsertTuple!!!")
	}
	currentPage.RemoveWLatchRecord(int32(txn.txn_id))
	currentPage.WUnlatch()
	// Update the transaction's write set.
	txn.AddIntoWriteSet(NewWriteRecord(*rid, INSERT, new(tuple.Tuple), t, oid))
	return rid, nil
}

// if specified nil to update_col_idxs and schema_, all data of existed tuple is replaced one of new_tuple
// if specified not nil, new_tuple also should have all columns defined in schema. but not update target value can be dummy value
func (t *TableHeap) UpdateTuple(tuple_ *tuple.Tuple, update_col_idxs []int, schema_ *schema.Schema, oid uint32, rid page.RID, txn *Transaction) (bool, *page.RID) {
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
	// Find the page which contains the tuple.
	page_ := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	// If the page could not be found, then abort the transaction.
	if page_ == nil {
		txn.SetState(ABORTED)
		return false, nil
	}
	// Update the tuple; but first save the old value for rollbacks.
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

	var new_rid *page.RID = nil
	var isUpdateWithDelInsert bool = false
	if is_updated == false && err == ErrNotEnoughSpace {
		//// TODO: (SDB) this early return with ABORTED state exists (TableHeap::UpdateTuple)
		////             because rollback and recovery when this cases fail currently
		//txn.SetState(ABORTED)
		//return false, &rid

		// delete and insert need_follow_tuple as updating

		// first, delete target tuple (old data)
		is_deleted := t.MarkDelete(&rid, oid, txn)
		if !is_deleted {
			fmt.Println("TableHeap::UpdateTuple(): MarkDelete failed")
			txn.SetState(ABORTED)
			return false, nil
		}

		var err error = nil
		new_rid, err = t.InsertTuple(need_follow_tuple, txn, oid)
		if err != nil {
			fmt.Println("TableHeap::UpdateTuple(): InsertTuple failed")
			txn.SetState(ABORTED)
			return false, nil
		}

		fmt.Printf("TableHeap::UpdateTuple(): new rid = %d %d\n", new_rid.PageId, new_rid.SlotNum)
		// change return flag to success
		is_updated = true
		isUpdateWithDelInsert = true
	}

	// TODO: (SDB) for debugging. this code should be removed after finish of debugging
	// last condition is for when rollback case
	common.SH_Assert(
		(txn.GetState() == ABORTED && is_updated == false) || (txn.GetState() != ABORTED && is_updated == true) || (txn.GetState() == ABORTED && is_updated == true && update_col_idxs == nil),
		fmt.Sprintf("illegal internal state! txnState:%d is_updated:%v", txn.state, is_updated))

	// Update the transaction's write set.
	// when txn is ABORTED state case, data is not updated. so adding a write set entry is not needed
	// when isUpdateWithDelInsert is true, Update operation write records are already added as two recoreds
	// (Delete & Insert)
	if is_updated && !isUpdateWithDelInsert && txn.GetState() != ABORTED {
		txn.AddIntoWriteSet(NewWriteRecord(rid, UPDATE, old_tuple, t, oid))
	}

	// TODO: (SDB) for debugging. this code should be removed after finish of debugging
	if is_updated && txn.GetState() != ABORTED {
		common.SH_Assert(len(txn.GetWriteSet()) != 0, "content of write should not be empty.")
	}

	return is_updated, new_rid
}

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
	// Find the page which contains the tuple.
	page_ := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	// If the page could not be found, then abort the transaction.
	if page_ == nil {
		txn.SetState(ABORTED)
		return false
	}
	// Otherwise, mark the tuple as deleted.
	page_.WLatch()
	page_.AddWLatchRecord(int32(txn.txn_id))
	is_marked := page_.MarkDelete(rid, txn, t.lock_manager, t.log_manager)
	t.bpm.UnpinPage(page_.GetPageId(), true)
	if common.EnableDebug && common.ActiveLogKindSetting&common.PIN_COUNT_ASSERT > 0 {
		common.SH_Assert(page_.PinCount() == 0, "PinCount is not zero when finish TablePage::MarkDelete!!!")
	}
	page_.RemoveWLatchRecord(int32(txn.txn_id))
	page_.WUnlatch()
	if is_marked {
		// Update the transaction's write set.
		txn.AddIntoWriteSet(NewWriteRecord(*rid, DELETE, new(tuple.Tuple), t, oid))
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
	// Find the page which contains the tuple.
	page_ := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	common.SH_Assert(page_ != nil, "Couldn't find a page containing that RID.")
	// Delete the tuple from the page.
	page_.WLatch()
	page_.AddWLatchRecord(int32(txn.txn_id))
	page_.ApplyDelete(rid, txn, t.log_manager)
	//t.lock_manager.WUnlock(txn, []page.RID{*rid})
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
	// Find the page which contains the tuple.
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

// GetTuple reads a tuple from the table
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
	if !txn.IsSharedLocked(rid) && !txn.IsExclusiveLocked(rid) && !t.lock_manager.LockShared(txn, rid) {
		txn.SetState(ABORTED)
		return nil, ErrGeneral
	}
	page := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	page.RLatch()
	page.AddRLatchRecord(int32(txn.txn_id))
	ret, err := page.GetTuple(rid, t.log_manager, t.lock_manager, txn)
	page.RemoveRLatchRecord(int32(txn.txn_id))
	page.RUnlatch()
	//page.WLatch()
	//page.AddWLatchRecord(int32(txn.txn_id))
	t.bpm.UnpinPage(page.GetPageId(), false)
	//page.RemoveWLatchRecord(int32(txn.txn_id))
	//page.WUnlatch()

	return ret, err
}

// GetFirstTuple reads the first tuple from the table
func (t *TableHeap) GetFirstTuple(txn *Transaction) *tuple.Tuple {
	var rid *page.RID = nil
	pageId := t.firstPageId
	for pageId.IsValid() {
		page := CastPageAsTablePage(t.bpm.FetchPage(pageId))
		page.WLatch()
		page.AddWLatchRecord(int32(txn.txn_id))
		rid = page.GetTupleFirstRID()
		t.bpm.UnpinPage(pageId, false)
		if rid != nil {
			page.RemoveWLatchRecord(int32(txn.txn_id))
			page.WUnlatch()
			break
		}
		pageId = page.GetNextPageId()
		page.RemoveWLatchRecord(int32(txn.txn_id))
		page.WUnlatch()
	}
	if rid == nil {
		return nil
	}

	// here thread has no pin and latch of page which contains got tuple
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
