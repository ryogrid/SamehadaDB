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
	firstPageID  types.PageID
	logManager  *recovery.LogManager
	lockManager *LockManager
	// for make faster to insert tuple, memorize last page id (not persisted)
	// but, seek from last means waste unused space of former pages
	// when delete or update occurs it is initialized to firstPageID
	lastPageID types.PageID
}

// NewTableHeap creates a table heap without a  (open table)
func NewTableHeap(bpm *buffer.BufferPoolManager, logManager *recovery.LogManager, lockManager *LockManager, txn *Transaction) *TableHeap {
	p := bpm.NewPage()

	firstPage := CastPageAsTablePage(p)
	firstPage.WLatch()
	firstPage.AddWLatchRecord(int32(txn.txnID))
	firstPage.Init(p.GetPageID(), types.InvalidPageID, logManager, lockManager, txn, false)
	// flush page for recovery process works...
	bpm.FlushPage(p.GetPageID())
	bpm.UnpinPage(p.GetPageID(), true)
	firstPage.RemoveWLatchRecord(int32(txn.txnID))
	firstPage.WUnlatch()
	return &TableHeap{bpm, p.GetPageID(), logManager, lockManager, p.GetPageID()}
}

// InitTableHeap ...
func InitTableHeap(bpm *buffer.BufferPoolManager, pageID types.PageID, logManager *recovery.LogManager, lockManager *LockManager) *TableHeap {
	return &TableHeap{bpm, pageID, logManager, lockManager, pageID}
}

// GetFirstPageID returns firstPageID
func (t *TableHeap) GetFirstPageID() types.PageID {
	return t.firstPageID
}

// InsertTuple inserts a tuple1 into the table
// PAY ATTENTION: index entry is not inserted
//
// It fetches the first page and tries to insert the tuple1 there.
// If the tuple1 is too large (>= page_size):
// 1. It tries to insert in the next page
// 2. If there is no next page, it creates a new page and insert in it
func (t *TableHeap) InsertTuple(tpl *tuple.Tuple, txn *Transaction, oid uint32, isForUpdate bool) (rid *page.RID, err error) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("TableHeap::InsertTuple called. txn.txnID:%v dbgInfo:%s tpl:%v\n", txn.txnID, txn.dbgInfo, *tpl)
		}
		if common.ActiveLogKindSetting&common.BufferInternalState > 0 {
			t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::InsertTuple start. txn.txnID: %d dbgInfo:%s", txn.txnID, txn.dbgInfo))
			defer func() {
				t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::InsertTuple end. txn.txnID: %d dbgInfo:%s", txn.txnID, txn.dbgInfo))
			}()
		}
	}

	// seek from last (almost case)
	currentPage := CastPageAsTablePage(t.bpm.FetchPage(t.lastPageID))
	//currentPage := CastPageAsTablePage(t.bpm.FetchPage(t.firstPageID))

	currentPage.WLatch()
	currentPage.AddWLatchRecord(int32(txn.txnID))
	// Insert into the first page with enough space. If no such page exists, create a new page and insert into that.
	// INVARIANT: currentPage is WLatched if you leave the loop normally.

	for {
		rid, err = currentPage.InsertTuple(tpl, t.logManager, t.lockManager, txn)
		if err == nil || err == ErrEmptyTuple {
			break
		}

		nextPageID := currentPage.GetNextPageID()
		if nextPageID.IsValid() {
			nextPage := CastPageAsTablePage(t.bpm.FetchPage(nextPageID))
			nextPage.WLatch()
			nextPage.AddWLatchRecord(int32(txn.txnID))
			t.bpm.UnpinPage(currentPage.GetPageID(), false)
			if common.EnableDebug && common.ActiveLogKindSetting&common.PinCountAssert > 0 {
				common.SHAssert(currentPage.PinCount() == 0, "PinCount is not zero at TableHeap::InsertTuple!!!")
			}
			currentPage.RemoveWLatchRecord(int32(txn.txnID))
			currentPage.WUnlatch()
			currentPage = nextPage
			// holding WLatch of currentPage here
		} else {
			p := t.bpm.NewPage()
			newPage := CastPageAsTablePage(p)
			newPage.WLatch()
			newPage.AddWLatchRecord(int32(txn.txnID))
			currentPage.SetNextPageID(p.GetPageID())
			currentPageID := currentPage.GetPageID()
			newPage.Init(p.GetPageID(), currentPageID, t.logManager, t.lockManager, txn, false)
			t.bpm.UnpinPage(currentPage.GetPageID(), true)
			if common.EnableDebug && common.ActiveLogKindSetting&common.PinCountAssert > 0 {
				common.SHAssert(currentPage.PinCount() == 0, "PinCount is not zero when finish TablePage::UpdateTuple!!!")
			}
			currentPage.RemoveWLatchRecord(int32(txn.txnID))
			currentPage.WUnlatch()
			currentPage = newPage
			// holding WLatch of currentPage here
		}
	}

	currentPageID := currentPage.GetPageID()
	// memorize last page id
	t.lastPageID = currentPageID
	t.bpm.UnpinPage(currentPageID, true)
	if common.EnableDebug && common.ActiveLogKindSetting&common.PinCountAssert > 0 {
		common.SHAssert(currentPage.PinCount() == 0, "PinCount is not zero when finish TablePage::InsertTuple!!!")
	}
	currentPage.RemoveWLatchRecord(int32(txn.txnID))
	currentPage.WUnlatch()
	if !isForUpdate {
		// Update the transaction's write set.
		txn.AddIntoWriteSet(NewWriteRecord(rid, nil, INSERT, tpl, nil, t, oid))
	}
	//note: when isForUpdate is true, write record of Update is created in caller

	return rid, nil
}

// if specified nil to updateColIdxs and sc, all data of existed tuple1 is replaced one of newTuple
// if specified not nil, newTuple also should have all columns defined in schema. but not update target value can be dummy value
func (t *TableHeap) UpdateTuple(tpl *tuple.Tuple, updateColIdxs []int, sc *schema.Schema, oid uint32, rid page.RID, txn *Transaction, isRollback bool) (isSuccess bool, newRIDVal *page.RID, errVal error, updateTupleVal *tuple.Tuple, oldTupleVal *tuple.Tuple) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("TableHeap::UpadteTuple called. txn.txnID:%v dbgInfo:%s updateColIdxs:%v rid1:%v\n", txn.txnID, txn.dbgInfo, updateColIdxs, rid)
		}
		if common.ActiveLogKindSetting&common.BufferInternalState > 0 {
			t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::UpdateTuple start.  txn.txnID: %d dbgInfo:%s", txn.txnID, txn.dbgInfo))
			defer func() {
				t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::UpdateTuple end.  txn.txnID: %d dbgInfo:%s", txn.txnID, txn.dbgInfo))
			}()
		}
	}
	// Find the page which contains the tuple1.
	pg := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageID()))
	// If the page could not be found, then abort the transaction.
	if pg == nil {
		txn.SetState(ABORTED)
		return false, nil, ErrGeneral, nil, nil
	}
	// Update the tuple1; but first save the old value for rollbacks.
	oldTuple := new(tuple.Tuple)
	oldTuple.SetRID(new(page.RID))

	pg.WLatch()
	pg.AddWLatchRecord(int32(txn.txnID))
	isUpdated, err, needFollowTuple := pg.UpdateTuple(tpl, updateColIdxs, sc, oldTuple, &rid, txn, t.lockManager, t.logManager, false)
	t.bpm.UnpinPage(pg.GetPageID(), isUpdated)
	if common.EnableDebug && common.ActiveLogKindSetting&common.PinCountAssert > 0 {
		common.SHAssert(pg.PinCount() == 0, "PinCount is not zero when finish TablePage::UpdateTuple!!!")
	}
	pg.RemoveWLatchRecord(int32(txn.txnID))
	pg.WUnlatch()

	var newRID *page.RID
	var isUpdateWithDelInsert = false
	if isUpdated == false && (err == ErrNotEnoughSpace || err == ErrRollbackDifficult) {
		// delete oldTuple(rid1)
		// and insert needFollowTuple(newRID)
		// as updating

		// first, delete target tuple1 (old data)
		var isDeleted bool
		if isRollback {
			// when this method is used on TransactinManager::Abort,
			// ApplyDelete should be used because there is no occasion to commit deleted mark
			t.ApplyDelete(&rid, txn)
			// above ApplyDelete does not fail
			isDeleted = true
		} else {
			isDeleted = t.MarkDelete(&rid, oid, txn, true)
		}

		if !isDeleted {
			//fmt.Println("TableHeap::UpdateTuple(): MarkDelete failed")
			txn.SetState(ABORTED)
			return false, nil, ErrGeneral, nil, nil
		}

		var err2 error = nil
		newRID, err2 = t.InsertTuple(needFollowTuple, txn, oid, true)
		if err2 != nil {
			panic("InsertTuple does not fail on normal system condition!!!")
		}

		// change return values to success
		isUpdated = true
		isUpdateWithDelInsert = true
	}

	// add appropriate transaction's write set of Update.
	// when txn is ABORTED state case, data is not updated. so adding a write set entry is not needed
	// when err == ErrNotEnoughSpace route and old tuple delete is only succeeded, DELETE write set entry is added above (no come here)
	if isUpdated && txn.GetState() != ABORTED {
		if isUpdateWithDelInsert {
			//t.lastPageID = t.firstPageID
			txn.AddIntoWriteSet(NewWriteRecord(&rid, newRID, UPDATE, oldTuple, needFollowTuple, t, oid))
		} else {
			// reset seek start point of Insert to first page
			//t.lastPageID = t.firstPageID
			txn.AddIntoWriteSet(NewWriteRecord(&rid, &rid, UPDATE, oldTuple, needFollowTuple, t, oid))
		}
	}

	return isUpdated, newRID, nil, needFollowTuple, oldTuple
}

// when isForUpdate arg is true, write record is not created
func (t *TableHeap) MarkDelete(rid *page.RID, oid uint32, txn *Transaction, isForUpdate bool) bool {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("TableHeap::MarkDelete called. txn.txnID:%v rid1:%v  dbgInfo:%s\n", txn.txnID, *rid, txn.dbgInfo)
		}
		if common.ActiveLogKindSetting&common.BufferInternalState > 0 {
			t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::MarkDelete start.  txn.txnID: %d dbgInfo:%s", txn.txnID, txn.dbgInfo))
			defer func() {
				t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::MarkDelete end.  txn.txnID: %d dbgInfo:%s", txn.txnID, txn.dbgInfo))
			}()
		}
	}
	// Find the page which contains the tuple1.
	pg := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageID()))
	// If the page could not be found, then abort the transaction.
	if pg == nil {
		txn.SetState(ABORTED)
		return false
	}
	// Otherwise, mark the tuple1 as deleted.
	pg.WLatch()
	pg.AddWLatchRecord(int32(txn.txnID))
	isMarked, markedTuple := pg.MarkDelete(rid, txn, t.lockManager, t.logManager)
	t.bpm.UnpinPage(pg.GetPageID(), true)
	if common.EnableDebug && common.ActiveLogKindSetting&common.PinCountAssert > 0 {
		common.SHAssert(pg.PinCount() == 0, "PinCount is not zero when finish TablePage::MarkDelete!!!")
	}
	pg.RemoveWLatchRecord(int32(txn.txnID))
	pg.WUnlatch()
	if isMarked && !isForUpdate {
		// Update the transaction's write set.
		txn.AddIntoWriteSet(NewWriteRecord(rid, nil, DELETE, markedTuple, nil, t, oid))
	}
	//note: when isForUpdate is true, write record of Update is created in caller

	return isMarked
}

func (t *TableHeap) ApplyDelete(rid *page.RID, txn *Transaction) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("TableHeap::ApplyDelete called. txn.txnID:%v rid1:%v dbgInfo:%s\n", txn.txnID, *rid, txn.dbgInfo)
		}
		if common.ActiveLogKindSetting&common.BufferInternalState > 0 {
			t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::ApplyDelete start. txn.txnID: %d dbgInfo:%s", txn.txnID, txn.dbgInfo))
			defer func() {
				t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::ApplyDelete end. txn.txnID: %d dbgInfo:%s", txn.txnID, txn.dbgInfo))
			}()
		}
	}
	// Find the page which contains the tuple1.
	pg := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageID()))
	common.SHAssert(pg != nil, "Couldn't find a page containing that RID.")
	// Delete the tuple1 from the page.
	pg.WLatch()
	pg.AddWLatchRecord(int32(txn.txnID))
	pg.ApplyDelete(rid, txn, t.logManager)

	// reset seek start point of Insert to first page
	t.lastPageID = t.firstPageID

	t.bpm.UnpinPage(pg.GetPageID(), true)
	if common.EnableDebug && common.ActiveLogKindSetting&common.PinCountAssert > 0 {
		common.SHAssert(pg.PinCount() == 0, "PinCount is not zero when finish TablePage::ApplyDelete!!!")
	}
	pg.RemoveWLatchRecord(int32(txn.txnID))
	pg.WUnlatch()
}

func (t *TableHeap) RollbackDelete(rid *page.RID, txn *Transaction) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("TableHeap::RollBackDelete called. txn.txnID:%v  dbgInfo:%s rid1:%v\n", txn.txnID, txn.dbgInfo, *rid)
		}
		if common.ActiveLogKindSetting&common.BufferInternalState > 0 {
			t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::RollBackDelete start. txn.txnID: %d dbgInfo:%s", txn.txnID, txn.dbgInfo))
			defer func() {
				t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::RollBackDelete end. txn.txnID: %d dbgInfo:%s", txn.txnID, txn.dbgInfo))
			}()
		}
	}
	// Find the page which contains the tuple1.
	pg := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageID()))
	common.SHAssert(pg != nil, "Couldn't find a page containing that RID.")
	// Rollback the delete.
	pg.WLatch()
	pg.AddWLatchRecord(int32(txn.txnID))
	pg.RollbackDelete(rid, txn, t.logManager)
	t.bpm.UnpinPage(pg.GetPageID(), true)
	if common.EnableDebug && common.ActiveLogKindSetting&common.PinCountAssert > 0 {
		common.SHAssert(pg.PinCount() == 0, "PinCount is not zero when finish TablePage::RollbackDelete!!!")
	}
	pg.RemoveWLatchRecord(int32(txn.txnID))
	pg.WUnlatch()
}

// GetTuple reads a tuple1 from the table
func (t *TableHeap) GetTuple(rid *page.RID, txn *Transaction) (*tuple.Tuple, error) {
	if common.EnableDebug {
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("TableHeap::GetTuple called. txn.txnID:%v rid1:%v dbgInfo:%s\n", txn.txnID, *rid, txn.dbgInfo)
		}
		if common.ActiveLogKindSetting&common.BufferInternalState > 0 {
			t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::GetTuple start. txn.txnID: %d dbgInfo:%s", txn.txnID, txn.dbgInfo))
			defer func() {
				t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::GetTuple end. txn.txnID: %d dbgInfo:%s", txn.txnID, txn.dbgInfo))
			}()
		}
	}
	if !txn.IsRecoveryPhase() {
		if !txn.IsSharedLocked(rid) && !txn.IsExclusiveLocked(rid) && !t.lockManager.LockShared(txn, rid) {
			txn.SetState(ABORTED)
			return nil, ErrGeneral
		}
	}
	page := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageID()))
	page.RLatch()
	ret, err := page.GetTuple(rid, t.logManager, t.lockManager, txn)
	page.RUnlatch()
	t.bpm.UnpinPage(page.GetPageID(), false)

	return ret, err
}

// GetFirstTuple reads the first tuple1 from the table
func (t *TableHeap) GetFirstTuple(txn *Transaction) *tuple.Tuple {
	var rid *page.RID = nil
	pageID := t.firstPageID
	for pageID.IsValid() {
		page := CastPageAsTablePage(t.bpm.FetchPage(pageID))
		page.WLatch()
		rid = page.GetTupleFirstRID()
		t.bpm.UnpinPage(pageID, false)
		if rid != nil {
			page.WUnlatch()
			break
		}
		pageID = page.GetNextPageID()
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
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("TableHeap::Iterator called. txn.txnID:%v  dbgInfo:%s\n", txn.txnID, txn.dbgInfo)
		}
		if common.ActiveLogKindSetting&common.BufferInternalState > 0 {
			t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::Iterator start. txn.txnID: %d dbgInfo:%s", txn.txnID, txn.dbgInfo))
			defer func() {
				t.bpm.PrintBufferUsageState(fmt.Sprintf("TableHeap::Iterator end. txn.txnID: %d dbgInfo:%s", txn.txnID, txn.dbgInfo))
			}()
		}
	}
	return NewTableHeapIterator(t, t.lockManager, txn)
}

func (t *TableHeap) GetBufferPoolManager() *buffer.BufferPoolManager {
	return t.bpm
}
