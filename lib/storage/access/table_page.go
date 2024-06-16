// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

// package tablepage
package access

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/errors"
	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

const deleteMask = uint32(1 << ((8 * 4) - 1))
const reservedMask = uint32(1 << ((8 * 4) - 2))

const sizeTablePageHeader = uint32(24)
const sizeTuple = uint32(8)
const offSetPrevPageId = uint32(8)
const offSetNextPageId = uint32(12)
const offsetFreeSpace = uint32(16)
const offSetTupleCount = uint32(20)
const offsetTupleOffset = uint32(24)
const offsetTupleSize = uint32(28)

const ErrEmptyTuple = errors.Error("tuple1 cannot be empty.")
const ErrNotEnoughSpace = errors.Error("there is not enough space.")
const ErrSelfDeletedCase = errors.Error("encont self deleted tuple1.")
const ErrGeneral = errors.Error("some error is occured!")

// Slotted page format:
//
//	---------------------------------------------------------
//	| HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
//	---------------------------------------------------------
//	                              ^
//	                              free space pointer
//	Header format (size in bytes):
//	----------------------------------------------------------------------------
//	| PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
//	----------------------------------------------------------------------------
//	----------------------------------------------------------------
//	| TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
//	----------------------------------------------------------------
type TablePage struct {
	page.Page
}

// CastPageAsTablePage casts the abstract Page struct into TablePage
func CastPageAsTablePage(page *page.Page) *TablePage {
	if page == nil {
		return nil
	}

	return (*TablePage)(unsafe.Pointer(page))
}

// Inserts a tuple1 into the table
func (tp *TablePage) InsertTuple(tuple *tuple.Tuple, log_manager *recovery.LogManager, lock_manager *LockManager, txn *Transaction) (*page.RID, error) {
	if common.EnableDebug {
		defer func() {
			if common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				common.SH_Assert(tp.GetFreeSpacePointer() <= common.PageSize, fmt.Sprintf("FreeSpacePointer value is illegal! value:%d txnId:%v dbgInfo:%s", tp.GetFreeSpacePointer(), txn.GetTransactionId(), txn.dbgInfo))
				common.SH_Assert(tp.GetPageId() == tp.GetPageId(), fmt.Sprintf("pageId data is inconsitent! %d != %d txnId:%d txnState:%d txn.dbgInfo:%s", tp.GetPageId(), tp.GetPageId(), txn.GetTransactionId(), txn.state, txn.dbgInfo))
			}
			if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
				fmt.Printf("TablePage::InsertTuple returned. pageId:%d GetPageId():%d txn.txn_id:%v txnState:%d dbgInfo:%s FreeSpacePointer:%d TupleCount:%d FreeSpaceRemaining:%d tuple1:%v\n", tp.GetPageId(), tp.GetPageId(), txn.txn_id, txn.state, txn.dbgInfo, tp.GetFreeSpacePointer(), tp.GetTupleCount(), tp.getFreeSpaceRemaining(), *tuple)
			}
		}()
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("TablePage::InsertTuple called. pageId:%d txn.txn_id:%v dbgInfo:%s tuple1:%v\n", tp.GetPageId(), txn.txn_id, txn.dbgInfo, *tuple)
		}
	}

	if tuple.Size() <= 0 {
		panic("tuple size is illegal!!!")
		return nil, ErrEmptyTuple
	}

	if tp.getFreeSpaceRemaining() < tuple.Size()+sizeTuple {
		return nil, ErrNotEnoughSpace
	}

	var slot uint32

	// try to find a free slot
	for slot = uint32(0); slot < tp.GetTupleCount(); slot++ {
		if tp.GetTupleSize(slot) == 0 {
			break
		}
	}

	rid := &page.RID{}
	rid.Set(tp.GetPageId(), slot)

	if !txn.IsRecoveryPhase() {
		// Acquire an exclusive lock on the new tuple1.
		locked := lock_manager.LockExclusive(txn, rid)
		if !locked {
			//txn.SetState(ABORTED)
			return nil, errors.Error("could not acquire an exclusive lock of found slot (=RID)")
			// fmt.Printf("Locking a new tuple1 should always work. rid1: %v\n", rid1)
			// lock_manager.PrintLockTables()
			// os.Stdout.Sync()
			// panic("")
		}
	}

	tuple.SetRID(rid)

	if common.EnableDebug {
		setFSP := tp.GetFreeSpacePointer() - tuple.Size()
		common.SH_Assert(setFSP <= common.PageSize, fmt.Sprintf("illegal pointer value!! txnId:%d txnState:%d txn.dbgInfo:%s rid1:%v GetPageId():%d setFSP:%d", txn.txn_id, txn.state, txn.dbgInfo, *rid, tp.GetPageId(), setFSP))
	}

	tp.SetFreeSpacePointer(tp.GetFreeSpacePointer() - tuple.Size())
	tp.setTuple(slot, tuple)

	if slot == tp.GetTupleCount() {
		tp.SetTupleCount(tp.GetTupleCount() + 1)
	}

	// Write the log record.
	if log_manager.IsEnabledLogging() {
		log_record := recovery.NewLogRecordInsertDelete(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.INSERT, *rid, tuple)
		lsn := log_manager.AppendLogRecord(log_record)
		tp.Page.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	return rid, nil
}

// if specified nil to update_col_idxs and schema_, all data of existed tuple1 is replaced one of new_tuple
// if specified not nil, new_tuple also should have all columns defined in schema. but not update target value can be dummy value
// if isForRollbackOrUndo is false, new_tuple occupies the same memory space as the old_tuple
// for ensureing existance of enough space at rollback on abort or undo
// return Tuple pointer when updated tuple1 need to be moved new page location and it should be inserted after old data deleted, otherwise returned nil
func (tp *TablePage) UpdateTuple(new_tuple *tuple.Tuple, update_col_idxs []int, schema_ *schema.Schema, old_tuple *tuple.Tuple, rid *page.RID, txn *Transaction,
	lock_manager *LockManager, log_manager *recovery.LogManager, isForRollbackUndo bool) (bool, error, *tuple.Tuple, *page.RID, *tuple.Tuple) {
	if common.EnableDebug {
		defer func() {
			if common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				common.SH_Assert(tp.GetFreeSpacePointer() <= common.PageSize, fmt.Sprintf("FreeSpacePointer value is illegal! value:%d txnId:%v dbgInfo:%s", tp.GetFreeSpacePointer(), txn.GetTransactionId(), txn.dbgInfo))
				common.SH_Assert(tp.GetPageId() == tp.GetPageId(), fmt.Sprintf("pageId data is inconsitent! %d != %d txnId:%d txnState:%d txn.dbgInfo:%s", tp.GetPageId(), tp.GetPageId(), txn.GetTransactionId(), txn.state, txn.dbgInfo))
			}
			if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
				fmt.Printf("TablePage::UpdateTuple returned. pageId:%d GetPageId():%d txn.txn_id:%v txnState:%d dbgInfo:%s FreeSpacePointer:%d TupleCount:%d FreeSpaceRemaining:%d new_tuple:%v\n", tp.GetPageId(), tp.GetPageId(), txn.txn_id, txn.state, txn.dbgInfo, tp.GetFreeSpacePointer(), tp.GetTupleCount(), tp.getFreeSpaceRemaining(), *new_tuple)
			}
		}()
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("TablePage::UpdateTuple called. pageId:%d txn.txn_id:%v dbgInfo:%s new_tuple:%v update_col_idxs:%v rid1:%v\n", tp.GetPageId(), txn.txn_id, txn.dbgInfo, *new_tuple, update_col_idxs, *rid)
		}
	}
	common.SH_Assert(new_tuple.Size() > 0, "Cannot have empty tuples.")

	if !txn.IsRecoveryPhase() {
		// Acquire an exclusive lock, upgrading from shared if necessary.
		if txn.IsSharedLocked(rid) {
			if !lock_manager.LockUpgrade(txn, rid) {
				txn.SetState(ABORTED)
				return false, nil, nil, nil, nil
			}
		} else if !txn.IsExclusiveLocked(rid) && !lock_manager.LockExclusive(txn, rid) {
			txn.SetState(ABORTED)
			return false, nil, nil, nil, nil
		}
	}

	slot_num := rid.GetSlotNum()
	// If the slot number is invalid, abort the transaction.
	if slot_num >= tp.GetTupleCount() {
		if !txn.IsRecoveryPhase() {
			txn.SetState(ABORTED)
		}
		return false, nil, nil, nil, nil
	}
	tuple_size := tp.GetTupleSize(slot_num)
	// If the tuple1 is deleted, abort the transaction.
	if IsDeleted(tuple_size) {
		if !txn.IsRecoveryPhase() {
			txn.SetState(ABORTED)
		}
		return false, nil, nil, nil, nil
	}
	if IsReserved(tuple_size) {
		// ignore dummy tuple
		return false, nil, nil, nil, nil
	}

	// Copy out the old value.
	tuple_offset := tp.GetTupleOffsetAtSlot(slot_num)
	old_tuple.SetSize(tuple_size)
	old_tuple_data := make([]byte, old_tuple.Size())
	copy(old_tuple_data, tp.GetData()[tuple_offset:tuple_offset+old_tuple.Size()])
	old_tuple.SetData(old_tuple_data)
	old_tuple.SetRID(rid)

	// setup tuple1 for updating
	var update_tuple *tuple.Tuple = nil
	if update_col_idxs == nil || schema_ == nil {
		update_tuple = new_tuple
	} else {
		// update specifed columns only case

		var update_tuple_values = make([]types.Value, 0)
		matched_cnt := int(0)
		for idx := range schema_.GetColumns() {
			if matched_cnt < len(update_col_idxs) && idx == update_col_idxs[matched_cnt] {
				update_tuple_values = append(update_tuple_values, new_tuple.GetValue(schema_, uint32(idx)))
				matched_cnt++
			} else {
				update_tuple_values = append(update_tuple_values, old_tuple.GetValue(schema_, uint32(idx)))
			}
		}
		update_tuple = tuple.NewTupleFromSchema(update_tuple_values, schema_)
	}

	if update_tuple.Size() <= 0 {
		panic("tuple size is illegal!!!")
	}

	if tp.getFreeSpaceRemaining()+tuple_size < update_tuple.Size() {
		fmt.Println("getFreeSpaceRemaining", tp.getFreeSpaceRemaining(), "tuple_size", tuple_size, "update_tuple.Size()", update_tuple.Size(), "RID", *rid)
		return false, ErrNotEnoughSpace, update_tuple, nil, nil
	}

	if log_manager.IsEnabledLogging() {
		log_record := recovery.NewLogRecordUpdate(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.UPDATE, *rid, *old_tuple, *update_tuple)
		lsn := log_manager.AppendLogRecord(log_record)
		tp.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	// Perform the update.
	free_space_pointer := tp.GetFreeSpacePointer()
	common.SH_Assert(tuple_offset >= free_space_pointer, "Offset should appear after current free space position.")

	copy(tp.GetData()[free_space_pointer+tuple_size-update_tuple.Size():], tp.GetData()[free_space_pointer:tuple_offset])
	tp.SetFreeSpacePointer(free_space_pointer + tuple_size - update_tuple.Size())
	copy(tp.GetData()[tuple_offset+tuple_size-update_tuple.Size():], update_tuple.Data()[:update_tuple.Size()])
	tp.SetTupleSize(slot_num, update_tuple.Size())

	// Update all tuples offsets.
	tuple_cnt := int(tp.GetTupleCount())
	for ii := 0; ii < tuple_cnt; ii++ {
		tuple_offset_i := tp.GetTupleOffsetAtSlot(uint32(ii))
		if tp.GetTupleSize(uint32(ii)) > 0 && tuple_offset_i < tuple_offset+tuple_size {
			tp.SetTupleOffsetAtSlot(uint32(ii), tuple_offset_i+tuple_size-update_tuple.Size())
		}
	}

	var dummy_rid *page.RID
	var dummy_tuple *tuple.Tuple
	// whether dummy tuple insertion is needed or not
	if update_tuple.Size() < tuple_size && !isForRollbackUndo {
		// addition of sizeTuple is needed because sizeTuple bytes is keep used
		// after dummy tuple is deleted
		reserve_size := tuple_size - update_tuple.Size() + sizeTuple
		//reserve_size := tuple_size*2 + sizeTuple
		// sizeTuple is needed metadata space additonaly
		if tp.getFreeSpaceRemaining() >= sizeTuple+1 {
			usableSpaceForDummy := tp.getFreeSpaceRemaining() - sizeTuple
			if usableSpaceForDummy < reserve_size {
				fmt.Println("fill few rest space for rollback for ", *rid, usableSpaceForDummy)
				fmt.Println("new tuple size", update_tuple.Size(), "old tuple size", tuple_size)
				dummy_rid, dummy_tuple = tp.ReserveSpaceForRollbackUpdate(nil, usableSpaceForDummy, txn, log_manager, lock_manager)
				//if usableSpaceForDummy < 1 {
				//	// no need to reserve space for rollback
				//} else {
				//	// add dummy tuple which fill space for rollback of update
				//	dummy_rid, dummy_tuple = tp.ReserveSpaceForRollbackUpdate(nil, usableSpaceForDummy, txn, log_manager, lock_manager)
				//}
			} else {
				// add dummy tuple which reserves space for rollback of update
				dummy_rid, dummy_tuple = tp.ReserveSpaceForRollbackUpdate(nil, reserve_size, txn, log_manager, lock_manager)
			}

			//// add dummy tuple which fill rest all space of the page for rollback of update
			//dummy_rid, dummy_tuple = tp.ReserveSpaceForRollbackUpdate(nil, usableSpaceForDummy, txn, log_manager, lock_manager)
		}
	}

	return true, nil, update_tuple, dummy_rid, dummy_tuple
}

// rid1 is not null when caller is Redo
func (tp *TablePage) ReserveSpaceForRollbackUpdate(rid *page.RID, size uint32, txn *Transaction, log_manager *recovery.LogManager, lock_manager *LockManager) (*page.RID, *tuple.Tuple) {
	maxSlotNum := tp.GetTupleCount()
	buf := make([]byte, size)

	// set dummy tuple for rollback
	var dummy_rid *page.RID
	if rid != nil {
		dummy_rid = rid
	} else {
		dummy_rid = &page.RID{tp.GetPageId(), maxSlotNum}
	}

	if !txn.IsRecoveryPhase() {
		// Acquire an exclusive lock for inserting a dummy tuple
		locked := lock_manager.LockExclusive(txn, dummy_rid)
		if !locked {
			panic("could not acquire an exclusive lock of found slot (=RID)")
		}
	}

	dummy_tuple := tuple.NewTuple(dummy_rid, size, buf[:size])
	tp.SetFreeSpacePointer(tp.GetFreeSpacePointer() - dummy_tuple.Size())
	tp.setTuple(dummy_rid.GetSlotNum(), dummy_tuple)

	if dummy_rid.GetSlotNum() == tp.GetTupleCount() {
		tp.SetTupleCount(tp.GetTupleCount() + 1)
	}

	if size > 0 {
		tp.SetTupleSize(dummy_rid.GetSlotNum(), SetReservedFlag(size))
	}

	if log_manager.IsEnabledLogging() {
		log_record := recovery.NewLogRecordReserveSpace(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.RESERVE_SPACE, *dummy_rid, dummy_tuple)
		lsn := log_manager.AppendLogRecord(log_record)
		tp.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	return dummy_rid, dummy_tuple
}

func (tp *TablePage) MarkDelete(rid *page.RID, txn *Transaction, lock_manager *LockManager, log_manager *recovery.LogManager) (isMarked bool, markedTuple *tuple.Tuple) {
	if common.EnableDebug {
		defer func() {
			if common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				common.SH_Assert(tp.GetFreeSpacePointer() <= common.PageSize, fmt.Sprintf("FreeSpacePointer value is illegal! value:%d txnId:%v dbgInfo:%s", tp.GetFreeSpacePointer(), txn.GetTransactionId(), txn.dbgInfo))
				common.SH_Assert(tp.GetPageId() == tp.GetPageId(), fmt.Sprintf("pageId data is inconsitent! %d != %d txnId:%d txnState:%d txn.dbgInfo:%s", tp.GetPageId(), tp.GetPageId(), txn.GetTransactionId(), txn.state, txn.dbgInfo))
			}
			if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
				fmt.Printf("TablePage::MarkDelete returned. pageId:%d GetPageId():%d txn.txn_id:%v txnState:%d dbgInfo:%s FreeSpacePointer:%d TupleCount:%d FreeSpaceRemaining:%d\n", tp.GetPageId(), tp.GetPageId(), txn.txn_id, txn.state, txn.dbgInfo, tp.GetFreeSpacePointer(), tp.GetTupleCount(), tp.getFreeSpaceRemaining())
			}
		}()
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("TablePage::MarkDelete called. pageId:%d txn.txn_id:%v dbgInfo:%s  rid1:%v\n", tp.GetPageId(), txn.txn_id, txn.dbgInfo, *rid)
		}
	}

	if !txn.IsRecoveryPhase() {
		// Acquire an exclusive lock, upgrading from a shared lock if necessary.
		if txn.IsSharedLocked(rid) {
			if !lock_manager.LockUpgrade(txn, rid) {
				txn.SetState(ABORTED)
				return false, nil
			}
		} else if !txn.IsExclusiveLocked(rid) && !lock_manager.LockExclusive(txn, rid) {
			txn.SetState(ABORTED)
			return false, nil
		}
	}

	slot_num := rid.GetSlotNum()
	// If the slot number is invalid, abort the transaction.
	if slot_num >= tp.GetTupleCount() {
		if !txn.IsRecoveryPhase() {
			txn.SetState(ABORTED)
		}
		return false, nil
	}

	tuple_size := tp.GetTupleSize(slot_num)
	// If the tuple1 is already deleted, abort the transaction.
	if IsDeleted(tuple_size) {
		if !txn.IsRecoveryPhase() {
			txn.SetState(ABORTED)
		}
		return false, nil
	}

	// GetTuple for rollback of Index...
	tuple_, err := tp.GetTuple(rid, log_manager, lock_manager, txn)
	if tuple_ == nil || err != nil {
		txn.SetState(ABORTED)
		return false, nil
	}

	if log_manager.IsEnabledLogging() {
		dummy_tuple := new(tuple.Tuple)
		log_record := recovery.NewLogRecordInsertDelete(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.MARKDELETE, *rid, dummy_tuple)
		lsn := log_manager.AppendLogRecord(log_record)
		tp.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	// Mark the tuple1 as deleted.
	if tuple_size > 0 {
		tp.SetTupleSize(slot_num, SetDeletedFlag(tuple_size))
	}
	return true, tuple_
}

func (tp *TablePage) ApplyDelete(rid *page.RID, txn *Transaction, log_manager *recovery.LogManager) {
	if common.EnableDebug {
		defer func() {
			if common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				common.SH_Assert(tp.GetFreeSpacePointer() <= common.PageSize, fmt.Sprintf("FreeSpacePointer value is illegal! value:%d txnId:%v dbgInfo:%s", tp.GetFreeSpacePointer(), txn.GetTransactionId(), txn.dbgInfo))
				common.SH_Assert(tp.GetPageId() == tp.GetPageId(), fmt.Sprintf("pageId data is inconsitent! %d != %d txnId:%d txnState:%d txn.dbgInfo:%s", tp.GetPageId(), tp.GetPageId(), txn.GetTransactionId(), txn.state, txn.dbgInfo))
			}
			if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
				fmt.Printf("TablePage::ApplyDelete returned. pageId:%d GetPageId():%d txn.txn_id:%v txnState:%d dbgInfo:%s FreeSpacePointer:%d TupleCount:%d FreeSpaceRemaining:%d\n", tp.GetPageId(), tp.GetPageId(), txn.txn_id, txn.state, txn.dbgInfo, tp.GetFreeSpacePointer(), tp.GetTupleCount(), tp.getFreeSpaceRemaining())
			}
		}()
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("TablePage::ApplyDelete called. pageId:%d txn.txn_id:%v dbgInfo:%s rid1:%v\n", tp.GetPageId(), txn.txn_id, txn.dbgInfo, *rid)
		}
	}

	spaceRemaining := tp.getFreeSpaceRemaining()

	slot_num := rid.GetSlotNum()
	common.SH_Assert(slot_num < tp.GetTupleCount(), "Cannot have more slots than tuples.")

	tuple_offset := tp.GetTupleOffsetAtSlot(slot_num)
	tuple_size := tp.GetTupleSize(slot_num)
	// Check if this is a delete operation, i.e. commit a delete.
	if IsDeleted(tuple_size) {
		tuple_size = UnsetDeletedFlag(tuple_size)
	}
	isReserved := false
	if IsReserved(tuple_size) {
		tuple_size = UnsetReservedFlag(tuple_size)
		isReserved = true
		fmt.Println("ApplyDelete: freed dummy tuple: ", tuple_size)
	}
	// Otherwise we are rolling back an insert.

	if tuple_size < 0 {
		panic("TablePage::ApplyDelete: target tuple size is illegal!!!")
	}

	if !txn.IsRecoveryPhase() {
		common.SH_Assert(txn.IsExclusiveLocked(rid), "We must own the exclusive lock!")
	}

	if log_manager.IsEnabledLogging() {
		// We need to copy out the deleted tuple1 for undo purposes.
		var delete_tuple = new(tuple.Tuple)
		delete_tuple.SetSize(tuple_size)
		delete_tuple.SetData(make([]byte, delete_tuple.Size()))
		copy(delete_tuple.Data(), tp.Data()[tuple_offset:tuple_offset+delete_tuple.Size()])
		delete_tuple.SetRID(rid)

		log_record := recovery.NewLogRecordInsertDelete(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.APPLYDELETE, *rid, delete_tuple)
		lsn := log_manager.AppendLogRecord(log_record)
		tp.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	free_space_pointer := tp.GetFreeSpacePointer()
	common.SH_Assert(tuple_offset >= free_space_pointer, "Free space appears before tuples.")
	//if tuple_offset >= free_space_pointer {
	//	copy(tp.Data()[free_space_pointer+tuple_size:], tp.Data()[free_space_pointer:tuple_offset])
	//}
	// note: copy doesn't occur dummy tuple deleted and free space pointer value is not normal position

	//if isReserved && spaceRemaining == 0 {
	//	// this case is few rest rest space filled dummy tuple is deleted
	//	// so header info of tuple also should be deleted
	//	fmt.Println("ApplyDelete: delete dummy tuple (special case): ", tuple_size)
	//	tp.SetFreeSpacePointer(free_space_pointer + tuple_size + sizeTuple)
	//	tp.SetTupleCount(tp.GetTupleCount() - 1)
	//} else {
	// normal case
	tp.SetFreeSpacePointer(free_space_pointer + tuple_size)
	tp.SetTupleSize(slot_num, 0)
	tp.SetTupleOffsetAtSlot(slot_num, 0)
	//}

	// Update all tuple offsets.
	tuple_count := int(tp.GetTupleCount())
	for ii := 0; ii < tuple_count; ii++ {
		tuple_offset_ii := tp.GetTupleOffsetAtSlot(uint32(ii))
		if tp.GetTupleSize(uint32(ii)) != 0 && tuple_offset_ii < tuple_offset {
			tp.SetTupleOffsetAtSlot(uint32(ii), tuple_offset_ii+tuple_size)
		}
	}
}

func (tp *TablePage) RollbackDelete(rid *page.RID, txn *Transaction, log_manager *recovery.LogManager) {
	if common.EnableDebug {
		defer func() {
			if common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				common.SH_Assert(tp.GetFreeSpacePointer() <= common.PageSize, fmt.Sprintf("FreeSpacePointer value is illegal! value:%d txnId:%v dbgInfo:%s", tp.GetFreeSpacePointer(), txn.GetTransactionId(), txn.dbgInfo))
				common.SH_Assert(tp.GetPageId() == tp.GetPageId(), fmt.Sprintf("pageId data is inconsitent! %d != %d txnId:%d txnState:%d txn.dbgInfo:%s", tp.GetPageId(), tp.GetPageId(), txn.GetTransactionId(), txn.state, txn.dbgInfo))
			}
			if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
				fmt.Printf("TablePage::RollbackDelete returned. pageId:%d GetPageId():%d txn.txn_id:%v txnState:%d dbgInfo:%s FreeSpacePointer:%d TupleCount:%d FreeSpaceRemaining:%d\n", tp.GetPageId(), tp.GetPageId(), txn.txn_id, txn.state, txn.dbgInfo, tp.GetFreeSpacePointer(), tp.GetTupleCount(), tp.getFreeSpaceRemaining())
			}
		}()
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("TablePage::RollbackDelete called. pageId:%d txn.txn_id:%v dbgInfo:%s rid1:%v\n", tp.GetPageId(), txn.txn_id, txn.dbgInfo, *rid)
		}
	}

	if !txn.IsRecoveryPhase() {
		common.SH_Assert(txn.IsExclusiveLocked(rid), "We must own an exclusive lock on the RID.")
	}

	// Log the rollback.
	if log_manager.IsEnabledLogging() {
		dummy_tuple := new(tuple.Tuple)
		log_record := recovery.NewLogRecordInsertDelete(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.ROLLBACKDELETE, *rid, dummy_tuple)
		lsn := log_manager.AppendLogRecord(log_record)
		tp.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	slot_num := rid.GetSlotNum()
	common.SH_Assert(slot_num < tp.GetTupleCount(), "We can't have more slots than tuples.")
	tuple_size := tp.GetTupleSize(slot_num)

	// Unset the deleted flag.
	if IsDeleted(tuple_size) {
		tp.SetTupleSize(slot_num, UnsetDeletedFlag(tuple_size))
	}

	if tuple_size <= 0 {
		panic("TablePage::RollbackDelete: target tuple size is illegal!!!")
	}
}

// Init initializes the table header
func (tp *TablePage) Init(pageId types.PageID, prevPageId types.PageID, log_manager *recovery.LogManager, lock_manager *LockManager, txn *Transaction) {
	if common.EnableDebug {
		defer func() {
			if common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				common.SH_Assert(tp.GetFreeSpacePointer() <= common.PageSize, fmt.Sprintf("FreeSpacePointer value is illegal! value:%d txnId:%v dbgInfo:%s", tp.GetFreeSpacePointer(), txn.GetTransactionId(), txn.dbgInfo))
				common.SH_Assert(tp.GetPageId() == tp.GetPageId(), fmt.Sprintf("pageId data is inconsitent! %d != %d txnId:%d txnState:%d txn.dbgInfo:%s", tp.GetPageId(), tp.GetPageId(), txn.GetTransactionId(), txn.state, txn.dbgInfo))
			}
			if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
				fmt.Printf("TablePage::Init returned. pageId:%d GetPageId():%d txn.txn_id:%v txnState:%d dbgInfo:%s FreeSpacePointer:%d TupleCount:%d FreeSpaceRemaining:%d\n", tp.GetPageId(), tp.GetPageId(), txn.txn_id, txn.state, txn.dbgInfo, tp.GetFreeSpacePointer(), tp.GetTupleCount(), tp.getFreeSpaceRemaining())
			}
		}()
	}
	// Log that we are creating a new page.
	if log_manager.IsEnabledLogging() {
		log_record := recovery.NewLogRecordNewPage(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.NEWPAGE, prevPageId)
		lsn := log_manager.AppendLogRecord(log_record)
		tp.Page.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}
	tp.SetSerializedPageId(pageId)
	tp.SetPrevPageId(prevPageId)
	tp.SetNextPageId(types.InvalidPageID)
	tp.SetTupleCount(0)
	tp.SetFreeSpacePointer(common.PageSize) // point to the end of the page
}

// set value to Page::data memory area. not to Page::id
func (tp *TablePage) SetSerializedPageId(pageId types.PageID) {
	tp.Copy(0, pageId.Serialize())
}

func (tp *TablePage) SetPrevPageId(pageId types.PageID) {
	tp.Copy(offSetPrevPageId, pageId.Serialize())
}

func (tp *TablePage) SetNextPageId(pageId types.PageID) {
	tp.Copy(offSetNextPageId, pageId.Serialize())
}

func (tp *TablePage) SetFreeSpacePointer(freeSpacePointer uint32) {
	if common.EnableDebug {
		common.SH_Assert(freeSpacePointer <= common.PageSize, "illegal pointer value!!")
	}
	if freeSpacePointer < offsetTupleSize {
		panic(fmt.Sprintf("freeSpacePointer value to be set is illegal !!! fsp:%d\n", freeSpacePointer))
	}
	tp.Copy(offsetFreeSpace, types.UInt32(freeSpacePointer).Serialize())
}

func (tp *TablePage) SetTupleCount(tupleCount uint32) {
	tp.Copy(offSetTupleCount, types.UInt32(tupleCount).Serialize())
}

func (tp *TablePage) setTuple(slot uint32, tuple *tuple.Tuple) {
	fsp := tp.GetFreeSpacePointer()
	tp.Copy(fsp, tuple.Data())                                                      // copy tuple1 to data starting at free space pointer
	tp.Copy(offsetTupleOffset+sizeTuple*slot, types.UInt32(fsp).Serialize())        // set tuple1 offset at slot
	tp.Copy(offsetTupleSize+sizeTuple*slot, types.UInt32(tuple.Size()).Serialize()) // set tuple1 size at slot
}

func (tp *TablePage) GetNextPageId() types.PageID {
	return types.NewPageIDFromBytes(tp.Data()[offSetNextPageId:])
}

func (tp *TablePage) GetTupleCount() uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offSetTupleCount:]))
}

func (tp *TablePage) GetTupleOffsetAtSlot(slot_num uint32) uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetTupleOffset+sizeTuple*slot_num:]))
}

/** Set tuple1 offset at slot slot_num. */
func (tp *TablePage) SetTupleOffsetAtSlot(slot_num uint32, offset uint32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, offset)
	offsetInBytes := buf.Bytes()
	copy(tp.Data()[offsetTupleOffset+sizeTuple*slot_num:], offsetInBytes)
}

func (tp *TablePage) GetTupleSize(slot_num uint32) uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetTupleSize+sizeTuple*slot_num:]))
}

/** Set tuple1 size at slot slot_num. */
func (tp *TablePage) SetTupleSize(slot_num uint32, size uint32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, size)
	sizeInBytes := buf.Bytes()
	copy(tp.Data()[offsetTupleSize+sizeTuple*slot_num:], sizeInBytes)
}

func (tp *TablePage) getFreeSpaceRemaining() uint32 {
	ret := tp.GetFreeSpacePointer() - sizeTablePageHeader - sizeTuple*tp.GetTupleCount()
	return ret
}

func (tp *TablePage) GetFreeSpacePointer() uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetFreeSpace:]))
}

func (tp *TablePage) GetTuple(rid *page.RID, log_manager *recovery.LogManager, lock_manager *LockManager, txn *Transaction) (*tuple.Tuple, error) {
	if common.EnableDebug {
		defer func() {
			if common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				common.SH_Assert(tp.GetFreeSpacePointer() <= common.PageSize, fmt.Sprintf("FreeSpacePointer value is illegal! value:%d txnId:%v dbgInfo:%s", tp.GetFreeSpacePointer(), txn.GetTransactionId(), txn.dbgInfo))
				common.SH_Assert(tp.GetPageId() == tp.GetPageId(), fmt.Sprintf("pageId data is inconsitent! %d != %d txnId:%d txnState:%d txn.dbgInfo:%s", tp.GetPageId(), tp.GetPageId(), txn.GetTransactionId(), txn.state, txn.dbgInfo))
			}
			if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
				fmt.Printf("TablePage::GetTuple returned. pageId:%d GetPageId():%d txn.txn_id:%v txnState:%d dbgInfo:%s FreeSpacePointer:%d TupleCount:%d FreeSpaceRemaining:%d\n", tp.GetPageId(), tp.GetPageId(), txn.txn_id, txn.state, txn.dbgInfo, tp.GetFreeSpacePointer(), tp.GetTupleCount(), tp.getFreeSpaceRemaining())
			}
		}()
		if common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
			fmt.Printf("TablePage::GetTuple called. pageId:%d txn.txn_id:%v  dbgInfo:%s rid1:%v\n", tp.GetPageId(), txn.txn_id, txn.dbgInfo, *rid)
		}
	}

	// check having appropriate lock or gettable at least a shared access.
	if !txn.IsRecoveryPhase() {
		if !txn.IsSharedLocked(rid) && !txn.IsExclusiveLocked(rid) && !lock_manager.LockShared(txn, rid) {
			txn.SetState(ABORTED)
			return nil, ErrGeneral
		}
	}

	// If somehow we have more slots than tuples, abort transaction
	if rid.GetSlotNum() >= tp.GetTupleCount() {
		if !txn.IsRecoveryPhase() {
			txn.SetState(ABORTED)
		}
		return nil, ErrGeneral
	}

	slot := rid.GetSlotNum()
	tupleOffset := tp.GetTupleOffsetAtSlot(slot)
	tupleSize := tp.GetTupleSize(slot)

	// target tuple1 should be deleted completely (= operation is commited)
	if tupleOffset == 0 && tupleSize == 0 {
		if !txn.IsRecoveryPhase() {
			if txn.IsExclusiveLocked(rid) {
				// txn which deletes target tuple1 is current txn
				fmt.Println("TablePage:GetTuple ErrSelfDeletedCase (1)!")
				return tuple.NewTuple(rid, 0, make([]byte, 0)), ErrSelfDeletedCase
			} else {
				// when Index returned RID of deleted by other txn
				// in current implementation, this case occurs in not illegal situation

				fmt.Printf("TablePage::GetTuple rid1 of deleted record is passed. rid1:%v\n", *rid)
				txn.SetState(ABORTED)
				return nil, ErrGeneral
			}
		} else {
			// txn which deletes target tuple1 is current txn
			fmt.Println("TablePage:GetTuple ErrSelfDeletedCase (1)!")
			return tuple.NewTuple(rid, 0, make([]byte, 0)), ErrSelfDeletedCase
		}
	}

	// If the tuple1 is marked as deleted
	if IsDeleted(tupleSize) {
		if !txn.IsRecoveryPhase() {
			if txn.IsExclusiveLocked(rid) {
				// txn which deletes target tuple1 is current txn
				fmt.Println("TablePage:GetTuple ErrSelfDeletedCase (2)!")
				return tuple.NewTuple(rid, 0, make([]byte, 0)), ErrSelfDeletedCase
			} else {
				// when RangeSanWithIndexExecutor or PointScanWithIndexExecutor which uses SkipListIterator as RID itrator is called,
				// the txn enter here.

				fmt.Printf("TablePage::GetTuple faced deleted marked record . rid1:%v tupleSize:%d tupleOffset:%d\n", *rid, tupleSize, tupleOffset)

				txn.SetState(ABORTED)
				return nil, ErrGeneral
			}
		} else {
			// txn which deletes target tuple1 is current txn
			fmt.Println("TablePage:GetTuple ErrSelfDeletedCase (2)!")
			return tuple.NewTuple(rid, 0, make([]byte, 0)), ErrSelfDeletedCase
		}
	}

	if IsReserved(tupleSize) {
		// handle as same as deleted tuple
		return tuple.NewTuple(rid, 0, make([]byte, 0)), ErrSelfDeletedCase
	}

	tupleData := make([]byte, tupleSize)
	copy(tupleData, tp.Data()[tupleOffset:])

	return tuple.NewTuple(rid, tupleSize, tupleData), nil
}

func (tp *TablePage) GetTupleFirstRID() *page.RID {
	firstRID := &page.RID{}

	// Find and return the first valid tuple1.
	tupleCount := tp.GetTupleCount()
	for ii := uint32(0); ii < tupleCount; ii++ {
		if tp.GetTupleSize(ii) > 0 {
			firstRID.Set(tp.GetPageId(), ii)
			return firstRID
		}

	}
	return nil
}

func (tp *TablePage) GetNextTupleRID(curRID *page.RID, isNextPage bool) *page.RID {
	nextRID := &page.RID{}

	// Find and return the first valid tuple1 after our current slot number.
	tupleCount := tp.GetTupleCount()
	var init_val uint32 = 0
	if !isNextPage {
		init_val = uint32(curRID.GetSlotNum() + 1)
	}
	for ii := init_val; ii < tupleCount; ii++ {
		if tp.GetTupleSize(ii) > 0 {
			nextRID.Set(tp.GetPageId(), ii)
			return nextRID
		}
	}
	return nil
}

/** @return true if the tuple1 is deleted or empty */
func IsDeleted(tuple_size uint32) bool {
	return tuple_size&uint32(deleteMask) == uint32(deleteMask) || tuple_size == 0
}

/** @return tuple1 size with the deleted flag set */
func SetDeletedFlag(tuple_size uint32) uint32 {
	return tuple_size | uint32(deleteMask)
}

/** @return tuple1 size with the deleted flag unset */
func UnsetDeletedFlag(tuple_size uint32) uint32 {
	return tuple_size & (^uint32(deleteMask))
}

func IsReserved(tuple_size uint32) bool {
	return tuple_size&uint32(reservedMask) == uint32(reservedMask) || tuple_size == 0
}

func SetReservedFlag(tuple_size uint32) uint32 {
	return tuple_size | uint32(reservedMask)
}

func UnsetReservedFlag(tuple_size uint32) uint32 {
	return tuple_size & (^uint32(reservedMask))
}
