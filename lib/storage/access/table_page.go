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
const offSetPrevPageID = uint32(8)
const offSetNextPageID = uint32(12)
const offsetFreeSpace = uint32(16)
const offSetTupleCount = uint32(20)
const offsetTupleOffset = uint32(24)
const offsetTupleSize = uint32(28)

const ErrEmptyTuple = errors.Error("tuple1 cannot be empty.")
const ErrNotEnoughSpace = errors.Error("there is not enough space.")
const ErrRollbackDifficult = errors.Error("rollback is difficult. tuple need to be moved.")
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
//	| PageID (4)| LSN (4)| PrevPageID (4)| NextPageID (4)| FreeSpacePointer(4) |
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
func (tp *TablePage) InsertTuple(tuple *tuple.Tuple, logManager *recovery.LogManager, lockManager *LockManager, txn *Transaction) (*page.RID, error) {
	if common.EnableDebug {
		defer func() {
			if common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				common.SHAssert(tp.GetFreeSpacePointer() <= common.PageSize, fmt.Sprintf("FreeSpacePointer value is illegal! value:%d txnId:%v dbgInfo:%s", tp.GetFreeSpacePointer(), txn.GetTransactionID(), txn.dbgInfo))
				common.SHAssert(tp.GetPageID() == tp.GetPageID(), fmt.Sprintf("pageID data is inconsitent! %d != %d txnId:%d txnState:%d txn.dbgInfo:%s", tp.GetPageID(), tp.GetPageID(), txn.GetTransactionID(), txn.state, txn.dbgInfo))
			}
			if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
				fmt.Printf("TablePage::InsertTuple returned. pageID:%d GetPageID():%d txn.txnID:%v txnState:%d dbgInfo:%s FreeSpacePointer:%d TupleCount:%d FreeSpaceRemaining:%d tuple1:%v\n", tp.GetPageID(), tp.GetPageID(), txn.txnID, txn.state, txn.dbgInfo, tp.GetFreeSpacePointer(), tp.GetTupleCount(), tp.getFreeSpaceRemaining(), *tuple)
			}
		}()
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("TablePage::InsertTuple called. pageID:%d txn.txnID:%v dbgInfo:%s tuple1:%v\n", tp.GetPageID(), txn.txnID, txn.dbgInfo, *tuple)
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
	rid.Set(tp.GetPageID(), slot)

	if !txn.IsRecoveryPhase() {
		// Acquire an exclusive lock on the new tuple1.
		locked := lockManager.LockExclusive(txn, rid)
		if !locked {
			//txn.SetState(ABORTED)
			return nil, errors.Error("could not acquire an exclusive lock of found slot (=RID)")
			// fmt.Printf("Locking a new tuple1 should always work. rid1: %v\n", rid1)
			// lockManager.PrintLockTables()
			// os.Stdout.Sync()
			// panic("")
		}
	}

	tuple.SetRID(rid)

	if common.EnableDebug {
		setFSP := tp.GetFreeSpacePointer() - tuple.Size()
		common.SHAssert(setFSP <= common.PageSize, fmt.Sprintf("illegal pointer value!! txnId:%d txnState:%d txn.dbgInfo:%s rid1:%v GetPageID():%d setFSP:%d", txn.txnID, txn.state, txn.dbgInfo, *rid, tp.GetPageID(), setFSP))
	}

	tp.SetFreeSpacePointer(tp.GetFreeSpacePointer() - tuple.Size())
	tp.setTuple(slot, tuple)

	if slot == tp.GetTupleCount() {
		tp.SetTupleCount(tp.GetTupleCount() + 1)
	}

	// Write the log record.
	if logManager.IsEnabledLogging() {
		logRecord := recovery.NewLogRecordInsertDelete(txn.GetTransactionID(), txn.GetPrevLSN(), recovery.INSERT, *rid, tuple)
		lsn := logManager.AppendLogRecord(logRecord)
		tp.Page.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	return rid, nil
}

// if specified nil to updateColIdxs and sc, all data of existed tuple1 is replaced one of newTuple
// if specified not nil, newTuple also should have all columns defined in schema. but not update target value can be dummy value
// if isForRollbackOrUndo is false, newTuple occupies the same memory space as the oldTuple
// for ensureing existance of enough space at rollback on abort or undo
// return Tuple pointer when updated tuple1 need to be moved new page location and it should be inserted after old data deleted, otherwise returned nil
func (tp *TablePage) UpdateTuple(newTuple *tuple.Tuple, updateColIdxs []int, sc *schema.Schema, oldTuple *tuple.Tuple, rid *page.RID, txn *Transaction,
	lockManager *LockManager, logManager *recovery.LogManager, isRollbackOrUndo bool) (bool, error, *tuple.Tuple) {
	if common.EnableDebug {
		defer func() {
			if common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				common.SHAssert(tp.GetFreeSpacePointer() <= common.PageSize, fmt.Sprintf("FreeSpacePointer value is illegal! value:%d txnId:%v dbgInfo:%s", tp.GetFreeSpacePointer(), txn.GetTransactionID(), txn.dbgInfo))
				common.SHAssert(tp.GetPageID() == tp.GetPageID(), fmt.Sprintf("pageID data is inconsitent! %d != %d txnId:%d txnState:%d txn.dbgInfo:%s", tp.GetPageID(), tp.GetPageID(), txn.GetTransactionID(), txn.state, txn.dbgInfo))
			}
			if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
				fmt.Printf("TablePage::UpdateTuple returned. pageID:%d GetPageID():%d txn.txnID:%v txnState:%d dbgInfo:%s FreeSpacePointer:%d TupleCount:%d FreeSpaceRemaining:%d newTuple:%v\n", tp.GetPageID(), tp.GetPageID(), txn.txnID, txn.state, txn.dbgInfo, tp.GetFreeSpacePointer(), tp.GetTupleCount(), tp.getFreeSpaceRemaining(), *newTuple)
			}
		}()
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("TablePage::UpdateTuple called. pageID:%d txn.txnID:%v dbgInfo:%s newTuple:%v updateColIdxs:%v rid1:%v\n", tp.GetPageID(), txn.txnID, txn.dbgInfo, *newTuple, updateColIdxs, *rid)
		}
	}
	common.SHAssert(newTuple.Size() > 0, "Cannot have empty tuples.")

	if !txn.IsRecoveryPhase() {
		// Acquire an exclusive lock, upgrading from shared if necessary.
		if txn.IsSharedLocked(rid) {
			if !lockManager.LockUpgrade(txn, rid) {
				txn.SetState(ABORTED)
				return false, nil, nil
			}
		} else if !txn.IsExclusiveLocked(rid) && !lockManager.LockExclusive(txn, rid) {
			txn.SetState(ABORTED)
			return false, nil, nil
		}
	}

	slotNum := rid.GetSlotNum()
	// If the slot number is invalid, abort the transaction.
	if slotNum >= tp.GetTupleCount() {
		if !txn.IsRecoveryPhase() {
			txn.SetState(ABORTED)
		}
		return false, nil, nil
	}
	tupleSize := tp.GetTupleSize(slotNum)
	// If the tuple1 is deleted, abort the transaction.
	if IsDeleted(tupleSize) {
		if !txn.IsRecoveryPhase() {
			txn.SetState(ABORTED)
		}
		return false, nil, nil
	}

	// Copy out the old value.
	tupleOffset := tp.GetTupleOffsetAtSlot(slotNum)
	oldTuple.SetSize(tupleSize)
	oldTupleData := make([]byte, oldTuple.Size())
	copy(oldTupleData, tp.GetData()[tupleOffset:tupleOffset+oldTuple.Size()])
	oldTuple.SetData(oldTupleData)
	oldTuple.SetRID(rid)

	// setup tuple1 for updating
	var updateTuple *tuple.Tuple = nil
	if updateColIdxs == nil || sc == nil {
		updateTuple = newTuple
	} else {
		// update specifed columns only case

		var updateTupleValues = make([]types.Value, 0)
		matchedCnt := int(0)
		for idx := range sc.GetColumns() {
			if matchedCnt < len(updateColIdxs) && idx == updateColIdxs[matchedCnt] {
				updateTupleValues = append(updateTupleValues, newTuple.GetValue(sc, uint32(idx)))
				matchedCnt++
			} else {
				updateTupleValues = append(updateTupleValues, oldTuple.GetValue(sc, uint32(idx)))
			}
		}
		updateTuple = tuple.NewTupleFromSchema(updateTupleValues, sc)
	}

	if updateTuple.Size() <= 0 {
		panic("tuple size is illegal!!!")
	}

	if tp.getFreeSpaceRemaining()+tupleSize < updateTuple.Size() {
		//fmt.Println("ErrNotEnoughSpace: getFreeSpaceRemaining", tp.getFreeSpaceRemaining(), "tupleSize", tupleSize, "updateTuple.Size()", updateTuple.Size(), "RID", *rid)
		return false, ErrNotEnoughSpace, updateTuple
	}

	if tupleSize > updateTuple.Size() && !isRollbackOrUndo {
		//fmt.Println("ErrRollbackDifficult: getFreeSpaceRemaining", tp.getFreeSpaceRemaining(), "tupleSize", tupleSize, "updateTuple.Size()", updateTuple.Size(), "RID", *rid)
		return false, ErrRollbackDifficult, updateTuple
	}

	if logManager.IsEnabledLogging() {
		logRecord := recovery.NewLogRecordUpdate(txn.GetTransactionID(), txn.GetPrevLSN(), recovery.UPDATE, *rid, *oldTuple, *updateTuple)
		lsn := logManager.AppendLogRecord(logRecord)
		tp.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	// Perform the update.
	freeSpacePointer := tp.GetFreeSpacePointer()
	common.SHAssert(tupleOffset >= freeSpacePointer, "Offset should appear after current free space position.")

	copy(tp.GetData()[freeSpacePointer+tupleSize-updateTuple.Size():], tp.GetData()[freeSpacePointer:tupleOffset])
	tp.SetFreeSpacePointer(freeSpacePointer + tupleSize - updateTuple.Size())
	copy(tp.GetData()[tupleOffset+tupleSize-updateTuple.Size():], updateTuple.Data()[:updateTuple.Size()])
	tp.SetTupleSize(slotNum, updateTuple.Size())

	// Update all tuples offsets.
	tupleCnt := int(tp.GetTupleCount())
	for ii := 0; ii < tupleCnt; ii++ {
		tupleOffsetI := tp.GetTupleOffsetAtSlot(uint32(ii))
		if tp.GetTupleSize(uint32(ii)) > 0 && tupleOffsetI < tupleOffset+tupleSize {
			tp.SetTupleOffsetAtSlot(uint32(ii), tupleOffsetI+tupleSize-updateTuple.Size())
		}
	}

	return true, nil, updateTuple
}

func (tp *TablePage) MarkDelete(rid *page.RID, txn *Transaction, lockManager *LockManager, logManager *recovery.LogManager) (isMarked bool, markedTuple *tuple.Tuple) {
	if common.EnableDebug {
		defer func() {
			if common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				common.SHAssert(tp.GetFreeSpacePointer() <= common.PageSize, fmt.Sprintf("FreeSpacePointer value is illegal! value:%d txnId:%v dbgInfo:%s", tp.GetFreeSpacePointer(), txn.GetTransactionID(), txn.dbgInfo))
				common.SHAssert(tp.GetPageID() == tp.GetPageID(), fmt.Sprintf("pageID data is inconsitent! %d != %d txnId:%d txnState:%d txn.dbgInfo:%s", tp.GetPageID(), tp.GetPageID(), txn.GetTransactionID(), txn.state, txn.dbgInfo))
			}
			if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
				fmt.Printf("TablePage::MarkDelete returned. pageID:%d GetPageID():%d txn.txnID:%v txnState:%d dbgInfo:%s FreeSpacePointer:%d TupleCount:%d FreeSpaceRemaining:%d\n", tp.GetPageID(), tp.GetPageID(), txn.txnID, txn.state, txn.dbgInfo, tp.GetFreeSpacePointer(), tp.GetTupleCount(), tp.getFreeSpaceRemaining())
			}
		}()
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("TablePage::MarkDelete called. pageID:%d txn.txnID:%v dbgInfo:%s  rid1:%v\n", tp.GetPageID(), txn.txnID, txn.dbgInfo, *rid)
		}
	}

	if !txn.IsRecoveryPhase() {
		// Acquire an exclusive lock, upgrading from a shared lock if necessary.
		if txn.IsSharedLocked(rid) {
			if !lockManager.LockUpgrade(txn, rid) {
				txn.SetState(ABORTED)
				return false, nil
			}
		} else if !txn.IsExclusiveLocked(rid) && !lockManager.LockExclusive(txn, rid) {
			txn.SetState(ABORTED)
			return false, nil
		}
	}

	slotNum := rid.GetSlotNum()
	// If the slot number is invalid, abort the transaction.
	if slotNum >= tp.GetTupleCount() {
		if !txn.IsRecoveryPhase() {
			txn.SetState(ABORTED)
		}
		return false, nil
	}

	tupleSize := tp.GetTupleSize(slotNum)
	// If the tuple1 is already deleted, abort the transaction.
	if IsDeleted(tupleSize) {
		if !txn.IsRecoveryPhase() {
			txn.SetState(ABORTED)
		}
		return false, nil
	}

	// GetTuple for rollback of Index...
	tpl, err := tp.GetTuple(rid, logManager, lockManager, txn)
	if tpl == nil || err != nil {
		txn.SetState(ABORTED)
		return false, nil
	}

	if logManager.IsEnabledLogging() {
		dummyTuple := new(tuple.Tuple)
		logRecord := recovery.NewLogRecordInsertDelete(txn.GetTransactionID(), txn.GetPrevLSN(), recovery.MARKDELETE, *rid, dummyTuple)
		lsn := logManager.AppendLogRecord(logRecord)
		tp.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	// Mark the tuple1 as deleted.
	if tupleSize > 0 {
		tp.SetTupleSize(slotNum, SetDeletedFlag(tupleSize))
	}
	return true, tpl
}

func (tp *TablePage) ApplyDelete(rid *page.RID, txn *Transaction, logManager *recovery.LogManager) {
	if common.EnableDebug {
		defer func() {
			if common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				common.SHAssert(tp.GetFreeSpacePointer() <= common.PageSize, fmt.Sprintf("FreeSpacePointer value is illegal! value:%d txnId:%v dbgInfo:%s", tp.GetFreeSpacePointer(), txn.GetTransactionID(), txn.dbgInfo))
				common.SHAssert(tp.GetPageID() == tp.GetPageID(), fmt.Sprintf("pageID data is inconsitent! %d != %d txnId:%d txnState:%d txn.dbgInfo:%s", tp.GetPageID(), tp.GetPageID(), txn.GetTransactionID(), txn.state, txn.dbgInfo))
			}
			if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
				fmt.Printf("TablePage::ApplyDelete returned. pageID:%d GetPageID():%d txn.txnID:%v txnState:%d dbgInfo:%s FreeSpacePointer:%d TupleCount:%d FreeSpaceRemaining:%d\n", tp.GetPageID(), tp.GetPageID(), txn.txnID, txn.state, txn.dbgInfo, tp.GetFreeSpacePointer(), tp.GetTupleCount(), tp.getFreeSpaceRemaining())
			}
		}()
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("TablePage::ApplyDelete called. pageID:%d txn.txnID:%v dbgInfo:%s rid1:%v\n", tp.GetPageID(), txn.txnID, txn.dbgInfo, *rid)
		}
	}

	//spaceRemaining := tp.getFreeSpaceRemaining()

	slotNum := rid.GetSlotNum()
	common.SHAssert(slotNum < tp.GetTupleCount(), "Cannot have more slots than tuples.")

	tupleOffset := tp.GetTupleOffsetAtSlot(slotNum)
	tupleSize := tp.GetTupleSize(slotNum)
	// Check if this is a delete operation, i.e. commit a delete.
	if IsDeleted(tupleSize) {
		tupleSize = UnsetDeletedFlag(tupleSize)
	}
	// Otherwise we are rolling back an insert.

	if tupleSize < 0 {
		panic("TablePage::ApplyDelete: target tuple size is illegal!!!")
	}

	if !txn.IsRecoveryPhase() {
		common.SHAssert(txn.IsExclusiveLocked(rid), "We must own the exclusive lock!")
	}

	if logManager.IsEnabledLogging() {
		// We need to copy out the deleted tuple1 for undo purposes.
		var deleteTuple = new(tuple.Tuple)
		deleteTuple.SetSize(tupleSize)
		deleteTuple.SetData(make([]byte, deleteTuple.Size()))
		copy(deleteTuple.Data(), tp.Data()[tupleOffset:tupleOffset+deleteTuple.Size()])
		deleteTuple.SetRID(rid)

		logRecord := recovery.NewLogRecordInsertDelete(txn.GetTransactionID(), txn.GetPrevLSN(), recovery.APPLYDELETE, *rid, deleteTuple)
		lsn := logManager.AppendLogRecord(logRecord)
		tp.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	freeSpacePointer := tp.GetFreeSpacePointer()
	common.SHAssert(tupleOffset >= freeSpacePointer, "Free space appears before tuples.")
	copy(tp.Data()[freeSpacePointer+tupleSize:], tp.Data()[freeSpacePointer:tupleOffset])

	tp.SetFreeSpacePointer(freeSpacePointer + tupleSize)
	tp.SetTupleSize(slotNum, 0)
	tp.SetTupleOffsetAtSlot(slotNum, 0)

	// Update all tuple offsets.
	tupleCount := int(tp.GetTupleCount())
	for ii := 0; ii < tupleCount; ii++ {
		tupleOffsetII := tp.GetTupleOffsetAtSlot(uint32(ii))
		if tp.GetTupleSize(uint32(ii)) != 0 && tupleOffsetII < tupleOffset {
			tp.SetTupleOffsetAtSlot(uint32(ii), tupleOffsetII+tupleSize)
		}
	}
}

func (tp *TablePage) RollbackDelete(rid *page.RID, txn *Transaction, logManager *recovery.LogManager) {
	if common.EnableDebug {
		defer func() {
			if common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				common.SHAssert(tp.GetFreeSpacePointer() <= common.PageSize, fmt.Sprintf("FreeSpacePointer value is illegal! value:%d txnId:%v dbgInfo:%s", tp.GetFreeSpacePointer(), txn.GetTransactionID(), txn.dbgInfo))
				common.SHAssert(tp.GetPageID() == tp.GetPageID(), fmt.Sprintf("pageID data is inconsitent! %d != %d txnId:%d txnState:%d txn.dbgInfo:%s", tp.GetPageID(), tp.GetPageID(), txn.GetTransactionID(), txn.state, txn.dbgInfo))
			}
			if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
				fmt.Printf("TablePage::RollbackDelete returned. pageID:%d GetPageID():%d txn.txnID:%v txnState:%d dbgInfo:%s FreeSpacePointer:%d TupleCount:%d FreeSpaceRemaining:%d\n", tp.GetPageID(), tp.GetPageID(), txn.txnID, txn.state, txn.dbgInfo, tp.GetFreeSpacePointer(), tp.GetTupleCount(), tp.getFreeSpaceRemaining())
			}
		}()
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("TablePage::RollbackDelete called. pageID:%d txn.txnID:%v dbgInfo:%s rid1:%v\n", tp.GetPageID(), txn.txnID, txn.dbgInfo, *rid)
		}
	}

	if !txn.IsRecoveryPhase() {
		common.SHAssert(txn.IsExclusiveLocked(rid), "We must own an exclusive lock on the RID.")
	}

	// Log the rollback.
	if logManager.IsEnabledLogging() {
		dummyTuple := new(tuple.Tuple)
		logRecord := recovery.NewLogRecordInsertDelete(txn.GetTransactionID(), txn.GetPrevLSN(), recovery.ROLLBACKDELETE, *rid, dummyTuple)
		lsn := logManager.AppendLogRecord(logRecord)
		tp.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	slotNum := rid.GetSlotNum()
	common.SHAssert(slotNum < tp.GetTupleCount(), "We can't have more slots than tuples.")
	tupleSize := tp.GetTupleSize(slotNum)

	// Unset the deleted flag.
	if IsDeleted(tupleSize) {
		tp.SetTupleSize(slotNum, UnsetDeletedFlag(tupleSize))
	}

	if tupleSize <= 0 {
		panic("TablePage::RollbackDelete: target tuple size is illegal!!!")
	}
}

// Init initializes the table header
func (tp *TablePage) Init(pageID types.PageID, prevPageID types.PageID, logManager *recovery.LogManager, lockManager *LockManager, txn *Transaction, isForRedo bool) {
	if common.EnableDebug {
		defer func() {
			if common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				common.SHAssert(tp.GetFreeSpacePointer() <= common.PageSize, fmt.Sprintf("FreeSpacePointer value is illegal! value:%d txnId:%v dbgInfo:%s", tp.GetFreeSpacePointer(), txn.GetTransactionID(), txn.dbgInfo))
				common.SHAssert(tp.GetPageID() == tp.GetPageID(), fmt.Sprintf("pageID data is inconsitent! %d != %d txnId:%d txnState:%d txn.dbgInfo:%s", tp.GetPageID(), tp.GetPageID(), txn.GetTransactionID(), txn.state, txn.dbgInfo))
			}
			if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
				fmt.Printf("TablePage::Init returned. pageID:%d GetPageID():%d txn.txnID:%v txnState:%d dbgInfo:%s FreeSpacePointer:%d TupleCount:%d FreeSpaceRemaining:%d\n", tp.GetPageID(), tp.GetPageID(), txn.txnID, txn.state, txn.dbgInfo, tp.GetFreeSpacePointer(), tp.GetTupleCount(), tp.getFreeSpaceRemaining())
			}
		}()
	}
	// Log that we are creating a new page.
	if logManager.IsEnabledLogging() {
		logRecord := recovery.NewLogRecordNewPage(txn.GetTransactionID(), txn.GetPrevLSN(), recovery.NewTablePage, prevPageID, pageID)
		lsn := logManager.AppendLogRecord(logRecord)
		tp.Page.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}
	tp.SetSerializedPageID(pageID)
	tp.SetPrevPageID(prevPageID)
	if !isForRedo {
		tp.SetNextPageID(types.InvalidPageID)
		tp.SetTupleCount(0)
		tp.SetFreeSpacePointer(common.PageSize) // point to the end of the page
	}
}

// set value to Page::data memory area. not to Page::id
func (tp *TablePage) SetSerializedPageID(pageID types.PageID) {
	tp.Copy(0, pageID.Serialize())
}

func (tp *TablePage) SetPrevPageID(pageID types.PageID) {
	tp.Copy(offSetPrevPageID, pageID.Serialize())
}

func (tp *TablePage) SetNextPageID(pageID types.PageID) {
	tp.Copy(offSetNextPageID, pageID.Serialize())
}

func (tp *TablePage) SetFreeSpacePointer(freeSpacePointer uint32) {
	if common.EnableDebug {
		common.SHAssert(freeSpacePointer <= common.PageSize, "illegal pointer value!!")
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

func (tp *TablePage) GetNextPageID() types.PageID {
	return types.NewPageIDFromBytes(tp.Data()[offSetNextPageID:])
}

func (tp *TablePage) GetTupleCount() uint32 {
	ret := uint32(types.NewUInt32FromBytes(tp.Data()[offSetTupleCount:]))
	return ret
}

func (tp *TablePage) GetTupleOffsetAtSlot(slotNum uint32) uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetTupleOffset+sizeTuple*slotNum:]))
}

/** Set tuple1 offset at slot slotNum. */
func (tp *TablePage) SetTupleOffsetAtSlot(slotNum uint32, offset uint32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, offset)
	offsetInBytes := buf.Bytes()
	copy(tp.Data()[offsetTupleOffset+sizeTuple*slotNum:], offsetInBytes)
}

func (tp *TablePage) GetTupleSize(slotNum uint32) uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetTupleSize+sizeTuple*slotNum:]))
}

/** Set tuple1 size at slot slotNum. */
func (tp *TablePage) SetTupleSize(slotNum uint32, size uint32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, size)
	sizeInBytes := buf.Bytes()
	copy(tp.Data()[offsetTupleSize+sizeTuple*slotNum:], sizeInBytes)
}

func (tp *TablePage) getFreeSpaceRemaining() uint32 {
	ret := tp.GetFreeSpacePointer() - sizeTablePageHeader - sizeTuple*tp.GetTupleCount()
	return ret
}

func (tp *TablePage) GetFreeSpacePointer() uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetFreeSpace:]))
}

func (tp *TablePage) GetTuple(rid *page.RID, logManager *recovery.LogManager, lockManager *LockManager, txn *Transaction) (*tuple.Tuple, error) {
	if common.EnableDebug {
		defer func() {
			if common.ActiveLogKindSetting&common.DEBUGGING > 0 {
				common.SHAssert(tp.GetFreeSpacePointer() <= common.PageSize, fmt.Sprintf("FreeSpacePointer value is illegal! value:%d txnId:%v dbgInfo:%s", tp.GetFreeSpacePointer(), txn.GetTransactionID(), txn.dbgInfo))
				common.SHAssert(tp.GetPageID() == tp.GetPageID(), fmt.Sprintf("pageID data is inconsitent! %d != %d txnId:%d txnState:%d txn.dbgInfo:%s", tp.GetPageID(), tp.GetPageID(), txn.GetTransactionID(), txn.state, txn.dbgInfo))
			}
			if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
				fmt.Printf("TablePage::GetTuple returned. pageID:%d GetPageID():%d txn.txnID:%v txnState:%d dbgInfo:%s FreeSpacePointer:%d TupleCount:%d FreeSpaceRemaining:%d\n", tp.GetPageID(), tp.GetPageID(), txn.txnID, txn.state, txn.dbgInfo, tp.GetFreeSpacePointer(), tp.GetTupleCount(), tp.getFreeSpaceRemaining())
			}
		}()
		if common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
			fmt.Printf("TablePage::GetTuple called. pageID:%d txn.txnID:%v  dbgInfo:%s rid1:%v\n", tp.GetPageID(), txn.txnID, txn.dbgInfo, *rid)
		}
	}

	// check having appropriate lock or gettable at least a shared access.
	if !txn.IsRecoveryPhase() {
		if !txn.IsSharedLocked(rid) && !txn.IsExclusiveLocked(rid) && !lockManager.LockShared(txn, rid) {
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
			firstRID.Set(tp.GetPageID(), ii)
			return firstRID
		}

	}
	return nil
}

func (tp *TablePage) GetNextTupleRID(curRID *page.RID, isNextPage bool) *page.RID {
	nextRID := &page.RID{}

	// Find and return the first valid tuple1 after our current slot number.
	tupleCount := tp.GetTupleCount()
	var initVal uint32 = 0
	if !isNextPage {
		initVal = uint32(curRID.GetSlotNum() + 1)
	}
	for ii := initVal; ii < tupleCount; ii++ {
		if tp.GetTupleSize(ii) > 0 {
			nextRID.Set(tp.GetPageID(), ii)
			return nextRID
		}
	}
	return nil
}

/** @return true if the tuple1 is deleted or empty */
func IsDeleted(tupleSize uint32) bool {
	return tupleSize&uint32(deleteMask) == uint32(deleteMask) || tupleSize == 0
}

/** @return tuple1 size with the deleted flag set */
func SetDeletedFlag(tupleSize uint32) uint32 {
	return tupleSize | uint32(deleteMask)
}

/** @return tuple1 size with the deleted flag unset */
func UnsetDeletedFlag(tupleSize uint32) uint32 {
	return tupleSize & (^uint32(deleteMask))
}
