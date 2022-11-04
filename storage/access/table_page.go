// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

// package tablepage
package access

import (
	"bytes"
	"encoding/binary"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/errors"
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

// static constexpr uint64_t DELETE_MASK = (1U << (8 * sizeof(uint32_t) - 1));
const deleteMask = uint32(1 << ((8 * 4) - 1))

const sizeTablePageHeader = uint32(24)
const sizeTuple = uint32(8)
const offSetPrevPageId = uint32(8)
const offSetNextPageId = uint32(12)
const offsetFreeSpace = uint32(16)
const offSetTupleCount = uint32(20)
const offsetTupleOffset = uint32(24)
const offsetTupleSize = uint32(28)

const ErrEmptyTuple = errors.Error("tuple cannot be empty")
const ErrNotEnoughSpace = errors.Error("there is not enough space")
const ErrNoFreeSlot = errors.Error("could not find a free slot")

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
	//rwlatch_ common.ReaderWriterLatch
}

// CastPageAsTablePage casts the abstract Page struct into TablePage
func CastPageAsTablePage(page *page.Page) *TablePage {
	if page == nil {
		return nil
	}

	return (*TablePage)(unsafe.Pointer(page))
}

// Inserts a tuple into the table
func (tp *TablePage) InsertTuple(tuple *tuple.Tuple, log_manager *recovery.LogManager, lock_manager *LockManager, txn *Transaction) (*page.RID, error) {
	if common.EnableDebug {
		common.ShPrintf(common.RDB_OP_FUNC_CALL, "TablePage::InsertTuple called. txn.txn_id:%v tuple:%v\n", txn.txn_id, *tuple)
	}
	if tuple.Size() == 0 {
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

	if tp.GetTupleCount() == slot && tuple.Size()+sizeTuple > tp.getFreeSpaceRemaining() {
		return nil, ErrNoFreeSlot
	}

	rid := &page.RID{}
	rid.Set(tp.GetTablePageId(), slot)

	if log_manager.IsEnabledLogging() {
		// Acquire an exclusive lock on the new tuple.
		locked := lock_manager.LockExclusive(txn, rid)
		if !locked {
			txn.SetState(ABORTED)
			return nil, errors.Error("could not acquire an exclusive lock on the new tuple")
			// fmt.Printf("Locking a new tuple should always work. rid: %v\n", rid)
			// lock_manager.PrintLockTables()
			// os.Stdout.Sync()
			// panic("")
		}
	}

	tuple.SetRID(rid)

	tp.SetFreeSpacePointer(tp.GetFreeSpacePointer() - tuple.Size())
	tp.setTuple(slot, tuple)

	if slot == tp.GetTupleCount() {
		tp.SetTupleCount(tp.GetTupleCount() + 1)
	}

	// Write the log record.
	if log_manager.IsEnabledLogging() {
		//common.SH_Assert(!txn.IsSharedLocked(rid) && !txn.IsExclusiveLocked(rid), "A new tuple should not be locked.")
		//common.SH_Assert(locked, "Locking a new tuple should always work.")
		log_record := recovery.NewLogRecordInsertDelete(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.INSERT, *rid, tuple)
		lsn := log_manager.AppendLogRecord(log_record)
		tp.Page.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}
	return rid, nil
}

// if specified nil to update_col_idxs and schema_, all data of existed tuple is replaced one of new_tuple
// if specified not nil, new_tuple also should have all columns defined in schema. but not update target value can be dummy value
// return Tuple pointer when updated tuple need to be moved new page location and it should be inserted after old data deleted, otherwise returned nil
func (tp *TablePage) UpdateTuple(new_tuple *tuple.Tuple, update_col_idxs []int, schema_ *schema.Schema, old_tuple *tuple.Tuple, rid *page.RID, txn *Transaction,
	lock_manager *LockManager, log_manager *recovery.LogManager) (bool, error, *tuple.Tuple) {
	if common.EnableDebug {
		common.ShPrintf(common.RDB_OP_FUNC_CALL, "TablePage::UpdateTuple called. txn.txn_id:%v new_tuple:%v update_col_idxs:%v rid:%v\n", txn.txn_id, *new_tuple, update_col_idxs, *rid)
	}
	common.SH_Assert(new_tuple.Size() > 0, "Cannot have empty tuples.")

	slot_num := rid.GetSlotNum()
	// If the slot number is invalid, abort the transaction.
	if slot_num >= tp.GetTupleCount() {
		if log_manager.IsEnabledLogging() {
			txn.SetState(ABORTED)
		}
		return false, nil, nil
	}
	tuple_size := tp.GetTupleSize(slot_num)
	// If the tuple is deleted, abort the transaction.
	if IsDeleted(tuple_size) {
		if log_manager.IsEnabledLogging() {
			txn.SetState(ABORTED)
		}
		return false, nil, nil
	}

	// Copy out the old value.
	tuple_offset := tp.GetTupleOffsetAtSlot(slot_num)
	old_tuple.SetSize(tuple_size)
	old_tuple_data := make([]byte, old_tuple.Size())
	copy(old_tuple_data, tp.GetData()[tuple_offset:tuple_offset+old_tuple.Size()])
	old_tuple.SetData(old_tuple_data)
	old_tuple.SetRID(rid)

	// setup tuple for updating
	var update_tuple *tuple.Tuple = nil
	if update_col_idxs == nil || schema_ == nil {
		update_tuple = new_tuple
	} else {
		// update specifed columns only case

		var update_tuple_values []types.Value = make([]types.Value, 0)
		matched_cnt := int(0)
		for idx, _ := range schema_.GetColumns() {
			if matched_cnt < len(update_col_idxs) && idx == update_col_idxs[matched_cnt] {
				update_tuple_values = append(update_tuple_values, new_tuple.GetValue(schema_, uint32(idx)))
				matched_cnt++
			} else {
				update_tuple_values = append(update_tuple_values, old_tuple.GetValue(schema_, uint32(idx)))
			}
		}
		update_tuple = tuple.NewTupleFromSchema(update_tuple_values, schema_)
	}

	if tp.getFreeSpaceRemaining()+tuple_size < update_tuple.Size() {
		//if common.EnableLogging {
		//	txn.SetState(ABORTED)
		//}
		//return false
		return false, ErrNotEnoughSpace, update_tuple
	}

	if log_manager.IsEnabledLogging() {
		// Acquire an exclusive lock, upgrading from shared if necessary.
		if txn.IsSharedLocked(rid) {
			if !lock_manager.LockUpgrade(txn, rid) {
				txn.SetState(ABORTED)
				return false, nil, nil
			}
		} else if !txn.IsExclusiveLocked(rid) && !lock_manager.LockExclusive(txn, rid) {
			txn.SetState(ABORTED)
			return false, nil, nil
		}
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

	// Update all tuple offsets.
	tuple_cnt := int(tp.GetTupleCount())
	for ii := 0; ii < tuple_cnt; ii++ {
		tuple_offset_i := tp.GetTupleOffsetAtSlot(uint32(ii))
		if tp.GetTupleSize(uint32(ii)) > 0 && tuple_offset_i < tuple_offset+tuple_size {
			tp.SetTupleOffsetAtSlot(uint32(ii), tuple_offset_i+tuple_size-update_tuple.Size())
		}
	}
	return true, nil, nil
}

func (tp *TablePage) MarkDelete(rid *page.RID, txn *Transaction, lock_manager *LockManager, log_manager *recovery.LogManager) bool {
	if common.EnableDebug {
		common.ShPrintf(common.RDB_OP_FUNC_CALL, "TablePage::MarkDelete called. txn.txn_id:%v rid:%v\n", txn.txn_id, *rid)
	}
	slot_num := rid.GetSlotNum()
	// If the slot number is invalid, abort the transaction.
	if slot_num >= tp.GetTupleCount() {
		if log_manager.IsEnabledLogging() {
			txn.SetState(ABORTED)
		}
		return false
	}

	tuple_size := tp.GetTupleSize(slot_num)
	// If the tuple is already deleted, abort the transaction.
	if IsDeleted(tuple_size) {
		if log_manager.IsEnabledLogging() {
			txn.SetState(ABORTED)
		}
		return false
	}

	if log_manager.IsEnabledLogging() {
		// Acquire an exclusive lock, upgrading from a shared lock if necessary.
		if txn.IsSharedLocked(rid) {
			if !lock_manager.LockUpgrade(txn, rid) {
				txn.SetState(ABORTED)
				return false
			}
		} else if !txn.IsExclusiveLocked(rid) && !lock_manager.LockExclusive(txn, rid) {
			txn.SetState(ABORTED)
			return false
		}
		dummy_tuple := new(tuple.Tuple)
		log_record := recovery.NewLogRecordInsertDelete(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.MARKDELETE, *rid, dummy_tuple)
		lsn := log_manager.AppendLogRecord(log_record)
		tp.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	// Mark the tuple as deleted.
	if tuple_size > 0 {
		tp.SetTupleSize(slot_num, SetDeletedFlag(tuple_size))
	}
	return true
}

func (table_page *TablePage) ApplyDelete(rid *page.RID, txn *Transaction, log_manager *recovery.LogManager) {
	if common.EnableDebug {
		common.ShPrintf(common.RDB_OP_FUNC_CALL, "TablePage::ApplyDelete called. txn.txn_id:%v rid:%v\n", txn.txn_id, *rid)
	}
	slot_num := rid.GetSlotNum()
	common.SH_Assert(slot_num < table_page.GetTupleCount(), "Cannot have more slots than tuples.")

	tuple_offset := table_page.GetTupleOffsetAtSlot(slot_num)
	tuple_size := table_page.GetTupleSize(slot_num)
	// Check if this is a delete operation, i.e. commit a delete.
	if IsDeleted(tuple_size) {
		tuple_size = UnsetDeletedFlag(tuple_size)
	}
	// Otherwise we are rolling back an insert.

	// We need to copy out the deleted tuple for undo purposes.
	var delete_tuple *tuple.Tuple = new(tuple.Tuple)
	delete_tuple.SetSize(tuple_size)
	delete_tuple.SetData(make([]byte, delete_tuple.Size()))
	//memcpy(delete_tuple.Data(), table_page.Data()+tuple_offset, delete_tuple.Size())
	copy(delete_tuple.Data(), table_page.Data()[tuple_offset:tuple_offset+delete_tuple.Size()])
	delete_tuple.SetRID(rid)
	//delete_tuple.allocated = true

	if log_manager.IsEnabledLogging() {
		common.SH_Assert(txn.IsExclusiveLocked(rid), "We must own the exclusive lock!")
		log_record := recovery.NewLogRecordInsertDelete(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.APPLYDELETE, *rid, delete_tuple)
		lsn := log_manager.AppendLogRecord(log_record)
		table_page.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	free_space_pointer := table_page.GetFreeSpacePointer()
	common.SH_Assert(tuple_offset >= free_space_pointer, "Free space appears before tuples.")

	// memmove(GetData() + free_space_pointer + tuple_size, GetData() + free_space_pointer,
	// tuple_offset - free_space_pointer);
	copy(table_page.Data()[free_space_pointer+tuple_size:], table_page.Data()[free_space_pointer:tuple_offset])

	table_page.SetFreeSpacePointer(free_space_pointer + tuple_size)
	table_page.SetTupleSize(slot_num, 0)
	table_page.SetTupleOffsetAtSlot(slot_num, 0)

	// Update all tuple offsets.
	tuple_count := int(table_page.GetTupleCount())
	for ii := 0; ii < tuple_count; ii++ {
		tuple_offset_ii := table_page.GetTupleOffsetAtSlot(uint32(ii))
		if table_page.GetTupleSize(uint32(ii)) != 0 && tuple_offset_ii < tuple_offset {
			table_page.SetTupleOffsetAtSlot(uint32(ii), tuple_offset_ii+tuple_size)
		}
	}
}

func (tp *TablePage) RollbackDelete(rid *page.RID, txn *Transaction, log_manager *recovery.LogManager) {
	if common.EnableDebug {
		common.ShPrintf(common.RDB_OP_FUNC_CALL, "TablePage::RollbackDelete called. txn.txn_id:%v rid:%v\n", txn.txn_id, *rid)
	}
	// Log the rollback.
	if log_manager.IsEnabledLogging() {
		common.SH_Assert(txn.IsExclusiveLocked(rid), "We must own an exclusive lock on the RID.")
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
}

// Init initializes the table header
func (tp *TablePage) Init(pageId types.PageID, prevPageId types.PageID, log_manager *recovery.LogManager, lock_manager *LockManager, txn *Transaction) {
	// Log that we are creating a new page.
	if log_manager.IsEnabledLogging() {
		//txn_ := (*Transaction)(unsafe.Pointer(&txn))
		log_record := recovery.NewLogRecordNewPage(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.NEWPAGE, prevPageId)
		lsn := log_manager.AppendLogRecord(log_record)
		tp.Page.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}
	tp.SetPageId(pageId)
	tp.SetPrevPageId(prevPageId)
	tp.SetNextPageId(types.InvalidPageID)
	tp.SetTupleCount(0)
	tp.SetFreeSpacePointer(common.PageSize) // point to the end of the page
}

func (tp *TablePage) SetPageId(pageId types.PageID) {
	tp.Copy(0, pageId.Serialize())
}

func (tp *TablePage) SetPrevPageId(pageId types.PageID) {
	tp.Copy(offSetPrevPageId, pageId.Serialize())
}

func (tp *TablePage) SetNextPageId(pageId types.PageID) {
	tp.Copy(offSetNextPageId, pageId.Serialize())
}

func (tp *TablePage) SetFreeSpacePointer(freeSpacePointer uint32) {
	tp.Copy(offsetFreeSpace, types.UInt32(freeSpacePointer).Serialize())
}

func (tp *TablePage) SetTupleCount(tupleCount uint32) {
	tp.Copy(offSetTupleCount, types.UInt32(tupleCount).Serialize())
}

func (tp *TablePage) setTuple(slot uint32, tuple *tuple.Tuple) {
	fsp := tp.GetFreeSpacePointer()
	tp.Copy(fsp, tuple.Data())                                                      // copy tuple to data starting at free space pointer
	tp.Copy(offsetTupleOffset+sizeTuple*slot, types.UInt32(fsp).Serialize())        // set tuple offset at slot
	tp.Copy(offsetTupleSize+sizeTuple*slot, types.UInt32(tuple.Size()).Serialize()) // set tuple size at slot
}

func (tp *TablePage) GetTablePageId() types.PageID {
	return types.NewPageIDFromBytes(tp.Data()[:])
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

/** Set tuple offset at slot slot_num. */
func (tp *TablePage) SetTupleOffsetAtSlot(slot_num uint32, offset uint32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, offset)
	offsetInBytes := buf.Bytes()
	//memcpy(GetData() + OFFSET_TUPLE_OFFSET + SIZE_TUPLE * slot_num, &offset, sizeof(uint32_t));
	copy(tp.Data()[offsetTupleOffset+sizeTuple*slot_num:], offsetInBytes)
}

func (tp *TablePage) GetTupleSize(slot_num uint32) uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetTupleSize+sizeTuple*slot_num:]))
}

/** Set tuple size at slot slot_num. */
func (tp *TablePage) SetTupleSize(slot_num uint32, size uint32) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, size)
	sizeInBytes := buf.Bytes()
	//memcpy(GetData() + OFFSET_TUPLE_SIZE + SIZE_TUPLE * slot_num, &size, sizeof(uint32_t));
	copy(tp.Data()[offsetTupleSize+sizeTuple*slot_num:], sizeInBytes)
}

func (tp *TablePage) getFreeSpaceRemaining() uint32 {
	return tp.GetFreeSpacePointer() - sizeTablePageHeader - sizeTuple*tp.GetTupleCount()
}

func (tp *TablePage) GetFreeSpacePointer() uint32 {
	return uint32(types.NewUInt32FromBytes(tp.Data()[offsetFreeSpace:]))
}

func (tp *TablePage) GetTuple(rid *page.RID, log_manager *recovery.LogManager, lock_manager *LockManager, txn *Transaction) *tuple.Tuple {
	// If somehow we have more slots than tuples, abort transaction
	if rid.GetSlotNum() >= tp.GetTupleCount() {
		if log_manager.IsEnabledLogging() {
			txn.SetState(ABORTED)
		}
		return nil
	}

	slot := rid.GetSlotNum()
	tupleOffset := tp.GetTupleOffsetAtSlot(slot)
	tupleSize := tp.GetTupleSize(slot)

	// If the tuple is deleted, abort the access.
	if IsDeleted(tupleSize) {
		if log_manager.IsEnabledLogging() {
			txn.SetState(ABORTED)
		}
		return nil
	}

	// Otherwise we have a valid tuple, try to acquire at least a shared access.
	if log_manager.IsEnabledLogging() {
		if !txn.IsSharedLocked(rid) && !txn.IsExclusiveLocked(rid) && !lock_manager.LockShared(txn, rid) {
			//if !lock_manager.LockShared(txn, rid) && !txn.IsExclusiveLocked(rid) {
			txn.SetState(ABORTED)
			return nil
		}
	}

	tupleData := make([]byte, tupleSize)
	copy(tupleData, tp.Data()[tupleOffset:])

	return tuple.NewTuple(rid, tupleSize, tupleData)
}

func (tp *TablePage) GetTupleFirstRID() *page.RID {
	firstRID := &page.RID{}

	// Find and return the first valid tuple.
	tupleCount := tp.GetTupleCount()
	for ii := uint32(0); ii < tupleCount; ii++ {
		if tp.GetTupleSize(ii) > 0 {
			firstRID.Set(tp.GetTablePageId(), ii)
			return firstRID
		}

	}
	return nil
}

func (tp *TablePage) GetNextTupleRID(curRID *page.RID, isNextPage bool) *page.RID {
	nextRID := &page.RID{}

	// Find and return the first valid tuple after our current slot number.
	tupleCount := tp.GetTupleCount()
	var init_val uint32 = 0
	if !isNextPage {
		init_val = uint32(curRID.GetSlotNum() + 1)
	}
	for ii := init_val; ii < tupleCount; ii++ {
		if tp.GetTupleSize(ii) > 0 {
			nextRID.Set(tp.GetTablePageId(), ii)
			return nextRID
		}
	}
	return nil
}

/** @return true if the tuple is deleted or empty */
func IsDeleted(tuple_size uint32) bool {
	return tuple_size&uint32(deleteMask) == uint32(deleteMask) || tuple_size == 0
}

/** @return tuple size with the deleted flag set */
func SetDeletedFlag(tuple_size uint32) uint32 {
	return tuple_size | uint32(deleteMask)
}

/** @return tuple size with the deleted flag unset */
func UnsetDeletedFlag(tuple_size uint32) uint32 {
	return tuple_size & (^uint32(deleteMask))
}
