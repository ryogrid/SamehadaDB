// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

//package tablepage
package access

import (
	"bytes"
	"encoding/binary"
	"unsafe"

	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/errors"
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

//static constexpr uint64_t DELETE_MASK = (1U << (8 * sizeof(uint32_t) - 1));
const deleteMask = uint64((1<<(8*4) - 1))

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
//  ---------------------------------------------------------
//  | HEADER | ... FREE SPACE ... | ... INSERTED TUPLES ... |
//  ---------------------------------------------------------
//                                ^
//                                free space pointer
//  Header format (size in bytes):
//  ----------------------------------------------------------------------------
//  | PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
//  ----------------------------------------------------------------------------
//  ----------------------------------------------------------------
//  | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
//  ----------------------------------------------------------------
type TablePage struct {
	page.Page
}

// TODO: (SDB) not ported methods exist at TablePage.
//             And additional loggings are needed when implement the methods
//             ex: MarkDelete, UpdateTuple methods.
//                 adding codes to related methods on other classes is also neede.
//                 ex: logging/recovery, executer(maybe)

// CastPageAsTablePage casts the abstract Page struct into TablePage
func CastPageAsTablePage(page *page.Page) *TablePage {
	if page == nil {
		return nil
	}
	return (*TablePage)(unsafe.Pointer(page))
}

// Inserts a tuple into the table
func (tp *TablePage) InsertTuple(tuple *tuple.Tuple, log_manager *recovery.LogManager, lock_manager *LockManager, txn *Transaction) (*page.RID, error) {
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

	tp.SetFreeSpacePointer(tp.GetFreeSpacePointer() - tuple.Size())
	tp.setTuple(slot, tuple)

	rid := &page.RID{}
	rid.Set(tp.GetTablePageId(), slot)
	if slot == tp.GetTupleCount() {
		tp.SetTupleCount(tp.GetTupleCount() + 1)
	}

	// Write the log record.
	if common.EnableLogging {
		// BUSTUB_ASSERT(!txn.IsSharedLocked(*rid) && !txn.IsExclusiveLocked(*rid), "A new tuple should not be locked.");
		// Acquire an exclusive lock on the new tuple.
		// bool locked = lock_manager.Exclusive(txn, *rid);
		//txn_ := (*Transaction)(unsafe.Pointer(&txn))

		// TODO: (SDB) need to check having lock
		//locked := LockExclusive(txn, rid)
		//fmt.Print(locked)

		//BUSTUB_ASSERT(locked, "Locking a new tuple should always work.");
		log_record := recovery.NewLogRecordInsertDelete(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.INSERT, *rid, tuple)
		lsn := log_manager.AppendLogRecord(log_record)
		tp.Page.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}
	return rid, nil
}

func (tp *TablePage) MarkDelete(rid *page.RID, txn *Transaction, lock_manager *LockManager, log_manager *recovery.LogManager) bool {
	slot_num := rid.GetSlotNum()
	// If the slot number is invalid, abort the transaction.
	if slot_num >= tp.GetTupleCount() {
		if common.EnableLogging {
			txn.SetState(ABORTED)
		}
		return false
	}

	tuple_size := tp.GetTupleSize(slot_num)
	// If the tuple is already deleted, abort the transaction.
	if IsDeleted(tuple_size) {
		if common.EnableLogging {
			txn.SetState(ABORTED)
		}
		return false
	}

	if common.EnableLogging {
		// Acquire an exclusive lock, upgrading from a shared lock if necessary.
		if txn.IsSharedLocked(rid) {
			if !lock_manager.LockUpgrade(txn, rid) {
				return false
			}
		} else if !txn.IsExclusiveLocked(rid) && !lock_manager.LockExclusive(txn, rid) {
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
	slot_num := rid.GetSlotNum()
	//BUSTUB_ASSERT(slot_num < GetTupleCount(), "Cannot have more slots than tuples.")

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

	if common.EnableLogging {
		//BUSTUB_ASSERT(txn.IsExclusiveLocked(rid), "We must own the exclusive lock!")
		log_record := recovery.NewLogRecordInsertDelete(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.APPLYDELETE, *rid, delete_tuple)
		lsn := log_manager.AppendLogRecord(log_record)
		table_page.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	free_space_pointer := table_page.GetFreeSpacePointer()
	//BUSTUB_ASSERT(tuple_offset >= free_space_pointer, "Free space appears before tuples.")

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
	// Log the rollback.
	if common.EnableLogging {
		//BUSTUB_ASSERT(txn->IsExclusiveLocked(rid), "We must own an exclusive lock on the RID.");
		dummy_tuple := new(tuple.Tuple)
		log_record := recovery.NewLogRecordInsertDelete(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.ROLLBACKDELETE, *rid, dummy_tuple)
		lsn := log_manager.AppendLogRecord(log_record)
		tp.SetLSN(lsn)
		txn.SetPrevLSN(lsn)
	}

	slot_num := rid.GetSlotNum()
	//BUSTUB_ASSERT(slot_num < GetTupleCount(), "We can't have more slots than tuples.");
	tuple_size := tp.GetTupleSize(slot_num)

	// Unset the deleted flag.
	if IsDeleted(tuple_size) {
		tp.SetTupleSize(slot_num, UnsetDeletedFlag(tuple_size))
	}
}

// Init initializes the table header
func (tp *TablePage) Init(pageId types.PageID, prevPageId types.PageID, log_manager *recovery.LogManager, lock_manager *LockManager, txn *Transaction) {
	// Log that we are creating a new page.
	if common.EnableLogging {
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
		if common.EnableLogging {
			txn.SetState(ABORTED)
		}
		return nil
	}

	slot := rid.GetSlotNum()
	tupleOffset := tp.GetTupleOffsetAtSlot(slot)
	tupleSize := tp.GetTupleSize(slot)

	// If the tuple is deleted, abort the access.
	if IsDeleted(tupleSize) {
		if common.EnableLogging {
			txn.SetState(ABORTED)
		}
		return nil
	}

	// Otherwise we have a valid tuple, try to acquire at least a shared access.
	if common.EnableLogging {
		//txn_ := (*Transaction)(unsafe.Pointer(&txn))
		if !txn.IsSharedLocked(rid) && !txn.IsExclusiveLocked(rid) && !lock_manager.LockShared(txn, rid) {
			//return false
			// TODO: (SDB) need care of being returned nil
			//return nil
			// TODO: (SDB) [logging/recovery] need implement
			// do nothing now
		}
	}

	tupleData := make([]byte, tupleSize)
	copy(tupleData, tp.Data()[tupleOffset:])

	//return &tuple.Tuple{rid, tupleSize, tupleData}
	return tuple.NewTuple(rid, tupleSize, tupleData)
}

func (tp *TablePage) GetTupleFirstRID() *page.RID {
	firstRID := &page.RID{}

	tupleCount := tp.GetTupleCount()
	for i := uint32(0); i < tupleCount; i++ {
		// TODO: (SDB) need implement (if is deleted)
		// if is deleted
		firstRID.Set(tp.GetTablePageId(), i)
		return firstRID
	}
	return nil
}

func (tp *TablePage) GetNextTupleRID(rid *page.RID) *page.RID {
	firstRID := &page.RID{}

	tupleCount := tp.GetTupleCount()
	for i := rid.GetSlotNum() + 1; i < tupleCount; i++ {
		// TODO: (SDB) need implement (if is deleted)
		// if is deleted
		firstRID.Set(tp.GetTablePageId(), i)
		return firstRID
	}
	return nil
}

// TODO: (SDB) need port WLatch of TablePage
/** Acquire the page write latch. */
func (tp *TablePage) WLatch() { /*rwlatch_.WLock()*/ }

// TODO: (SDB) need port WUnlatch of TablePage
/** Release the page write latch. */
func (tp *TablePage) WUnlatch() { /*rwlatch_.WUnlock()*/ }

// TODO: (SDB) need port RLatch of TablePage
/** Acquire the page read latch. */
func (tp *TablePage) RLatch() { /*rwlatch_.RLock()*/ }

// TODO: (SDB) need port RLatch of TablePage
/** Release the page read latch. */
func (tp *TablePage) RUnlatch() { /*rwlatch_.RUnlock()*/ }

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
