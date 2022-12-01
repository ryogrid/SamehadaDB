// package concurrency
// package transaction
package access

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * Transaction states:
 *
 *     _________________________
 *    |                         v
 * GROWING -> SHRINKING -> COMMITTED   ABORTED
 *    |__________|________________________^
 *
 **/

type TransactionState int32

const (
	GROWING TransactionState = iota
	SHRINKING
	COMMITTED
	ABORTED
)

/**
 * Type of write operation.
 */
type WType int32

const (
	INSERT WType = iota
	DELETE
	UPDATE
)

/**
 * WriteRecord tracks information related to a write.
 */
type WriteRecord struct {
	rid   page.RID
	wtype WType
	/** The tuple is used only for the updateoperation. */
	tuple *tuple.Tuple
	/** The table heap specifies which table this write record is for. */
	table *TableHeap
	oid   uint32 // for rollback of index data
}

func NewWriteRecord(rid page.RID, wtype WType, tuple *tuple.Tuple, table *TableHeap, oid uint32) *WriteRecord {
	ret := new(WriteRecord)
	ret.rid = rid
	ret.wtype = wtype
	ret.tuple = tuple
	ret.table = table
	ret.oid = oid
	return ret
}

/**
 * Transaction tracks information related to a transaction.
 */
type Transaction struct {
	/** The current transaction state. */
	state TransactionState

	// /** The thread GetPageId, used in single-threaded transactions. */
	// thread_id ThreadID

	/** The GetPageId of this access. */
	txn_id types.TxnID

	// /** The undo set of the access. */
	write_set []*WriteRecord

	/** The LSN of the last record written by the access. */
	prev_lsn types.LSN

	// /** Concurrent index: the pages that were latched during index operation. */
	// page_set deque<*Page>
	// /** Concurrent index: the page IDs that were deleted during index operation.*/
	// deleted_page_set unordered_set<PageID>

	// /** LockManager: the set of shared-locked tuples held by this access. */
	shared_lock_set []page.RID
	// /** LockManager: the set of exclusive-locked tuples held by this access. */
	exclusive_lock_set []page.RID
	dbgInfo            string
}

func NewTransaction(txn_id types.TxnID) *Transaction {
	return &Transaction{
		GROWING,
		// std::this_thread::get_id(),
		txn_id,
		make([]*WriteRecord, 0),
		common.InvalidLSN,
		// deque<*Page>,
		// unordered_set<PageID>
		make([]page.RID, 0),
		make([]page.RID, 0),
		"",
	}
}

/** @return the id of this transaction */
func (txn *Transaction) GetTransactionId() types.TxnID { return txn.txn_id }

// /** @return the id of the thread running the transaction */
// func (txn *Transaction) GetThreadId() ThreadID { return txn.thread_id }

/** @return the list of of write records of this transaction */
func (txn *Transaction) GetWriteSet() []*WriteRecord { return txn.write_set }

func (txn *Transaction) SetWriteSet(write_set []*WriteRecord) { txn.write_set = write_set }

func (txn *Transaction) AddIntoWriteSet(write_record *WriteRecord) {
	txn.write_set = append(txn.write_set, write_record)
}

// /** @return the set of resources under a shared lock */
func (txn *Transaction) GetSharedLockSet() []page.RID {
	ret := txn.shared_lock_set
	return ret
}

// /** @return the set of resources under an exclusive lock */
func (txn *Transaction) GetExclusiveLockSet() []page.RID {
	ret := txn.exclusive_lock_set
	return ret
}

func (txn *Transaction) SetSharedLockSet(set []page.RID)    { txn.shared_lock_set = set }
func (txn *Transaction) SetExclusiveLockSet(set []page.RID) { txn.exclusive_lock_set = set }

func isContainsRID(list []page.RID, rid page.RID) bool {
	for _, r := range list {
		if rid == r {
			return true
		}
	}
	return false
}

/** @return true if rid is shared locked by this transaction */
func (txn *Transaction) IsSharedLocked(rid *page.RID) bool {
	ret := isContainsRID(txn.shared_lock_set, *rid)
	//fmt.Printf("called IsSharedLocked: %v\n", ret)
	return ret
}

/** @return true if rid is exclusively locked by this transaction */
func (txn *Transaction) IsExclusiveLocked(rid *page.RID) bool {
	ret := isContainsRID(txn.exclusive_lock_set, *rid)
	//fmt.Printf("called IsExclusiveLocked: %v\n", ret)
	return ret
}

/** @return the current state of the transaction */
func (txn *Transaction) GetState() TransactionState { return txn.state }

/**
* Set the state of the access.
* @param state new state
 */
func (txn *Transaction) SetState(state TransactionState) {
	if common.EnableDebug {
		if state == ABORTED {
			common.ShPrintf(common.RDB_OP_FUNC_CALL, "Transaction::SetState called. txn.txn_id:%d dbgInfo:%s state:ABORTED\n", txn.txn_id, txn.dbgInfo)
		}
	}
	txn.state = state
}

/** @return the previous LSN */
func (txn *Transaction) GetPrevLSN() types.LSN { return txn.prev_lsn }

/**
* Set the previous LSN.
* @param prev_lsn new previous lsn
 */
func (txn *Transaction) SetPrevLSN(prev_lsn types.LSN) { txn.prev_lsn = prev_lsn }

func (txn *Transaction) GetDebugInfo() string { return txn.dbgInfo }

func (txn *Transaction) SetDebugInfo(dbgInfo string) { txn.dbgInfo = dbgInfo }
