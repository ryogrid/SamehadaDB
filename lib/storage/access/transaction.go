// package concurrency
// package transaction
package access

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
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
	RESERVE_SPACE
)

/**
 * WriteRecord tracks information related to a write.
 */
type WriteRecord struct {
	rid   *page.RID
	wtype WType
	/** The tuple1 is used only for the updateoperation. */
	tuple1 *tuple.Tuple
	tuple2 *tuple.Tuple
	/** The table heap specifies which table this write record is for. */
	table *TableHeap
	oid   uint32 // for rollback of index data
}

func NewWriteRecord(rid *page.RID, wtype WType, tuple1 *tuple.Tuple, tuple2 *tuple.Tuple, table *TableHeap, oid uint32) *WriteRecord {
	ret := new(WriteRecord)
	ret.rid = rid
	ret.wtype = wtype
	ret.tuple1 = tuple1
	ret.tuple2 = tuple2
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

	/** The GetPageId of this access. */
	txn_id types.TxnID

	// The undo set of the access.
	write_set []*WriteRecord

	/** The LSN of the last record written by the access. */
	prev_lsn types.LSN

	// the set of shared-locked tuples held by this access.
	shared_lock_set []page.RID
	// the set of exclusive-locked tuples held by this access.
	exclusive_lock_set []page.RID
	dbgInfo            string
	abortable          bool // for debugging and testing
	isRecoveryPhase    bool
}

func NewTransaction(txn_id types.TxnID) *Transaction {
	return &Transaction{
		GROWING,
		txn_id,
		make([]*WriteRecord, 0),
		common.InvalidLSN,
		make([]page.RID, 0),
		make([]page.RID, 0),
		"",
		true,
		false,
	}
}

/** @return the id of this transaction */
func (txn *Transaction) GetTransactionId() types.TxnID { return txn.txn_id }

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
	if common.EnableDebug && common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
		if state == ABORTED {
			fmt.Printf("Transaction::SetState called. txn.txn_id:%d dbgInfo:%s state:ABORTED\n", txn.txn_id, txn.dbgInfo)

		}
	}

	if common.ActiveLogKindSetting&common.NOT_ABORABLE_TXN_FEATURE > 0 {
		if state == ABORTED && txn.abortable == false {
			fmt.Printf("debuginfo: %s\n", txn.dbgInfo)
			for _, wr := range txn.GetWriteSet() {
				fmt.Printf("write set item: %v\n", *wr)
				if wr.tuple1 != nil {
					fmt.Printf("tuple1: %v\n", *(wr.tuple1))
				}
				if wr.tuple2 != nil {
					fmt.Printf("tuple1: %v\n", *(wr.tuple2))
				}
			}
			panic("this txn is not abortable!!!")
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

// when this txn is set ABORTED, panic is called
func (txn *Transaction) MakeNotAbortable() { txn.abortable = false }

func (txn *Transaction) IsAbortable() bool { return txn.abortable }

func (txn *Transaction) IsRecoveryPhase() bool { return txn.isRecoveryPhase }

func (txn *Transaction) SetIsRecoveryPhase(val bool) { txn.isRecoveryPhase = val }
