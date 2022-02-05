package interfaces

import (
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
)

type TransactionState int32

const (
	GROWING TransactionState = iota
	SHRINKING
	COMMITTED
	ABORTED
)

type ITransaction interface {
	/** @return the id of this transaction */
	GetTransactionId() types.TxnID

	// /** @return the id of the thread running the transaction */
	// func (txn *Transaction) GetThreadId() ThreadID { return txn.thread_id }

	// /** @return the list of of write records of this transaction */
	// func (txn *Transaction) GetWriteSet() deque<WriteRecord> { return txn.write_set }

	// /** @return the page set */
	// func (txn *Transaction) GetPageSet() deque<*Page> { return txn.page_set }

	// /**
	// * Adds a page into the page set.
	// * @param page page to be added
	// */
	// func (txn *Transaction) AddIntoPageSet(page *Page) { txn.page_set.push_back(page) }

	// /** @return the deleted page set */
	// func (txn *Transaction) GetDeletedPageSet() unordered_set<PageID> { return txn.deleted_page_set }

	// /**
	// * Adds a page to the deleted page set.
	// * @param page_id id of the page to be marked as deleted
	// */
	// func (txn *Transaction) AddIntoDeletedPageSet(page_id PageID) { txn.deleted_page_set.insert(page_id) }

	// /** @return the set of resources under a shared lock */
	// func (txn *Transaction) GetSharedLockSet() unordered_set<RID> { return txn.shared_lock_set }

	// /** @return the set of resources under an exclusive lock */
	// func (txn *Transaction) GetExclusiveLockSet() unordered_set<RID> { return txn.exclusive_lock_set }

	/** @return true if rid is shared locked by this transaction */
	IsSharedLocked(rid *page.RID) bool

	/** @return true if rid is exclusively locked by this transaction */
	IsExclusiveLocked(rid *page.RID) bool

	/** @return the current state of the transaction */
	GetState() TransactionState

	/**
	* Set the state of the access.
	* @param state new state
	 */
	SetState(state TransactionState)

	/** @return the previous LSN */
	GetPrevLSN() types.LSN

	/**
	* Set the previous LSN.
	* @param prev_lsn new previous lsn
	 */
	SetPrevLSN(prev_lsn types.LSN)
}
