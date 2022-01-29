package concurrency

type Transaction struct {
	/** The current transaction state. */
	state TransactionState
	// /** The thread ID, used in single-threaded transactions. */
	// thread_id ThreadID
	/** The ID of this transaction. */
	txn_id TxnID
  
	// /** The undo set of the transaction. */
	// write_set deque<WriteRecord>
	/** The LSN of the last record written by the transaction. */
	prev_lsn LSN
  
	// /** Concurrent index: the pages that were latched during index operation. */
	// page_set deque<*Page>
	// /** Concurrent index: the page IDs that were deleted during index operation.*/
	// deleted_page_set unordered_set<PageID>
  
	// /** LockManager: the set of shared-locked tuples held by this transaction. */
	// shared_lock_set unordered_set<RID>
	// /** LockManager: the set of exclusive-locked tuples held by this transaction. */
	// exclusive_lock_set unordered_set<RID>
}

func NewTransaction(txn_id TxnID) *Transaction {
	return &Transaction{
		TransactionState::GROWING,
		// std::this_thread::get_id(),
		txn_id,
		// deque<WriteRecord>
		INVALID_LSN,
		// deque<*Page>,
		// unordered_set<PageID>
		// unordered_set<RID>,
		// unordered_set<RID>,
	}
}

// /** @return the id of the thread running the transaction */
// func (txn *Transaction) GetThreadId() ThreadID { return txn.thread_id }

/** @return the id of this transaction */
func (txn *Transaction) GetTransactionId() TxnID { return txn.txn_id }

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

// /** @return true if rid is shared locked by this transaction */
// func (txn *Transaction) IsSharedLocked(rid *RID ) { return txn.shared_lock_set.find(rid) != txn.shared_lock_set.end() }

// /** @return true if rid is exclusively locked by this transaction */
// func (txn *Transaction) IsExclusiveLocked(rid *RID) bool { return txn.exclusive_lock_set.find(rid) != txn.exclusive_lock_set.end() }

/** @return the current state of the transaction */
func (txn *Transaction) GetState() TransactionState { return txn.state }

/**
* Set the state of the transaction.
* @param state new state
*/
func (txn *Transaction) SetState(state TransactionState) { txn.state = state }

/** @return the previous LSN */
func (txn *Transaction) GetPrevLSN() LSN { return txn.prev_lsn }

/**
* Set the previous LSN.
* @param prev_lsn new previous lsn
*/
func (txn *Transaction) SetPrevLSN(prev_lsn LSN) { txn.prev_lsn = prev_lsn }