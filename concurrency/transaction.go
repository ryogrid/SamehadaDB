package concurrency

type Transaction struct {
	/** The current transaction state. */
	TransactionState state
	/** The thread ID, used in single-threaded transactions. */
	std::thread::id thread_id
	/** The ID of this transaction. */
	txn_id_t txn_id
  
	/** The undo set of the transaction. */
	std::shared_ptr<std::deque<WriteRecord>> write_set
	/** The LSN of the last record written by the transaction. */
	lsn_t prev_lsn;
  
	/** Concurrent index: the pages that were latched during index operation. */
	std::shared_ptr<std::deque<Page *>> page_set
	/** Concurrent index: the page IDs that were deleted during index operation.*/
	std::shared_ptr<std::unordered_set<page_id_t>> deleted_page_set
  
	/** LockManager: the set of shared-locked tuples held by this transaction. */
	std::shared_ptr<std::unordered_set<RID>> shared_lock_set
	/** LockManager: the set of exclusive-locked tuples held by this transaction. */
	std::shared_ptr<std::unordered_set<RID>> exclusive_lock_set
}

func New(txn_id_t txn_id) *Transaction
: state_(TransactionState::GROWING),
thread_id_(std::this_thread::get_id()),
txn_id_(txn_id),
prev_lsn_(INVALID_LSN),
shared_lock_set_{new std::unordered_set<RID>},
exclusive_lock_set_{new std::unordered_set<RID>} {
  // Initialize the sets that will be tracked.
  write_set = std::make_shared<std::deque<WriteRecord>>();
  page_set = std::make_shared<std::deque<bustub::Page *>>();
  deleted_page_set = std::make_shared<std::unordered_set<page_id_t>>();
}

/** @return the id of the thread running the transaction */
func (txn *Transaction) std::thread::id GetThreadId() const { return thread_id }

/** @return the id of this transaction */
func (txn *Transaction) txn_id_t GetTransactionId() const { return txn_id }

/** @return the list of of write records of this transaction */
func (txn *Transaction) std::shared_ptr<std::deque<WriteRecord>> GetWriteSet() { return write_set }

/** @return the page set */
func (txn *Transaction) std::shared_ptr<std::deque<Page *>> GetPageSet() { return page_set }

/**
* Adds a page into the page set.
* @param page page to be added
*/
func (txn *Transaction) void AddIntoPageSet(Page *page) { page_set_->push_back(page) }

/** @return the deleted page set */
func (txn *Transaction) std::shared_ptr<std::unordered_set<page_id_t>> GetDeletedPageSet() { return deleted_page_set }

/**
* Adds a page to the deleted page set.
* @param page_id id of the page to be marked as deleted
*/
func (txn *Transaction) void AddIntoDeletedPageSet(page_id_t page_id) { deleted_page_set->insert(page_id) }

/** @return the set of resources under a shared lock */
func (txn *Transaction) std::shared_ptr<std::unordered_set<RID>> GetSharedLockSet() { return shared_lock_set }

/** @return the set of resources under an exclusive lock */
func (txn *Transaction) std::shared_ptr<std::unordered_set<RID>> GetExclusiveLockSet() { return exclusive_lock_set }

/** @return true if rid is shared locked by this transaction */
func (txn *Transaction) IsSharedLocked(const RID &rid) { return shared_lock_set->find(rid) != shared_lock_set->end() }

/** @return true if rid is exclusively locked by this transaction */
func (txn *Transaction) bool IsExclusiveLocked(const RID &rid) { return exclusive_lock_set->find(rid) != exclusive_lock_set->end() }

/** @return the current state of the transaction */
func (txn *Transaction) TransactionState GetState() { return state; }

/**
* Set the state of the transaction.
* @param state new state
*/
func (txn *Transaction) void SetState(TransactionState state) { state = state }

/** @return the previous LSN */
func (txn *Transaction) lsn_t GetPrevLSN() { return prev_lsn }

/**
* Set the previous LSN.
* @param prev_lsn new previous lsn
*/
func (txn *Transaction) void SetPrevLSN(lsn_t prev_lsn) { prev_lsn = prev_lsn }