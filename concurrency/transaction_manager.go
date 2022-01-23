package concurrency

type TransactinManager struct {
	std::atomic<txn_id_t> next_txn_id{0}
	LockManager *lock_manager_ __attribute__((__unused__))
	LogManager *log_manager_ __attribute__((__unused__))
	/** The global transaction latch is used for checkpointing. */
	ReaderWriterLatch global_txn_latch
}

std::unordered_map<txn_id_t, Transaction *> txn_map = {};

func (transaction_manager *TransactionManager) Begin(Transaction *txn) *Transaction {
  // Acquire the global transaction latch in shared mode.
  global_txn_latch.RLock()

  if (txn == nullptr) {
    txn = new Transaction(next_txn_id++)
  }

  if (enable_logging) {
    // TODO(student): Add logging here.
    LogRecord log_record(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::BEGIN)
    lsn_t lsn = log_manager_->AppendLogRecord(&log_record)
    txn->SetPrevLSN(lsn)
  }

  txn_map[txn->GetTransactionId()] = txn
  return txn;
}

func (transaction_manager *TransactionManager) Commit(Transaction *txn) {
  txn->SetState(TransactionState::COMMITTED);

// TODO: (SDB) need implement
/*
  // Perform all deletes before we commit.
  auto write_set = txn->GetWriteSet()
  while (!write_set->empty()) {
    auto &item = write_set->back()
    auto table = item.table;
    if (item.wtype == WType::DELETE) {
      // Note that this also releases the lock when holding the page latch.
      table->ApplyDelete(item.rid, txn)
    }
    write_set->pop_back()
  }
  write_set->clear()
*/

  if (enable_logging) {
    // TODO(student): add logging here
    LogRecord log_record(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::COMMIT)
    lsn_t lsn = log_manager->AppendLogRecord(&log_record)
    txn->SetPrevLSN(lsn)
    log_manager->Flush()
  }

  // Release all the locks.
  ReleaseLocks(txn)
  // Release the global transaction latch.
  global_txn_latch_.RUnlock()
}

func (transaction_manager *TransactionManager) Abort(Transaction *txn) {
  txn->SetState(TransactionState::ABORTED);

// TODO: (SDB) need implement
/*
  // Rollback before releasing the lock.
  auto write_set = txn->GetWriteSet();
  while (!write_set->empty()) {
    auto &item = write_set->back()
    auto table = item.table
    if (item.wtype == WType::DELETE) {
      table->RollbackDelete(item.rid_, txn)
    } else if (item.wtype == WType::INSERT) {
      // Note that this also releases the lock when holding the page latch.
      table->ApplyDelete(item.rid, txn)
    } else if (item.wtype_ == WType::UPDATE) {
      table->UpdateTuple(item.tuple, item.rid_, txn)
    }
    write_set->pop_back()
  }
  write_set->clear()
*/

  if (enable_logging) {
    // TODO(student): add logging here
    LogRecord log_record(txn->GetTransactionId(), txn->GetPrevLSN(), LogRecordType::ABORT)
    lsn_t lsn = log_manager_->AppendLogRecord(&log_record)
    txn->SetPrevLSN(lsn)
  }

  // Release all the locks.
  releaseLocks(txn)
  // Release the global transaction latch.
  global_txn_latch.RUnlock()
}

func (transaction_manager *TransactionManager) BlockAllTransactions() { global_txn_latch.WLock() }

func (transaction_manager *TransactionManager) ResumeTransactions() { global_txn_latch.WUnlock() }

func (transaction_manager *TransactionManager) void releaseLocks(Transaction *txn) {
    std::unordered_set<RID> lock_set;
    for (auto item : *txn->GetExclusiveLockSet()) {
      lock_set.emplace(item);
    }
    for (auto item : *txn->GetSharedLockSet()) {
      lock_set.emplace(item);
    }
    for (auto locked_rid : lock_set) {
      lock_manager->Unlock(txn, locked_rid);
    }
}

