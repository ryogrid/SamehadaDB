package access

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * TransactionManager keeps track of all the transactions running in the system.
 */
type TransactionManager struct {
	next_txn_id  types.TxnID
	lock_manager *LockManager
	log_manager  *recovery.LogManager
	/** The global transaction latch is used for checkpointing. */
	global_txn_latch common.ReaderWriterLatch
}

var txn_map map[types.TxnID]*Transaction = make(map[types.TxnID]*Transaction)

func NewTransactionManager(lock_manager *LockManager, log_manager *recovery.LogManager) *TransactionManager {
	return &TransactionManager{0, lock_manager, log_manager, common.NewRWLatch()}
}

func (transaction_manager *TransactionManager) Begin(txn *Transaction) *Transaction {
	// Acquire the global transaction latch in shared mode.
	transaction_manager.global_txn_latch.RLock()
	var txn_ret *Transaction = txn

	if txn_ret == nil {
		//transaction_manager.next_txn_id += 1
		transaction_manager.next_txn_id.AtomicAdd(1)
		txn_ret = NewTransaction(transaction_manager.next_txn_id)
		// fmt.Printf("new transactin ID: %d\n", transaction_manager.next_txn_id)
	}

	if common.EnableLogging {
		log_record := recovery.NewLogRecordTxn(txn_ret.GetTransactionId(), txn_ret.GetPrevLSN(), recovery.BEGIN)
		lsn := transaction_manager.log_manager.AppendLogRecord(log_record)
		txn_ret.SetPrevLSN(lsn)
	}

	txn_map[txn_ret.GetTransactionId()] = txn_ret
	return txn_ret
}

func (transaction_manager *TransactionManager) Commit(txn *Transaction) {
	txn.SetState(COMMITTED)

	// Perform all deletes before we commit.
	write_set := txn.GetWriteSet()
	for len(write_set) != 0 {
		item := write_set[len(write_set)-1]
		table := item.table
		rid := item.rid
		if item.wtype == DELETE {
			// Note that this also releases the lock when holding the page latch.
			pageID := rid.GetPageId()
			tpage := CastPageAsTablePage(table.bpm.FetchPage(pageID))
			tpage.WLatch()
			tpage.ApplyDelete(&item.rid, txn, transaction_manager.log_manager)
			tpage.WUnlatch()
		}
		write_set = write_set[:len(write_set)-1]
	}
	txn.SetWriteSet(write_set)

	if common.EnableLogging {
		log_record := recovery.NewLogRecordTxn(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.COMMIT)
		lsn := transaction_manager.log_manager.AppendLogRecord(log_record)
		txn.SetPrevLSN(lsn)
		transaction_manager.log_manager.Flush()
	}

	// Release all the locks.
	transaction_manager.releaseLocks(txn)
	// Release the global transaction latch.
	transaction_manager.global_txn_latch.RUnlock()
}

func (transaction_manager *TransactionManager) Abort(txn *Transaction) {
	txn.SetState(ABORTED)

	// Rollback before releasing the access.
	write_set := txn.GetWriteSet()
	for len(write_set) != 0 {
		item := write_set[len(write_set)-1]
		table := item.table
		if item.wtype == DELETE {
			table.RollbackDelete(&item.rid, txn)
		} else if item.wtype == INSERT {
			rid := item.rid
			// Note that this also releases the lock when holding the page latch.
			pageID := rid.GetPageId()
			tpage := CastPageAsTablePage(table.bpm.FetchPage(pageID))
			tpage.WLatch()
			tpage.ApplyDelete(&item.rid, txn, transaction_manager.log_manager)
			tpage.WUnlatch()
		} else if item.wtype == UPDATE {
			table.UpdateTuple(item.tuple, item.rid, txn)
		}
		write_set = write_set[:len(write_set)-1]
	}
	txn.SetWriteSet(write_set)

	if common.EnableLogging {
		log_record := recovery.NewLogRecordTxn(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.ABORT)
		lsn := transaction_manager.log_manager.AppendLogRecord(log_record)
		txn.SetPrevLSN(lsn)
	}

	// Release all the locks.
	transaction_manager.releaseLocks(txn)
	// Release the global transaction latch.
	transaction_manager.global_txn_latch.RUnlock()
}

func (transaction_manager *TransactionManager) BlockAllTransactions() {
	transaction_manager.global_txn_latch.WLock()
}

func (transaction_manager *TransactionManager) ResumeTransactions() {
	transaction_manager.global_txn_latch.WUnlock()
}

func (transaction_manager *TransactionManager) releaseLocks(txn *Transaction) {
	var lock_set []page.RID
	lock_set = append(lock_set, txn.GetExclusiveLockSet()...)
	lock_set = append(lock_set, txn.GetSharedLockSet()...)
	transaction_manager.lock_manager.Unlock(txn, lock_set)
	// for _, locked_rid := range lock_set {
	// 	transaction_manager.lock_manager.Unlock(txn, &locked_rid)
	// }
}
