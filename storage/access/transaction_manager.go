package access

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog/catalog_interface"
	"github.com/ryogrid/SamehadaDB/storage/index"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"sync"

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
	mutex            *sync.Mutex
}

var txn_map map[types.TxnID]*Transaction = make(map[types.TxnID]*Transaction)

func NewTransactionManager(lock_manager *LockManager, log_manager *recovery.LogManager) *TransactionManager {
	return &TransactionManager{0, lock_manager, log_manager, common.NewRWLatch(), new(sync.Mutex)}
}

func (transaction_manager *TransactionManager) Begin(txn *Transaction) *Transaction {
	// Acquire the global transaction latch in shared mode.
	transaction_manager.global_txn_latch.RLock()
	var txn_ret *Transaction = txn

	if txn_ret == nil {
		transaction_manager.mutex.Lock()
		transaction_manager.next_txn_id += 1
		//transaction_manager.next_txn_id.AtomicAdd(1)
		txn_ret = NewTransaction(transaction_manager.next_txn_id)
		transaction_manager.mutex.Unlock()
		//fmt.Printf("new transactin GetPageId: %d\n", transaction_manager.next_txn_id)
	}

	if transaction_manager.log_manager.IsEnabledLogging() {
		log_record := recovery.NewLogRecordTxn(txn_ret.GetTransactionId(), txn_ret.GetPrevLSN(), recovery.BEGIN)
		lsn := transaction_manager.log_manager.AppendLogRecord(log_record)
		txn_ret.SetPrevLSN(lsn)
	}

	transaction_manager.mutex.Lock()
	txn_map[txn_ret.GetTransactionId()] = txn_ret
	transaction_manager.mutex.Unlock()
	return txn_ret
}

func (transaction_manager *TransactionManager) Commit(catalog_ catalog_interface.CatalogInterface, txn *Transaction) {
	if common.EnableDebug {
		common.ShPrintf(common.RDB_OP_FUNC_CALL, "TransactionManager::Commit called. txn.txn_id:%v dbgInfo:%s\n", txn.txn_id, txn.dbgInfo)
	}
	// on Commit, call of Transaction::SetState(ABORT) panics
	txn.MakeNotAbortable()
	//txn.SetState(COMMITTED)

	//indexMap := make(map[uint32][]index.Index, 0)
	// Perform all deletes before we commit.
	write_set := txn.GetWriteSet()
	if common.EnableDebug {
		writeSetStr := ""
		for _, writeItem := range write_set {
			//common.ShPrintf(common.RDB_OP_FUNC_CALL, "%v ", *writeItem)
			writeSetStr += fmt.Sprintf("%v ", *writeItem)
		}
		common.ShPrintf(common.RDB_OP_FUNC_CALL, "TransactionManager::Commit txn.txn_id:%v dbgInfo:%s write_set:%s\n", txn.txn_id, txn.dbgInfo, writeSetStr)
		//common.ShPrintf(common.RDB_OP_FUNC_CALL, "\n")
	}
	for len(write_set) != 0 {
		item := write_set[len(write_set)-1]
		table := item.table
		rid := item.rid
		if item.wtype == DELETE {
			// Note that this also releases the lock when holding the page latch.

			if common.EnableDebug && common.ActiveLogKindSetting&common.COMMIT_ABORT_HANDLE_INFO > 0 {
				fmt.Printf("TransactionManager::Commit handle DELETE write log. txn.txn_id:%v dbgInfo:%s rid:%v\n", txn.txn_id, txn.dbgInfo, rid)
			}
			pageID := rid.GetPageId()
			tpage := CastPageAsTablePage(table.bpm.FetchPage(pageID))
			tpage.WLatch()
			tpage.AddWLatchRecord(int32(txn.txn_id))
			tpage.ApplyDelete(&item.rid, txn, transaction_manager.log_manager)
			table.bpm.UnpinPage(tpage.GetPageId(), true)
			tpage.RemoveWLatchRecord(int32(txn.txn_id))
			tpage.WUnlatch()
			//if catalog_ != nil {
			//	indexes := catalog_.GetRollbackNeededIndexes(indexMap, item.oid)
			//	for _, index_ := range indexes {
			//		if index_ != nil {
			//			index_.DeleteEntry(item.tuple, item.rid, txn)
			//		}
			//	}
			//}
		}
		write_set = write_set[:len(write_set)-1]
	}
	txn.SetWriteSet(write_set)

	if transaction_manager.log_manager.IsEnabledLogging() {
		log_record := recovery.NewLogRecordTxn(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.COMMIT)
		lsn := transaction_manager.log_manager.AppendLogRecord(log_record)
		txn.SetPrevLSN(lsn)
		transaction_manager.log_manager.Flush()
	}

	// Release all the locks.
	transaction_manager.mutex.Lock()
	transaction_manager.releaseLocks(txn)
	transaction_manager.mutex.Unlock()
	// Release the global transaction latch.
	transaction_manager.global_txn_latch.RUnlock()
}

func (transaction_manager *TransactionManager) Abort(catalog_ catalog_interface.CatalogInterface, txn *Transaction) {
	if common.EnableDebug {
		common.ShPrintf(common.RDB_OP_FUNC_CALL, "TransactionManager::Abort called. txn.txn_id:%v dbgInfo:%s\n", txn.txn_id, txn.dbgInfo)
	}
	//txn.SetState(ABORTED)
	// on Abort, call of Transaction::SetState(ABORT) panics
	txn.MakeNotAbortable()

	indexMap := make(map[uint32][]index.Index, 0)
	write_set := txn.GetWriteSet()

	if common.EnableDebug && common.ActiveLogKindSetting&common.RDB_OP_FUNC_CALL > 0 {
		writeSetStr := ""
		for _, writeItem := range write_set {
			//common.ShPrintf(common.RDB_OP_FUNC_CALL, "%v ", *writeItem)
			writeSetStr += fmt.Sprintf("%v ", *writeItem)
		}
		fmt.Printf("TransactionManager::Abort txn.txn_id:%v  dbgInfo:%s write_set: %s\n", txn.txn_id, txn.dbgInfo, writeSetStr)
		//common.ShPrintf(common.RDB_OP_FUNC_CALL, "\n")
	}
	// Rollback before releasing the access.
	for len(write_set) != 0 {
		item := write_set[len(write_set)-1]
		table := item.table
		if item.wtype == DELETE {
			if common.EnableDebug && common.ActiveLogKindSetting&common.COMMIT_ABORT_HANDLE_INFO > 0 {
				fmt.Printf("TransactionManager::Abort handle DELETE write log. txn.txn_id:%v dbgInfo:%s rid:%v\n", txn.txn_id, txn.dbgInfo, item.rid)
			}

			// rollback record data
			table.RollbackDelete(&item.rid, txn)

			////rollback of index entry is not needed because entry is deleted at commit

			// rollback index data
			indexes := catalog_.GetRollbackNeededIndexes(indexMap, item.oid)
			for _, index_ := range indexes {
				if index_ != nil {
					index_.InsertEntry(item.tuple, item.rid, txn)
				}
			}
		} else if item.wtype == INSERT {
			if common.EnableDebug && common.ActiveLogKindSetting&common.COMMIT_ABORT_HANDLE_INFO > 0 {
				fmt.Printf("TransactionManager::Abort handle INSERT write log. txn.txn_id:%v dbgInfo:%s rid:%v\n", txn.txn_id, txn.dbgInfo, item.rid)
			}
			//insertedTuple, _ := item.table.GetTuple(&item.rid, txn)
			// rollback record data
			rid := item.rid
			// Note that this also releases the lock when holding the page latch.
			pageID := rid.GetPageId()
			tpage := CastPageAsTablePage(table.bpm.FetchPage(pageID))
			tpage.WLatch()
			tpage.AddWLatchRecord(int32(txn.txn_id))
			tpage.ApplyDelete(&item.rid, txn, transaction_manager.log_manager)
			table.bpm.UnpinPage(pageID, false)
			tpage.RemoveWLatchRecord(int32(txn.txn_id))
			tpage.WUnlatch()
			// rollback index data
			if catalog_ != nil {
				indexes := catalog_.GetRollbackNeededIndexes(indexMap, item.oid)
				for _, index_ := range indexes {
					if index_ != nil {
						//index_.DeleteEntry(insertedTuple, item.rid, txn)
						index_.DeleteEntry(item.tuple, item.rid, txn)
					}
				}
			}
		} else if item.wtype == UPDATE {
			if common.EnableDebug && common.ActiveLogKindSetting&common.COMMIT_ABORT_HANDLE_INFO > 0 {
				fmt.Printf("TransactionManager::Abort handle UPDATE write log. txn.txn_id:%v dbgInfo:%s rid:%v tuple=%v \n", txn.txn_id, txn.dbgInfo, item.rid, item.tuple)
			}
			beforRollbackTuple_, _ := item.table.GetTuple(&item.rid, txn)
			// rollback record data
			is_updated, new_rid, _, _ := table.UpdateTuple(item.tuple, nil, nil, item.oid, item.rid, txn, true)
			if !is_updated {
				panic("UpdateTuple at rollback failed!")
				//// TODO: (SDB) temporal impl for special case of UpdateTuple (Abort)
				//fmt.Println("TransactionManager::Abort make rate handling of current handling WriteRecord(Update)")
				//tmpList := make([]*WriteRecord, 0)
				//tmpList = append(tmpList, item)
				//write_set = append(tmpList, write_set[:len(write_set)-1]...)
				//continue
			}
			// rollback index data
			// when update is operated as delete and insert (rid change case),
			//  rollback is done for each separated operation
			if catalog_ != nil {
				indexes := catalog_.GetRollbackNeededIndexes(indexMap, item.oid)
				var tuple_ *tuple.Tuple
				var err error
				if new_rid != nil {
					tuple_, err = item.table.GetTuple(new_rid, txn)
				} else {
					tuple_, err = item.table.GetTuple(&item.rid, txn)
				}

				fmt.Printf("TransactionManager::Abort  rollback of Update! txn.txn_id:%d, tuple_:%v err:%v indexes:%v\n", txn.txn_id, tuple_, err, indexes)
				for _, index_ := range indexes {
					if index_ != nil {
						colIdx := index_.GetKeyAttrs()[0]
						bfRlbkKeyVal := catalog_.GetColValFromTupleForRollback(beforRollbackTuple_, colIdx, item.oid)
						rlbkKeyVal := catalog_.GetColValFromTupleForRollback(tuple_, colIdx, item.oid)
						if !bfRlbkKeyVal.CompareEquals(*rlbkKeyVal) {
							// rollback is needed only when column value changed case
							index_.DeleteEntry(beforRollbackTuple_, item.rid, txn)
							if new_rid != nil {
								index_.InsertEntry(tuple_, *new_rid, txn)
							} else {
								index_.InsertEntry(tuple_, item.rid, txn)
							}
						}
					}
				}
			}
		}
		write_set = write_set[:len(write_set)-1]
	}
	txn.SetWriteSet(write_set)

	if transaction_manager.log_manager.IsEnabledLogging() {
		log_record := recovery.NewLogRecordTxn(txn.GetTransactionId(), txn.GetPrevLSN(), recovery.ABORT)
		lsn := transaction_manager.log_manager.AppendLogRecord(log_record)
		txn.SetPrevLSN(lsn)
	}

	// Release all the locks.
	transaction_manager.mutex.Lock()
	transaction_manager.releaseLocks(txn)
	transaction_manager.mutex.Unlock()
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
	var lock_set []page.RID = make([]page.RID, 0)
	lock_set = append(lock_set, txn.GetExclusiveLockSet()...)
	lock_set = append(lock_set, txn.GetSharedLockSet()...)
	transaction_manager.lock_manager.Unlock(txn, lock_set)
	// for _, locked_rid := range lock_set {
	// 	transaction_manager.lock_manager.WUnlock(txn, &locked_rid)
	// }
}
