package access

import (
	"fmt"
	"sync"

	"github.com/ryogrid/SamehadaDB/lib/catalog/catalog_interface"
	"github.com/ryogrid/SamehadaDB/lib/storage/index"

	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

/**
 * TransactionManager keeps track of all the transactions running in the system.
 */
type TransactionManager struct {
	nextTxnID  types.TxnID
	lockManager *LockManager
	logManager  *recovery.LogManager
	/** The global transaction latch is used for checkpointing. */
	globalTxnLatch common.ReaderWriterLatch
	mutex            *sync.Mutex
}

// var txnMap = make(map[types.TxnID]*Transaction)
var txnMap = sync.Map{}

func NewTransactionManager(lockManager *LockManager, logManager *recovery.LogManager) *TransactionManager {
	return &TransactionManager{0, lockManager, logManager, common.NewRWLatch(), new(sync.Mutex)}
}

func (transactionManager *TransactionManager) Begin(txn *Transaction) *Transaction {
	// Acquire the global transaction latch in shared mode.
	transactionManager.globalTxnLatch.RLock()
	var txnRet = txn

	if txnRet == nil {
		transactionManager.mutex.Lock()
		transactionManager.nextTxnID += 1
		txnRet = NewTransaction(transactionManager.nextTxnID)
		transactionManager.mutex.Unlock()
		//fmt.Printf("new transactin GetPageID: %d\n", transactionManager.nextTxnID)
	}

	if transactionManager.logManager.IsEnabledLogging() {
		logRecord := recovery.NewLogRecordTxn(txnRet.GetTransactionID(), txnRet.GetPrevLSN(), recovery.BEGIN)
		lsn := transactionManager.logManager.AppendLogRecord(logRecord)
		txnRet.SetPrevLSN(lsn)
	}

	//transactionManager.mutex.Lock()
	//txnMap[txnRet.GetTransactionID()] = txnRet
	//transactionManager.mutex.Unlock()
	txnMap.Store(txnRet.GetTransactionID(), txnRet)
	return txnRet
}

func (transactionManager *TransactionManager) Commit(cat catalog_interface.CatalogInterface, txn *Transaction) {
	if common.EnableDebug {
		common.ShPrintf(common.RDBOpFuncCall, "TransactionManager::Commit called. txn.txnID:%v dbgInfo:%s\n", txn.txnID, txn.dbgInfo)
	}
	// on Commit, call of Transaction::SetState(ABORT) panics
	txn.MakeNotAbortable()

	// Perform all deletes before we commit.
	writeSet := txn.GetWriteSet()
	isReadOnlyTxn := len(writeSet) == 0
	if common.EnableDebug {
		writeSetStr := ""
		for _, writeItem := range writeSet {
			//common.ShPrintf(common.RDBOpFuncCall, "%v ", *writeItem)
			writeSetStr += fmt.Sprintf("%v ", *writeItem)
		}
		common.ShPrintf(common.RDBOpFuncCall, "TransactionManager::Commit txn.txnID:%v dbgInfo:%s writeSet:%s\n", txn.txnID, txn.dbgInfo, writeSetStr)
	}
	for len(writeSet) != 0 {
		item := writeSet[len(writeSet)-1]
		table := item.table
		rid := item.rid1
		if item.wtype == DELETE {
			if common.EnableDebug && common.ActiveLogKindSetting&common.CommitAbortHandleInfo > 0 {
				fmt.Printf("TransactionManager::Commit handle DELETE write log. txn.txnID:%v dbgInfo:%s rid1:%v\n", txn.txnID, txn.dbgInfo, rid)
			}
			pageID := rid.GetPageID()
			tpage := CastPageAsTablePage(table.bpm.FetchPage(pageID))
			tpage.WLatch()
			tpage.AddWLatchRecord(int32(txn.txnID))
			tpage.ApplyDelete(item.rid1, txn, transactionManager.logManager)
			table.bpm.UnpinPage(tpage.GetPageID(), true)
			tpage.RemoveWLatchRecord(int32(txn.txnID))
			tpage.WUnlatch()
		} else if item.wtype == UPDATE {
			if common.EnableDebug && common.ActiveLogKindSetting&common.CommitAbortHandleInfo > 0 {
				fmt.Printf("TransactionManager::Commit handle UPDATE write log. txn.txnID:%v dbgInfo:%s rid1:%v\n", txn.txnID, txn.dbgInfo, rid)
			}

			if *item.rid1 != *item.rid2 {
				// tuple location change occured case
				pageID := item.rid1.GetPageID()
				tpage := CastPageAsTablePage(table.bpm.FetchPage(pageID))
				tpage.WLatch()
				tpage.AddWLatchRecord(int32(txn.txnID))
				tpage.ApplyDelete(item.rid1, txn, transactionManager.logManager)
				table.bpm.UnpinPage(tpage.GetPageID(), true)
				tpage.RemoveWLatchRecord(int32(txn.txnID))
				tpage.WUnlatch()
			}
		}
		writeSet = writeSet[:len(writeSet)-1]
	}
	txn.SetWriteSet(writeSet)

	if transactionManager.logManager.IsEnabledLogging() {
		logRecord := recovery.NewLogRecordTxn(txn.GetTransactionID(), txn.GetPrevLSN(), recovery.COMMIT)
		lsn := transactionManager.logManager.AppendLogRecord(logRecord)
		txn.SetPrevLSN(lsn)
		if !isReadOnlyTxn {
			transactionManager.logManager.Flush()
		}
	}

	// Release all the locks.
	transactionManager.mutex.Lock()
	transactionManager.releaseLocks(txn)
	transactionManager.mutex.Unlock()
	// Release the global transaction latch.
	transactionManager.globalTxnLatch.RUnlock()
}

func (transactionManager *TransactionManager) Abort(cat catalog_interface.CatalogInterface, txn *Transaction) {
	if common.EnableDebug {
		common.ShPrintf(common.RDBOpFuncCall, "TransactionManager::Abort called. txn.txnID:%v dbgInfo:%s\n", txn.txnID, txn.dbgInfo)
	}

	//fmt.Printf("debuginfo: %s\n", txn.dbgInfo)
	//for _, wr := range txn.GetWriteSet() {
	//	fmt.Printf("write set item: %v\n", *wr)
	//	if wr.tuple1 != nil {
	//		fmt.Printf("tuple1: %v\n", *(wr.tuple1))
	//	}
	//	if wr.tuple2 != nil {
	//		fmt.Printf("tuple1: %v\n", *(wr.tuple2))
	//	}
	//}

	// on Abort, call of Transaction::SetState(ABORT) panics
	txn.MakeNotAbortable()

	indexMap := make(map[uint32][]index.Index, 0)

	writeSet := txn.GetWriteSet()
	if common.EnableDebug && common.ActiveLogKindSetting&common.RDBOpFuncCall > 0 {
		writeSetStr := ""
		for _, writeItem := range writeSet {
			writeSetStr += fmt.Sprintf("%v ", *writeItem)
		}
		fmt.Printf("TransactionManager::Abort txn.txnID:%v  dbgInfo:%s writeSet: %s\n", txn.txnID, txn.dbgInfo, writeSetStr)
	}

	// Rollback before releasing the access.
	for len(writeSet) != 0 {
		item := writeSet[len(writeSet)-1]
		table := item.table
		if item.wtype == DELETE {
			if common.EnableDebug && common.ActiveLogKindSetting&common.CommitAbortHandleInfo > 0 {
				fmt.Printf("TransactionManager::Abort handle DELETE write log. txn.txnID:%v dbgInfo:%s rid1:%v\n", txn.txnID, txn.dbgInfo, item.rid1)
			}

			// rollback record data
			table.RollbackDelete(item.rid1, txn)

			// rollback index data
			indexes := cat.GetRollbackNeededIndexes(indexMap, item.oid)
			for _, idx := range indexes {
				if idx != nil {
					idx.InsertEntry(item.tuple1, *item.rid1, txn)
				}
			}
		} else if item.wtype == INSERT {
			if common.EnableDebug && common.ActiveLogKindSetting&common.CommitAbortHandleInfo > 0 {
				fmt.Printf("TransactionManager::Abort handle INSERT write log. txn.txnID:%v dbgInfo:%s rid1:%v\n", txn.txnID, txn.dbgInfo, item.rid1)
			}

			// rollback record data
			rid := item.rid1
			pageID := rid.GetPageID()
			tpage := CastPageAsTablePage(table.bpm.FetchPage(pageID))
			tpage.WLatch()
			tpage.ApplyDelete(item.rid1, txn, transactionManager.logManager)
			table.bpm.UnpinPage(pageID, true)
			tpage.WUnlatch()

			// rollback index data
			if cat != nil {
				indexes := cat.GetRollbackNeededIndexes(indexMap, item.oid)
				for _, idx := range indexes {
					if idx != nil {
						idx.DeleteEntry(item.tuple1, *item.rid1, txn)
					}
				}
			}
		} else if item.wtype == UPDATE {
			if common.EnableDebug && common.ActiveLogKindSetting&common.CommitAbortHandleInfo > 0 {
				fmt.Printf("TransactionManager::Abort handle UPDATE write log. txn.txnID:%v dbgInfo:%s rid1:%v tuple1.Size()=%d \n", txn.txnID, txn.dbgInfo, item.rid1, item.tuple1.Size())
			}

			// rollback record data
			if *item.rid1 != *item.rid2 {
				// tuple location change occured case

				// rollback inserted record data
				pageID := item.rid2.GetPageID()
				tpage := CastPageAsTablePage(table.bpm.FetchPage(pageID))
				tpage.WLatch()
				tpage.ApplyDelete(item.rid2, txn, transactionManager.logManager)
				table.bpm.UnpinPage(pageID, true)
				tpage.WUnlatch()

				// rollback deleted record data
				table.RollbackDelete(item.rid1, txn)
			} else {
				// normal case

				rid := item.rid1
				// Note that this also releases the lock when holding the page latch.
				pageID := rid.GetPageID()
				tpage := CastPageAsTablePage(table.bpm.FetchPage(pageID))
				tpage.WLatch()
				isUpdated, err, _ := tpage.UpdateTuple(item.tuple1, nil, nil, item.tuple2, rid, txn, transactionManager.lockManager, transactionManager.logManager, true)
				if !isUpdated || err != nil {
					panic("rollback of normal UPDATE failed")
				}
				table.bpm.UnpinPage(pageID, true)
				tpage.WUnlatch()
			}

			// rollback index data
			if cat != nil {
				indexes := cat.GetRollbackNeededIndexes(indexMap, item.oid)

				//fmt.Printf("TransactionManager::Abort  rollback of Update! txn.txnID:%d, tpl.Size():%d err:%v indexes:%v\n", txn.txnID, tpl.Size(), err, indexes)
				for _, idx := range indexes {
					if idx != nil {
						colIdx := idx.GetKeyAttrs()[0]
						bfRlbkKeyVal := cat.GetColValFromTupleForRollback(item.tuple2, colIdx, item.oid)
						rlbkKeyVal := cat.GetColValFromTupleForRollback(item.tuple1, colIdx, item.oid)
						if !bfRlbkKeyVal.CompareEquals(*rlbkKeyVal) || *item.rid1 != *item.rid2 {
							idx.UpdateEntry(item.tuple2, *item.rid2, item.tuple1, *item.rid1, txn)
						}
					}
				}
			}
		}
		writeSet = writeSet[:len(writeSet)-1]
	}
	txn.SetWriteSet(writeSet)

	if transactionManager.logManager.IsEnabledLogging() {
		logRecord := recovery.NewLogRecordTxn(txn.GetTransactionID(), txn.GetPrevLSN(), recovery.ABORT)
		lsn := transactionManager.logManager.AppendLogRecord(logRecord)
		txn.SetPrevLSN(lsn)
	}

	// Release all the locks.
	transactionManager.mutex.Lock()
	transactionManager.releaseLocks(txn)
	transactionManager.mutex.Unlock()
	// Release the global transaction latch.
	transactionManager.globalTxnLatch.RUnlock()
}

func (transactionManager *TransactionManager) BlockAllTransactions() {
	transactionManager.globalTxnLatch.WLock()
}

func (transactionManager *TransactionManager) ResumeTransactions() {
	transactionManager.globalTxnLatch.WUnlock()
}

func (transactionManager *TransactionManager) releaseLocks(txn *Transaction) {
	var lockSet = make([]page.RID, 0)
	lockSet = append(lockSet, txn.GetExclusiveLockSet()...)
	lockSet = append(lockSet, txn.GetSharedLockSet()...)
	transactionManager.lockManager.Unlock(txn, lockSet)
}
