package access

import (
	"fmt"
	"sync"

	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

/** Two-Phase Locking mode. */
type TwoPLMode int32

const (
	REGULAR TwoPLMode = iota
	STRICT
)

/** Deadlock mode. */
type DeadlockMode int32

const (
	PREVENTION DeadlockMode = iota
	DETECTION
	SS2PLMode
)

type LockMode int32

const (
	SHARED LockMode = iota
	EXCLUSIVE
)

type LockRequest struct {
	txnID    types.TxnID
	lockMode LockMode
	granted   bool
}

func NewLockRequest(txnID types.TxnID, lockMode LockMode) *LockRequest {
	ret := new(LockRequest)
	ret.txnID = txnID
	ret.lockMode = lockMode
	ret.granted = false
	return ret
}

type LockRequestQueue struct {
	requestQueue []*LockRequest
	upgrading     bool
}

/**
 * LockManager handles transactions asking for locks on records.
 */
type LockManager struct {
	twoPLMode   TwoPLMode
	deadlockMode DeadlockMode

	mutex                  *sync.Mutex
	enableCycleDetection bool

	sharedLockTable    map[page.RID][]types.TxnID
	exclusiveLockTable map[page.RID]types.TxnID
}

/**
* Creates a new lock manager configured for the given type of 2-phase locking and deadlock policy.
* @param twoPLMode 2-phase locking mode
* @param deadlockMode deadlock policy
 */
func NewLockManager(twoPLMode TwoPLMode, deadlockMode DeadlockMode /*= DeadlockMode::PREVENTION*/) *LockManager {
	ret := new(LockManager)
	ret.twoPLMode = twoPLMode
	ret.deadlockMode = deadlockMode
	ret.mutex = new(sync.Mutex)
	ret.sharedLockTable = make(map[page.RID][]types.TxnID)
	ret.exclusiveLockTable = make(map[page.RID]types.TxnID)
	return ret
}

func (lockManager *LockManager) Detection() bool  { return lockManager.deadlockMode == DETECTION }
func (lockManager *LockManager) Prevention() bool { return lockManager.deadlockMode == PREVENTION }

/*
* [LOCK_NOTE]: For all locking functions, we:
* 1. return false if the transaction is aborted; and
* 2. block on wait, return true when the lock request is granted; and
* 3. it is undefined behavior to try locking an already locked RID in the same transaction, i.e. the transaction
*    is responsible for keeping track of its current locks.
 */

func removeRID(list []page.RID, rid page.RID) []page.RID {
	lst := append(make([]page.RID, 0), list...)
	for i, r := range list {
		if r == rid {
			lst = append(list[:i], list[i+1:]...)
			break
		}
	}
	return lst
}

func removeTxnID(list []types.TxnID, txnID types.TxnID) []types.TxnID {
	lst := append(make([]types.TxnID, 0), list...)
	for i, t := range list {
		if t == txnID {
			lst = append(list[:i], list[i+1:]...)
			break
		}
	}
	return lst
}

func isContainTxnID(list []types.TxnID, txnID types.TxnID) bool {
	for _, t := range list {
		if t == txnID {
			return true
		}
	}
	return false
}

/**
* Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
* @param txn the transaction requesting the shared lock
* @param rid1 the RID to be locked in shared mode
* @return true if the lock is granted, false otherwise
 */
func (lockManager *LockManager) LockShared(txn *Transaction, rid *page.RID) bool {
	//fmt.Printf("called LockShared, %v\n", rid1)
	lockManager.mutex.Lock()
	defer lockManager.mutex.Unlock()

	if txn.IsRecoveryPhase() {
		return true
	}

	slockSet := txn.GetSharedLockSet()
	if txnID, ok := lockManager.exclusiveLockTable[*rid]; ok {
		if txnID == txn.GetTransactionID() {
			return true
		} else {
			return false
		}
	} else {
		if arr, ok := lockManager.sharedLockTable[*rid]; ok {
			if isContainTxnID(arr, txn.GetTransactionID()) {
				return true
			} else {
				curArr := lockManager.sharedLockTable[*rid]
				curArr = append(curArr, txn.GetTransactionID())
				lockManager.sharedLockTable[*rid] = curArr
				slockSet = append(slockSet, *rid)
				txn.SetSharedLockSet(slockSet)
				return true
			}
		} else {
			newArr := make([]types.TxnID, 0)
			newArr = append(newArr, txn.GetTransactionID())
			lockManager.sharedLockTable[*rid] = newArr
			slockSet = append(slockSet, *rid)
			txn.SetSharedLockSet(slockSet)
			return true
		}
	}
}

/**
* Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
* @param txn the transaction requesting the exclusive lock
* @param rid1 the RID to be locked in exclusive mode
* @return true if the lock is granted, false otherwise
 */
func (lockManager *LockManager) LockExclusive(txn *Transaction, rid *page.RID) bool {
	//fmt.Printf("called LockExclusive, %v\n", rid1)
	lockManager.mutex.Lock()
	defer lockManager.mutex.Unlock()

	if txn.IsRecoveryPhase() {
		return true
	}

	exlockSet := txn.GetExclusiveLockSet()
	if txnID, ok := lockManager.exclusiveLockTable[*rid]; ok {
		if txnID == txn.GetTransactionID() {
			return true
		} else {
			return false
		}
	} else {
		if arr, ok_ := lockManager.sharedLockTable[*rid]; ok_ {
			if !(arr == nil || len(arr) == 0 || (len(arr) == 1 && arr[0] == txn.GetTransactionID())) {
				// not only this txn has shared lock
				return false
			}
		}

		lockManager.exclusiveLockTable[*rid] = txn.GetTransactionID()
		exlockSet = append(exlockSet, *rid)
		txn.SetExclusiveLockSet(exlockSet)
		return true
	}
}

/**
* Upgrade a lock from a shared lock to an exclusive access.
* @param txn the transaction requesting the lock upgrade
* @param rid1 the RID that should already be locked in shared mode by the requesting transaction
* @return true if the upgrade is successful, false otherwise
 */
func (lockManager *LockManager) LockUpgrade(txn *Transaction, rid *page.RID) bool {
	//fmt.Printf("called LockUpgrade %v\n", rid1)
	lockManager.mutex.Lock()
	defer lockManager.mutex.Unlock()

	if txn.IsRecoveryPhase() {
		return true
	}

	elockSet := txn.GetExclusiveLockSet()
	if txn.IsSharedLocked(rid) {
		if txnID, ok := lockManager.exclusiveLockTable[*rid]; ok {
			if txnID == txn.GetTransactionID() {
				return true
			} else {
				return false
			}
		} else {
			// always success
			txnIds, _ := lockManager.sharedLockTable[*rid]

			if len(txnIds) != 1 {
				// not only this txn
				return false
			} else {
				lockManager.exclusiveLockTable[*rid] = txn.GetTransactionID()
				elockSet = append(elockSet, *rid)
				txn.SetExclusiveLockSet(elockSet)
				return true
			}
		}
	} else {
		panic("LockUpgrade: RID is not locked in shared mode")
	}
}

/**
* Release the lock held by the access.
* @param txn the transaction releasing the lock, it should actually hold the lock
* @param rid1 the RID that is locked by the transaction
* @return true if the unlock is successful, false otherwise
 */
func (lockManager *LockManager) Unlock(txn *Transaction, ridList []page.RID) bool {
	lockManager.mutex.Lock()
	defer lockManager.mutex.Unlock()
	for _, lockedRID := range ridList {
		if _, ok := lockManager.exclusiveLockTable[lockedRID]; ok {
			if lockManager.exclusiveLockTable[lockedRID] == txn.GetTransactionID() {
				// fmt.Println("delete exclusiveLockTable entry")
				// fmt.Println(lockedRID)
				delete(lockManager.exclusiveLockTable, lockedRID)
			}
		}
		if arr, ok := lockManager.sharedLockTable[lockedRID]; ok {
			if isContainTxnID(arr, txn.GetTransactionID()) {
				// fmt.Println("remove txnid from sharedLockTable entry")
				// fmt.Println(txn.GetTransactionID())
				lockManager.sharedLockTable[lockedRID] = removeTxnID(arr, txn.GetTransactionID())
			}
		}
	}

	return true
}

func (lockManager *LockManager) PrintLockTables() {
	fmt.Printf("len of sharedLockTable at WUnlock %d\n", len(lockManager.sharedLockTable))
	fmt.Printf("len of exclusiveLockTable at WUnlock %d\n", len(lockManager.exclusiveLockTable))
	for k, v := range lockManager.sharedLockTable {
		fmt.Printf("%v: %v\n", k, v)
	}
	for k, v := range lockManager.exclusiveLockTable {
		fmt.Printf("%v: %v\n", k, v)
	}
}

func (lockManager *LockManager) ClearLockTablesForDebug() {
	lockManager.sharedLockTable = make(map[page.RID][]types.TxnID, 0)
	lockManager.exclusiveLockTable = make(map[page.RID]types.TxnID, 0)
}
