package access

import (
	"fmt"
	"sync"

	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
)

//class TransactionManager;

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
	SS2PL_MODE
)

type LockMode int32

const (
	SHARED LockMode = iota
	EXCLUSIVE
)

type LockRequest struct {
	txn_id    types.TxnID
	lock_mode LockMode
	granted   bool
}

func NewLockRequest(txn_id types.TxnID, lock_mode LockMode) *LockRequest {
	ret := new(LockRequest)
	ret.txn_id = txn_id
	ret.lock_mode = lock_mode
	ret.granted = false
	return ret
}

type LockRequestQueue struct {
	request_queue []*LockRequest
	//std::condition_variable cv  // for notifying blocked transactions on this rid
	upgrading bool
}

/**
 * LockManager handles transactions asking for locks on records.
 */
type LockManager struct {
	two_pl_mode   TwoPLMode //__attribute__((__unused__));
	deadlock_mode DeadlockMode

	mutex                  *sync.Mutex
	enable_cycle_detection bool
	//cycle_detection_thread *std::thread

	// /** WLock table for lock requests. */
	// lock_table map[page.RID]*LockRequestQueue
	// /** Waits-for graph representation. */
	// waits_for map[types.TxnID][]types.TxnID

	shared_lock_table    map[page.RID][]types.TxnID
	exclusive_lock_table map[page.RID]types.TxnID
}

/**
* Creates a new lock manager configured for the given type of 2-phase locking and deadlock policy.
* @param two_pl_mode 2-phase locking mode
* @param deadlock_mode deadlock policy
 */
func NewLockManager(two_pl_mode TwoPLMode, deadlock_mode DeadlockMode /*= DeadlockMode::PREVENTION*/) *LockManager {
	ret := new(LockManager)
	ret.two_pl_mode = two_pl_mode
	ret.deadlock_mode = deadlock_mode
	ret.mutex = new(sync.Mutex)
	ret.shared_lock_table = make(map[page.RID][]types.TxnID)
	ret.exclusive_lock_table = make(map[page.RID]types.TxnID)
	// // If Detection() is enabled, we should launch a background cycle detection thread.
	// if ret.Detection() {
	// 	ret.enable_cycle_detection = true
	// 	//ret.cycle_detection_thread = new std::thread(&LockManager::RunCycleDetection, this)
	// 	//LOG_INFO("Cycle detection thread launched")
	// }
	return ret
}

// ~LockManager() {
//  if (Detection()) {
//    enable_cycle_detection = false
//    cycle_detection_thread.join()
//    delete cycle_detection_thread
//    LOG_INFO("Cycle detection thread stopped")
//  }
// }

func (lock_manager *LockManager) Detection() bool  { return lock_manager.deadlock_mode == DETECTION }
func (lock_manager *LockManager) Prevention() bool { return lock_manager.deadlock_mode == PREVENTION }

/*
* [LOCK_NOTE]: For all locking functions, we:
* 1. return false if the transaction is aborted; and
* 2. block on wait, return true when the lock request is granted; and
* 3. it is undefined behavior to try locking an already locked RID in the same transaction, i.e. the transaction
*    is responsible for keeping track of its current locks.
 */

func removeRID(list []page.RID, rid page.RID) []page.RID {
	list_ := append(make([]page.RID, 0), list...)
	for i, r := range list {
		if r == rid {
			list_ = append(list[:i], list[i+1:]...)
			break
		}
	}
	return list_
}

func removeTxnID(list []types.TxnID, txnID types.TxnID) []types.TxnID {
	list_ := append(make([]types.TxnID, 0), list...)
	for i, t := range list {
		if t == txnID {
			list_ = append(list[:i], list[i+1:]...)
			break
		}
	}
	return list_
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
* @param rid the RID to be locked in shared mode
* @return true if the lock is granted, false otherwise
 */
func (lock_manager *LockManager) LockShared(txn *Transaction, rid *page.RID) bool {
	//fmt.Printf("called LockShared, %v\n", rid)
	lock_manager.mutex.Lock()
	defer lock_manager.mutex.Unlock()
	slock_set := txn.GetSharedLockSet()
	if txnID, ok := lock_manager.exclusive_lock_table[*rid]; ok {
		if txnID == txn.GetTransactionId() {
			return true
		} else {
			return false
		}
	} else {
		if arr, ok := lock_manager.shared_lock_table[*rid]; ok {
			if isContainTxnID(arr, txn.GetTransactionId()) {
				return true
			} else {
				cur_arr := lock_manager.shared_lock_table[*rid]
				cur_arr = append(cur_arr, txn.GetTransactionId())
				lock_manager.shared_lock_table[*rid] = cur_arr
				slock_set = append(slock_set, *rid)
				txn.SetSharedLockSet(slock_set)
				return true
			}
		} else {
			new_arr := make([]types.TxnID, 0)
			new_arr = append(new_arr, txn.GetTransactionId())
			lock_manager.shared_lock_table[*rid] = new_arr
			slock_set = append(slock_set, *rid)
			txn.SetSharedLockSet(slock_set)
			return true
		}
	}
}

/**
* Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
* @param txn the transaction requesting the exclusive lock
* @param rid the RID to be locked in exclusive mode
* @return true if the lock is granted, false otherwise
 */
func (lock_manager *LockManager) LockExclusive(txn *Transaction, rid *page.RID) bool {
	//fmt.Printf("called LockExclusive, %v\n", rid)
	lock_manager.mutex.Lock()
	defer lock_manager.mutex.Unlock()
	exlock_set := txn.GetExclusiveLockSet()
	if txnID, ok := lock_manager.exclusive_lock_table[*rid]; ok {
		if txnID == txn.GetTransactionId() {
			return true
		} else {
			return false
		}
	} else {
		if arr, ok_ := lock_manager.shared_lock_table[*rid]; ok_ {
			if !(arr == nil || len(arr) == 0 || (len(arr) == 1 && arr[0] == txn.GetTransactionId())) {
				// not only this txn has shared lock
				return false
			}
		}

		lock_manager.exclusive_lock_table[*rid] = txn.GetTransactionId()
		exlock_set = append(exlock_set, *rid)
		txn.SetExclusiveLockSet(exlock_set)
		return true
	}
}

/**
* Upgrade a lock from a shared lock to an exclusive access.
* @param txn the transaction requesting the lock upgrade
* @param rid the RID that should already be locked in shared mode by the requesting transaction
* @return true if the upgrade is successful, false otherwise
 */
func (lock_manager *LockManager) LockUpgrade(txn *Transaction, rid *page.RID) bool {
	//fmt.Printf("called LockUpgrade %v\n", rid)
	lock_manager.mutex.Lock()
	defer lock_manager.mutex.Unlock()
	//slock_set := txn.GetSharedLockSet()
	elock_set := txn.GetExclusiveLockSet()
	if txn.IsSharedLocked(rid) {
		if txnID, ok := lock_manager.exclusive_lock_table[*rid]; ok {
			if txnID == txn.GetTransactionId() {
				return true
			} else {
				return false
			}
		} else {
			// always success
			txnIds, _ := lock_manager.shared_lock_table[*rid]

			if len(txnIds) != 1 {
				// not only this txn
				return false
			} else {
				lock_manager.exclusive_lock_table[*rid] = txn.GetTransactionId()
				elock_set = append(elock_set, *rid)
				txn.SetExclusiveLockSet(elock_set)
				//slock_set = removeRID(slock_set, *rid)
				//txn.SetSharedLockSet(slock_set)
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
* @param rid the RID that is locked by the transaction
* @return true if the unlock is successful, false otherwise
 */
func (lock_manager *LockManager) Unlock(txn *Transaction, rid_list []page.RID) bool {
	lock_manager.mutex.Lock()
	defer lock_manager.mutex.Unlock()
	for _, locked_rid := range rid_list {
		// slock_set := txn.GetSharedLockSet()
		// slock_set = removeRID(slock_set, locked_rid)
		// elock_set := txn.GetExclusiveLockSet()
		// elock_set = removeRID(elock_set, locked_rid)

		if _, ok := lock_manager.exclusive_lock_table[locked_rid]; ok {
			if lock_manager.exclusive_lock_table[locked_rid] == txn.GetTransactionId() {
				// fmt.Println("delete exclusive_lock_table entry")
				// fmt.Println(locked_rid)
				delete(lock_manager.exclusive_lock_table, locked_rid)
			}
		}
		if arr, ok := lock_manager.shared_lock_table[locked_rid]; ok {
			if isContainTxnID(arr, txn.GetTransactionId()) {
				// fmt.Println("remove txnid from shared_lock_table entry")
				// fmt.Println(txn.GetTransactionId())
				lock_manager.shared_lock_table[locked_rid] = removeTxnID(arr, txn.GetTransactionId())
			}
		}
	}
	// txn.SetSharedLockSet(slock_set)
	// txn.SetExclusiveLockSet(elock_set)

	return true
}

func (lock_manager *LockManager) PrintLockTables() {
	fmt.Printf("len of shared_lock_table at WUnlock %d\n", len(lock_manager.shared_lock_table))
	fmt.Printf("len of exclusive_lock_table at WUnlock %d\n", len(lock_manager.exclusive_lock_table))
	for k, v := range lock_manager.shared_lock_table {
		fmt.Printf("%v: %v\n", k, v)
	}
	for k, v := range lock_manager.exclusive_lock_table {
		fmt.Printf("%v: %v\n", k, v)
	}
}

func (lock_manager *LockManager) ClearLockTablesForDebug() {
	lock_manager.shared_lock_table = make(map[page.RID][]types.TxnID, 0)
	lock_manager.exclusive_lock_table = make(map[page.RID]types.TxnID, 0)
}

/*** Graph API ***/
/**
* Adds edge t1->t2
 */

/** Adds an edge from t1 -> t2. */
func (lock_manager *LockManager) AddEdge(t1 types.TxnID, t2 types.TxnID) { /* assert(Detection()) */ }

/** Removes an edge from t1 -> t2. */
func (lock_manager *LockManager) RemoveEdge(t1 types.TxnID, t2 types.TxnID) { /* assert(Detection()) */
}

/**
* Checks if the graph has a cycle, returning the newest transaction GetPageId in the cycle if so.
* @param[out] txn_id if the graph has a cycle, will contain the newest transaction GetPageId
* @return false if the graph has no cycle, otherwise stores the newest transaction GetPageId in the cycle to txn_id
 */
func (lock_manager *LockManager) HasCycle(txn_id *types.TxnID) bool {
	/*
	   BUSTUB_ASSERT(Detection(), "Detection should be enabled!")
	*/
	return false
}

// /** @return the set of all edges in the graph, used for testing only! */
// func (lock_manager *LockManager) GetEdgeList() std::vector<std::pair<txn_id_t, txn_id_t>> {
// /*
//   BUSTUB_ASSERT(Detection(), "Detection should be enabled!")
// */
//   return {}
// }

/** Runs cycle detection in the background. */
func (lock_manager *LockManager) RunCycleDetection() {
	/*
	   	BUSTUB_ASSERT(Detection(), "Detection should be enabled!")
	     while (enable_cycle_detection) {
	       std::this_thread::sleep_for(cycle_detection_interval)
	       {
	         std::unique_lock<std::mutex> l(latch)
	         continue
	       }
	     }
	*/
}
