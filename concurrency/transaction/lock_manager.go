//package concurrency
//package lock
package transaction

// TODO: need impl
//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

import (
	"github.com/ryogrid/SamehadaDB/common"
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
	// TODO: not ported yet
	//std::condition_variable cv  // for notifying blocked transactions on this rid
	upgrading bool
}

/**
 * LockManager handles transactions asking for locks on records.
 */
type LockManager struct {
	two_pl_mode   TwoPLMode //__attribute__((__unused__));
	deadlock_mode DeadlockMode

	latch common.ReaderWriterLatch
	// TODO: (SDB) need ensure atomicity
	enable_cycle_detection bool
	// TODO: (SDB) not poreted yet
	//cycle_detection_thread *std::thread

	/** Lock table for lock requests. */
	lock_table map[page.RID]*LockRequestQueue
	/** Waits-for graph representation. */
	waits_for map[types.TxnID][]types.TxnID
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
	// If Detection() is enabled, we should launch a background cycle detection thread.
	if ret.Detection() {
		ret.enable_cycle_detection = true
		// TODO: (SDB) not ported yet
		//ret.cycle_detection_thread = new std::thread(&LockManager::RunCycleDetection, this)
		//LOG_INFO("Cycle detection thread launched")
	}
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

/**
* Acquire a lock on RID in shared mode. See [LOCK_NOTE] in header file.
* @param txn the transaction requesting the shared lock
* @param rid the RID to be locked in shared mode
* @return true if the lock is granted, false otherwise
 */
func LockShared(txn *Transaction, rid *page.RID) bool {
	// TODO: (SDB) not ported yet
	// txn.GetSharedLockSet().emplace(rid)
	return true
}

/**
* Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
* @param txn the transaction requesting the exclusive lock
* @param rid the RID to be locked in exclusive mode
* @return true if the lock is granted, false otherwise
 */
func LockExclusive(txn *Transaction, rid *page.RID) bool {
	// TODO: (SDB) not ported yet
	// txn.GetExclusiveLockSet().emplace(rid)
	return true
}

/**
* Upgrade a lock from a shared lock to an exclusive transaction.
* @param txn the transaction requesting the lock upgrade
* @param rid the RID that should already be locked in shared mode by the requesting transaction
* @return true if the upgrade is successful, false otherwise
 */
func LockUpgrade(txn *Transaction, rid *page.RID) bool {
	// TODO: (SDB) not ported yet
	// txn.GetSharedLockSet().erase(rid)
	// txn.GetExclusiveLockSet().emplace(rid)
	return true
}

/**
* Release the lock held by the transaction.
* @param txn the transaction releasing the lock, it should actually hold the lock
* @param rid the RID that is locked by the transaction
* @return true if the unlock is successful, false otherwise
 */
func Unlock(txn *Transaction, rid *page.RID) bool {
	// TODO: (SDB) not ported yet
	// txn.GetSharedLockSet().erase(rid)
	// txn.GetExclusiveLockSet().erase(rid)
	return true
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
* Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
* @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
* @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
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
	         // TODO(student): remove the continue and add your cycle detection and abort code here
	         continue
	       }
	     }
	*/
}
