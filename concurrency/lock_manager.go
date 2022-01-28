package concurrency

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

//class TransactionManager;

/** Two-Phase Locking mode. */
enum class TwoPLMode { REGULAR, STRICT };

/** Deadlock mode. */
enum class DeadlockMode { PREVENTION, DETECTION };

enum class LockMode { SHARED, EXCLUSIVE };

type LockRequest struct {
	txn_id_t txn_id_;
	LockMode lock_mode_;
	bool granted_;
}
NewLockRequest(txn_id_t txn_id, LockMode lock_mode) : txn_id_(txn_id), lock_mode_(lock_mode), granted_(false) {}

type LockRequestQueue struct {
	std::list<LockRequest> request_queue_;
	std::condition_variable cv_;  // for notifying blocked transactions on this rid
	bool upgrading_ = false;
}

/**
 * LockManager handles transactions asking for locks on records.
 */
type LockManager struct {
  TwoPLMode two_pl_mode_ __attribute__((__unused__));
  DeadlockMode deadlock_mode_;

  std::mutex latch_;
  std::atomic<bool> enable_cycle_detection_;
  std::thread *cycle_detection_thread_;

  /** Lock table for lock requests. */
  std::unordered_map<RID, LockRequestQueue> lock_table_;
  /** Waits-for graph representation. */
  std::unordered_map<txn_id_t, std::vector<txn_id_t>> waits_for_;
}

/**
* Creates a new lock manager configured for the given type of 2-phase locking and deadlock policy.
* @param two_pl_mode 2-phase locking mode
* @param deadlock_mode deadlock policy
*/
explicit LockManager(TwoPLMode two_pl_mode, DeadlockMode deadlock_mode = DeadlockMode::PREVENTION)
   : two_pl_mode_(two_pl_mode), deadlock_mode_(deadlock_mode) {
 // If Detection() is enabled, we should launch a background cycle detection thread.
 if (Detection()) {
   enable_cycle_detection_ = true;
   cycle_detection_thread_ = new std::thread(&LockManager::RunCycleDetection, this);
   LOG_INFO("Cycle detection thread launched");
 }
}

~LockManager() {
 if (Detection()) {
   enable_cycle_detection_ = false;
   cycle_detection_thread_->join();
   delete cycle_detection_thread_;
   LOG_INFO("Cycle detection thread stopped");
 }
}

bool Detection() { return deadlock_mode_ == DeadlockMode::DETECTION; }
bool Prevention() { return deadlock_mode_ == DeadlockMode::PREVENTION; }

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
bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

/**
* Acquire a lock on RID in exclusive mode. See [LOCK_NOTE] in header file.
* @param txn the transaction requesting the exclusive lock
* @param rid the RID to be locked in exclusive mode
* @return true if the lock is granted, false otherwise
*/
bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

/**
* Upgrade a lock from a shared lock to an exclusive lock.
* @param txn the transaction requesting the lock upgrade
* @param rid the RID that should already be locked in shared mode by the requesting transaction
* @return true if the upgrade is successful, false otherwise
*/
bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

/**
* Release the lock held by the transaction.
* @param txn the transaction releasing the lock, it should actually hold the lock
* @param rid the RID that is locked by the transaction
* @return true if the unlock is successful, false otherwise
*/
bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);
  return true;
}

/*** Graph API ***/
/**
* Adds edge t1->t2
*/

/** Adds an edge from t1 -> t2. */
void LockManager::AddEdge(txn_id_t t1, txn_id_t t2) { assert(Detection()); }

/** Removes an edge from t1 -> t2. */
void LockManager::RemoveEdge(txn_id_t t1, txn_id_t t2) { assert(Detection()); }

/**
* Checks if the graph has a cycle, returning the newest transaction ID in the cycle if so.
* @param[out] txn_id if the graph has a cycle, will contain the newest transaction ID
* @return false if the graph has no cycle, otherwise stores the newest transaction ID in the cycle to txn_id
*/
bool LockManager::HasCycle(txn_id_t *txn_id) {
  BUSTUB_ASSERT(Detection(), "Detection should be enabled!");
  return false;
}

/** @return the set of all edges in the graph, used for testing only! */
std::vector<std::pair<txn_id_t, txn_id_t>> LockManager::GetEdgeList() {
  BUSTUB_ASSERT(Detection(), "Detection should be enabled!");
  return {};
}

/** Runs cycle detection in the background. */
void LockManager::RunCycleDetection() {
  BUSTUB_ASSERT(Detection(), "Detection should be enabled!");
  while (enable_cycle_detection_) {
    std::this_thread::sleep_for(cycle_detection_interval);
    {
      std::unique_lock<std::mutex> l(latch_);
      // TODO(student): remove the continue and add your cycle detection and abort code here
      continue;
    }
  }
}

