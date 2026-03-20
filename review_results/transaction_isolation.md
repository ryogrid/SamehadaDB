# Transaction Isolation & Lock Manager Review

Review date: 2026-03-20
Scope: SS2PL-NW concurrency control -- verifying REPEATABLE READ guarantees.
Phantom reads are a documented/known limitation and are excluded.

---

## 1. Lock Manager (`lib/storage/access/lock_manager.go`)

### Bug 1.1: LockUpgrade does not remove the shared lock entry

- **File:** `lib/storage/access/lock_manager.go`, lines 237-240
- **Description:** When `LockUpgrade` succeeds, it adds the RID to the transaction's exclusive lock set and adds an entry to `exclusiveLockTable`, but it never removes the transaction's ID from `sharedLockTable[*rid]` and never removes the RID from the transaction's `sharedLockSet`.
- **Why it is a bug:** After upgrade, the RID is tracked in both the shared and exclusive lock sets. On `Unlock` (called via `releaseLocks`), `Unlock` iterates both sets and removes entries. The shared lock table entry for this RID will still contain the upgrading txn's ID. While `Unlock` does clean it up at commit/abort time (since the RID remains in `sharedLockSet` and `Unlock` processes it), the stale shared-lock-table entry is visible during the transaction's lifetime. Specifically, if another transaction tries `LockExclusive` on the same RID, it checks `sharedLockTable` at line 193-197 and sees a non-empty array (containing the upgrading txn's ID). The condition `len(arr) == 1 && arr[0] == txn.GetTransactionID()` will NOT match for the *other* transaction, so it returns false as expected. However, if the upgrading transaction itself calls `LockExclusive` on the same RID later, the `exclusiveLockTable` check at line 186-188 returns true (re-entrant), so that path is fine. The real issue: `LockUpgrade` does not verify `txnIds[0] == txn.GetTransactionID()` at line 234 -- it only checks `len(txnIds) != 1`. If another transaction concurrently released its shared lock leaving a 1-element list containing a *different* txn ID, the upgrade would wrongly succeed. But given No-Wait semantics and the single global mutex, this specific race is prevented. **Net assessment:** This is not a correctness bug under current usage because `Unlock` cleans up both sets, and the IsSharedLocked pre-check (line 223) ensures the txn holds the lock. However, the missing cleanup of `sharedLockTable` during upgrade is a latent defect that could cause issues if the lock manager is extended.
- **Severity:** Minor (latent defect, no current isolation violation)

### Bug 1.2: `removeTxnID` and `removeRID` mutate the original input slice

- **File:** `lib/storage/access/lock_manager.go`, lines 105-113 (removeTxnID), lines 94-103 (removeRID)
- **Description:** Both functions create a copy (`lst`) on line 106/95 but then immediately operate on the original `list` at line 109/98: `lst = append(list[:i], list[i+1:]...)`. The `list[:i]` expression shares the underlying array with the original `list` parameter, so `append(list[:i], list[i+1:]...)` mutates the original slice's backing array. The variable `lst` initialized on line 106 is discarded (never used after line 106). The returned value is the result of mutating the original.
- **Why it is a bug:** `removeTxnID` is called from `Unlock` at line 270: `lockManager.sharedLockTable[lockedRID] = removeTxnID(arr, txn.GetTransactionID())`. The `arr` variable is a reference to the slice stored in the map. The function mutates `arr`'s backing array in-place AND returns a new slice header (possibly with different length). The map entry is then overwritten with the return value. Because the global lock manager mutex is held during the entire `Unlock` call, there is no concurrent access to the map. The mutation of the backing array is therefore not visible to concurrent readers. **Net assessment:** Under the single-mutex design, this does not cause a data race or correctness issue, but the code does not behave as the author likely intended (the copy on line 106 is dead code). If the locking discipline were ever relaxed (e.g., per-RID locks), this would become a race condition.
- **Severity:** Minor (dead code / latent defect, not a current isolation bug)

### Bug 1.3: `sharedLockTable` entries are never deleted, only emptied

- **File:** `lib/storage/access/lock_manager.go`, line 270
- **Description:** When the last transaction unlocks a shared lock on a RID, `removeTxnID` returns an empty slice, and this empty slice is stored back into `sharedLockTable`. The entry is never `delete()`-ed from the map. In `LockExclusive` (line 193), the check `arr == nil || len(arr) == 0` handles this correctly by treating an empty slice the same as absence.
- **Why it is a bug:** This is a memory leak, not an isolation violation. Over time, the `sharedLockTable` map grows unboundedly with empty slices for every RID that was ever shared-locked.
- **Severity:** Minor (memory leak, not an isolation bug)

### No bugs found: LockShared and LockExclusive grant/deny logic

The grant/deny logic is correct for No-Wait SS2PL:
- `LockShared` denies if another txn holds X-lock (line 144-146), correctly allows if same txn holds X-lock (line 142-143).
- `LockExclusive` denies if another txn holds X-lock (line 189-190) or if other txns hold S-locks (line 194-196).
- Re-entrant calls are handled correctly in both functions.
- The global mutex protects all lock table operations.

---

## 2. Transaction Manager (`lib/storage/access/transaction_manager.go`)

### Bug 2.1: `txnMap` entries are never removed (memory leak)

- **File:** `lib/storage/access/transaction_manager.go`, line 57 (Store), and absence of Delete in Commit/Abort
- **Description:** `txnMap.Store(txnRet.GetTransactionID(), txnRet)` is called in `Begin`, but neither `Commit` nor `Abort` calls `txnMap.Delete(...)`. The `txnMap` is a global `sync.Map` that grows monotonically.
- **Why it is a bug:** This is a memory leak. Every transaction that ever runs remains referenced in the global map forever, preventing GC of the Transaction object and its write set, lock sets, etc. This is not an isolation violation but a resource leak.
- **Severity:** Minor (memory leak, not an isolation bug)

### Bug 2.2: Transaction state is never set to COMMITTED or ABORTED at the end of Commit/Abort

- **File:** `lib/storage/access/transaction_manager.go`, lines 61-141 (Commit), lines 143-276 (Abort)
- **Description:** Neither `Commit` nor `Abort` sets `txn.SetState(COMMITTED)` or `txn.SetState(ABORTED)` at the end of the method. After `Commit` returns, the transaction state remains `GROWING`. After `Abort` returns, the transaction state also lacks a terminal state set by the manager (it may be `ABORTED` if set earlier by an executor, but the manager itself does not ensure it).
- **Why it is a bug:** This means code that checks `txn.GetState() == COMMITTED` will never see a committed transaction. In this codebase the state is primarily used to check for `ABORTED`, so the missing `COMMITTED` state is unlikely to cause incorrect behavior in practice. However, it means the state machine documented in `transaction.go` (lines 14-21: `GROWING -> SHRINKING -> COMMITTED` and `ABORTED`) is not actually implemented. The `SHRINKING` state is also never set.
- **Why it matters for isolation:** If any code path were to check for `COMMITTED` or `SHRINKING` state to make decisions (e.g., whether to allow new lock acquisitions in the shrinking phase of regular 2PL), it would silently fail. Under SS2PL-NW (strict mode), locks are held until commit, so the shrinking phase is effectively the instant of lock release. No current code checks for SHRINKING or COMMITTED, so this is not a live isolation violation.
- **Severity:** Minor (state machine not implemented per design, no current isolation violation)

### No bugs found: Lock release ordering in Commit

Commit correctly performs deferred deletes (ApplyDelete), then writes the COMMIT log record with forced flush for write transactions (line 131), and only then releases locks (line 137). This ensures strict 2PL: no other transaction can read data modified by this transaction until after the commit is durable.

### No bugs found: Write-set rollback in Abort

Abort processes the write set in reverse order (popping from the end, line 174-175) and correctly handles DELETE (RollbackDelete), INSERT (ApplyDelete + index rollback), and UPDATE (either restore old tuple or reverse del-insert) before releasing locks (line 272). The rollback-before-unlock ordering preserves isolation.

### No bugs found: nextTxnID race conditions

`nextTxnID` is protected by `transactionManager.mutex` in `Begin` (lines 41-44). However, note that the mutex is only held for the ID increment and Transaction creation, not for the subsequent log record append or txnMap store. This is acceptable because the log manager and sync.Map have their own concurrency control.

---

## 3. Transaction (`lib/storage/access/transaction.go`)

### Bug 3.1: No thread safety on Transaction fields

- **File:** `lib/storage/access/transaction.go`, entire struct (lines 74-94)
- **Description:** The `Transaction` struct has no mutex or synchronization. Fields like `state`, `writeSet`, `sharedLockSet`, `exclusiveLockSet`, and `prevLSN` are read and written without any locking.
- **Why it is a bug:** In the current architecture, a Transaction object is designed to be used by a single goroutine (the executing transaction). The lock manager accesses the transaction's lock sets (`GetSharedLockSet`, `SetSharedLockSet`, etc.) while holding the lock manager's global mutex, but the transaction's own fields are not protected. If multiple goroutines ever share a Transaction object (e.g., parallel execution within one transaction), this would be a data race. Under the current single-thread-per-transaction model, this is safe.
- **Severity:** Minor (safe under current single-thread-per-txn assumption, would be a race with parallel intra-txn execution)

---

## 4. Delete Executor (`lib/execution/executors/delete_executor.go`)

### No bugs found

The delete executor correctly:
1. Reads tuples from the child executor (which acquires S-locks via GetTuple).
2. Calls `tableMetadata.Table().MarkDelete()` (line 55) which acquires an X-lock (upgrading from S if needed) inside `TablePage.MarkDelete()` (table_page.go lines 282-292).
3. Defers actual deletion (ApplyDelete) and index entry deletion to the commit phase in `TransactionManager.Commit()` (lines 84-104). This correctly prevents dirty reads through the index.
4. Records the write set entry inside `TableHeap.MarkDelete()` (table_heap.go line 261).

---

## 5. Update Executor (`lib/execution/executors/update_executor.go`)

### No bugs found

The update executor correctly:
1. Reads tuples from the child executor (S-lock acquired).
2. Calls `TableHeap.UpdateTuple()` which calls `TablePage.UpdateTuple()` where X-lock acquisition (or upgrade from S-lock) happens (table_page.go lines 166-177).
3. Records write set entries in `TableHeap.UpdateTuple()` (table_heap.go lines 215-223).
4. Updates index entries immediately (lines 82-101). Index updates happen under the X-lock, which is held until commit/abort, preventing other transactions from seeing inconsistent index-to-heap state.

---

## 6. Insert Executor (`lib/execution/executors/insert_executor.go`)

### No bugs found

The insert executor correctly:
1. Calls `TableHeap.InsertTuple()` which calls `TablePage.InsertTuple()` where an X-lock is acquired on the new RID (table_page.go lines 105-116).
2. Records write set entries in `TableHeap.InsertTuple()` (table_heap.go line 134).
3. Inserts index entries (lines 48-56) under the X-lock, which is held until commit/abort.

---

## 7. Lock Coverage Analysis

### Callers of LockShared (non-test files)

1. `lib/storage/access/table_heap.go:340` -- `TableHeap.GetTuple()`: acquires S-lock before reading.
2. `lib/storage/access/table_page.go:570` -- `TablePage.GetTuple()`: acquires S-lock before reading.

### Callers of LockExclusive (non-test files)

1. `lib/storage/access/table_page.go:107` -- `TablePage.InsertTuple()`: X-lock on new RID.
2. `lib/storage/access/table_page.go:173` -- `TablePage.UpdateTuple()`: X-lock (or upgrade from S).
3. `lib/storage/access/table_page.go:289` -- `TablePage.MarkDelete()`: X-lock (or upgrade from S).

### Callers of LockUpgrade (non-test files)

1. `lib/storage/access/table_page.go:169` -- `TablePage.UpdateTuple()`: upgrade S to X.
2. `lib/storage/access/table_page.go:285` -- `TablePage.MarkDelete()`: upgrade S to X.

### Bug 7.1: Double S-lock acquisition in `TableHeap.GetTuple` path

- **File:** `lib/storage/access/table_heap.go`, line 340, and `lib/storage/access/table_page.go`, line 570
- **Description:** `TableHeap.GetTuple()` acquires an S-lock at line 340, then calls `page.GetTuple()` at line 347 which again tries to acquire an S-lock at line 570. Since `LockShared` is re-entrant (checks `isContainTxnID` at line 149), the second call succeeds as a no-op.
- **Why it is a bug:** This is not a correctness bug -- the re-entrant check handles it correctly. It is redundant work (two lock manager mutex acquisitions instead of one). This does not affect isolation.
- **Severity:** Minor (performance, not an isolation bug)

### Bug 7.2: `TablePage.MarkDelete` calls `GetTuple` internally, which re-acquires S-lock under X-lock

- **File:** `lib/storage/access/table_page.go`, line 314
- **Description:** `MarkDelete` acquires an X-lock (lines 282-292), then calls `tp.GetTuple(rid, ...)` at line 314 to copy the tuple for rollback purposes. `GetTuple` at line 570 tries to acquire an S-lock, but the transaction already holds an X-lock. `LockShared` at line 141-143 detects the X-lock held by the same txn and returns true. This is correct.
- **Why it is a bug:** This is not a bug. The re-entrant check handles it correctly.
- **Severity:** N/A (not a bug)

### All data access paths acquire locks

Every read path (`TableHeap.GetTuple`, `TablePage.GetTuple`, `TableHeapIterator.Next`) acquires at least an S-lock. Every write path (`InsertTuple`, `UpdateTuple`, `MarkDelete`) acquires an X-lock. No data access path was found that bypasses lock acquisition (outside of recovery mode, which correctly skips locking as the system is single-threaded during recovery).

---

## Summary

| ID | Location | Description | Severity |
|----|----------|-------------|----------|
| 1.1 | lock_manager.go:237-240 | LockUpgrade does not clean sharedLockTable entry | Minor |
| 1.2 | lock_manager.go:105-113, 94-103 | removeTxnID/removeRID mutate original slice; copy is dead code | Minor |
| 1.3 | lock_manager.go:270 | sharedLockTable entries never deleted (memory leak) | Minor |
| 2.1 | transaction_manager.go:57 | txnMap entries never removed (memory leak) | Minor |
| 2.2 | transaction_manager.go:61-141, 143-276 | Transaction state never set to COMMITTED/ABORTED by manager | Minor |
| 3.1 | transaction.go:74-94 | No thread safety on Transaction fields | Minor |
| 7.1 | table_heap.go:340 + table_page.go:570 | Redundant double S-lock acquisition | Minor |

**Overall isolation assessment:** Under the current SS2PL-NW design with a single global lock manager mutex and single-thread-per-transaction model, **REPEATABLE READ is correctly guaranteed**. No dirty reads, non-repeatable reads, or lost updates were found. All data access paths correctly acquire locks, and strict 2PL is enforced (locks are held until commit/abort, with commit durability ensured before lock release). The bugs found are all Minor severity -- memory leaks, dead code, missing state transitions, and latent defects that do not currently violate isolation guarantees.
