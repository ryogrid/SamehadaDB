# Tuple/Index Update Consistency

> This is the **critical document** for understanding the dirty-read-at-delete anomaly.

## 1. Overview

SamehadaDB executors perform **two separate steps** for each DML operation:
1. **Tuple mutation** (via `TableHeap`): modifies the physical tuple on a table page.
2. **Index mutation** (via index wrapper): updates the index to reflect the new tuple state.

These two steps are **not atomic** — there is a window between them where the tuple and index are inconsistent. For INSERT and UPDATE, this window is benign. For DELETE, it causes a **dirty read anomaly** when concurrent transactions use index scans.

## 2. INSERT Timing — Safe

**File:** `lib/execution/executors/insert_executor.go`

```
Step 1 (line 42):  tableHeap.InsertTuple(tuple, txn, oid, false)
                   → tuple physically written to page
                   → exclusive lock acquired on new RID
                   → write record added to txn.writeSet

Step 2 (lines 48-56): for each index:
                       idx.InsertEntry(tuple, rid, txn)
                       → index entry added
```

```mermaid
sequenceDiagram
    participant TxnA as Txn A (INSERT)
    participant Table as TableHeap
    participant Index as Index
    participant TxnB as Txn B (SELECT via index)

    TxnA->>Table: InsertTuple(tuple) → rid
    Note over Table: Tuple on page, X-lock on rid

    TxnA->>Index: InsertEntry(tuple, rid)
    Note over Index: Index entry added

    TxnB->>Index: ScanKey(key)
    Note over Index: Finds rid
    TxnB->>Table: GetTuple(rid)
    Note over Table: LockShared fails (X-lock held by A)
    Note over TxnB: No-Wait → Txn B aborted
```

**Why safe:**
- The tuple is physically present before the index entry is added. If Txn B finds the index entry, the tuple exists.
- Txn B's `GetTuple` attempts `LockShared(rid)`, which fails because Txn A holds an exclusive lock → Txn B is aborted (No-Wait). **No dirty read.**
- In the window between InsertTuple and InsertEntry (tuple exists but index doesn't point to it yet), Txn B simply doesn't find the row via index scan — this is equivalent to Txn B running before Txn A, which is a valid serialization order.

## 3. UPDATE Timing — Mostly Safe

**File:** `lib/execution/executors/update_executor.go`

```
Step 1 (lines 64-67):  tableHeap.UpdateTuple(newTuple, rid, txn, oid)
                        → tuple updated on page (in-place or relocated)
                        → exclusive lock acquired/upgraded on rid
                        → write record added to txn.writeSet

Step 2 (lines 80-102):  for each index:
                         idx.UpdateEntry(oldTuple, oldRID, newTuple, newRID, txn)
                         → exclusive Lock on wrapper mutex
                         → atomic: delete old entry + insert new entry
```

```mermaid
sequenceDiagram
    participant TxnA as Txn A (UPDATE)
    participant Table as TableHeap
    participant Index as Index
    participant TxnB as Txn B (SELECT via index)

    TxnA->>Table: UpdateTuple(newTuple, rid)
    Note over Table: Tuple updated, X-lock on rid

    TxnA->>Index: UpdateEntry(old→new)
    Note over Index: Exclusive Lock held<br/>Delete old + Insert new atomically

    TxnB->>Index: ScanKey(newKey)
    Note over Index: Blocked until UpdateEntry releases Lock
    Note over Index: Finds new rid
    TxnB->>Table: GetTuple(newRid)
    Note over Table: LockShared fails (X-lock by A)
    Note over TxnB: No-Wait → Txn B aborted
```

**Why mostly safe:**
- `UpdateEntry` acquires the **exclusive wrapper lock** (`updateMtx.Lock()`), so the delete+insert pair is atomic with respect to scans. No scan can see the intermediate state.
- After `UpdateEntry` releases the lock, a scan will find the new entry. But `GetTuple` on the new RID will fail because Txn A still holds the X-lock → Txn B aborted. **No dirty read.**
- The window between `UpdateTuple` and `UpdateEntry` (tuple updated but index still points to old state) could allow a scan to find the old entry and read the new tuple data. The X-lock on the RID prevents this.

## 4. DELETE Timing — **UNSAFE**

**File:** `lib/execution/executors/delete_executor.go`

```
Step 1 (line 55):  tableMetadata.Table().MarkDelete(rid, oid, txn, false)
                   → MSB set on tuple size (tuple still physically present)
                   → exclusive lock acquired/upgraded on rid
                   → write record added to txn.writeSet

Step 2 (lines 65-73): for each index:
                       idx.DeleteEntry(tuple, rid, txn)
                       → index entry REMOVED IMMEDIATELY
```

> ⚠️ **Known Issue: Dirty Read at DELETE**
> The index entry is removed during execution (Step 2), NOT at commit time. A concurrent transaction using an index scan will not find the entry, effectively observing an uncommitted delete. This violates Read Committed isolation.

### Scenario A: Index Scan Before DeleteEntry

```mermaid
sequenceDiagram
    participant TxnA as Txn A (DELETE)
    participant Table as TablePage
    participant Index as Index
    participant TxnB as Txn B (SELECT via index)

    TxnB->>Index: ScanKey(key)
    Note over Index: Finds rid (entry still exists)

    TxnA->>Table: MarkDelete(rid)
    Note over Table: MSB set on tuple size

    TxnA->>Index: DeleteEntry(tuple, rid)
    Note over Index: Entry removed

    TxnB->>Table: GetTuple(rid)
    Note over Table: Tuple is mark-deleted
    Note over Table: table_page.go:623 → Txn B ABORTED
```

**What happens:** Txn B's index iterator obtained the RID before DeleteEntry removed it. When Txn B reads the tuple, it finds the mark-deleted flag. `GetTuple` (`table_page.go:611-631`) detects this and **aborts Txn B** — a false abort (Txn A hasn't committed, so the delete might be rolled back).

**Impact:** Txn B is unnecessarily aborted. Not a data integrity issue, but causes spurious failures. The `RequestManager` (`lib/samehada/request_manager.go`) mitigates this by re-queuing aborted queries at the head.

### Scenario B: Index Scan After DeleteEntry — **The Dirty Read**

```mermaid
sequenceDiagram
    participant TxnA as Txn A (DELETE)
    participant Table as TablePage
    participant Index as Index
    participant TxnB as Txn B (SELECT via index)

    TxnA->>Table: MarkDelete(rid)
    Note over Table: MSB set, X-lock on rid

    TxnA->>Index: DeleteEntry(tuple, rid)
    Note over Index: Entry REMOVED

    TxnB->>Index: ScanKey(key)
    Note over Index: Entry NOT FOUND
    Note over TxnB: Row invisible — dirty read!
    Note over TxnB: Txn A has NOT committed

    Note over TxnA: Later: Txn A aborts → rollback<br/>InsertEntry restores index
    Note over TxnB: But Txn B already observed<br/>the uncommitted delete
```

**What happens:** Txn B's index scan occurs after `DeleteEntry` has removed the entry. The row is completely invisible to Txn B via the index. If Txn A later aborts, the row should have been visible — Txn B observed an uncommitted delete.

**Root cause:** `DeleteEntry` is called at executor time, not deferred to commit. The comment at `delete_executor.go:62` says "removing index entry is done at commit phase because delete operation uses marking technique" — but the code actually removes the index entry immediately.

### Scenario C: Sequential Scan (No Index)

When using a sequential scan (no index), the situation is different:

1. `GetNextTupleRID` iterates through all slots on each page.
2. For each RID, `GetTuple` is called.
3. `GetTuple` (`table_page.go:611-631`) checks the mark-delete flag:
   - If marked by **same** transaction: returns the tuple (transaction can see its own deletes).
   - If marked by **another** transaction: **aborts the reading transaction** (line 623).

This means sequential scans don't produce dirty reads — they produce **false aborts** instead. The reading transaction is forced to abort when it encounters a mark-deleted tuple from another uncommitted transaction.

## 5. Reproduction Conditions

The dirty read at DELETE (Scenario B) occurs when **all** of the following are true:

1. **Index access path**: The query uses an index scan (not sequential scan).
2. **Concurrent delete**: Another transaction has executed `MarkDelete` + `DeleteEntry` but not yet committed.
3. **Timing**: The scan's `ScanKey` call occurs **after** `DeleteEntry` removes the index entry.
4. **No rollback yet**: The deleting transaction has not aborted (which would re-insert the index entry).

**Affected code paths:**
- `delete_executor.go:55` (`MarkDelete`) and `delete_executor.go:71` (`DeleteEntry`)
- Any executor or scan that uses index-based lookup: `index_join_executor.go`, `selection_executor.go` with index predicate, etc.

## 6. Why Commit-Time Index Deletion Would Fix This

If `DeleteEntry` were deferred to the commit phase (alongside `ApplyDelete`), the index entry would remain present until the transaction commits:

- Txn B's index scan would still find the entry.
- Txn B's `GetTuple` would attempt `LockShared`, which would fail (Txn A holds X-lock) → Txn B aborted with No-Wait, but **not** a dirty read.
- If Txn A aborts, the entry was never removed from the index — no inconsistency.

The trade-off: keeping stale index entries until commit increases the chance of false aborts (Scenario A), but eliminates dirty reads (Scenario B).

## 7. Summary Table

| Operation | Tuple Step | Index Step | Window Risk | Dirty Read? |
|---|---|---|---|---|
| **INSERT** | `InsertTuple` (line 42) | `InsertEntry` (line 54) | Row invisible via index before InsertEntry | No — valid serialization |
| **DELETE** | `MarkDelete` (line 55) | `DeleteEntry` (line 71) | Row invisible via index after DeleteEntry | **Yes — uncommitted delete visible** |
| **UPDATE** | `UpdateTuple` (line 64) | `UpdateEntry` (line 90) | Atomic under exclusive lock | No — X-lock prevents read |

## 8. Cross-References

- **LockManager No-Wait behavior**: [01_lock_manager.md](01_lock_manager.md)
- **Page latch patterns in TableHeap**: [02_page_latch_and_pinning.md](02_page_latch_and_pinning.md)
- **Index latch protocols**: [03_index_concurrency.md](03_index_concurrency.md)
- **UPDATE RID change mechanics**: [05_update_rollback_rid.md](05_update_rollback_rid.md)
- **Rollback restoring index entries**: [06_rollback_handling.md](06_rollback_handling.md)
- **Isolation guarantee analysis**: [07_isolation_guarantees.md](07_isolation_guarantees.md)
- **Transaction and recovery overview**: [../overview/05_transaction_recovery.md](../overview/05_transaction_recovery.md)
