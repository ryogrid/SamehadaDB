# Fix Design: Defer DeleteEntry to Commit Phase

## 1. Approach Comparison

| Approach | Description | Pros | Cons |
|---|---|---|---|
| **A: Defer DeleteEntry to Commit (RECOMMENDED)** | Remove DeleteEntry from executor, add to Commit() alongside ApplyDelete | Simple, ~10 LOC net change, aligns index lifecycle with tuple lifecycle | Increases false-abort window (index entry stays, so GetTuple sees mark-delete) |
| B: Visibility check at index scan | Add tuple visibility check after ScanKey | No change to executor/commit | Complex, perf overhead on every scan, doesn't fix root cause |
| C: Separate pending-delete list in Transaction | Store pending index deletes in a new list, apply at commit | Clean separation | Redundant with writeSet, adds new data structure |

## 2. Recommended: Approach A — Defer DeleteEntry to Commit Phase

The index entry should only be removed when the delete is committed — the same moment `ApplyDelete` physically removes the tuple. This aligns the index lifecycle with the tuple lifecycle.

## 3. Exact Changes (3 files, ~10 LOC net)

### File 1: `lib/execution/executors/delete_executor.go` (lines 64-73)

**Action:** DELETE the `DeleteEntry` loop. The `MarkDelete` at line 55 already stores a `WriteRecord{rid1, tuple1, DELETE, oid}` in the write set, which is sufficient for the commit phase to find and delete index entries.

**Before:**
```go
// removing index entry is done at commit phase because delete operation uses marking technique

colNum := tableMetadata.GetColumnNum()
for ii := 0; ii < int(colNum); ii++ {
    ret := tableMetadata.GetIndex(ii)
    if ret == nil {
        continue
    } else {
        idx := ret
        idx.DeleteEntry(t, *rid, e.txn)
    }
}
```

**After:**
```go
// index entry deletion is deferred to commit phase (transaction_manager.go)
// to prevent dirty reads via index scan
```

### File 2: `lib/storage/access/transaction_manager.go` — `Commit()` method (after line 94)

**Action:** Add `DeleteEntry` calls for DELETE write records, after `ApplyDelete` completes and the page WLatch is released. Add `indexMap` declaration before the writeSet loop.

**Added code (in Commit, after `tpage.WUnlatch()` for DELETE case):**
```go
// delete index entries now that the delete is committed
if cat != nil {
    indexes := cat.GetRollbackNeededIndexes(indexMap, item.oid)
    for _, idx := range indexes {
        if idx != nil {
            idx.DeleteEntry(item.tuple1, *item.rid1, txn)
        }
    }
}
```

This mirrors the exact pattern from `Abort()` (lines 197-203 for INSERT rollback).

### File 3: `lib/storage/access/transaction_manager.go` — `Abort()` method (lines 175-181)

**Action:** Remove the index re-insertion block for the DELETE case. Since index entries are never deleted at execution time (they're deferred to commit), there's nothing to roll back.

**Before:**
```go
// rollback index data
indexes := cat.GetRollbackNeededIndexes(indexMap, item.oid)
for _, idx := range indexes {
    if idx != nil {
        idx.InsertEntry(item.tuple1, *item.rid1, txn)
    }
}
```

**After:**
```go
// index entries were never deleted (deferred to commit), so no index rollback needed
```

## 4. Latch Ordering Analysis

**Commit path:**
1. Page WLatch acquired → `ApplyDelete` → page WLatch released
2. Index latch acquired → `DeleteEntry` → index latch released

No concurrent holding of page latch and index latch → **no deadlock risk**.

This matches the existing INSERT-rollback pattern in `Abort()` (lines 188-203), which does `ApplyDelete` under page WLatch, then `DeleteEntry` after releasing it.

## 5. Impact on Other Paths

| Path | Impact |
|---|---|
| **INSERT** | Unchanged — `InsertEntry` at executor time is correct |
| **UPDATE** | Unchanged — `UpdateEntry` is atomic delete+insert under exclusive lock |
| **SELECT (index scan)** | Index entry now found for uncommitted deletes → `GetTuple` detects mark-delete → false abort (consistent with sequential scan behavior) |
| **SELECT (sequential scan)** | Unchanged — already produces false abort for mark-deleted tuples |
| **Crash recovery** | Unchanged — `LogRecovery.Undo` only does table operations; indexes rebuilt from scratch |

## 6. Trade-offs

**Before fix:** Index scan sees uncommitted delete (dirty read) — **data integrity violation**.

**After fix:** Index scan finds entry, `GetTuple` detects mark-delete, reading txn is aborted (false abort) — **same behavior as sequential scan**. The `RequestManager` already handles this by re-queuing aborted queries.

The false-abort rate may increase slightly for index scans during concurrent deletes, but this is correct behavior that preserves isolation guarantees.
