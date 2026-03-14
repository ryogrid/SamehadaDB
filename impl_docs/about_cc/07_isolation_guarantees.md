# Isolation Level Analysis

## 1. Overview

SamehadaDB implements **SS2PL-NW (Strict Two-Phase Locking, No-Wait)**. In theory, S2PL guarantees **serializability** — the strongest isolation level. In practice, SamehadaDB's implementation has gaps in index-level consistency that weaken isolation for certain access paths.

This document catalogs the anomalies that can and cannot occur, with concrete scenarios.

## 2. Protocol Guarantees

**What SS2PL provides:**
- **Strict**: All locks (shared and exclusive) held until transaction end → no cascading aborts for lock-protected reads.
- **Two-Phase**: No lock is acquired after any lock is released → serializable ordering for lock-protected operations.
- **No-Wait**: Conflicting lock requests fail immediately → no deadlocks, but higher abort rate.

**What SS2PL does NOT provide in this implementation:**
- **Predicate/gap locking**: Only RID-level locks exist. No protection against phantom inserts.
- **Index-level isolation**: Index entries are modified during execution, not at commit. The lock protocol protects tuples but not index entries.

## 3. Anomaly Analysis Matrix

| Anomaly | Sequential Scan | Index Scan | Notes |
|---|---|---|---|
| **Dirty Write** | Prevented | Prevented | X-lock on RID serializes writes |
| **Dirty Read** | Prevented (false abort instead) | **Possible** (DELETE) | Index entry removed before commit |
| **Non-Repeatable Read** | Prevented | Prevented | S-lock held until txn end |
| **Phantom Read** | **Possible** | **Possible** | No predicate locking |
| **Write Skew** | Prevented | Prevented | X-lock serializes conflicting writes |
| **Lost Update** | Prevented | Prevented | X-lock on RID before write |

## 4. Dirty Read at DELETE (Index Scan)

> ⚠️ **Known Issue**

**Scenario:**

| Time | Txn A | Txn B (index scan) |
|---|---|---|
| T1 | `MarkDelete(rid)` — tuple marked | |
| T2 | `DeleteEntry(key, rid)` — index entry removed | |
| T3 | | `ScanKey(key)` — entry NOT FOUND |
| T4 | | Row invisible — **dirty read** |
| T5 | `ABORT` — `InsertEntry` restores index | |
| T6 | | Txn B already observed uncommitted state |

**Why it happens:** `DeleteEntry` is called at executor time (not commit time). The index entry vanishes before the transaction commits.

**Why sequential scan is safe:** `GetTuple` checks the mark-delete flag and aborts the reader if another transaction marked the tuple. This is a **false abort** (not a dirty read) — pessimistic but safe.

**Full analysis:** [04_tuple_index_consistency.md](04_tuple_index_consistency.md)

## 5. False Aborts

When Txn B reads a tuple that Txn A has mark-deleted (but not committed):

- **Via sequential scan**: `GetTuple` (`table_page.go:611-631`) detects the mark-delete flag from another transaction and **aborts Txn B** (line 623).
- **Via index scan (Scenario A)**: Same — if the index entry hasn't been removed yet, Txn B finds the RID but GetTuple detects the mark and aborts Txn B.

False aborts are not data integrity violations, but they reduce throughput. The **RequestManager** (`lib/samehada/request_manager.go`) mitigates this by re-queuing aborted queries at the head of the queue, giving them priority on retry.

## 6. Non-Repeatable Read — Prevented

**Why prevented:**

1. Txn B reads a tuple → `LockShared(txn_B, rid)` succeeds.
2. Txn A tries to update/delete the same tuple → `LockExclusive(txn_A, rid)` fails (Txn B holds S-lock).
3. Txn A is aborted (No-Wait).
4. Txn B can re-read the same tuple — value unchanged.

The S-lock is held until Txn B commits or aborts (strict 2PL), so no other transaction can modify the tuple while Txn B's lock is active.

## 7. Phantom Read — Possible

> ⚠️ **Known Issue**

**Scenario:**

| Time | Txn A | Txn B |
|---|---|---|
| T1 | | `SELECT * WHERE age > 20` → returns {row1, row2} |
| T2 | `INSERT (id=3, age=25)` → new RID | |
| T3 | `COMMIT` | |
| T4 | | `SELECT * WHERE age > 20` → returns {row1, row2, **row3**} |

**Why it happens:** SamehadaDB only locks individual RIDs. There is no predicate lock or gap lock on the range `age > 20`. Txn A's insert creates a new RID that Txn B never locked, so there is no conflict.

**Affected access paths:** Both sequential and index scans. Index scans are equally vulnerable because index entries are added immediately during insert (which is actually safe for insert — see [04_tuple_index_consistency.md](04_tuple_index_consistency.md) §2).

## 8. Write Skew — Prevented

**Why prevented:** Write skew requires two transactions to read overlapping data and then write to different rows based on what they read. In SS2PL-NW:

1. Both transactions acquire S-locks on the rows they read.
2. When either tries to write, it needs an X-lock.
3. The X-lock request fails because the other transaction holds an S-lock.
4. One transaction is aborted (No-Wait).

This is correct as long as both transactions read via tuple access (not just index lookup). The S-locks on the read set prevent the conflicting writes.

## 9. Lost Update — Prevented

**Why prevented:**

1. Txn A reads tuple → `LockShared(txn_A, rid)`.
2. Txn B reads same tuple → `LockShared(txn_B, rid)`.
3. Txn A tries to update → `LockUpgrade(txn_A, rid)` → fails (Txn B holds S-lock, upgrade requires sole holder).
4. Txn A aborted. Txn B can safely update.

The upgrade check (`lock_manager.go:234-236`) ensures only a sole shared-lock holder can promote to exclusive.

## 10. Dirty Write — Prevented

**Why prevented:** `LockExclusive` (or `LockUpgrade`) is required before any write. If another transaction holds any lock on the RID, the write fails immediately (No-Wait). Two transactions cannot simultaneously hold X-locks on the same RID.

## 11. Summary of Isolation Gaps

| Gap | Severity | Root Cause | Mitigation |
|---|---|---|---|
| **Dirty read at DELETE (index)** | High | `DeleteEntry` at executor time, not commit time | Defer `DeleteEntry` to commit phase |
| **Phantom read** | Medium | No predicate/gap locking | Add predicate locks or use MVCC |
| **False aborts** | Low | Mark-delete detected by concurrent reader | `RequestManager` retry with priority re-queue |

## 12. Cross-References

- **LockManager compatibility matrix**: [01_lock_manager.md](01_lock_manager.md) §3
- **Page latch patterns**: [02_page_latch_and_pinning.md](02_page_latch_and_pinning.md)
- **Index latch protocols**: [03_index_concurrency.md](03_index_concurrency.md)
- **Dirty-read-at-delete root cause**: [04_tuple_index_consistency.md](04_tuple_index_consistency.md)
- **UPDATE RID mechanics**: [05_update_rollback_rid.md](05_update_rollback_rid.md)
- **Rollback handling**: [06_rollback_handling.md](06_rollback_handling.md)
- **Transaction overview**: [../overview/05_transaction_recovery.md](../overview/05_transaction_recovery.md)
