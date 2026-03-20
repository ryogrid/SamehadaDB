# Recovery, Executors & Remaining Components Bug Review

## 1. Recovery / WAL (log_manager.go, log_record.go)

### Bug 1.1: Stale header written to log buffer before flush, then overwritten but only if record is too large

**File:** `/home/ryo/work/SamehadaDB/lib/recovery/log_manager.go`, lines 117-125
**Description:** In `AppendLogRecord`, the header is written to the log buffer at line 118 **before** the size check at line 120. If the record's total size exceeds the remaining buffer space, the method flushes and re-copies the header at line 124. However, the first copy at line 118 writes partial/stale data (the header only, without the body) into what gets flushed. The flush at line 122 calls `Flush()` which swaps the buffers and writes the current `logBuffer` to disk -- but `logMgr.offset` was not yet advanced past the header copy at line 118. So the flushed data should be limited to `[0:offset]` from the *previous* buffer state. This is actually safe because `offset` has not been incremented yet (it happens at line 131). The real issue is a **correctness concern**: the first header copy at line 118 writes 20 bytes into the buffer that will remain there as garbage after the buffer swap (in the old buffer now used as `flushBuffer`), but since `offset` controls the write range, those bytes are beyond the flushed region. **No actual data corruption** occurs due to the `offset` guard, but the logic is fragile.

**Severity:** Minor (no corruption due to offset guard, but fragile logic)

### Bug 1.2: Race on `nextLSN` and `persistentLSN` without synchronization

**File:** `/home/ryo/work/SamehadaDB/lib/recovery/log_manager.go`, lines 48-50
**Description:** `GetNextLSN()`, `SetNextLSN()`, and `GetPersistentLSN()` read/write `nextLSN` and `persistentLSN` without holding any lock. These fields are mutated under `latch.WLock()` (in `AppendLogRecord`) and `wlogMutex` (in `Flush`), but the getter/setter methods acquire neither. If called from another goroutine (e.g., during recovery setup or from a checkpoint thread checking persistent LSN), this is a data race.

**Severity:** Major (data race on shared mutable state accessed from multiple goroutines)

### Bug 1.3: `isEnableLogging` flag not used for gating in `AppendLogRecord` or `Flush`

**File:** `/home/ryo/work/SamehadaDB/lib/recovery/log_manager.go`, lines 88-95, 101
**Description:** The `isEnableLogging` flag is set by `ActivateLogging()`/`DeactivateLogging()`, but `AppendLogRecord` never checks it. Log records will be appended regardless of whether logging is "enabled". This flag also has no synchronization (not atomic), so reads/writes from different goroutines constitute a data race.

**Severity:** Minor (flag exists but is unused in gating; currently logging always works, so in practice no harm, but the race on the bool is technically undefined behavior)

### Bug 1.4: `offset` field is `uint32` -- subtraction can underflow

**File:** `/home/ryo/work/SamehadaDB/lib/recovery/log_manager.go`, lines 105, 120
**Description:** `logMgr.offset` is `uint32`. The expressions `common.LogBufferSize - logMgr.offset` are unsigned subtraction. If for any reason `logMgr.offset` exceeds `LogBufferSize`, the subtraction wraps to a very large number, bypassing the size check. In the current code flow, `offset` should never exceed `LogBufferSize` because it is incremented by `logRecord.Size` which was previously validated. However, there is no explicit bounds check, so a corrupted or excessively large `logRecord.Size` could cause `offset` to exceed the buffer size before the check triggers, leading to an out-of-bounds write.

**Severity:** Minor (defensive coding issue; hard to trigger in practice)

---

## 2. Log Recovery (log_recovery.go)

### Bug 2.1: ABORT log records not handled -- aborted transactions are never removed from `activeTxn`

**File:** `/home/ryo/work/SamehadaDB/lib/recovery/log_recovery/log_recovery.go`, lines 174-179
**Description:** During the redo phase, `BEGIN` adds a transaction to `activeTxn` and `COMMIT` removes it. However, there is no case for `recovery.ABORT`. If a transaction was explicitly aborted before the crash and an ABORT log record was written, that transaction will remain in `activeTxn` and will be unnecessarily undone during the undo phase. This means already-aborted-and-rolled-back transactions get their operations reversed again, potentially corrupting data (e.g., re-deleting already-deleted rows, re-inserting already-removed rows).

**Severity:** Critical (double-undo of already aborted transactions can corrupt data)

### Bug 2.2: `greatestLSN` initialized as `int` with value 0 -- DeallocatePage/ReusePage records with LSN=-1 tracked in `activeTxn`

**File:** `/home/ryo/work/SamehadaDB/lib/recovery/log_recovery/log_recovery.go`, lines 115, 129-130
**Description:** Line 129 unconditionally adds `logRecord.TxnID` to `activeTxn` and line 130 maps `logRecord.Lsn` to file offset for all log records, including DeallocatePage and ReusePage which have `Lsn=-1` and `TxnID=math.MaxInt32`. While the dummy txn `math.MaxInt32` is cleaned up at line 214, the `lsnMapping` entry for LSN=-1 is never cleaned up and could collide with the sentinel `common.InvalidLSN` (-1) used as the PrevLSN chain terminator in the undo phase. This is mitigated by the cleanup of the dummy txn from `activeTxn` at line 214, so the undo phase should not follow this chain, but the `lsnMapping[-1]` entry is unnecessary pollution.

**Severity:** Minor (mitigated by dummy txn cleanup, but logically incorrect)

### Bug 2.3: Undo phase does not write CLR (Compensation Log Records)

**File:** `/home/ryo/work/SamehadaDB/lib/recovery/log_recovery/log_recovery.go`, lines 226-281
**Description:** The undo phase directly applies inverse operations (e.g., `ApplyDelete` to undo an INSERT) but does not write compensation log records (CLRs). If the system crashes during undo/recovery, the partially completed undo work will be invisible to the next recovery pass, which will attempt to undo the same operations again. Without CLRs, repeated crashes during recovery can lead to data corruption (e.g., trying to delete a tuple that was already deleted in a previous partial undo pass).

**Severity:** Major (crash during recovery causes repeated undo without idempotency, risking corruption)

### Bug 2.4: `NewTablePage` redo does not check page LSN

**File:** `/home/ryo/work/SamehadaDB/lib/recovery/log_recovery/log_recovery.go`, lines 180-188
**Description:** For `NewTablePage` log records, the redo logic unconditionally calls `newPage.Init(...)` without checking `pg.GetLSN() < logRecord.GetLSN()`. All other data-modifying redo operations (INSERT, UPDATE, DELETE variants) correctly check the page LSN before applying. This means if a `NewTablePage` was already applied (page is already on disk with a valid LSN), the page will be re-initialized, potentially overwriting valid data that was written to that page after its creation.

**Severity:** Major (unconditional re-initialization can destroy page contents during recovery)

---

## 3. Catalog (table_catalog.go)

### Bug 3.1: `nextTableID` hardcoded to 1 in recovery path

**File:** `/home/ryo/work/SamehadaDB/lib/catalog/table_catalog.go`, line 116
**Description:** `RecoveryCatalogFromCatalogPage` constructs the Catalog with `nextTableID = 1`. If the database had more than 2 tables before the crash (OID 0 for columns_catalog, OID 1 for the first user table, OID 2+ for subsequent tables), the next `CreateTable` call after recovery would assign OID 1 again, colliding with an existing table. The function should compute `max(oid) + 1` from all recovered tables.

**Severity:** Critical (OID collision causes table overwrite/corruption after recovery when there are more than 2 tables)

### Bug 3.2: Dual mutex non-atomic update in `CreateTable`

**File:** `/home/ryo/work/SamehadaDB/lib/catalog/table_catalog.go`, lines 177-182
**Description:** `CreateTable` locks `tableIDsMutex` to insert into `tableIDs`, unlocks it, then locks `tableNamesMutex` to insert into `tableNames`. Between these two operations, another thread could call `GetTableByName` and not find the table (even though `GetTableByOID` would find it), or vice versa -- the two maps are temporarily inconsistent. Additionally, `insertTable` at line 220 accesses `c.tableIDs[ColumnsCatalogOID]` without holding `tableIDsMutex`.

**Severity:** Major (concurrent reads can see inconsistent catalog state; direct map access without lock in insertTable)

### Bug 3.3: `nextTableID` read-then-increment is not atomic

**File:** `/home/ryo/work/SamehadaDB/lib/catalog/table_catalog.go`, lines 167-168
**Description:** Line 167 reads `c.nextTableID` and line 168 atomically increments it. But the read at line 167 is a plain (non-atomic) load. If two goroutines call `CreateTable` concurrently, both could read the same value from `c.nextTableID` before either's `atomic.AddUint32` executes, resulting in duplicate OIDs. The correct pattern would be `oid := atomic.AddUint32(&c.nextTableID, 1) - 1`.

**Severity:** Critical (concurrent CreateTable calls can produce duplicate OIDs, corrupting the catalog)

---

## 4. Executors

### Bug 4.1: Aggregation HAVING clause skips last valid row

**File:** `/home/ryo/work/SamehadaDB/lib/execution/executors/aggregation_executor.go`, lines 237-249
**Description:** The HAVING filter loop at line 237 uses `!e.ahtIterator.IsNextEnd()` as the loop condition. `IsNextEnd()` returns true when `index+1 >= len(keys)`, meaning the loop stops when the current element is the last one. If the last element satisfies the HAVING predicate, it is correctly returned. But if the last element does NOT satisfy the HAVING predicate, the loop never processes it -- instead it falls through to line 240 where `IsEnd()` returns false (since `index` was not advanced past the end), and the non-matching row is incorrectly returned. In short: the last row in the aggregation hash table bypasses HAVING filtering.

**Severity:** Major (HAVING clause does not filter the last aggregation group, returning incorrect query results)

### Bug 4.2: `AggregateHTIterator.Next()` returns wrong value

**File:** `/home/ryo/work/SamehadaDB/lib/execution/executors/aggregation_executor.go`, lines 35-38
**Description:** `Next()` increments `index` and then returns `it.index >= keyLen`. The return value is meant to indicate whether the iterator has reached the end, but callers (like the HAVING filter loop and the result emission at line 248) call `Next()` and ignore the return value. The semantics are confusing but the return value is not used in practice, so the actual bug is in the HAVING filter logic (Bug 4.1 above).

**Severity:** Minor (confusing API, but return value is unused)

### Bug 4.3: Hash join `Init` returns early on nil left tuple without cleaning up

**File:** `/home/ryo/work/SamehadaDB/lib/execution/executors/hash_join_executor.go`, lines 90-91
**Description:** If `e.left.Next()` returns a nil tuple (which can happen on error per SeqScanExecutor), `Init()` returns immediately. The `tmpPageID` from a previously allocated temporary page (line 95-96 check) would not be unpinned if a page was allocated just before the nil tuple was returned. Additionally, the error from `e.left.Next()` is silently ignored (the error return value is captured but never checked in the for loop at line 89).

**Severity:** Minor (resource leak on error path; error silently ignored)

### Bug 4.4: Hash join ignores errors from child executor

**File:** `/home/ryo/work/SamehadaDB/lib/execution/executors/hash_join_executor.go`, line 89
**Description:** The `Init` loop `for leftTuple, done, _ := e.left.Next()` discards the error. If the child executor returns an error, the hash join silently produces partial results from whatever tuples were successfully read before the error.

**Severity:** Minor (silent data loss on error during hash table build phase)

### Bug 4.5: Hash join temporary pages not unpinned for the last page

**File:** `/home/ryo/work/SamehadaDB/lib/execution/executors/hash_join_executor.go`, lines 89-114
**Description:** When building the hash table, the code unpins the previous temp page only when a new page is needed (lines 95-97). The last temp page used is never unpinned because the loop ends after the last tuple is inserted. This temp page remains pinned until `DeallocatePage` is called in `Next()` (lines 132-133), but `DeallocatePage` may not properly handle a still-pinned page.

**Severity:** Major (last temporary page remains pinned in buffer pool; may prevent eviction or cause issues)

### Bug 4.6: Aggregation hash table uses only hash value as key -- hash collisions cause incorrect grouping

**File:** `/home/ryo/work/SamehadaDB/lib/execution/executors/aggregation_executor.go`, lines 153-166
**Description:** `InsertCombine` uses `HashValuesOnAggregateKey(aggKey)` (a uint32 MurmurHash) as the map key. Two different aggregate keys that hash to the same uint32 value will be treated as the same group, silently combining their aggregated values. Unlike the join hash table which stores all matching entries per hash bucket and re-validates, the aggregation hash table stores exactly one key per hash value.

**Severity:** Major (hash collisions produce silently incorrect aggregation results; probability increases with more distinct groups)

---

## 5. Utilities (samehada_util.go)

### Bug 5.1: Self-assignment `SlotNum = SlotNum` is a no-op

**File:** `/home/ryo/work/SamehadaDB/lib/samehada/samehada_util/samehada_util.go`, line 95
**Description:** Line 95 reads `SlotNum = SlotNum`, which is a self-assignment and has no effect. Looking at the function context (`UnpackUint64toRID`), lines 90-91 copy bytes from `packedDataInBytes[4:]` into `buf[2:]` to extract `PageID`, then line 93 copies from `packedDataInBytes[:4]` into `buf[2:]` for `SlotNum`. However, `buf` was never zeroed between the two copy operations -- `buf[0]` and `buf[1]` still contain the residual bytes from the PageID extraction. Since `buf` is `make([]byte, 4)` and only `buf[2:]` is overwritten, `buf[0:2]` retains whatever was written there for `PageID`. This means `SlotNum` is read with corrupt high bytes. The self-assignment at line 95 was likely meant to be something like clearing the high bytes, but as written it does nothing. The paired function `PackRIDtoUint64` at line 58 packs `SlotNum` into the first 4 bytes and `PageID` into the last 4 bytes, so the unpack should extract bytes `[0:4]` as SlotNum and `[4:8]` as PageID. Looking more carefully: line 90 copies `packedDataInBytes[4:]` into `buf[2:]` -- but buf is 4 bytes, so `buf[2:]` is 2 bytes. So `PageID` only uses the last 2 bytes of the packed 4-byte value, losing the high 2 bytes. Similarly for `SlotNum`. This entire function is subtly broken for values exceeding 16 bits.

**Severity:** Major (SlotNum has corrupted high bytes; RID unpacking produces incorrect results for PageID/SlotNum values exceeding 16 bits -- though for the uint32-packed variant this is the same limitation as `PackRIDtoUint32`, the uint64 version claims to use the full 4 bytes but the unpack logic only reads 2 bytes of each)

---

## 6. Request Manager (request_manager.go)

### Bug 6.1: Race condition on `isExecutionActive`

**File:** `/home/ryo/work/SamehadaDB/lib/samehada/request_manager.go`, lines 63, 103
**Description:** `StopTh()` sets `isExecutionActive = false` from one goroutine, while `Run()` reads it from another goroutine, without synchronization (no mutex, no atomic). This is a data race.

**Severity:** Minor (data race on boolean; in practice likely works on x86 due to hardware memory model, but technically undefined behavior in Go)

### Bug 6.2: Potential queue underflow in `RetrieveRequest`

**File:** `/home/ryo/work/SamehadaDB/lib/samehada/request_manager.go`, lines 52-55
**Description:** `RetrieveRequest` accesses `execQue[0]` without checking if the queue is empty. It is called from `executeQuedTxns` (line 69) which is only called when `len(reqManager.execQue) > 0` (line 107). However, `executeQuedTxns` is called while holding `queMutex`, so this is currently safe. No bug.

**Severity:** N/A (safe due to caller check)

### Bug 6.3: Unbounded retry for aborted transactions

**File:** `/home/ryo/work/SamehadaDB/lib/samehada/request_manager.go`, lines 75-78
**Description:** When a transaction is aborted due to concurrency control (`QueryAbortedErr`), it is re-inserted at the head of the queue. If two transactions perpetually conflict with each other (livelock scenario), they will be endlessly retried with no backoff, no retry limit, and no notification to the caller. The caller's channel will block forever.

**Severity:** Major (potential livelock with no backoff or retry limit; caller goroutine blocks indefinitely)

---

## 7. Checkpoint Manager (checkpoint_manager.go)

### Bug 7.1: Race condition on `isCheckpointActive`

**File:** `/home/ryo/work/SamehadaDB/lib/concurrency/checkpoint_manager.go`, lines 62, 66
**Description:** `StopCheckpointTh()` writes `isCheckpointActive = false` from one goroutine while the checkpoint goroutine reads it via `IsCheckpointActive()` -- without any synchronization. This is a data race.

**Severity:** Minor (data race on boolean; same reasoning as Bug 6.1)

### Bug 7.2: Checkpoint flushes dirty pages before flushing WAL

**File:** `/home/ryo/work/SamehadaDB/lib/concurrency/checkpoint_manager.go`, lines 49-53
**Description:** `BeginCheckpoint` calls `FlushAllDirtyPages()` (line 49) before `logManager.Flush()` (line 53). The Write-Ahead Logging (WAL) protocol requires that log records be flushed to disk **before** the corresponding dirty data pages. By flushing pages first, a crash between lines 49 and 53 would leave dirty pages on disk whose log records have not yet been persisted, violating the WAL invariant. If recovery is needed, those page modifications would appear on disk without corresponding log records, making it impossible to undo uncommitted changes.

**Severity:** Critical (violates WAL protocol; crash during checkpoint can make recovery impossible for uncommitted transactions)

---

## 8. Statistics Updater (statistics_updater.go)

### Bug 8.1: Race condition on `isUpdaterActive`

**File:** `/home/ryo/work/SamehadaDB/lib/concurrency/statistics_updater.go`, lines 64, 68
**Description:** Same pattern as checkpoint manager and request manager -- `isUpdaterActive` is read and written from different goroutines without synchronization.

**Severity:** Minor (data race on boolean)

### Bug 8.2: `UpdateAllTablesStatistics` commits even on partial failure

**File:** `/home/ryo/work/SamehadaDB/lib/concurrency/statistics_updater.go`, lines 40-53
**Description:** The `defer` at line 41 ensures `Commit` is always called. If `stat.Update` returns an error at line 47, the function returns early at line 50 but the deferred `Commit` still executes. This commits a transaction that only partially updated statistics. The comment on line 48 acknowledges this ("already updated table's statistics are not rollbacked"), but this means some tables have fresh statistics while others have stale ones, which can cause the query optimizer to make inconsistent decisions.

**Severity:** Minor (partial statistics update committed; acknowledged in code comments but can cause suboptimal query plans)

### Bug 8.3: Statistics updater reads table data concurrently with DML without isolation

**File:** `/home/ryo/work/SamehadaDB/lib/concurrency/statistics_updater.go`, lines 40-53
**Description:** The statistics updater begins a transaction and iterates all tables to gather statistics. Under SS2PL-NW, it should acquire shared locks on rows it reads. However, there is no indication that the table scan in `stat.Update` acquires proper locks. If the statistics scan does not acquire locks, it reads data that may be concurrently modified by other transactions, leading to inconsistent statistics. If it does acquire locks, it could conflict with ongoing DML operations and abort, but the abort case is not handled (the defer always commits).

**Severity:** Minor (statistics accuracy issue; does not affect data correctness)

---

## Summary of Critical/Major Findings

| # | Severity | Component | Description |
|---|----------|-----------|-------------|
| 2.1 | Critical | Log Recovery | ABORT records not handled; double-undo corrupts data |
| 3.1 | Critical | Catalog | `nextTableID=1` after recovery causes OID collision |
| 3.3 | Critical | Catalog | Non-atomic read-then-increment of `nextTableID` causes duplicate OIDs |
| 7.2 | Critical | Checkpoint | Dirty pages flushed before WAL, violating WAL protocol |
| 1.2 | Major | WAL | Data race on `nextLSN`/`persistentLSN` via unsynchronized getters |
| 2.3 | Major | Log Recovery | No CLR during undo; crash during recovery causes repeated undo |
| 2.4 | Major | Log Recovery | `NewTablePage` redo skips LSN check, may re-initialize pages |
| 3.2 | Major | Catalog | Dual mutex gap allows inconsistent catalog reads; lockless map access in insertTable |
| 4.1 | Major | Aggregation | HAVING clause does not filter last aggregation group |
| 4.5 | Major | Hash Join | Last temporary page never unpinned during hash table build |
| 4.6 | Major | Aggregation | Hash collisions cause incorrect grouping |
| 5.1 | Major | Utilities | `UnpackUint64toRID` self-assignment + corrupted high bytes |
| 6.3 | Major | Request Mgr | Unbounded retry on abort with no backoff (livelock risk) |
