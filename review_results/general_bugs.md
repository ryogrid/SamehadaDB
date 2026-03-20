# SamehadaDB General Bug Review

Review date: 2026-03-20

This document consolidates bug findings from buffer pool/page management, index/container layer, recovery/WAL, executors, catalog, and supporting components.

---

# Part A: Buffer Pool & Page Management

## A1. Buffer Pool Manager (`lib/storage/buffer/buffer_pool_manager.go`)

### BPM-1: Mutex not unlocked on ReadPage error path — deadlock

**File:** `buffer_pool_manager.go`, lines 120-122
**Description:** When `ReadPage` returns an error that is NOT `DeallocatedPageErr`, the function returns `nil` without calling `b.mutex.Unlock()`. The mutex was acquired at line 49 and is still held. Any subsequent BPM call will block forever.
**Additionally:** The frame obtained from `getFrameID()` is consumed but never returned to the free list or replacer — the frame slot is permanently lost.
**Severity:** Critical

### BPM-2: Buffer + frame leak on DeallocatedPageErr path in FetchPage

**File:** `buffer_pool_manager.go`, lines 114-118
**Description:** When `ReadPage` returns `DeallocatedPageErr`, the mutex is properly unlocked, but the buffer allocated via `GetBuffer()` is never returned to the pool. The frame from `getFrameID()` is consumed but not placed back in the free list or replacer.
**Severity:** Major

### BPM-3: FlushPage accesses page data outside mutex without page latch

**File:** `buffer_pool_manager.go`, lines 208-225
**Description:** `FlushPage` releases the BPM mutex before calling `pg.Data()`, `pg.SetIsDirty(false)`, and `WritePage()`. Between unlock and write, another thread could evict the page, causing the buffer to be reused. FlushPage does not acquire any page latch.
**Severity:** Major

### BPM-4: DeallocatePage (isNoWait=true) orphans the frame

**File:** `buffer_pool_manager.go`, lines 297-312
**Description:** Deletes the page from `pageTable` and adds to `reUsablePageList`, but does not: (a) unpin the frame, (b) return the frame to the replacer/free list, (c) return the buffer. The frame is permanently leaked.
**Severity:** Major

### BPM-5: DeallocatePage (isNoWait=false) never marks page as deallocated

**File:** `buffer_pool_manager.go`, lines 297-312
**Description:** `SetIsDeallocated(true)` is never called. The eviction path checks `IsDeallocated()` which is always false, so page IDs are never reclaimed via `reUsablePageList`.
**Severity:** Major

### BPM-7: `reUsablePageList` accessors not mutex-protected

**File:** `buffer_pool_manager.go`, lines 391-398
**Description:** `SetReusablePageIDs` and `GetReusablePageIDs` access the slice without `b.mutex`. Safe if recovery is single-threaded, but not enforced.
**Severity:** Minor

### BPM-8: getFrameID unlock/relock for debug print creates TOCTOU window

**File:** `buffer_pool_manager.go`, lines 367-371
**Description:** Unlocks mutex, calls debug print (re-acquires mutex), then panics. Latent issue since it panics, but the pattern is dangerous.
**Severity:** Minor

## A2. Clock Replacer (`lib/storage/buffer/clock_replacer.go`)

### CR-1: Mutex field allocated but never used

**File:** `clock_replacer.go`, line 23
**Description:** `mutex *sync.Mutex` is initialized but no method ever acquires it. Thread safety relies entirely on the BPM mutex.
**Severity:** Minor

### CR-2: Victim() panics instead of returning nil on empty list

**File:** `clock_replacer.go`, lines 28-32
**Description:** Converts a recoverable "buffer pool full" condition into a crash. The caller has nil-check code that is unreachable.
**Severity:** Major

### CR-3: Stale clockHand after last-element removal

**File:** `clock_replacer.go`, line 44; `circular_list.go`, lines 75-80
**Description:** After removing the last element, `clockHand` still points to the freed node. Masked by CR-2's panic; would become a bug if CR-2 were fixed.
**Severity:** Minor

## A3. Page (`lib/storage/page/page.go`)

### PG-1: `isDirty` and `isDeallocated` not atomic or latch-protected

**File:** `page.go`, lines 37, 79-94
**Description:** Plain `bool` fields read/written without synchronization. Go race detector would flag this. Benign on x86 in practice.
**Severity:** Minor

### PG-2: Debug latch maps allocated but unused

**File:** `page.go`, lines 41-43
**Description:** `WLatchMap` and `RLatchMap` are allocated but all methods that write to them are commented out. Memory waste.
**Severity:** Minor

## A4. Table Page (`lib/storage/access/table_page.go`)

### TP-1: Unreachable return after panic in InsertTuple

**File:** `table_page.go`, lines 85-86
**Description:** `panic()` at line 85 makes `return nil, ErrEmptyTuple` at line 86 unreachable.
**Severity:** Minor

### TP-2: Same panic-before-return pattern in UpdateTuple

**File:** `table_page.go`, lines 224-226
**Severity:** Minor

### TP-3: RID iterators include delete-marked tuples

**File:** `table_page.go`, lines 639-670
**Description:** `GetTupleSize(ii) > 0` is true for delete-marked tuples (size ORed with `0x80000000`). Callers handle this, but it's surprising.
**Severity:** Minor

## A5. Table Heap (`lib/storage/access/table_heap.go`)

### TH-1: `lastPageID` data race

**File:** `table_heap.go`, lines 27, 125, 289
**Description:** Read and written without synchronization from potentially concurrent goroutines.
**Severity:** Minor

### TH-2: GetFirstTuple uses WLatch but only reads

**File:** `table_heap.go`, lines 355-377
**Description:** RLatch would suffice; WLatch degrades concurrency unnecessarily.
**Severity:** Minor

### TH-3: No nil check after FetchPage(lastPageID)

**File:** `table_heap.go`, line 77
**Description:** If FetchPage returns nil, the next line panics with nil dereference. Pin and WLatch are leaked.
**Severity:** Major

### TH-4: No nil check after FetchPage(nextPageID) in loop

**File:** `table_heap.go`, line 93
**Description:** Same as TH-3, plus current page's pin and WLatch are leaked on crash.
**Severity:** Major

### TH-5: No nil check after NewPage() in InsertTuple

**File:** `table_heap.go`, line 105
**Description:** If buffer pool is full, nil dereference panic. Current page's pin and WLatch are leaked.
**Severity:** Major

## A6. Pin/Unpin Balance

### PU-1: Index reconstruction leaks header page pin

**File:** `lib/samehada/samehada.go`, line 67
**Description:** `FetchPage(indexHeaderPageID)` is called but never unpinned after use.
**Severity:** Major

---

# Part B: Index & Container Layer

## B1. Skip List Index (`lib/storage/index/skip_list_index.go`)

### IDX-1: ScanKey non-defer unlock — panic leaks lock

**File:** `skip_list_index.go`, lines 89-97
**Description:** Manual `RUnlock()` instead of `defer`. A panic during iteration permanently deadlocks the index.
**Severity:** Minor

## B2. Unique Skip List Index (`lib/storage/index/uniq_skip_list_index.go`)

### IDX-2: No uniqueness constraint enforcement — silently overwrites entries

**File:** `uniq_skip_list_index.go`, lines 34-43
**Description:** `InsertEntry` calls `container.Insert()` which silently replaces existing entries with the same key. The overwritten row's RID is lost from the index.
**Severity:** Critical

### IDX-3: TOCTOU race — RLock for both ScanKey and InsertEntry

**File:** `uniq_skip_list_index.go`, lines 67-80 (ScanKey) and 34-43 (InsertEntry)
**Description:** Both methods use RLock, so concurrent check-then-insert patterns are inherently racy. Two transactions can both find the key absent and both insert.
**Severity:** Critical

### IDX-4: Misleading comment (MaxUint32 vs MaxUint64)

**File:** `uniq_skip_list_index.go`, line 76
**Severity:** Minor

## B3. BTree Index (`lib/storage/index/btree_index.go`)

### IDX-5: ScanKey non-defer unlock — same as IDX-1

**File:** `btree_index.go`, lines 161-173
**Severity:** Minor

### IDX-6: BtreeIndexIterator.Next checks length before `ok` flag

**File:** `btree_index.go`, lines 31-41
**Description:** Inverted control flow; relies on BLTree iterator returning invalid-length data on exhaustion.
**Severity:** Minor

## B4. Linear Probe Hash Table Index (`lib/storage/index/linear_probe_hash_table_index.go`)

### IDX-7: UpdateEntry panics — unusable for mutable columns

**File:** `linear_probe_hash_table_index.go`, line 70
**Description:** `panic("not implemented yet")` — any UPDATE on a column using this index type crashes.
**Severity:** Major

## B5. Skip List Container (`lib/container/skip_list/skip_list.go`)

### SL-1: GetValue ignores FindNode failure — nil pointer dereference

**File:** `skip_list.go`, line 224
**Description:** `FindNode` can return `isSuccess == false` with `nil` node under concurrent modifications. `GetValue` ignores the flag and dereferences `nil`.
**Severity:** Critical

### SL-2: FindNodeWithEntryIdxForItr ignores FindNode failure — nil pointer dereference

**File:** `skip_list.go`, lines 202-208
**Description:** Same pattern as SL-1. Called from iterator construction.
**Severity:** Critical

### SL-3: FindNode pin/unpin imbalance in node-remove path

**File:** `skip_list.go`, lines 131-149
**Description:** When the first node after startNode needs removal and `predOfPredId` is still `InvalidPageID`, fetching an invalid page panics or leaks pins.
**Severity:** Major

### SL-4: `rand.Float32()` concurrency safety depends on Go version

**File:** `skip_list.go`, lines 331-338
**Description:** Unsafe on Go < 1.20; safe on Go >= 1.20.
**Severity:** Minor

## B6. Linear Probe Hash Table Container (`lib/container/hash/linear_probe_hash_table.go`)

### HT-1: Hash collision causes incorrect results — stores hash not key

**File:** `linear_probe_hash_table.go`, lines 82, 122, 162
**Description:** Stores `uint64(hash)` as key, not the original bytes. Two different keys with same hash are treated as identical.
**Severity:** Major

### HT-2: Insert silently drops entries when table is full

**File:** `linear_probe_hash_table.go`, lines 114-138
**Description:** Wraps around the table without finding a slot, returns nil error. Index entry is silently lost.
**Severity:** Major

### HT-3: FetchPage nil dereference in GetValue and Insert

**File:** `linear_probe_hash_table.go`, lines 68, 102
**Description:** No nil check on FetchPage return before calling `.Data()`.
**Severity:** Minor

### HT-4: Iterator FetchPage nil dereference

**File:** `linear_probe_hash_table_iterator.go`, line 47
**Severity:** Minor

---

# Part C: Recovery / WAL

## C1. Log Manager (`lib/recovery/log_manager.go`)

### WAL-1: Fragile header pre-write in AppendLogRecord

**File:** `log_manager.go`, lines 117-125
**Description:** Header is written to buffer before the size check. No actual corruption due to offset guard, but fragile.
**Severity:** Minor

### WAL-2: Data race on `nextLSN` / `persistentLSN`

**File:** `log_manager.go`, lines 48-50
**Description:** Getter/setter methods access these fields without holding any lock.
**Severity:** Major

### WAL-3: `isEnableLogging` flag unused and unsynchronized

**File:** `log_manager.go`, lines 88-95, 101
**Description:** Flag exists but `AppendLogRecord` never checks it. Also a data race on the bool.
**Severity:** Minor

### WAL-4: `offset` uint32 subtraction can underflow

**File:** `log_manager.go`, lines 105, 120
**Description:** Unsigned subtraction wraps on overflow, bypassing size check. Hard to trigger in practice.
**Severity:** Minor

## C2. Log Recovery (`lib/recovery/log_recovery/log_recovery.go`)

### REC-1: ABORT records not handled — double-undo corrupts data

**File:** `log_recovery.go`, lines 174-179
**Description:** No case for `recovery.ABORT` in redo phase. Aborted transactions remain in `activeTxn` and are undone again.
**Severity:** Critical

### REC-2: DeallocatePage/ReusePage records pollute activeTxn/lsnMapping

**File:** `log_recovery.go`, lines 115, 129-130
**Description:** Dummy txn `math.MaxInt32` is cleaned up, but `lsnMapping[-1]` entry persists. Mitigated but logically incorrect.
**Severity:** Minor

### REC-3: No CLR during undo — crash during recovery repeats undo

**File:** `log_recovery.go`, lines 226-281
**Description:** Undo phase does not write compensation log records. A crash during recovery makes undo non-idempotent.
**Severity:** Major

### REC-4: `NewTablePage` redo skips LSN check — may re-initialize pages

**File:** `log_recovery.go`, lines 180-188
**Description:** Unconditional `newPage.Init()` without checking `pg.GetLSN() < logRecord.GetLSN()`. Can overwrite valid data.
**Severity:** Major

---

# Part D: Catalog

## D1. Table Catalog (`lib/catalog/table_catalog.go`)

### CAT-1: `nextTableID` hardcoded to 1 after recovery — OID collision

**File:** `table_catalog.go`, line 116
**Description:** Recovery constructs catalog with `nextTableID = 1`. Databases with >2 tables will have OID collisions on next CreateTable.
**Severity:** Critical

### CAT-2: Dual mutex non-atomic update in CreateTable

**File:** `table_catalog.go`, lines 177-182
**Description:** `tableIDs` and `tableNames` maps updated under separate mutexes. Concurrent reads can see inconsistent state. Also, `insertTable` accesses `tableIDs` without holding the mutex.
**Severity:** Major

### CAT-3: Non-atomic read-then-increment of `nextTableID`

**File:** `table_catalog.go`, lines 167-168
**Description:** Plain read of `nextTableID` followed by `atomic.AddUint32`. Concurrent `CreateTable` calls can produce duplicate OIDs.
**Severity:** Critical

---

# Part E: Executors

### EXEC-1: Aggregation HAVING clause skips last group

**File:** `aggregation_executor.go`, lines 237-249
**Description:** Loop condition `!IsNextEnd()` stops before the last element. If the last group doesn't satisfy HAVING, it bypasses filtering and is incorrectly returned.
**Severity:** Major

### EXEC-2: `AggregateHTIterator.Next()` confusing return value

**File:** `aggregation_executor.go`, lines 35-38
**Description:** Return value unused by callers. Not a runtime bug, but confusing API.
**Severity:** Minor

### EXEC-3: Hash join Init() resource leak on nil tuple

**File:** `hash_join_executor.go`, lines 90-91
**Description:** Early return on nil left tuple without unpinning temp page. Error from child executor silently ignored.
**Severity:** Minor

### EXEC-4: Hash join ignores errors from child executor

**File:** `hash_join_executor.go`, line 89
**Description:** Error return from `e.left.Next()` is discarded. Silent partial results.
**Severity:** Minor

### EXEC-5: Hash join last temp page never unpinned

**File:** `hash_join_executor.go`, lines 89-114
**Description:** Last temp page remains pinned after build loop. May prevent eviction.
**Severity:** Major

### EXEC-6: Aggregation hash table uses hash as key — collisions cause incorrect grouping

**File:** `aggregation_executor.go`, lines 153-166
**Description:** `uint32` MurmurHash as map key. Different aggregate keys with same hash are silently merged.
**Severity:** Major

---

# Part F: Utilities & Supporting Components

### UTIL-1: `UnpackUint64toRID` self-assignment + corrupted high bytes

**File:** `samehada_util.go`, line 95
**Description:** `SlotNum = SlotNum` is a no-op. The function only reads 2 bytes of each 4-byte field due to `buf[2:]` copy, corrupting high bytes for values exceeding 16 bits.
**Severity:** Major

### REQ-1: Race condition on `isExecutionActive`

**File:** `request_manager.go`, lines 63, 103
**Description:** Unsynchronized bool read/write across goroutines.
**Severity:** Minor

### REQ-2: Unbounded retry for aborted transactions — livelock risk

**File:** `request_manager.go`, lines 75-78
**Description:** No backoff, retry limit, or notification. Caller channel blocks forever on persistent conflicts.
**Severity:** Major

### CKP-1: Race condition on `isCheckpointActive`

**File:** `checkpoint_manager.go`, lines 62, 66
**Description:** Unsynchronized bool read/write across goroutines.
**Severity:** Minor

### CKP-2: Checkpoint flushes dirty pages before WAL — violates WAL protocol

**File:** `checkpoint_manager.go`, lines 49-53
**Description:** `FlushAllDirtyPages()` before `logManager.Flush()`. A crash between these calls leaves pages on disk without corresponding log records, making recovery impossible.
**Severity:** Critical

### STAT-1: Race condition on `isUpdaterActive`

**File:** `statistics_updater.go`, lines 64, 68
**Severity:** Minor

### STAT-2: Partial statistics commit on failure

**File:** `statistics_updater.go`, lines 40-53
**Description:** Deferred `Commit` runs even after partial failure.
**Severity:** Minor

### STAT-3: Statistics scan may lack proper lock acquisition

**File:** `statistics_updater.go`, lines 40-53
**Description:** Unclear if table scan acquires proper locks; may read uncommitted data.
**Severity:** Minor
