# Buffer Pool & Page Management Bug Review

Review date: 2026-03-20

---

## 1. Buffer Pool Manager (`lib/storage/buffer/buffer_pool_manager.go`)

### BPM-1: Mutex not held on error path after ReadPage failure (non-DeallocatedPageErr)

**File:** `lib/storage/buffer/buffer_pool_manager.go`, lines 120-122
**Description:** When `ReadPage` returns an error that is NOT `DeallocatedPageErr`, the function returns `nil` without calling `b.mutex.Unlock()`. The mutex was acquired at line 49 and is still held at this point (it was not released between lines 71-119 on this code path).
**Why it is a bug:** This causes a permanent deadlock. Any subsequent call to FetchPage, UnpinPage, NewPage, or any other method that acquires `b.mutex` will block forever.
**Severity:** Critical

Additionally, on this same error path, the frame obtained from `getFrameID()` (line 71) is consumed but never returned to the free list or replacer. If the previous page was evicted (lines 77-104), it has already been deleted from the `pageTable` and its buffer returned. The frame slot is now lost.

### BPM-2: Buffer leak on DeallocatedPageErr path in FetchPage

**File:** `lib/storage/buffer/buffer_pool_manager.go`, lines 114-118
**Description:** When `ReadPage` returns `DeallocatedPageErr`, the mutex is properly unlocked, but the buffer allocated at line 109 (`data := b.GetBuffer()`) is never returned to the pool. Similarly, the frame obtained from `getFrameID()` is consumed but the frame slot `b.pages[*frameID]` is left as the old (already-returned) page or as nil, and the frame is not placed back in the free list or replacer.
**Why it is a bug:** Each occurrence permanently leaks one buffer from `sync.Pool` and permanently loses one frame slot from the buffer pool. Over time this shrinks the effective pool size.
**Severity:** Major

### BPM-3: FlushPage accesses page data outside mutex protection

**File:** `lib/storage/buffer/buffer_pool_manager.go`, lines 208-225
**Description:** `FlushPage` acquires the BPM mutex to look up the page in the page table (line 210), but then releases the mutex at line 212 before calling `pg.Data()`, `pg.SetIsDirty(false)`, and `b.diskManager.WritePage()`. Between the unlock and the actual write, another thread could evict this page from the buffer pool (via FetchPage or NewPage), causing the page's buffer to be returned to the pool and potentially reused.
**Why it is a bug:** The `WritePage` call could write corrupted/reused buffer data to disk. Also, `SetIsDirty(false)` is called on a page that might have already been evicted and repurposed, clearing the dirty flag of a different logical page. Note that the eviction paths in FetchPage (line 94) and NewPage (line 252) do acquire WLatch before writing, but FlushPage does not acquire any page latch before reading data and clearing the dirty flag.
**Severity:** Major

### BPM-4: DeallocatePage (isNoWait=true) does not free the frame or handle pin count

**File:** `lib/storage/buffer/buffer_pool_manager.go`, lines 297-312
**Description:** When `isNoWait` is true, `DeallocatePage` deletes the page from `pageTable` and adds the pageID to `reUsablePageList`, but it does not: (a) unpin the frame, (b) return the frame to the replacer or free list, (c) return the buffer via `ReturnBuffer`. The frame slot in `b.pages[frameID]` still holds the old page object, but the page is no longer reachable via `pageTable`.
**Why it is a bug:** The frame is orphaned -- it cannot be found by pageID lookup (deleted from pageTable), but it also cannot be victimized because it may still have a non-zero pin count and is not returned to the free list. This permanently leaks a frame from the buffer pool.
**Severity:** Major

### BPM-5: DeallocatePage (isNoWait=false) is a no-op for buffered pages

**File:** `lib/storage/buffer/buffer_pool_manager.go`, lines 297-312
**Description:** When `isNoWait` is false, `DeallocatePage` only writes a log record and flushes. It does not mark the in-memory page as deallocated (`SetIsDeallocated(true)` is never called). The page remains in the buffer pool as a normal page. The deallocation is supposed to occur "when the page becomes victim" (per the comment at line 299), but during eviction (lines 90-91, 248-249), the code checks `currentPage.IsDeallocated()` which will always be false since nobody set it.
**Why it is a bug:** Pages that are "deallocated" via the normal (non-NoWait) path are never actually reclaimed. Their page IDs are never added to `reUsablePageList` upon eviction, causing gradual exhaustion of page ID space. The only callers that actually set `IsDeallocated(true)` are in test code, not in the DeallocatePage flow.
**Severity:** Major

### BPM-6: UnpinPage can clear dirty flag incorrectly

**File:** `lib/storage/buffer/buffer_pool_manager.go`, lines 170-174
**Description:** The dirty flag logic is: if `pg.IsDirty() || isDirty` then set dirty to true, else set dirty to false. This means if thread A previously set the page dirty via an earlier UnpinPage call, but thread B later calls `UnpinPage(pageID, false)`, it will NOT clear the flag (correct). However, if thread A calls `UnpinPage(pageID, true)` and then thread B calls `UnpinPage(pageID, false)` before the page is flushed, thread B will NOT clear the flag because `pg.IsDirty()` is still true from thread A's call. This part is correct. BUT: `isDirty` and `IsDirty()` are plain bool fields read/written without atomics or page latch. Under concurrent UnpinPage calls, there is a data race on the `isDirty` field.
**Why it is a bug:** The `isDirty` field on `Page` (line 37 of page.go) is a plain `bool`. Multiple goroutines can call `UnpinPage` concurrently for different pages that map to different frames, but if two goroutines call UnpinPage for the same page concurrently (possible since pin count can be > 1), both read `pg.IsDirty()` and write `pg.SetIsDirty()` outside of any page-level latch. While the BPM mutex serializes the reads, `SetIsDirty` at line 171/173 is called while still holding the BPM mutex, so this specific race may be mitigated by the BPM mutex. After closer analysis, since the entire IsDirty check and SetIsDirty are within the BPM mutex lock (lines 146-175), this is actually serialized. **Revised: Not a bug under the current BPM mutex scope.**

### BPM-7: `reUsablePageList` is not thread-safe outside BPM mutex in several accessors

**File:** `lib/storage/buffer/buffer_pool_manager.go`, lines 391-398
**Description:** `SetReusablePageIDs` and `GetReusablePageIDs` access the `reUsablePageList` slice without acquiring `b.mutex`. These are called during recovery, but if any other operation were to run concurrently, there would be a data race.
**Why it is a bug:** While recovery is likely single-threaded, there is no enforcement of this. If these methods were called concurrently with NewPage (which reads `reUsablePageList` at line 270), there would be a data race on the slice header.
**Severity:** Minor (recovery is typically single-threaded)

### BPM-8: getFrameID unlocks/relocks mutex for debug printing, creating a TOCTOU window

**File:** `lib/storage/buffer/buffer_pool_manager.go`, lines 367-371
**Description:** When `Victim()` returns nil, `getFrameID` unlocks `b.mutex`, calls `PrintBufferUsageState` (which re-acquires the mutex), then re-acquires the mutex. Although the code panics immediately after, if the panic were ever removed or caught, the unlock/relock creates a window where another goroutine could modify the buffer pool state.
**Why it is a bug:** Mostly a latent issue since it panics, but the pattern of unlock-print-relock is dangerous. If the panic were removed, it would create inconsistent state.
**Severity:** Minor (currently followed by panic)

---

## 2. Clock Replacer (`lib/storage/buffer/clock_replacer.go`)

### CR-1: ClockReplacer has a mutex field but never uses it

**File:** `lib/storage/buffer/clock_replacer.go`, line 23
**Description:** The `ClockReplacer` struct has a `mutex *sync.Mutex` field (line 23), and it is initialized in `NewClockReplacer` (line 93), but none of the methods (Victim, Unpin, Pin, Size, isContain) ever lock or unlock it. All thread safety is delegated to the BPM's mutex.
**Why it is a bug:** The ClockReplacer is not independently thread-safe. If it were ever used without the BPM mutex held (e.g., `PrintList` at line 87 is called from `PrintReplacerInternalState` which is called from `getFrameID` after an unlock/relock cycle at line 370), there would be data races on the circular list.
**Severity:** Minor (currently always called under BPM mutex, but the unused mutex suggests the original design intended independent locking)

### CR-2: Victim() panics instead of returning nil when the list is empty

**File:** `lib/storage/buffer/clock_replacer.go`, lines 28-32
**Description:** When `c.cList.size == 0`, `Victim()` panics instead of returning nil. The `nil` return is commented out. The caller `getFrameID` (line 363) checks for nil return and has a code path for it, but that path is unreachable because Victim panics first.
**Why it is a bug:** This converts what should be a recoverable "buffer pool full" condition into an unrecoverable crash. The original design (as evidenced by the commented-out `return nil` and the caller's nil check) intended graceful degradation.
**Severity:** Major

### CR-3: clockHand may point to a freed node after size becomes 0

**File:** `lib/storage/buffer/clock_replacer.go`, line 44, and `lib/storage/buffer/circular_list.go`, lines 75-80
**Description:** In `Victim()`, after finding the victim, the clock hand is advanced to `currentNode.next` (line 44) BEFORE `c.cList.remove(currentNode.key)` is called (line 46). If this removal causes size to become 0, the circular list sets `head = nil` and `tail = nil` (lines 76-77), but `clockHand` still points to the now-removed node whose `next` pointer is stale (it pointed to itself when it was the last node, but head/tail are now nil).
**Why it is a bug:** On the next call to `Unpin`, if `size == 1` after insertion, `clockHand` is set to `&c.cList.head` (line 57). This is correct. But if `Victim()` is called again before any `Unpin`, `clockHand` dereferences a stale pointer. However, `Victim()` does check `size == 0` first and panics (CR-2), so this is unreachable under current code. If CR-2 were fixed to return nil, this would become a latent bug.
**Severity:** Minor (currently masked by CR-2's panic)

---

## 3. Page (`lib/storage/page/page.go`)

### PG-1: `isDirty` and `isDeallocated` fields are not atomic or latch-protected

**File:** `lib/storage/page/page.go`, lines 37, 79-94
**Description:** The `isDirty` and `isDeallocated` fields are plain `bool` values. `SetIsDirty` and `IsDirty` (and the Deallocated equivalents) have no synchronization. While `pinCount` uses `atomic.AddInt32`/`atomic.LoadInt32`, `isDirty` and `isDeallocated` do not.
**Why it is a bug:** When `FlushAllDirtyPages` reads `IsDirty()` under an RLatch (BPM line 337), and a concurrent `UnpinPage` writes `SetIsDirty()` under the BPM mutex but without the page latch, there is a data race on the `isDirty` field. The Go memory model requires explicit synchronization for shared mutable state. In practice on x86, plain bool reads/writes are atomic at the hardware level, so this is unlikely to cause visible corruption, but it is technically a data race detectable by the Go race detector.
**Severity:** Minor

### PG-2: WLatchMap and RLatchMap are plain maps, not thread-safe

**File:** `lib/storage/page/page.go`, lines 41-43
**Description:** `WLatchMap` and `RLatchMap` are exported `map[int32]bool` fields. They have a `RLatchMapMutex` for RLatchMap but not for WLatchMap. However, all the methods that would write to them (AddWLatchRecord, RemoveWLatchRecord, AddRLatchRecord, RemoveRLatchRecord) have their bodies commented out (lines 180-195).
**Why it is a bug:** Currently not a runtime issue since the map operations are commented out. However, the maps are still allocated (line 103-104, 108) wasting memory, and the exported fields are accessed in panic messages (BPM lines 83, 245) via `currentPage.WLatchMap` and `currentPage.RLatchMap`. Reading these maps from the BPM while another goroutine could theoretically write to them (if the debug code were re-enabled) would be a race. As-is, they are only read, so no current bug.
**Severity:** Minor (dead code with memory waste; latent race if debug code is re-enabled)

No other bugs found in `page.go`. The RLatch/WLatch implementations correctly delegate to `sync.RWMutex`.

---

## 4. Table Page (`lib/storage/access/table_page.go`)

### TP-1: Unreachable code after panic in InsertTuple

**File:** `lib/storage/access/table_page.go`, lines 85-86
**Description:** Line 85 has `panic("tuple size is illegal!!!")` and line 86 has `return nil, ErrEmptyTuple`. The return statement is unreachable because `panic` never returns.
**Why it is a bug:** The intended behavior is ambiguous. If the intent was to return an error (allowing the caller to handle it gracefully), the panic prevents that. If the intent was to crash on invalid input, the return is dead code. Given that the check is `tuple.Size() <= 0` and the error is `ErrEmptyTuple`, the likely intent was to return an error. The panic makes this a crash instead of a recoverable error.
**Severity:** Minor (the panic is overly aggressive for what could be a recoverable error, and the dead return is confusing)

### TP-2: Same issue in UpdateTuple

**File:** `lib/storage/access/table_page.go`, lines 224-226
**Description:** `panic("tuple size is illegal!!!")` at line 225 makes any subsequent code unreachable. However, in this case there is no return after the panic, so there is no dead code -- but the panic converts what UpdateTuple's callers might handle as an error into a crash.
**Severity:** Minor

### TP-3: GetTupleFirstRID and GetNextTupleRID include deleted-marked tuples

**File:** `lib/storage/access/table_page.go`, lines 639-670
**Description:** Both `GetTupleFirstRID` (line 645) and `GetNextTupleRID` (line 664) check `tp.GetTupleSize(ii) > 0` to determine if a slot is valid. However, a delete-marked tuple has its size ORed with `deleteMask` (0x80000000), which makes it a very large uint32 value that is `> 0`. This means these functions return RIDs of delete-marked tuples.
**Why it is a bug:** Iteration via `GetTupleFirstRID`/`GetNextTupleRID` will yield delete-marked tuples. The callers (table heap iterator) must then handle delete-marked tuples themselves via GetTuple, which does check for the delete flag. This is technically by-design (the callers do handle it), but it causes unnecessary work and confusing error paths.
**Severity:** Minor (callers handle it, but the semantics are surprising)

No bugs found in the slot management math, free space pointer management, or the delete/undelete flag operations. The copy operations in `ApplyDelete` and `UpdateTuple` correctly shift tuples and update offsets.

---

## 5. Table Heap (`lib/storage/access/table_heap.go`)

### TH-1: `lastPageID` is not protected by any synchronization

**File:** `lib/storage/access/table_heap.go`, lines 27, 125, 289
**Description:** The `lastPageID` field is read at line 77 (`t.bpm.FetchPage(t.lastPageID)`) and written at line 125 (`t.lastPageID = currentPageID`) and line 289 (`t.lastPageID = t.firstPageID`). There is no mutex or atomic operation protecting this field.
**Why it is a bug:** If two goroutines concurrently call `InsertTuple` (or one calls InsertTuple while another calls ApplyDelete), there is a data race on `lastPageID`. On x86, a PageID (int32) write is likely atomic at the hardware level, but Go's memory model does not guarantee this without explicit synchronization. The race detector would flag this.
**Severity:** Minor (likely benign on x86 since PageID is 32 bits, but technically a data race)

### TH-2: GetFirstTuple uses WLatch but only reads

**File:** `lib/storage/access/table_heap.go`, lines 355-377
**Description:** `GetFirstTuple` acquires WLatch (line 360) for each page it visits, but only calls `GetTupleFirstRID()` which is a read-only operation. A RLatch would suffice and would allow better concurrency.
**Why it is a bug:** Not a correctness bug, but an unnecessary serialization point that degrades concurrent performance.
**Severity:** Minor (performance, not correctness)

### TH-3: InsertTuple does not handle nil return from FetchPage for lastPageID

**File:** `lib/storage/access/table_heap.go`, line 77
**Description:** `CastPageAsTablePage(t.bpm.FetchPage(t.lastPageID))` could return nil if the page was deallocated or eviction fails. The result is immediately used on line 80 (`currentPage.WLatch()`) without a nil check, which would cause a nil pointer dereference panic.
**Why it is a bug:** If `lastPageID` points to a deallocated page (possible since `lastPageID` is set without coordination), or if the buffer pool is exhausted, this crashes.
**Severity:** Major

### TH-4: InsertTuple does not handle nil return from FetchPage in the next-page loop

**File:** `lib/storage/access/table_heap.go`, line 93
**Description:** Inside the loop, `CastPageAsTablePage(t.bpm.FetchPage(nextPageID))` could return nil. The result is used on line 94 (`nextPage.WLatch()`) without a nil check.
**Why it is a bug:** If FetchPage returns nil (buffer pool full, page deallocated), this crashes with a nil pointer dereference. Additionally, on this crash path, the current page's pin and WLatch are never released.
**Severity:** Major

### TH-5: InsertTuple does not handle nil return from NewPage

**File:** `lib/storage/access/table_heap.go`, line 105
**Description:** `t.bpm.NewPage()` could return nil if the buffer pool is full and no pages can be evicted. The result is used on line 106 without a nil check, leading to nil pointer dereference. On this crash path, the current page's pin and WLatch are leaked.
**Severity:** Major

---

## 6. Pin/Unpin Balance Analysis (Cross-Codebase)

### PU-1: FetchPage without UnpinPage in `samehada.go` (index reconstruction)

**File:** `lib/samehada/samehada.go`, line 67
**Description:** `bpm.FetchPage(indexHeaderPageID)` fetches the hash table header page, uses its data to iterate block pages, but never calls `UnpinPage` for the header page.
**Why it is a bug:** The header page remains pinned forever after index reconstruction. This permanently consumes one frame in the buffer pool.
**Severity:** Major

### PU-2: FetchPage without UnpinPage in `linear_probe_hash_table_iterator.go` constructor

**File:** `lib/container/hash/linear_probe_hash_table_iterator.go`, line 26
**Description:** `bpm.FetchPage(blockPageID)` is called in `newHashTableIterator` to fetch the initial block page. The unpin for this page happens in the caller (e.g., line 93 of `linear_probe_hash_table.go`). However, if the iterator is created but the loop in the caller encounters an error/break before reaching the unpin, the page could leak. Looking at the three callers (GetValue, Insert, Remove), all of them unpin `iterator.blockID` after the loop, so this is balanced in practice.
**Why it is a bug:** Not a current bug -- all call sites properly unpin. Noted for completeness.
**Severity:** N/A (no bug found)

### PU-3: FetchPage in `table_heap_iterator.go` Next() with nil nextPage

**File:** `lib/storage/access/table_heap_iterator.go`, lines 51-57
**Description:** When `nextPage` is nil (line 52), the code correctly unpins `currentPage` (line 54) and returns. This path is handled correctly.
**Severity:** N/A (no bug found)

### PU-4: FetchPage without UnpinPage in `skip_list_header_page.go`

**File:** `lib/storage/page/skip_list_page/skip_list_header_page.go`, line 137
**Description:** `FetchAndCastToHeaderPage` calls `bpm.FetchPage(pageID)` and the comment says "caller must call UnpinPage." This delegates unpin responsibility to callers. This is a documented pattern, not a bug.
**Severity:** N/A (delegated to caller by documented contract)

### PU-5: Missing UnpinPage on early-exit error paths in `log_recovery.go` Undo phase

**File:** `lib/recovery/log_recovery/log_recovery.go`, lines 266-272
**Description:** In the UPDATE case of Undo (line 267), if `UpdateTuple` fails (err != nil), the code panics at line 270. If this panic were converted to an error return, the page fetched at line 267 would be unpinned at line 272. As written, the panic prevents the unpin, but since it is a panic (process termination), pin balance is moot.
**Severity:** Minor (unpin skipped before panic, but panic terminates the process anyway)

### PU-6: DeallocatePage(isNoWait=true) deletes from pageTable without unpinning

**File:** `lib/storage/buffer/buffer_pool_manager.go`, lines 300-306
**Description:** (Same as BPM-4 above.) When a page is deallocated with isNoWait=true, it is removed from the page table but the frame's pin count is not decremented, the frame is not returned to the replacer/free list, and the buffer is not reclaimed.
**Severity:** Major (same as BPM-4)

---

## Summary of Findings

| ID | Component | Severity | Short Description |
|------|-----------|----------|-------------------|
| BPM-1 | Buffer Pool Manager | Critical | Mutex not unlocked on ReadPage error path (deadlock) |
| BPM-2 | Buffer Pool Manager | Major | Buffer + frame leak on DeallocatedPageErr path |
| BPM-3 | Buffer Pool Manager | Major | FlushPage accesses page data outside mutex without page latch |
| BPM-4 | Buffer Pool Manager | Major | DeallocatePage(isNoWait=true) orphans the frame |
| BPM-5 | Buffer Pool Manager | Major | DeallocatePage(isNoWait=false) never marks page as deallocated |
| BPM-7 | Buffer Pool Manager | Minor | reUsablePageList accessors not mutex-protected |
| BPM-8 | Buffer Pool Manager | Minor | getFrameID unlock/relock for debug print |
| CR-1 | Clock Replacer | Minor | Mutex field allocated but never used |
| CR-2 | Clock Replacer | Major | Victim() panics instead of returning nil |
| CR-3 | Clock Replacer | Minor | Stale clockHand after last-element removal |
| PG-1 | Page | Minor | isDirty/isDeallocated not atomic |
| PG-2 | Page | Minor | Debug latch maps allocated but unused |
| TP-1 | Table Page | Minor | Unreachable return after panic in InsertTuple |
| TP-3 | Table Page | Minor | RID iterators include delete-marked tuples |
| TH-1 | Table Heap | Minor | lastPageID data race |
| TH-2 | Table Heap | Minor | WLatch used where RLatch suffices in GetFirstTuple |
| TH-3 | Table Heap | Major | No nil check after FetchPage(lastPageID) |
| TH-4 | Table Heap | Major | No nil check after FetchPage(nextPageID) in loop |
| TH-5 | Table Heap | Major | No nil check after NewPage() in InsertTuple |
| PU-1 | Pin/Unpin Balance | Major | samehada.go index reconstruction leaks header page pin |
