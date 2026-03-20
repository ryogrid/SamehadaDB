# Index & Container Layer Bug Review

## 1. Skip List Index (`lib/storage/index/skip_list_index.go`)

### Bug 1.1: GetRangeScanIterator returns iterator that is used after lock is released

- **File:** `lib/storage/index/skip_list_index.go`, lines 127-130
- **Description:** `GetRangeScanIterator` acquires `updateMtx.RLock()` with `defer RUnlock()`, then returns an iterator from `sl.container.Iterator()`. The iterator's constructor (`NewSkipListIterator`) calls `initRIDList` which traverses the skip list, collecting all matching entries into an in-memory slice. This traversal completes within the lock, and the returned iterator serves from the snapshot. While the snapshot approach is safe for iteration, the lock is held for the entire traversal duration. This is functionally correct but worth noting for completeness.
- **Verdict:** No bug found on closer analysis. The iterator materializes results during construction while the RLock is held.

### Bug 1.2: ScanKey holds RLock across iteration but uses manual unlock instead of defer

- **File:** `lib/storage/index/skip_list_index.go`, lines 89-97
- **Description:** `ScanKey` calls `updateMtx.RLock()` on line 89 but unlocks on line 97 with a direct call rather than `defer`. If `rangeItr.Next()` or any operation within the loop panics, the RLock will never be released, causing all future `UpdateEntry` calls (which need a write lock) to deadlock permanently.
- **Why it's a bug:** A panic during iteration permanently deadlocks the index for updates.
- **Severity:** Minor (panics during normal iteration are unlikely, but the pattern is unsafe)

No other concurrency or correctness bugs found in this file.

---

## 2. Unique Skip List Index (`lib/storage/index/uniq_skip_list_index.go`)

### Bug 2.1: No uniqueness constraint enforcement on InsertEntry

- **File:** `lib/storage/index/uniq_skip_list_index.go`, lines 34-43
- **Description:** `InsertEntry`/`insertEntryInner` calls `slidx.container.Insert()` directly without first checking whether the key already exists. The skip list's `Insert` method (in `skip_list_block_page.go` line 239-276) silently overwrites the existing value if the key is found (it does a replacement, not a rejection). This means the "unique" index does not actually enforce uniqueness -- it silently replaces the old RID with the new one.
- **Why it's a bug:** An index named `UniqSkipListIndex` should enforce uniqueness constraints. Two concurrent inserts of rows with the same key value will both succeed, with the second silently overwriting the first. The overwritten row's RID is lost from the index, making it unreachable via index scan while still physically present in the table. This is data corruption from an RDBMS perspective.
- **Severity:** Critical

### Bug 2.2: TOCTOU race between ScanKey and InsertEntry for uniqueness checks

- **File:** `lib/storage/index/uniq_skip_list_index.go`, lines 67-80 (ScanKey) and lines 34-43 (insertEntryInner)
- **Description:** Even if a higher-level caller implements a "check then insert" pattern using `ScanKey` followed by `InsertEntry`, both methods only acquire `updateMtx.RLock()`. Since RLock is shared, two concurrent transactions can both call `ScanKey`, both find the key absent, and both proceed to `InsertEntry` concurrently. The result is that only one RID survives in the index.
- **Why it's a bug:** The locking protocol (RLock for both read and insert, WLock only for UpdateEntry) makes it impossible for callers to implement a safe check-then-insert pattern externally.
- **Severity:** Critical

### Bug 2.3: ScanKey comment refers to MaxUint32 but code compares against MaxUint64

- **File:** `lib/storage/index/uniq_skip_list_index.go`, line 76
- **Description:** The comment says `when packed_vale == math.MaxUint32 => true, keyVal is not found on index` but the code actually compares against `math.MaxUint64` (line 75). The comment is wrong, but the code is correct (the skip list `GetValue` returns `math.MaxUint64` for not-found). This is just a misleading comment, not a runtime bug.
- **Severity:** Minor (misleading comment, no runtime impact)

---

## 3. BTree Index (`lib/storage/index/btree_index.go`)

### Bug 3.1: ScanKey holds RLock across iteration but uses manual unlock instead of defer

- **File:** `lib/storage/index/btree_index.go`, lines 161-173
- **Description:** Identical pattern to Skip List Index Bug 1.2. `ScanKey` calls `rwMtx.RLock()` on line 161 and `rwMtx.RUnlock()` on line 173 without using `defer`. If any operation within the iteration loop panics, the lock will never be released.
- **Why it's a bug:** Same as Bug 1.2 -- a panic during iteration permanently deadlocks the index for updates.
- **Severity:** Minor

### Bug 3.2: BtreeIndexIterator.Next returns success before checking `ok`

- **File:** `lib/storage/index/btree_index.go`, lines 31-41
- **Description:** In the `Next()` method, `packedRID` is checked for length (`len(packedRID) != 6`) at line 32 before `ok` is checked at line 39. When the BLTree iterator reaches the end, it may return `ok == false` with an empty or invalid `packedRID`. The length check at line 32 returns `done=true` which happens to be the correct result, but the logic is fragile -- it relies on the BLTree iterator returning an invalid-length packedRID when iteration is complete, rather than checking the canonical `ok` flag first.
- **Why it's a bug:** The control flow is inverted. If the BLTree iterator ever returns `ok == false` with a valid 6-byte packedRID (e.g., the last entry's data), the code would proceed to decode it and return `done=false` with stale data, then return `done=true` on the next call. However, this appears to work in practice because the BLTree iterator returns empty data on exhaustion.
- **Severity:** Minor (latent; depends on BLTree iterator contract)

No other bugs found in this file. The RLock/WLock usage pattern for InsertEntry/DeleteEntry/UpdateEntry mirrors the skip list index and is correct for its purpose (allowing concurrent inserts/deletes but serializing updates).

---

## 4. Linear Probe Hash Table Index (`lib/storage/index/linear_probe_hash_table_index.go`)

### Bug 4.1: No concurrency protection at the index layer

- **File:** `lib/storage/index/linear_probe_hash_table_index.go`, lines 14-29 and 43-67
- **Description:** Unlike `SkipListIndex` and `BTreeIndex`, `LinearProbeHashTableIndex` has no `sync.RWMutex` or any locking mechanism. The underlying `LinearProbeHashTable` does have its own `tableLatch`, so individual operations (Insert, Remove, GetValue) are each atomic. However, there is no `UpdateEntry` implementation (line 70: panics with "not implemented yet"), and there is no way to compose atomic operations (e.g., delete+insert) under a single lock.
- **Why it's a bug:** While the panic in `UpdateEntry` prevents silent corruption, any code path that needs to atomically update an entry in this index type will crash the system. If this index type is used on a column that gets updated, it will panic.
- **Severity:** Major (unusable for mutable columns)

No other concurrency bugs found. The underlying hash table has its own WLock for Insert/Remove and RLock for GetValue.

---

## 5. Skip List Container (`lib/container/skip_list/skip_list.go`)

### Bug 5.1: GetValue ignores FindNode failure, nil pointer dereference

- **File:** `lib/container/skip_list/skip_list.go`, line 224
- **Description:** `GetValue` calls `FindNode` and discards the `isSuccess` return value: `_, node, _, _ := sl.FindNode(key, SkipListOpGet)`. When `FindNode` returns `isSuccess == false`, it returns `nil` for `node` (line 156: `return false, nil, nil, nil`). The very next line (226) calls `node.FindEntryByKey(key)` which will panic with a nil pointer dereference.
- **Why it's a bug:** Under concurrent modifications where a node is removed between the `predOfPredLSN` snapshot and the re-latch (FindNode lines 152-173), `FindNode` can legitimately return `isSuccess == false`. Unlike `Insert` and `Remove` which have retry loops, `GetValue` has no retry logic and will crash.
- **Severity:** Critical

### Bug 5.2: FindNodeWithEntryIdxForItr ignores FindNode failure, nil pointer dereference

- **File:** `lib/container/skip_list/skip_list.go`, lines 202-208
- **Description:** Same pattern as Bug 5.1. `FindNodeWithEntryIdxForItr` calls `FindNode` on line 204 and ignores `isSuccess`. When it returns false with `node == nil`, line 207 calls `node.FindEntryByKey(key)` which will panic.
- **Why it's a bug:** This method is called from `SkipListIterator.initRIDList` (skip_list_iterator.go line 46). Creating an iterator during concurrent modifications can crash the system.
- **Severity:** Critical

### Bug 5.3: FindNode pin/unpin imbalance for startNode in the node-remove path

- **File:** `lib/container/skip_list/skip_list.go`, lines 131-149
- **Description:** In the `FindNode` method, when `pred` is the startNode, it is not unpinned on line 132 (`if pred.GetPageID() != sl.getStartNode().GetPageID()`). However, when the code enters the node-remove path (lines 138-173), it checks the same condition on line 147 before unpinning `pred`. If `pred` IS the startNode at this point, it won't be unpinned, but a new pin is acquired via `FetchAndCastToBlockPage` on line 153 (for `predOfPredId`). The `predOfPredId` was set to the previous `pred`'s ID before `pred` was advanced. If `predOfPredId` equals `InvalidPageID` (its initial value at line 106), `FetchAndCastToBlockPage` will be called with `InvalidPageID`, likely panicking or returning nil (which is checked on line 154, but then the extra pin from line 104 is leaked).
- **Why it's a bug:** When the first node after startNode is a single-entry node that needs removal, `predOfPredId` may still be `InvalidPageID` (if `pred` was never advanced beyond startNode before reaching level 0). This leads to fetching an invalid page. Even if this specific scenario is prevented by data invariants, the pin count management around startNode is fragile.
- **Severity:** Major

### Bug 5.4: rand.Float32() is not safe for concurrent use

- **File:** `lib/container/skip_list/skip_list.go`, lines 331-338
- **Description:** `GetNodeLevel()` calls `rand.Float32()` which uses the global default `rand.Source`. Before Go 1.20, the global source was not safe for concurrent use and could produce duplicate or correlated sequences. From Go 1.20 onward, the global source is safe. If this code runs on Go < 1.20 with concurrent inserts, the random level generation can produce corrupted values.
- **Why it's a bug:** Depends on Go version. On Go < 1.20, concurrent calls to `rand.Float32()` can race on the shared global source.
- **Severity:** Minor (Go version dependent; may be a non-issue on modern Go)

---

## 6. Linear Probe Hash Table Container (`lib/container/hash/linear_probe_hash_table.go`)

### Bug 6.1: Hash collision causes incorrect results (stores hash, not key)

- **File:** `lib/container/hash/linear_probe_hash_table.go`, lines 82, 122, 162
- **Description:** The hash table stores `uint64(hash)` as the key in each slot (lines 122, 128) rather than the original key bytes. When looking up values (line 82) or removing entries (line 162), it compares `blockPage.KeyAt(offset) == uint64(hash)`. Two different original keys that produce the same 64-bit murmur3 hash will be treated as identical. `GetValue` will return values from both keys mixed together, and `Remove` could delete entries belonging to a different key.
- **Why it's a bug:** While 64-bit hash collisions are rare, they are possible. When a collision occurs: (1) `GetValue` returns wrong results (values from an unrelated key), (2) `Remove` may delete an entry belonging to a different key, and (3) `Insert` duplicate detection (`ValueAt(offset) == value`) may reject a legitimate insert if a colliding key has the same value. This is a correctness issue, not just a performance issue.
- **Severity:** Major (rare but causes silent data corruption when it occurs)

### Bug 6.2: Insert does not return an error when the table is full

- **File:** `lib/container/hash/linear_probe_hash_table.go`, lines 114-138
- **Description:** The `Insert` method's loop breaks when `bucket == originalBucketIndex && offset == originalBucketOffset` (line 135-136), which means it wrapped around the entire table without finding an empty or deleted slot. When this happens, `err` is still its zero value (`nil`), so the caller receives no indication that the insert failed. The entry is silently dropped.
- **Why it's a bug:** When the hash table is full, inserts are silently lost. The caller (the index layer) has no way to detect this. This means rows can be inserted into the table but their index entries are lost, making them invisible to index scans.
- **Severity:** Major

### Bug 6.3: Pin leak on header page in GetValue and Insert when FetchPage returns nil

- **File:** `lib/container/hash/linear_probe_hash_table.go`, lines 68, 102
- **Description:** `GetValue` (line 68) and `Insert` (line 102) call `ht.bpm.FetchPage(ht.headerPageID).Data()` without nil-checking the FetchPage return. If the buffer pool is exhausted and FetchPage returns nil, calling `.Data()` on nil will panic. The header page pin from FetchPage (when it succeeds) is properly unpinned at the end, but there is no protection against a nil return.
- **Why it's a bug:** Under heavy load when the buffer pool is full, FetchPage can return nil, causing a nil pointer dereference panic.
- **Severity:** Minor (requires buffer pool exhaustion)

### Bug 6.4: Iterator's next() does not nil-check FetchPage

- **File:** `lib/container/hash/linear_probe_hash_table_iterator.go`, line 47
- **Description:** When the iterator crosses a block boundary, it calls `itr.bpm.FetchPage(itr.blockID).Data()` without nil-checking. If FetchPage returns nil, this panics.
- **Why it's a bug:** Same as Bug 6.3 -- buffer pool exhaustion during iteration causes a panic.
- **Severity:** Minor (requires buffer pool exhaustion)

### Bug 6.5: Remove does not break after finding and removing the target entry

- **File:** `lib/container/hash/linear_probe_hash_table.go`, lines 161-171
- **Description:** When `Remove` finds a matching entry (line 162-163), it calls `blockPage.Remove(offset)` but does NOT break out of the loop. It continues scanning. This is intentional for supporting non-unique keys (the hash table claims to support them). However, combined with the `Insert` duplicate check on line 115 which prevents inserting the same (hash, value) pair twice, there should never be more than one matching entry. The continued scan is unnecessary work but not incorrect.
- **Verdict:** Not a bug, just wasteful. The continued scan is harmless since at most one match exists.

---

## Summary of Findings by Severity

### Critical (3)
1. **Bug 2.1** -- `UniqSkipListIndex` does not enforce uniqueness; silently overwrites entries
2. **Bug 5.1** -- `SkipList.GetValue` nil dereference when `FindNode` fails under concurrency
3. **Bug 5.2** -- `SkipList.FindNodeWithEntryIdxForItr` nil dereference when `FindNode` fails under concurrency

### Major (4)
4. **Bug 2.2** -- TOCTOU race in unique index: RLock for both check and insert allows concurrent duplicate inserts
5. **Bug 4.1** -- `LinearProbeHashTableIndex.UpdateEntry` panics (unimplemented); unusable for mutable columns
6. **Bug 5.3** -- `FindNode` pin/unpin imbalance in node-remove path with startNode
7. **Bug 6.1** -- Hash table stores hash instead of key; hash collisions cause silent data corruption
8. **Bug 6.2** -- Hash table `Insert` silently drops entries when table is full

### Minor (5)
9. **Bug 1.2** -- `SkipListIndex.ScanKey` non-defer unlock; panic leaks lock
10. **Bug 2.3** -- Misleading comment (MaxUint32 vs MaxUint64)
11. **Bug 3.1** -- `BTreeIndex.ScanKey` non-defer unlock; panic leaks lock
12. **Bug 3.2** -- `BtreeIndexIterator.Next` checks packedRID length before ok flag
13. **Bug 5.4** -- `rand.Float32()` concurrency safety depends on Go version
14. **Bug 6.3** -- Hash table nil-dereference on FetchPage failure
15. **Bug 6.4** -- Hash table iterator nil-dereference on FetchPage failure
