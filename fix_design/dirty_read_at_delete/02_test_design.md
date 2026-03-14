# Test Design: Dirty Read at DELETE Fix

## Test 1: `TestDeleteDoesNotRemoveIndexEntryBeforeCommit`

**Type:** Unit test
**Purpose:** Verify that after a DELETE executes but before commit, the index entry still exists.

**Steps:**
1. Create table with SkipList index, insert rows, commit
2. Txn A: DELETE WHERE a=20 (do NOT commit)
3. Verify: `idx.ScanKey(key=20)` still returns the RID (entry exists)
4. Commit Txn A
5. Verify: `idx.ScanKey(key=20)` returns empty (entry removed)

**Expected behavior:** Index entry persists until commit.

## Test 2: `TestDeleteAbortLeavesIndexIntact`

**Type:** Unit test
**Purpose:** Verify that aborting a DELETE leaves the index entry intact (no rollback needed since entry was never removed).

**Steps:**
1. Create table with SkipList index, insert rows, commit
2. Txn A: DELETE WHERE a=20, then Abort
3. Txn B: point scan via index for a=20
4. Verify: row found (index entry was never removed, mark-delete was rolled back)

**Expected behavior:** After abort, the index entry and tuple are both intact.

## Test 3: `TestConcurrentDeleteAndIndexScan`

**Type:** Stress test (~9 min)
**Purpose:** Verify no dirty reads under concurrent delete and index scan workload.

**Setup:**
- Table with SkipList index, 100 rows (keys 0-99)
- 8 goroutines: 4 deleters (random key, commit/abort randomly), 4 readers (index point scan)

**Execution:**
- ~500 iterations per goroutine
- Each deleter: picks random key → DELETE → randomly commit or abort
- Each reader: picks random key → index point scan → verify result

**Invariant:** No reader observes a missing index entry for a row whose delete hasn't been committed.

**Detection:** Count dirty-read occurrences (should be 0); aborts are OK and expected.

**Run command:** `go test -run TestConcurrentDeleteAndIndexScan -timeout 600s`

## Test 4: Regression

Existing tests that must continue to pass:

| Test | File | Purpose |
|---|---|---|
| `TestAbortWIthDeleteUpdate` | `executor_test.go:1106` | DELETE+UPDATE abort rollback |
| `TestAbortWthDeleteUpdateUniqSkipListIndexCasePointScan` | `uniq_skiplist_index_executor_test.go:304` | DELETE+UPDATE abort with index point scan |
| `TestParallelQueryIssue` | `samehada_test.go:251` | Concurrent INSERT+SELECT via SQL API |
| Full short tests | `cd lib && go test ./... -short` | All existing functionality |
