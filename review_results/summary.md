# SamehadaDB Bug-Focused Code Review — Summary

Review date: 2026-03-20
Scope: Bug and correctness issues only. No refactoring or style suggestions. Phantom reads excluded (documented limitation).

---

## Total Findings

| Severity | Count |
|----------|-------|
| **Critical** | 8 |
| **Major** | 22 |
| **Minor** | 25 |
| **Total** | 55 |

### By review area:

| Area | Critical | Major | Minor | Total |
|------|----------|-------|-------|-------|
| Transaction Isolation & Lock Manager | 0 | 0 | 7 | 7 |
| Buffer Pool & Page Management | 1 | 8 | 8 | 17 |
| Index & Container Layer | 4 | 5 | 7 | 16 |
| Recovery / WAL / Log Recovery | 1 | 3 | 4 | 8 |
| Catalog | 2 | 1 | 0 | 3 |
| Executors | 0 | 3 | 3 | 6 |
| Utilities & Supporting Components | 0 | 2 | 4 | 6 |
| go vet | 0 | 0 | 2 | 2 |

---

## Critical Findings (8)

| # | ID | Component | File | Description |
|---|-----|-----------|------|-------------|
| 1 | BPM-1 | Buffer Pool Manager | `buffer_pool_manager.go:120` | **Mutex not unlocked on ReadPage error path** — causes permanent deadlock + frame leak |
| 2 | IDX-2 | Unique Skip List Index | `uniq_skip_list_index.go:34` | **No uniqueness enforcement** — silently overwrites entries, losing RIDs from index |
| 3 | IDX-3 | Unique Skip List Index | `uniq_skip_list_index.go:67` | **TOCTOU race** — RLock for both read and insert makes safe check-then-insert impossible |
| 4 | SL-1 | Skip List Container | `skip_list.go:224` | **GetValue nil dereference** — ignores FindNode failure, crashes under concurrent modifications |
| 5 | SL-2 | Skip List Container | `skip_list.go:204` | **Iterator nil dereference** — same as SL-1 but in iterator construction path |
| 6 | REC-1 | Log Recovery | `log_recovery.go:174` | **ABORT records not handled** — already-aborted txns are double-undone, corrupting data |
| 7 | CAT-1 | Catalog | `table_catalog.go:116` | **nextTableID=1 after recovery** — OID collision when database has >2 tables |
| 8 | CKP-2 | Checkpoint Manager | `checkpoint_manager.go:49` | **WAL protocol violation** — dirty pages flushed before WAL, making crash recovery impossible |

Note: CAT-3 (non-atomic `nextTableID` read-then-increment) is also Critical in severity but is closely related to CAT-1. Both affect the same field and would likely be fixed together.

---

## Top Major Findings (selected, 22 total)

| ID | Component | File | Description |
|-----|-----------|------|-------------|
| BPM-2 | Buffer Pool Manager | `buffer_pool_manager.go:114` | Buffer + frame leak on DeallocatedPageErr path |
| BPM-3 | Buffer Pool Manager | `buffer_pool_manager.go:208` | FlushPage accesses data outside mutex without latch |
| BPM-4 | Buffer Pool Manager | `buffer_pool_manager.go:297` | DeallocatePage(isNoWait=true) orphans frame |
| BPM-5 | Buffer Pool Manager | `buffer_pool_manager.go:297` | DeallocatePage(isNoWait=false) never marks page |
| CR-2 | Clock Replacer | `clock_replacer.go:28` | Victim() panics instead of returning nil |
| TH-3/4/5 | Table Heap | `table_heap.go:77,93,105` | Missing nil checks on FetchPage/NewPage — nil dereference panics with resource leaks |
| PU-1 | Pin/Unpin Balance | `samehada.go:67` | Index reconstruction leaks header page pin |
| IDX-7 | Hash Table Index | `linear_probe_hash_table_index.go:70` | UpdateEntry panics "not implemented" |
| SL-3 | Skip List Container | `skip_list.go:131` | Pin/unpin imbalance in FindNode node-remove path |
| HT-1 | Hash Table Container | `linear_probe_hash_table.go:82` | Stores hash not key — collisions cause silent corruption |
| HT-2 | Hash Table Container | `linear_probe_hash_table.go:114` | Insert silently drops entries when full |
| WAL-2 | Log Manager | `log_manager.go:48` | Data race on nextLSN/persistentLSN |
| REC-3 | Log Recovery | `log_recovery.go:226` | No CLR during undo — crash during recovery repeats undo |
| REC-4 | Log Recovery | `log_recovery.go:180` | NewTablePage redo skips LSN check |
| CAT-2 | Catalog | `table_catalog.go:177` | Dual mutex gap — inconsistent catalog reads |
| CAT-3 | Catalog | `table_catalog.go:167` | Non-atomic nextTableID — duplicate OIDs |
| EXEC-1 | Aggregation Executor | `aggregation_executor.go:237` | HAVING clause skips last group |
| EXEC-5 | Hash Join Executor | `hash_join_executor.go:89` | Last temp page never unpinned |
| EXEC-6 | Aggregation Executor | `aggregation_executor.go:153` | Hash-as-key causes incorrect grouping |
| UTIL-1 | Utilities | `samehada_util.go:95` | UnpackUint64toRID corrupts high bytes |
| REQ-2 | Request Manager | `request_manager.go:75` | Unbounded retry livelock risk |

---

## Cross-Cutting Themes

### 1. Missing nil checks after FetchPage / NewPage
Found in: table_heap.go (3 sites), linear_probe_hash_table.go (2 sites), hash_table_iterator.go (1 site). When the buffer pool is full or a page is deallocated, FetchPage returns nil. Most callers do not check, leading to nil-dereference panics and resource leaks (pins, latches) on the crash path.

### 2. Data races on boolean flags
Pattern: `isExecutionActive`, `isCheckpointActive`, `isUpdaterActive`, `isEnableLogging` — all plain `bool` fields read/written across goroutines without synchronization. Found in 4 components. Should use `atomic.Bool` or a mutex.

### 3. Hash-as-key instead of original key
Both the `LinearProbeHashTable` and `AggregationHashTable` store hash values as keys instead of original key data. Any hash collision silently produces incorrect results. The linear probe table uses 64-bit hash (rare collisions); the aggregation table uses 32-bit hash (more likely collisions with many groups).

### 4. DeallocatePage is fundamentally broken
Both code paths in `DeallocatePage` have issues: the `isNoWait=true` path orphans frames, and the `isNoWait=false` path never marks pages as deallocated. Together with the eviction logic that checks `IsDeallocated()`, page ID space is never reclaimed.

### 5. Recovery subsystem has multiple gaps
ABORT records unhandled (Critical), no CLRs written during undo (Major), NewTablePage redo skips LSN check (Major), checkpoint violates WAL protocol (Critical). A crash during normal operation or during recovery itself can lead to unrecoverable data corruption.

### 6. Catalog concurrency is unsafe
Non-atomic `nextTableID`, dual-mutex inconsistency, and hardcoded `nextTableID=1` after recovery. Concurrent DDL or recovery from a multi-table database risks OID collisions and catalog corruption.

### 7. Transaction isolation is correct
Despite numerous lower-level bugs, the SS2PL-NW isolation guarantee (REPEATABLE READ minus phantoms) is correctly implemented. All data access paths acquire proper locks, strict 2PL is enforced, and locks are released only after durable commit/rollback.

---

## Output Files

| File | Contents |
|------|----------|
| `review_results/go_vet_results.md` | `go vet` output with annotations |
| `review_results/transaction_isolation.md` | Transaction isolation & lock manager review (7 Minor findings) |
| `review_results/general_bugs.md` | All other bug findings organized by component (Parts A-F) |
| `review_results/summary.md` | This file — synthesis with severity counts, top findings, and themes |

---

## Methodology

- **Step 0:** `go vet ./...` on both `lib/` and `server/` modules
- **Step 1:** Transaction isolation review — lock manager, transaction manager, executors (1 agent)
- **Step 2:** Buffer pool & page management review — BPM, clock replacer, page, table page, table heap (1 agent)
- **Step 3:** Index & container layer review — skip list, btree, hash table indices and containers (1 agent)
- **Step 4:** Recovery, executors & remaining components review — WAL, log recovery, catalog, executors, utilities (1 agent)
- **Step 5:** Synthesis — this document

Steps 1-4 ran as 4 parallel review agents. All files were read in full; no test files were included in findings.
