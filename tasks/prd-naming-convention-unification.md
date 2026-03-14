# PRD: SamehadaDB Naming Convention Unification Refactoring (Migration to camelCase)

## Introduction

SamehadaDB is a learning-oriented compact RDBMS implemented in Go, originating from "go-bustub," a Go port of CMU's educational RDBMS "BusTub." The codebase consists of approximately 151 Go source files totaling around 29,000 lines of code.

The current codebase has inconsistent naming conventions where three styles coexist:

1. **Go-standard camelCase** (e.g., `tableIds`, `nextTableId`, `pageTable`, `freeList`)
2. **snake_case** (e.g., `log_manager`, `lock_manager`, `first_tuple`, `new_tuple`)
3. **Exported snake_case with uppercase prefix** (e.g., `Log_manager`, `Lock_manager`)

This refactoring unifies all naming to **camelCase (lowerCamelCase / PascalCase)** in compliance with Go's standard naming conventions (Effective Go).

## Goals

- Unify all variable names, struct member names, and parameter names across the codebase to camelCase
- Adhere to Go's idiomatic naming conventions (Effective Go), including acronym rules
- Ensure zero functional degradation — all existing tests must pass at every stage
- Continuously verify build and test success throughout the process
- Resolve all naming-related warnings from `golint` / `go vet`

## User Stories

### US-001: Rename snake_case struct members to camelCase
**Description:** As a developer, I want all struct member names to follow camelCase so that the codebase is consistent and idiomatic Go.

**Acceptance Criteria:**
- [ ] All unexported struct members use `lowerCamelCase` (e.g., `log_manager` → `logManager`)
- [ ] All exported struct members use `PascalCase` (e.g., `Log_manager` → `LogManager`)
- [ ] All references to renamed members are updated across the entire codebase
- [ ] `go build` succeeds after each package is refactored
- [ ] `go test ./... -short` passes after each package is refactored

### US-002: Rename snake_case function parameters to camelCase
**Description:** As a developer, I want all function and method parameter names to follow camelCase so that function signatures are consistent.

**Acceptance Criteria:**
- [ ] All function/method parameters use `lowerCamelCase` (e.g., `log_manager` → `logManager`, `lock_manager` → `lockManager`)
- [ ] All usages of renamed parameters within function bodies are updated
- [ ] `go build` succeeds after each package is refactored
- [ ] `go test ./... -short` passes after each package is refactored

### US-003: Rename snake_case local variables to camelCase
**Description:** As a developer, I want all local variable names to follow camelCase so that the code reads idiomatically.

**Acceptance Criteria:**
- [ ] All local variables use `lowerCamelCase` (e.g., `first_tuple` → `firstTuple`, `new_tuple` → `newTuple`)
- [ ] No functional changes — only renaming
- [ ] `go build` succeeds after each package is refactored
- [ ] `go test ./... -short` passes after each package is refactored

### US-004: Apply Effective Go acronym rules
**Description:** As a developer, I want acronyms in identifiers to be all-uppercase or all-lowercase consistently, per Effective Go conventions.

**Acceptance Criteria:**
- [ ] `Id` suffixes/prefixes are changed to `ID` (e.g., `tableIds` → `tableIDs`, `nextTableId` → `nextTableID`, `pageId` → `pageID`)
- [ ] Other acronyms (`URL`, `HTTP`, `SQL`, `API`, `RPC`, `LSN`, `OID`, `BPM`) follow the same rule
- [ ] All references to renamed identifiers are updated across the codebase
- [ ] `go build` succeeds after each package is refactored
- [ ] `go test ./... -short` passes after each package is refactored

### US-005: Fix snake_case constants to PascalCase
**Description:** As a developer, I want any constants using snake_case to follow Go-idiomatic PascalCase.

**Acceptance Criteria:**
- [ ] Any `const` using snake_case is renamed to PascalCase
- [ ] All references to renamed constants are updated
- [ ] `go build` succeeds
- [ ] `go test ./... -short` passes

### US-006: Verify full build and test suite at completion
**Description:** As a developer, I want to confirm that the entire refactoring has zero functional impact.

**Acceptance Criteria:**
- [ ] `cd lib; go test ./... -v -short` passes with all tests succeeding (with `EnableOnMemStorage = true`)
- [ ] `cd server; go build -o samehada-db-server main.go` succeeds
- [ ] No snake_case variable names or parameter names remain in the codebase
- [ ] Naming-related warnings from `golint` are resolved

## Functional Requirements

- FR-1: All unexported struct members must use `lowerCamelCase` (e.g., `log_manager` → `logManager`)
- FR-2: All exported struct members must use `PascalCase` (e.g., `Log_manager` → `LogManager`)
- FR-3: All function/method parameters must use `lowerCamelCase` (e.g., `lock_manager` → `lockManager`)
- FR-4: All local variables must use `lowerCamelCase` (e.g., `first_tuple` → `firstTuple`)
- FR-5: Constants using snake_case must be renamed to PascalCase
- FR-6: Acronyms must follow Effective Go rules — all-uppercase or all-lowercase consistently (e.g., `pageID` / `PageID`, not `pageId` / `PageId`). Applies to: `ID`, `URL`, `HTTP`, `SQL`, `API`, `RPC`, `LSN`, `OID`, `BPM`, etc.
- FR-7: Type names and exported function/method names must retain Go-idiomatic PascalCase (no changes expected unless they contain snake_case or incorrect acronym casing)
- FR-8: Every reference to a renamed identifier must be updated across all files in the repository
- FR-9: Build and test verification must be performed after completing each phase

## Non-Goals

- No functional changes to the codebase — this is purely a naming refactoring
- No changes to package structure or file organization
- No changes to algorithms, data structures, or business logic
- No addition of new tests (existing tests serve as the regression safety net)
- No changes to the `EnableOnMemStorage` constant (must remain `true`)
- No changes to third-party dependencies or Go module configuration

## Before/After Conversion Examples

### BufferPoolManager struct

**Before:**
```go
type BufferPoolManager struct {
    diskManager      disk.DiskManager
    pages            []*page.Page
    replacer         *ClockReplacer
    freeList         []FrameID
    reUsablePageList []types.PageID
    pageTable        map[types.PageID]FrameID
    log_manager      *recovery.LogManager   // ← snake_case
    mutex            *sync.Mutex
}
```

**After:**
```go
type BufferPoolManager struct {
    diskManager      disk.DiskManager       // No change (already camelCase)
    pages            []*page.Page           // No change
    replacer         *ClockReplacer         // No change
    freeList         []FrameID              // No change
    reUsablePageList []types.PageID         // No change
    pageTable        map[types.PageID]FrameID // No change
    logManager       *recovery.LogManager   // ← snake_case → camelCase
    mutex            *sync.Mutex            // No change
}
```

### Catalog struct

**Before:**
```go
type Catalog struct {
    bpm             *buffer.BufferPoolManager
    tableIds        map[uint32]*TableMetadata
    tableNames      map[string]*TableMetadata
    nextTableId     uint32
    tableHeap       *access.TableHeap
    Log_manager     *recovery.LogManager    // ← exported snake_case
    Lock_manager    *access.LockManager     // ← exported snake_case
    tableIdsMutex   *sync.Mutex
    tableNamesMutex *sync.Mutex
}
```

**After:**
```go
type Catalog struct {
    bpm             *buffer.BufferPoolManager // No change
    tableIDs        map[uint32]*TableMetadata // tableIds → tableIDs (acronym rule)
    tableNames      map[string]*TableMetadata // No change
    nextTableID     uint32                    // nextTableId → nextTableID
    tableHeap       *access.TableHeap         // No change
    LogManager      *recovery.LogManager      // Log_manager → LogManager
    LockManager     *access.LockManager       // Lock_manager → LockManager
    tableIDsMutex   *sync.Mutex               // tableIdsMutex → tableIDsMutex
    tableNamesMutex *sync.Mutex               // No change
}
```

### Function parameters and local variables

**Before:**
```go
func BootstrapCatalog(bpm *buffer.BufferPoolManager, log_manager *recovery.LogManager, lock_manager *access.LockManager, txn *access.Transaction) *Catalog {
    first_tuple := tuple.NewTupleFromSchema(row, TableCatalogSchema())
    new_tuple := tuple.NewTupleFromSchema(row, ColumnsCatalogSchema())
}
```

**After:**
```go
func BootstrapCatalog(bpm *buffer.BufferPoolManager, logManager *recovery.LogManager, lockManager *access.LockManager, txn *access.Transaction) *Catalog {
    firstTuple := tuple.NewTupleFromSchema(row, TableCatalogSchema())
    newTuple := tuple.NewTupleFromSchema(row, ColumnsCatalogSchema())
}
```

## Technical Considerations

### Target Directory and Package Structure
```
lib/
├── catalog/          # Table catalog, metadata management
├── common/           # Common constants, configuration (config.go, etc.)
├── concurrency/      # Statistics updater, checkpoint manager
├── container/        # Data structures: B-tree, SkipList, HashTable
│   ├── btree/
│   ├── skip_list/
│   └── hash/
├── errors/           # Error type definitions
├── execution/        # Execution engine (executors, expressions, plans)
├── materialization/  # Materialization
├── parser/           # SQL parser
├── planner/          # Query planner, optimizer
├── recovery/         # Log management, recovery
├── samehada/         # Main entry point
├── storage/          # Storage engine
│   ├── access/       # Table heap, transactions, lock management
│   ├── buffer/       # Buffer pool manager
│   ├── disk/         # Disk manager
│   ├── index/        # Index implementations
│   ├── page/         # Page management
│   ├── table/        # Table, column, schema
│   └── tuple/        # Tuple management
├── testing/          # Test utilities
└── types/            # Type definitions (PageID, etc.)

server/               # REST API server (main.go)
```

### Naming Convention Rules

| Context | Convention | Example |
|---|---|---|
| Unexported struct members | `lowerCamelCase` | `logManager`, `lockManager` |
| Exported struct members | `PascalCase` | `LogManager`, `LockManager` |
| Function parameters | `lowerCamelCase` | `logManager`, `firstTuple` |
| Local variables | `lowerCamelCase` | `newTuple`, `pageTable` |
| Constants | `PascalCase` | `MaxPoolSize`, `DefaultTimeout` |
| Types | `PascalCase` | `PageID`, `FrameID` |
| Exported functions | `PascalCase` | `BootstrapCatalog` |
| Acronyms (unexported) | all-lowercase | `pageID`, `sqlParser` |
| Acronyms (exported) | all-uppercase | `PageID`, `SQLParser` |

### Build and Test Commands

Verification commands to run continuously during refactoring:

- **Build check:** `cd server; go build -o samehada-db-server main.go`
- **Unit tests:** `cd lib; go test ./... -v -short`

**Critical:** Tests must be run with on-memory storage enabled (`EnableOnMemStorage = true` in `lib/common/config.go`). This is the default setting and must NOT be changed.

### External API Impact

- Signature changes to exported APIs in packages such as `lib/samehada/` must be handled carefully with impact analysis
- Changes to exported struct member names (e.g., `Log_manager` → `LogManager`) affect external consumers and require special attention
- The `server/` package references exported names from `lib/` and must be updated in the final phase

## Recommended Work Order

Refactoring should proceed in dependency order (foundational packages first) to minimize cross-package breakage:

### Phase 1: `lib/types/` and `lib/common/`
Foundational types and constants. These are imported by nearly every other package, so they must be done first.

### Phase 2: `lib/errors/` and `lib/storage/page/`
Error definitions and page management. Low-level building blocks for storage.

### Phase 3: `lib/storage/disk/` and `lib/storage/buffer/`
Disk and buffer layers. Depend on types, common, and page.

### Phase 4: `lib/storage/table/` and `lib/storage/tuple/`
Table and tuple layers. Depend on page and buffer.

### Phase 5: `lib/storage/access/` and `lib/storage/index/`
Access and index layers. Depend on table, tuple, and buffer.

### Phase 6: `lib/catalog/`
Catalog management. Depends on access, storage, and recovery.

### Phase 7: `lib/container/`
Data structures (hash, skip_list, btree). Relatively self-contained.

### Phase 8: `lib/recovery/` and `lib/concurrency/`
Recovery and concurrency control. Depend on storage and access layers.

### Phase 9: `lib/parser/`, `lib/planner/`, and `lib/execution/`
Frontend and execution engine. Depend on catalog, storage, and container.

### Phase 10: `lib/samehada/`, `lib/materialization/`, and `lib/testing/`
Miscellaneous packages. Depend on many other packages.

### Phase 11: `server/`
REST API server. Final phase — depends on `lib/` packages.

**At the completion of each phase:** run `go build` and `go test ./... -short` and confirm all tests pass.

## Success Metrics

- All struct member names, variable names, and parameter names follow camelCase (`lowerCamelCase` / `PascalCase`)
- Effective Go acronym rules are applied consistently (e.g., `Id` → `ID`)
- `cd lib; go test ./... -v -short` passes with all tests succeeding (with `EnableOnMemStorage = true`)
- `cd server; go build -o samehada-db-server main.go` succeeds
- No snake_case variable names or parameter names remain in the codebase
- Naming-related warnings from `golint` are resolved

## Open Questions

- Are there any generated files (e.g., protobuf, parser generators) that should be excluded from renaming?
- Should comments referencing old names (e.g., in TODOs or documentation) be updated as well?
- Are there any external tools or scripts (outside `server/`) that depend on exported names from `lib/`?
