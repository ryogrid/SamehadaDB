# Code Style and Conventions

## Naming
- **CamelCase** throughout (recently refactored from snake_case - see commit "Ralph/camelcase refactoring")
- Exported types/functions: PascalCase (e.g., `TableHeap`, `NewTableHeap`, `GetFirstPageID`)
- Unexported fields: camelCase (e.g., `firstPageID`, `bpm`, `logManager`)
- Package names: snake_case with underscores (e.g., `samehada_util`, `index_constants`, `skip_list_page`)
- Test directories follow pattern: `*_test/` (e.g., `executor_test/`, `samehada_test/`)

## Code Style
- Standard Go formatting (gofmt)
- No strict docstring convention; comments are informal and sometimes minimal
- License/attribution comments at top of files ported from other projects
- No type hints beyond Go's type system
- Error handling: mix of returning errors and panics (common.SH_Assert for assertions)
- Fields sometimes have inconsistent alignment in structs

## Architecture Patterns
- Visitor pattern for SQL parsing
- Executor pattern (Volcano model) for query execution - each executor has Init() and Next() methods
- Plan nodes separate from executors
- Buffer pool manager pattern with pin/unpin semantics
- Write-Ahead Logging (WAL) for recovery
- Two-Phase Locking for concurrency control
- Latch (mutex) + Lock (transaction-level) distinction

## Testing
- Tests use Go's standard testing package
- Test utilities in `lib/testing/` package
- Tests frequently use `testing.Short()` to skip long-running tests
- Test files: `*_test.go` in dedicated test directories or alongside source
