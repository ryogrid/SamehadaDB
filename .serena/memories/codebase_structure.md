# Codebase Structure

## Top-level
- `lib/` - Core database library (main Go module)
- `server/` - REST API server (separate Go module, references lib via replace directive)
- `demo-client/` - Simple browser-based client (HTML/JS)
- `scripts/` - Utility scripts
- `.github/workflows/ci.yaml` - CI pipeline

## lib/ Directory Structure
- `common/` - Constants, config, utilities (PageSize=4096, logging levels, etc.)
- `types/` - Basic type definitions (PageID, TxnID, LSN, ColumnTypeID, etc.)
- `storage/` - Storage layer
  - `page/` - Page types (data page, hash table pages, skip list pages)
  - `disk/` - Disk manager (real and virtual/in-memory)
  - `tuple/` - Tuple representation
  - `buffer/` - Buffer pool manager with clock replacer
  - `access/` - Table heap, table page, transaction, lock manager, transaction manager
  - `index/` - Index implementations (hash, skip list, B-tree)
  - `table/` - Schema and column definitions
- `catalog/` - Table catalog, table metadata, statistics
- `execution/` - Query execution layer
  - `plans/` - Plan nodes (seq scan, index scan, insert, delete, update, joins, etc.)
  - `executors/` - Executor implementations corresponding to plans
  - `expression/` - Expression types (comparison, logical ops, column/constant values)
- `parser/` - SQL parser (visitor pattern over pingcap/parser AST)
- `planner/` - Query planner and Selinger optimizer
- `recovery/` - Log manager, log records, log recovery
- `concurrency/` - Checkpoint manager, statistics updater
- `materialization/` - Temporary tuple storage for materialization
- `container/` - Data structure implementations (hash table, skip list, B-tree wrapper)
- `samehada/` - Top-level DB interface (SamehadaDB struct, SamehadaInstance, request manager)
- `testing/` - Test utilities

## server/ Directory
- `main.go` - Server entry point (REST API on port 19999)
- `signalhandle/` - Signal handling
