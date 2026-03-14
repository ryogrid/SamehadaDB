# SamehadaDB - Project Overview

## Purpose
SamehadaDB is a learning-oriented compact Relational Database Management System (RDBMS) implemented in Go.
It originated from a Go port of BusTub (CMU's educational RDBMS): go-bustub.

The project prioritizes readability over performance and serves as an educational resource for database system developers.

## Key Features
- Embedded DB (library form, like SQLite) and REST API server mode
- SQL query execution (single query per request)
- Transaction support with SS2PL-NW (Strong Strict 2-Phase Locking - No Wait)
- Transaction isolation level: REPEATABLE READ (DIRTY READ when indexes are used)
- Multiple index types: Hash, SkipList, B-tree
- Query optimization (Selinger optimizer)
- Recovery via logging and checkpointing
- Supported data types: Integer (int32), Float (float32), Varchar (variable length, max ~4KB), Boolean

## Architecture
The project has two Go modules:
1. `lib/` - Core database library (module: github.com/ryogrid/SamehadaDB/lib)
2. `server/` - REST API server (module: github.com/ryogrid/SamehadaDB/server), depends on lib via replace directive

## Tech Stack
- Language: Go 1.21
- SQL Parser: pingcap/parser (from TiDB)
- B-tree: ryogrid/bltree-go-for-embedding
- REST server: ant0ine/go-json-rest
- Serialization: vmihailenco/msgpack (for MessagePack responses)
- CI: GitHub Actions

## Current Branch Context
The `fix-dirty-read-at-delete` branch addresses a known issue: dirty reads can occur when tables have indexes,
due to lack of consistency between table record and index entry at DELETE operations.
