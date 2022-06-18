# Overview
This code tree is based on Golang ported BusTub RDBMS codes: go-bustub.  
original codes of go-bustub are [here](https://github.com/brunocalza/go-bustub).

# What is Samehada?
- Samehada, which literally means shark skin, is a tool used to grate wasabi, usually for sushi, but also for other Japanese cuisines
- Samehada features its grid shape that forms air bubbles between the grated wasabi, minimizing unnecessary spiceness
- We are proud to call SamehadaDB because the grid produces a pleasant taste and aroma with a nice harmony
- (The text in this section was contributed by [ujihisa](https://github.com/ujihisa). Thanks ujihisa)

# Current Status
- **SamehadaDB has no entry point (main function) now**
- **There are several unit tests only**
- procedure described on next section executes all defined unit tests

## Procedure of Executing SamehadaDB
- Please install golang environment with package system your OS has (apt, yum, brew ...etc)
  - If you use Windows, you can select both Windows native environment and WSL Ubuntu environment
- If you select Windows native environments or golang environment which is installed with package system can't execute SamehadaDB, you should install official binary directly
  - Please refer [Download and Install - The Go Programming Language](https://go.dev/doc/install)
- Executing all unit tests which test several features and components of SamehadaDB
  - $ git clone https://github.com/ryogrid/SamehadaDB.git
  - $ cd SamehadaDB
  - $ go clean -testcache; go test ./... -v

## Roadmap

- [x] Predicates on Seq Scan
- [x] Multiple Item on Predicate: AND, OR
- [x] Predicates: <, >, <=, >=
- [x] Null
- [ ] Inline types (<del>integer, varchar, float</del>, bigint, smallint, decimal, timestamp, datetime)
- [x] Delete Tuple
- [x] Update Tuple
  - <del>RESTRICTION: a condition which update transaction aborts on exists</del>
- [x] LIMIT / OFFSET
- [x] Varchar
- [x] Persistent Catalog
- [ ] Updating of Table Schema 
- [ ] <del>LRU replacer</del>
- [x] Latches
- [x] Transactions
- [x] Rollback When Abort Occurs
- [x] Logging
- [ ] Checkpointing
  - [x] Simple Checkpointing (all transaction block until finish of checkpointing)
  - [ ] Fuzzy Checkpointing (ARIES)
- [x] Recovery from Logs
- [ ] Index
  - [x] Hash Index
    - Hash index can be used only equal(==) operator is specified to index having columns
    - When the system exits in not graceful, reconstruction of index data is needed at reboot of system now
  - [ ] Tree Based Index
- [ ] JOIN
  - [x] INNER JOIN (Hash Join)
    - Currently, only two tables JOIN is implemented and codition specified at ON clause should be composed of single item  
  - [ ] CROSS JOIN
- [x] Aggregations (COUNT, MAX, MIN ... on SELECT clause including Group by and Having)
- [x] Sort (ORDER BY clause) 
- [x] Tuple Level Locking With Strong Strict 2-Phase Locking (SS2PL) Protcol
- [x] Concurrent Execution of Transactions
- [ ] <del>Execution Planning from hard coded SQL like method call I/F (like some kind of embeded DB)</del>
- [ ] Execution Planning from Query Description text (SQL, SQL like description)
- [ ] Frontend Impl as Embeded DB Library (like SQLite)
- [ ] Eliminate Duplication (Distinct)
- [ ] Query Optimization
- [ ] AS clause
- [ ] JOIN (more than two tables)
- [ ] Nested Query
- [ ] DB Connector (Driver) or Other Kind Access Interface
  - [ ] Original Protcol
  - [ ] MySQL or PostgreSQL Compatble Protcol
  - [ ] REST
- [ ] Deallocate and Reuse Page
  - Need tracking page usage by BufferPoolManager or TableHeap and need bitmap in header page corresponding to the tracking
- [ ] UNION clause
- [ ] Eliminate Data Processing with Placing All Scanned Tuples on the Memory
- [ ] Communication over SSL/TLS
- [ ] Authentication

## Advisor
- [kumagi](https://github.com/kumagi) and more!
## Past work
[FunnelKVS: Rust implementation of autonomous distributed key-value store which has REST interfaces](https://github.com/ryogrid/rust_dkvs)
