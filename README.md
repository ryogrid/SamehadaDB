<img src="SamehadaDB_logo.png" width="500px" />

# Overview
- Simple Relational Database Implemented in Golang
- This code tree is based on Golang ported [BusTub](https://github.com/cmu-db/bustub) RDBMS: go-bustub
  - original code of go-bustub is [here](https://github.com/brunocalza/go-bustub)

# What is Samehada?
- Samehada, which literally means shark skin, is a tool used to grate wasabi, usually for sushi, but also for other Japanese cuisines
- Samehada features its grid shape that forms air bubbles between the grated wasabi, minimizing unnecessary spiciness
- We are proud to call SamehadaDB because the grid produces a pleasant taste and aroma with a nice harmony
- (The text in this section was contributed by [ujihisa](https://github.com/ujihisa). Thanks ujihisa)

# Current Status
- SamehadaDB can be used as simple embedded DB (library form)
  - [Simple Single Page Application Demo (TODO App)](http://samehada.ryogrid.net:8080/todo/)))
  - Above demo uses SamehadaDB on backend API server
    - [Modification for migration from PostgreSQL to SamehadaDB we did](https://github.com/ryogrid/TODO-Fullstack-App-Go-Gin-Postgres-React/commit/48e7a9f25570e15e29a279ebc24396698bf1d80a)
    - [All app codes](https://github.com/ryogrid/TODO-Fullstack-App-Go-Gin-Postgres-React/tree/8bb5a2cb57ce8bb694e6f9b49cd5926631519dd9)
      - builded frontend JS code is little bit modified directly for escape from re-compile...
      - above code tree can be deployed to AWS Elastic Beanstalk (on not https and use 8080 port)
        - PORT env should be configure to 8080 from AWS console
- **ATTENTION: SamehadaDB is not developed for productional use! There are no warranties!**
- By the way, procedure described on next section executes almost all defined unit tests except these of taking long time

## Procedure of Executing SamehadaDB (executing unit tests)
- Please install golang environment with package system your OS has (apt, yum, brew ...etc)
  - If you use Windows, you can select both Windows native environment and WSL Ubuntu environment
- If you select Windows native environments or golang environment which is installed with package system can't execute SamehadaDB, you should install official binary directly
  - Please refer [Download and Install - The Go Programming Language](https://go.dev/doc/install)
- Executing all unit tests which test several features and components of SamehadaDB except several tests taking long time
  - $ git clone https://github.com/ryogrid/SamehadaDB.git
  - $ cd SamehadaDB
  - $ go clean -testcache; go test ./... -short -v

## Roadmap
**Note:**  
**Statuses described berow are of backend and dont't mean supported features of SamehadaDB when you use our DB through frontend.**  
**(now, embeded DB form only exists)**  
  
- [x] Predicates on Seq Scan
- [x] Multiple Item on Predicate: AND, OR
- [x] Predicates: <, >, <=, >=
- [x] Null
- [ ] Inline types (<del>integer, varchar, float, boolean</del>, bigint, smallint, decimal, timestamp, datetime and etc)
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
- [x] Checkpointing
  - [x] Simple Checkpointing (all transaction block until finish of checkpointing)
  - [ ] Fuzzy Checkpointing (ARIES)
- [x] Recovery from Logs
- [ ] Index
  - [x] Hash Index
    - Hash index can be used only equal(==) operator is specified to index having columns
    - When the system exits in not graceful, reconstruction of index data is needed at reboot of system now
  - [x] SkipList Index
    - Alternative of B-tree Index 
  - <del>B-tree Index</del>
  - [ ] Logging/Recovery of Index Data (Redo/Undo)
- [ ] JOIN
  - [x] INNER JOIN (Hash Join)
    - Currently, only two tables JOIN is implemented and condition specified at ON clause should be composed of single item  
  - [ ] CROSS JOIN
- [x] Aggregations (COUNT, MAX, MIN, SUM on SELECT clause including Group by and Having)
- [x] Sort (ORDER BY clause) 
- [x] Tuple Level Locking With Strong Strict 2-Phase Locking (SS2PL) Protocol
- [x] Concurrent Execution of Transactions
  - Avoidance of phantom problem is not implemented yet
    - So, current transaction isolation level is **"REPEATABLE READ"**
  - Retry of txns aborted due to concurrency control protocol is not implemented yet
- [ ] <del>Execution Planning from hard coded SQL like method call I/F (like some kind of embedded DB)</del>
- [x] Execution Planning from Query Description text (SQL)
- [x] Frontend Impl as Embedded DB Library (like SQLite)
  - Currently, functions of the library are not thread safe and concurrent transaction is not supported
- [ ] Deduplication of Result Records (Distinct)
- [ ] Query Optimization
- [ ] AS clause
- [ ] JOIN (more than two tables)
- [ ] Nested Query
- [ ] DB Connector (Driver) or Other Kind Access Interface
  - [ ] Original Protocol
  - [ ] MySQL or PostgreSQL Compatible Protocol
  - [ ] REST
- [ ] Deallocate and Reuse Page
  - Need tracking page usage by BufferPoolManager or TableHeap and need bitmap in header page corresponding to the tracking
- [ ] UNION clause
- [ ] Eliminate Data Processing with Placing All Scanned Tuples on the Memory
- [ ] Communication over SSL/TLS
- [ ] Authentication

## More Info
- [Wiki on this repo](https://github.com/ryogrid/SamehadaDB/wiki)

## About Skip List Index SamehadaDB has
- [Implementation of On-Disk Concurrent Skip List as an Alternative to B-Tree Index](https://ryogrid.github.io/articles/skiplist_en)

## Advisor
- [kumagi](https://github.com/kumagi) and more!

## Past work
[FunnelKVS: Rust implementation of autonomous distributed key-value store which has REST interfaces](https://github.com/ryogrid/rust_dkvs)
