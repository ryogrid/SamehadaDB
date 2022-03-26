# Overview
This code tree is based on Golang ported BusTub RDBMS codes: go-bustub.  
original codes of go-bustub are [here](https://github.com/brunocalza/go-bustub).

# What is Samehada?
- Samehada, which literally means shark skin, is a tool used to grate wasabi, usually for sushi, but also for other Japanese cuisines
- Samehada features its grid shape that forms air bubbles between the grated wasabi, minimizing unnecessary spiceness
- We are proud to call SamehadaDB because the grid produces a pleasant taste and aroma with a nice harmony
- (The text in this section was contributed by [ujihisa](https://github.com/ujihisa). Thank you ujihisa)

## Roadmap

- [x] Predicates on Seq Scan
- [ ] Multiple Condition on Predicate: AND, OR
- [ ] Predicates: <, >, <=, >=
- [ ] Null
- [ ] Other inline types (boolean, bigint, smallint, decimal, timestamp)
- [ ] Deletion
- [ ] Update
- [x] LIMIT / OFFSET
- [x] Varchar
- [x] Persistent Catalog
- [ ] LRU replacer
- [ ] Latches
- [x] Transactions
- [x] Logging
- [x] Checkpointing
- [ ] Fuzzy Checkpointing (ARIES)
- [x] Recovery from Logs
- [x] Hash Index
- [ ] BTree Index
- [ ] Join (Hash Join)
- [ ] Aggregations (COUNT, MAX, MIN ... on SELECT clause)
- [ ] GROUP By caluese
- [ ] Sort (ORDER BY clause) 
- [ ] Concurrent Execution of Transactions
- [ ] Execution Planning from hard coded SQL like methods I/F (like several kind of embeded DB)
- [ ] Execution Planning from Query Description text (SQL, SQL like description)
- [ ] Query Optimazation
- [ ] Nested Query
- [ ] DB Connector or Other Kind Access Interface
- [ ] Deallocate and Reuse Page
  - need tracking page usage by BufferPoolManager or TableHeap and need bitmap in header page corresponding to the tracking
## Past work
[FunnelKVS: Rust implementation of autonomous distributed key-value store which has REST interfaces](https://github.com/ryogrid/rust_dkvs)
