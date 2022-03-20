# Overview
This code tree is based on Golang ported BusTub RDBMS codes: go-bustub.  
original codes of go-bustub are [here](https://github.com/brunocalza/go-bustub).

## Roadmap

- [x] Predicates on Seq Scan
- [ ] Making Schemas (Table Definitions) Persistent
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
- [ ] Join
- [ ] Aggregations (COUNT, MAX, MIN ... on SELECT clause)
- [ ] GROUP By caluese
- [ ] Sort (ORDER BY clause) 
- [ ] Query Optimazation
- [ ] Concurrent Execution of Transactions
- [ ] Nested Query
- [ ] Execution Planning from Query Description (SQL or SQL like description)
- [ ] DB Connector or Other Kind Access Interface

## Past work
[FunnelKVS: Rust implementation of autonomous distributed key-value store which has REST interfaces](https://github.com/ryogrid/rust_dkvs)
