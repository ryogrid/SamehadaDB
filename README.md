# Overview
This code tree is based on Golang ported BusTub RDBMS codes: go-bustub.  
original codes of go-bustub are [here](https://github.com/brunocalza/go-bustub).

## Roadmap

- [x] Predicates on Seq Scan
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
- [x] Recovery from Logs
- [ ] Hash Index
- [ ] BTree Index
- [ ] Join
- [ ] Aggregations (COUNT, MAX, MIN ... on SELECT clause)
- [ ] Sort (ORDER BY clause) 
- [ ] Index Scan
- [ ] Execution Planning from Query Description (SQL or SQL like description)
- [ ] Query Optimazation
- [ ] Concurrent Execution of Transactions
- [ ] Nested Query 

## Past work
[FunnelKVS: Rust implementation of autonomous distributed key-value store which has REST interfaces](https://github.com/ryogrid/rust_dkvs)