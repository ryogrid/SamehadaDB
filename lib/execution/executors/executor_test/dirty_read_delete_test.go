package executor_test

import (
	"fmt"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/samehada"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	testingpkg "github.com/ryogrid/SamehadaDB/lib/testing/testing_assert"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

// setupTestTableWithIndex creates a SamehadaInstance, catalog, table with SkipList index,
// inserts the given account IDs, and commits. Returns objects needed for subsequent test steps.
func setupTestTableWithIndex(t *testing.T, accountIDs []int32) (
	shi *samehada.SamehadaInstance,
	c *catalog.Catalog,
	tableMetadata *catalog.TableMetadata,
	txnMgr *access.TransactionManager,
) {
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi = samehada.NewSamehadaInstance(t.Name(), 200)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "logging should be active")

	txnMgr = shi.GetTransactionManager()
	txn := txnMgr.Begin(nil)

	c = catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)

	columnA := column.NewColumn("account_id", types.Integer, true, index_constants.IndexKindSkipList, types.PageID(-1), nil)
	columnB := column.NewColumn("balance", types.Integer, true, index_constants.IndexKindSkipList, types.PageID(-1), nil)
	sch := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata = c.CreateTable("test_1", sch, txn)

	for _, id := range accountIDs {
		insPlan := createSpecifiedValInsertPlanNode(id, id*100, c, tableMetadata, types.Integer)
		executePlan(c, shi.GetBufferPoolManager(), txn, insPlan)
	}

	txnMgr.Commit(c, txn)
	return
}

// makeScanKeyTuple creates a tuple suitable for ScanKey on the account_id index.
func makeScanKeyTuple(val int32, tableSchema *schema.Schema) *tuple.Tuple {
	values := make([]types.Value, tableSchema.GetColumnCount())
	values[0] = types.NewInteger(val)
	for i := uint32(1); i < tableSchema.GetColumnCount(); i++ {
		values[i] = types.NewInteger(0) // placeholder
	}
	return tuple.NewTupleFromSchema(values, tableSchema)
}

// TestDeleteDoesNotRemoveIndexEntryBeforeCommit verifies that after a DELETE
// executes but before commit, the index entry still exists. After commit, the
// index entry must be removed.
func TestDeleteDoesNotRemoveIndexEntryBeforeCommit(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true
	defer func() {
		common.TempSuppressOnMemStorage = false
		common.TempSuppressOnMemStorageMutex.Unlock()
	}()

	shi, c, tableMetadata, txnMgr := setupTestTableWithIndex(t, []int32{10, 20, 30})
	defer shi.Shutdown(samehada.ShutdownPatternRemoveFiles)

	idx := tableMetadata.GetIndex(0) // account_id index
	testingpkg.Assert(t, idx != nil, "index on account_id should exist")

	// Txn A: DELETE WHERE account_id=20 (do NOT commit yet)
	txnA := txnMgr.Begin(nil)
	delPlan := createSpecifiedValDeletePlanNode(int32(20), c, tableMetadata, types.Integer, index_constants.IndexKindSkipList)
	results := executePlan(c, shi.GetBufferPoolManager(), txnA, delPlan)
	testingpkg.Assert(t, len(results) == 1, fmt.Sprintf("should delete 1 row, got %d", len(results)))

	// Before commit: index entry for key=20 should still exist
	keyTuple := makeScanKeyTuple(20, tableMetadata.Schema())
	rids := idx.ScanKey(keyTuple, txnA)
	testingpkg.Assert(t, len(rids) > 0, "index entry for key=20 should exist before commit")

	// Commit
	txnMgr.Commit(c, txnA)

	// After commit: index entry for key=20 should be removed
	txnB := txnMgr.Begin(nil)
	rids = idx.ScanKey(keyTuple, txnB)
	testingpkg.Assert(t, len(rids) == 0, fmt.Sprintf("index entry for key=20 should be removed after commit, got %d", len(rids)))
	txnMgr.Commit(c, txnB)
}

// TestDeleteAbortLeavesIndexIntact verifies that aborting a DELETE leaves the
// index entry intact (no rollback needed since entry was never removed).
func TestDeleteAbortLeavesIndexIntact(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true
	defer func() {
		common.TempSuppressOnMemStorage = false
		common.TempSuppressOnMemStorageMutex.Unlock()
	}()

	shi, c, tableMetadata, txnMgr := setupTestTableWithIndex(t, []int32{10, 20, 30})
	defer shi.Shutdown(samehada.ShutdownPatternRemoveFiles)

	// Txn A: DELETE WHERE account_id=20, then Abort
	txnA := txnMgr.Begin(nil)
	delPlan := createSpecifiedValDeletePlanNode(int32(20), c, tableMetadata, types.Integer, index_constants.IndexKindSkipList)
	executePlan(c, shi.GetBufferPoolManager(), txnA, delPlan)
	txnMgr.Abort(c, txnA)

	// Txn B: point scan for account_id=20 should find the row
	txnB := txnMgr.Begin(nil)
	scanPlan := createSpecifiedPointScanPlanNode(int32(20), c, tableMetadata, types.Integer, index_constants.IndexKindSkipList)
	results := executePlan(c, shi.GetBufferPoolManager(), txnB, scanPlan)
	testingpkg.Assert(t, len(results) == 1, fmt.Sprintf("row with account_id=20 should exist after abort, got %d results", len(results)))
	txnMgr.Commit(c, txnB)
}

// TestConcurrentDeleteAndIndexScan is a stress test that verifies no dirty reads
// occur under concurrent delete and index scan workload.
func TestConcurrentDeleteAndIndexScan(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true
	defer func() {
		common.TempSuppressOnMemStorage = false
		common.TempSuppressOnMemStorageMutex.Unlock()
	}()

	const rowCount = 100
	const iterationsPerGoroutine = 500
	const deleterCount = 4
	const readerCount = 4

	accountIDs := make([]int32, rowCount)
	for i := int32(0); i < rowCount; i++ {
		accountIDs[i] = i
	}

	shi, c, tableMetadata, txnMgr := setupTestTableWithIndex(t, accountIDs)
	defer shi.Shutdown(samehada.ShutdownPatternRemoveFiles)

	var dirtyReadCount int64
	var abortCount int64
	var wg sync.WaitGroup

	// Deleter goroutines: pick random key, DELETE, commit
	for d := 0; d < deleterCount; d++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			for iter := 0; iter < iterationsPerGoroutine; iter++ {
				key := int32(rng.Intn(rowCount))

				txn := txnMgr.Begin(nil)
				delPlan := createSpecifiedValDeletePlanNode(key, c, tableMetadata, types.Integer, index_constants.IndexKindSkipList)
				executePlan(c, shi.GetBufferPoolManager(), txn, delPlan)

				if txn.GetState() == access.ABORTED {
					txnMgr.Abort(c, txn)
					atomic.AddInt64(&abortCount, 1)
					continue
				}

				// Randomly commit or abort
				if rng.Intn(2) == 0 {
					txnMgr.Commit(c, txn)
				} else {
					txnMgr.Abort(c, txn)
				}
			}
		}(d * 1000)
	}

	// Reader goroutines: pick random key, index point scan, verify consistency
	for r := 0; r < readerCount; r++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			for iter := 0; iter < iterationsPerGoroutine; iter++ {
				key := int32(rng.Intn(rowCount))

				txn := txnMgr.Begin(nil)
				scanPlan := createSpecifiedPointScanPlanNode(key, c, tableMetadata, types.Integer, index_constants.IndexKindSkipList)
				results := executePlan(c, shi.GetBufferPoolManager(), txn, scanPlan)

				if txn.GetState() == access.ABORTED {
					txnMgr.Abort(c, txn)
					atomic.AddInt64(&abortCount, 1)
					continue
				}

				// If we found results, verify the account_id matches
				for _, tpl := range results {
					if tpl == nil {
						continue
					}
					accountID := tpl.GetValue(tableMetadata.Schema(), 0)
					if accountID.ToInteger() != key {
						atomic.AddInt64(&dirtyReadCount, 1)
						fmt.Printf("DIRTY READ: expected account_id=%d, got %d\n", key, accountID.ToInteger())
					}
				}

				txnMgr.Commit(c, txn)
			}
		}(r*1000 + 500)
	}

	wg.Wait()

	fmt.Printf("Concurrent test complete: aborts=%d, dirtyReads=%d\n", abortCount, dirtyReadCount)
	testingpkg.Assert(t, dirtyReadCount == 0, fmt.Sprintf("dirty reads detected: %d", dirtyReadCount))
}
