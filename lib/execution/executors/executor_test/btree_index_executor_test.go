package executor_test

import (
	"fmt"
	"os"
	"testing"

	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/execution/executors"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/samehada"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	testingpkg "github.com/ryogrid/SamehadaDB/lib/testing/testing_assert"
	"github.com/ryogrid/SamehadaDB/lib/testing/testing_pattern_fw"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

func testKeyDuplicateInsertDeleteWithBTreeIndex[T float32 | int32 | string](t *testing.T, keyType types.TypeID) {
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), 500)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")
	txnMgr := shi.GetTransactionManager()

	txn := txnMgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)

	columnA := column.NewColumn("account_id", keyType, true, index_constants.INDEX_KIND_BTREE, types.PageID(-1), nil)
	columnB := column.NewColumn("balance", types.Integer, true, index_constants.INDEX_KIND_BTREE, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})
	tableMetadata := c.CreateTable("test_1", schema_, txn)

	txnMgr.Commit(c, txn)

	txn = txnMgr.Begin(nil)

	var accountId interface{}
	switch keyType {
	case types.Integer:
		accountId = int32(10)
	case types.Float:
		accountId = float32(-5.2)
	case types.Varchar:
		accountId = "duplicateTest"
	default:
		panic("unsuppoted value type")
	}

	insPlan1 := createSpecifiedValInsertPlanNode(accountId.(T), int32(100), c, tableMetadata, keyType)
	result := executePlan(c, shi.GetBufferPoolManager(), txn, insPlan1)
	insPlan2 := createSpecifiedValInsertPlanNode(accountId.(T), int32(101), c, tableMetadata, keyType)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, insPlan2)
	insPlan3 := createSpecifiedValInsertPlanNode(accountId.(T), int32(102), c, tableMetadata, keyType)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, insPlan3)

	txnMgr.Commit(c, txn)

	txn = txnMgr.Begin(nil)

	//rangeScanP := createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, nil, nil, index_constants.INDEX_KIND_BTREE)
	//results := executePlan(c, shi.GetBufferPoolManager(), txn, rangeScanP)
	//for _, foundVal := range results {
	//	fmt.Println(foundVal.GetValue(tableMetadata.Schema(), 0).ToString())
	//}

	scanP := createSpecifiedPointScanPlanNode(accountId.(T), c, tableMetadata, keyType, index_constants.INDEX_KIND_BTREE)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, scanP)
	testingpkg.Assert(t, len(result) == 3, "duplicated key point scan got illegal results.")
	rid1 := result[0].GetRID()
	val0_1 := result[0].GetValue(tableMetadata.Schema(), 0)
	val0_2 := result[0].GetValue(tableMetadata.Schema(), 1)
	fmt.Println(val0_1, val0_2)
	rid2 := result[1].GetRID()
	rid3 := result[2].GetRID()
	fmt.Printf("%v %v %v\n", *rid1, *rid2, *rid3)

	for _, foundTuple := range result {
		val := foundTuple.GetValue(tableMetadata.Schema(), 0)
		fmt.Println(val.ToString())
	}

	indexCol1 := tableMetadata.GetIndex(0)
	indexCol2 := tableMetadata.GetIndex(1)

	indexCol1.DeleteEntry(result[0], *rid1, txn)
	indexCol2.DeleteEntry(result[0], *rid1, txn)
	scanP = createSpecifiedPointScanPlanNode(accountId.(T), c, tableMetadata, keyType, index_constants.INDEX_KIND_BTREE)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, scanP)
	testingpkg.Assert(t, len(result) == 2, "duplicated key point scan got illegal results.")

	indexCol1.DeleteEntry(result[0], *rid2, txn)
	indexCol2.DeleteEntry(result[0], *rid2, txn)
	scanP = createSpecifiedPointScanPlanNode(accountId.(T), c, tableMetadata, keyType, index_constants.INDEX_KIND_BTREE)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, scanP)
	testingpkg.Assert(t, len(result) == 1, "duplicated key point scan got illegal results.")

	indexCol1.DeleteEntry(result[0], *rid3, txn)
	indexCol2.DeleteEntry(result[0], *rid3, txn)
	scanP = createSpecifiedPointScanPlanNode(accountId.(T), c, tableMetadata, keyType, index_constants.INDEX_KIND_BTREE)
	result = executePlan(c, shi.GetBufferPoolManager(), txn, scanP)
	testingpkg.Assert(t, len(result) == 0, "duplicated key point scan got illegal results.")

	txnMgr.Commit(c, txn)
	shi.Shutdown(samehada.ShutdownPatternCloseFiles)
}

func testBTreeParallelTxnStrideRoot[T int32 | float32 | string](t *testing.T, keyType types.TypeID) {
	//bpoolSize := int32(500)
	//bpoolSize := int32(1000)

	bpoolSize := int32(5000)

	switch keyType {
	case types.Integer:

		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 400, 30000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 400, 30000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, SERIAL_EXEC, 20)
		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 400, 300, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, SERIAL_EXEC, 20)
		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 400, 3000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, SERIAL_EXEC, 20)

		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 400, 3000, 13, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, PARALLEL_EXEC, 20)

		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 800, 30000, 13, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, PARALLEL_EXEC, 20)

		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 800, 30000, 13, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, SERIAL_EXEC, 1)

		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 800, 30000, 13, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, PARALLEL_EXEC, 20)
		InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 200, 10000, 13, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, PARALLEL_EXEC, 20)
	case types.Float:
		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 240, 1000, 13, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, PARALLEL_EXEC, 20)
		InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 200, 10000, 13, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, PARALLEL_EXEC, 20)
	case types.Varchar:
		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 400, 5000, 17, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 400, 5000, 17, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, PARALLEL_EXEC, 20)
		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 400, 1000, 17, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, SERIAL_EXEC, 1)
		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 800, 5000, 17, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, SERIAL_EXEC, 1)

		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 800, 3000, 17, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, SERIAL_EXEC, 1)
		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 800, 30000, 17, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, PARALLEL_EXEC, 20)
		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 200, 10000, 17, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, PARALLEL_EXEC, 20)
		InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 400, 1000, 17, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, PARALLEL_EXEC, 20)
		//InnerTestParallelTxnsQueryingIndexUsedColumns[T](t, keyType, 800, 30000, 17, 0, bpoolSize, index_constants.INDEX_KIND_BTREE, SERIAL_EXEC, 1)
	default:
		panic("not implemented!")
	}
}

func TestKeyDuplicateInsertDeleteWithBTreeIndexInt(t *testing.T) {
	testKeyDuplicateInsertDeleteWithBTreeIndex[int32](t, types.Integer)
}

func TestKeyDuplicateInsertDeleteWithBTreeIndexFloat(t *testing.T) {
	testKeyDuplicateInsertDeleteWithBTreeIndex[float32](t, types.Float)
}

func TestKeyDuplicateInsertDeleteWithBTreeIndexVarchar(t *testing.T) {
	testKeyDuplicateInsertDeleteWithBTreeIndex[string](t, types.Varchar)
}

func TestKeyDuplicateBTreePrallelTxnStrideInteger(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testBTreeParallelTxnStrideRoot[int32](t, types.Integer)
}

func TestKeyDuplicateBTreePrallelTxnStrideFloat(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testBTreeParallelTxnStrideRoot[float32](t, types.Float)
}

// BTreeIndex doesn't support Varchar

func TestKeyDuplicateBTreePrallelTxnStrideVarchar(t *testing.T) {
	t.Parallel()
	//if testing.Short() {
	//	t.Skip("skip this in short mode.")
	//}
	testBTreeParallelTxnStrideRoot[string](t, types.Varchar)
}

func TestRecounstructionOfBtreeIndex(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true

	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	sh := samehada.NewSamehadaDB(t.Name(), 10*1024)
	shi := sh.GetSamehadaInstance()
	c := sh.GetCatalogForTesting()

	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	bpm := shi.GetBufferPoolManager()
	txn := txn_mgr.Begin(nil)

	columnA := column.NewColumn("a", types.Integer, true, index_constants.INDEX_KIND_BTREE, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, true, index_constants.INDEX_KIND_BTREE, types.PageID(-1), nil)
	columnC := column.NewColumn("c", types.Varchar, true, index_constants.INDEX_KIND_BTREE, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB, columnC})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewInteger(22))
	row1 = append(row1, types.NewVarchar("foo"))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewInteger(55))
	row2 = append(row2, types.NewVarchar("bar"))

	row3 := make([]types.Value, 0)
	row3 = append(row3, types.NewInteger(1225))
	row3 = append(row3, types.NewInteger(712))
	row3 = append(row3, types.NewVarchar("baz"))

	row4 := make([]types.Value, 0)
	row4 = append(row4, types.NewInteger(1225))
	row4 = append(row4, types.NewInteger(712))
	row4 = append(row4, types.NewVarchar("baz"))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)
	rows = append(rows, row3)
	rows = append(rows, row4)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(nil, txn)

	txn = shi.GetTransactionManager().Begin(nil)

	cases := []testing_pattern_fw.IndexPointScanTestCase{{
		"select a ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}},
		testing_pattern_fw.Predicate{"b", expression.Equal, 55},
		[]testing_pattern_fw.Assertion{{"a", 99}},
		1,
	}, {
		"select b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"b", types.Integer}},
		testing_pattern_fw.Predicate{"b", expression.Equal, 55},
		[]testing_pattern_fw.Assertion{{"b", 55}},
		1,
	}, {
		"select a, b ... WHERE a = 20",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}},
		testing_pattern_fw.Predicate{"a", expression.Equal, 20},
		[]testing_pattern_fw.Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b ... WHERE a = 99",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}},
		testing_pattern_fw.Predicate{"a", expression.Equal, 99},
		[]testing_pattern_fw.Assertion{{"a", 99}, {"b", 55}},
		1,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			testing_pattern_fw.ExecuteIndexPointScanTestCase(t, test, index_constants.INDEX_KIND_BTREE)
		})
	}

	shi.GetTransactionManager().Commit(nil, txn)
	sh.Shutdown()

	// ----------- check recovery includes index data ----------

	// recovery catalog data and tuple datas

	sh = samehada.NewSamehadaDB(t.Name(), 10*1024)
	shi = sh.GetSamehadaInstance()
	c = sh.GetCatalogForTesting()
	tableMetadata = c.GetTableByName("test_1")

	// checking reconstruction result of index data by getting tuples using index
	txn = shi.GetTransactionManager().Begin(nil)

	txn.SetIsRecoveryPhase(false)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	executionEngine = &executors.ExecutionEngine{}
	executorContext = executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	cases = []testing_pattern_fw.IndexPointScanTestCase{{
		"select a ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}},
		testing_pattern_fw.Predicate{"b", expression.Equal, 55},
		[]testing_pattern_fw.Assertion{{"a", 99}},
		1,
	}, {
		"select b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"b", types.Integer}},
		testing_pattern_fw.Predicate{"b", expression.Equal, 55},
		[]testing_pattern_fw.Assertion{{"b", 55}},
		1,
	}, {
		"select a, b ... WHERE a = 20",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}},
		testing_pattern_fw.Predicate{"a", expression.Equal, 20},
		[]testing_pattern_fw.Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b ... WHERE a = 99",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}},
		testing_pattern_fw.Predicate{"a", expression.Equal, 99},
		[]testing_pattern_fw.Assertion{{"a", 99}, {"b", 55}},
		1,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			testing_pattern_fw.ExecuteIndexPointScanTestCase(t, test, index_constants.INDEX_KIND_BTREE)
		})
	}

	shi.GetTransactionManager().Commit(nil, txn)

	sh.Shutdown()
	common.TempSuppressOnMemStorage = false
	common.TempSuppressOnMemStorageMutex.Unlock()
}
