// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executor_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/execution/executors"
	"github.com/ryogrid/SamehadaDB/lib/recovery/log_recovery"
	"github.com/ryogrid/SamehadaDB/lib/samehada"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	testingpkg "github.com/ryogrid/SamehadaDB/lib/testing/testing_assert"
	"github.com/ryogrid/SamehadaDB/lib/testing/testing_pattern_fw"
	"github.com/ryogrid/SamehadaDB/lib/testing/testing_tbl_gen"
	"github.com/ryogrid/SamehadaDB/lib/testing/testing_util"
	"os"
	"testing"

	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/disk"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

func TestSimpleInsertAndSeqScan(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewInteger(22))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewInteger(55))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	outColumnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnA})

	seqPlan := plans.NewSeqScanPlanNode(c, outSchema, nil, tableMetadata.OID())

	results := executionEngine.Execute(seqPlan, executorContext)

	txn_mgr.Commit(nil, txn)

	testingpkg.Assert(t, types.NewInteger(20).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 20")
	testingpkg.Assert(t, types.NewInteger(99).CompareEquals(results[1].GetValue(outSchema, 0)), "value should be 99")
}

func TestSimpleInsertAndSeqScanFloat(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Float, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Float, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewFloat(0.5))
	row1 = append(row1, types.NewFloat(1.5))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewFloat(0.99))
	row2 = append(row2, types.NewFloat(0.55))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	outColumnA := column.NewColumn("a", types.Float, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnA})

	seqPlan := plans.NewSeqScanPlanNode(c, outSchema, nil, tableMetadata.OID())

	results := executionEngine.Execute(seqPlan, executorContext)

	txn_mgr.Commit(nil, txn)

	testingpkg.Assert(t, types.NewFloat(0.5).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 0.5")
	testingpkg.Assert(t, types.NewFloat(0.99).CompareEquals(results[1].GetValue(outSchema, 0)), "value should be 0.99")
}

func TestSimpleInsertAndSeqScanWithPredicateComparison(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnC := column.NewColumn("c", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
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

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(nil, txn)

	cases := []testing_pattern_fw.SeqScanTestCase{{
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
	}, {
		"select a, b ... WHERE a = 100",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}},
		testing_pattern_fw.Predicate{"a", expression.Equal, 100},
		[]testing_pattern_fw.Assertion{},
		0,
	}, {
		"select a, b ... WHERE b != 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}},
		testing_pattern_fw.Predicate{"b", expression.NotEqual, 55},
		[]testing_pattern_fw.Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b, c ... WHERE c = 'foo'",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		testing_pattern_fw.Predicate{"c", expression.Equal, "foo"},
		[]testing_pattern_fw.Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		1,
	}, {
		"select a, b, c ... WHERE c != 'foo'",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		testing_pattern_fw.Predicate{"c", expression.NotEqual, "foo"},
		[]testing_pattern_fw.Assertion{{"a", 99}, {"b", 55}, {"c", "bar"}},
		1,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			testing_pattern_fw.ExecuteSeqScanTestCase(t, test)
		})
	}
}

func TestInsertBoolAndSeqScanWithComparison(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Boolean, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnC := column.NewColumn("c", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB, columnC})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewBoolean(true))
	row1 = append(row1, types.NewVarchar("foo"))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewBoolean(false))
	row2 = append(row2, types.NewVarchar("bar"))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(nil, txn)

	cases := []testing_pattern_fw.SeqScanTestCase{{
		"select a ... WHERE b = true",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}},
		testing_pattern_fw.Predicate{"b", expression.Equal, true},
		[]testing_pattern_fw.Assertion{{"a", 20}},
		1,
	}, {
		"select c ... WHERE b = false",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"c", types.Varchar}},
		testing_pattern_fw.Predicate{"b", expression.Equal, false},
		[]testing_pattern_fw.Assertion{{"c", "bar"}},
		1,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			testing_pattern_fw.ExecuteSeqScanTestCase(t, test)
		})
	}
}

func TestSimpleInsertAndLimitExecution(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewInteger(22))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewInteger(55))

	row3 := make([]types.Value, 0)
	row3 = append(row3, types.NewInteger(11))
	row3 = append(row3, types.NewInteger(44))

	row4 := make([]types.Value, 0)
	row4 = append(row4, types.NewInteger(76))
	row4 = append(row4, types.NewInteger(90))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)
	rows = append(rows, row3)
	rows = append(rows, row4)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(nil, txn)

	// TEST 1: select a, b ... LIMIT 1
	func() {
		a := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		b := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(c, outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 1, 1)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 1, len(results))
		testingpkg.Assert(t, types.NewInteger(99).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 99 but was %d", results[0].GetValue(outSchema, 0).ToInteger())
		testingpkg.Assert(t, types.NewInteger(55).CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 55 but was %d", results[0].GetValue(outSchema, 1).ToInteger())
	}()

	// TEST 1: select a, b ... LIMIT 2
	func() {
		a := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		b := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(c, outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 2, 0)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 2, len(results))
	}()

	// TEST 1: select a, b ... LIMIT 3
	func() {
		a := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		b := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(c, outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 3, 0)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 3, len(results))
	}()
}

func TestSimpleInsertAndLimitExecutionMultiTable(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewInteger(22))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewInteger(55))

	row3 := make([]types.Value, 0)
	row3 = append(row3, types.NewInteger(11))
	row3 = append(row3, types.NewInteger(44))

	row4 := make([]types.Value, 0)
	row4 = append(row4, types.NewInteger(76))
	row4 = append(row4, types.NewInteger(90))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)
	rows = append(rows, row3)
	rows = append(rows, row4)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	// construct second table

	columnA = column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB = column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ = schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata2 := c.CreateTable("test_2", schema_, txn)

	row1 = make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewInteger(22))

	row2 = make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewInteger(55))

	row3 = make([]types.Value, 0)
	row3 = append(row3, types.NewInteger(11))
	row3 = append(row3, types.NewInteger(44))

	row4 = make([]types.Value, 0)
	row4 = append(row4, types.NewInteger(76))
	row4 = append(row4, types.NewInteger(90))

	rows = make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)
	rows = append(rows, row3)
	rows = append(rows, row4)

	insertPlanNode = plans.NewInsertPlanNode(rows, tableMetadata2.OID())

	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(nil, txn)

	// TEST 1: select a, b ... LIMIT 1
	func() {
		a := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		b := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(c, outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 1, 1)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 1, len(results))
		testingpkg.Assert(t, types.NewInteger(99).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 99 but was %d", results[0].GetValue(outSchema, 0).ToInteger())
		testingpkg.Assert(t, types.NewInteger(55).CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 55 but was %d", results[0].GetValue(outSchema, 1).ToInteger())
	}()

	// TEST 1: select a, b ... LIMIT 2
	func() {
		a := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		b := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(c, outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 2, 0)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 2, len(results))
	}()

	// TEST 1: select a, b ... LIMIT 1
	func() {
		a := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		b := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(c, outSchema, nil, tableMetadata2.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 1, 1)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 1, len(results))
		testingpkg.Assert(t, types.NewInteger(99).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 99 but was %d", results[0].GetValue(outSchema, 0).ToInteger())
		testingpkg.Assert(t, types.NewInteger(55).CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 55 but was %d", results[0].GetValue(outSchema, 1).ToInteger())
	}()

	// TEST 1: select a, b ... LIMIT 3
	func() {
		a := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		b := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(c, outSchema, nil, tableMetadata2.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 3, 0)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 3, len(results))
	}()
}

func TestHashTableIndex(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true

	diskManager := disk.NewDiskManagerTest()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)

	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, true, index_constants.INDEX_KIND_HASH, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, true, index_constants.INDEX_KIND_HASH, types.PageID(-1), nil)
	columnC := column.NewColumn("c", types.Varchar, true, index_constants.INDEX_KIND_HASH, types.PageID(-1), nil)
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

	bpm.FlushAllPages()

	txn_mgr.Commit(nil, txn)

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
	}, {
		"select a, b ... WHERE a = 100",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}},
		testing_pattern_fw.Predicate{"a", expression.Equal, 100},
		[]testing_pattern_fw.Assertion{},
		0,
	}, {
		"select a, b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}},
		testing_pattern_fw.Predicate{"b", expression.Equal, 55},
		[]testing_pattern_fw.Assertion{{"a", 99}, {"b", 55}},
		1,
	}, {
		"select a, b, c ... WHERE c = 'foo'",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		testing_pattern_fw.Predicate{"c", expression.Equal, "foo"},
		[]testing_pattern_fw.Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		1,
	}, {
		"select a, b ... WHERE c = 'baz'",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}},
		testing_pattern_fw.Predicate{"c", expression.Equal, "baz"},
		[]testing_pattern_fw.Assertion{{"a", 1225}, {"b", 712}},
		2,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			testing_pattern_fw.ExecuteIndexPointScanTestCase(t, test, index_constants.INDEX_KIND_HASH)
		})
	}

	common.TempSuppressOnMemStorage = false
	diskManager.ShutDown()
	common.TempSuppressOnMemStorageMutex.Unlock()
}

func TestSimpleDelete(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnC := column.NewColumn("c", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
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

	bpm.FlushAllPages()

	txn_mgr.Commit(nil, txn)

	cases := []testing_pattern_fw.DeleteTestCase{{
		"delete ... WHERE c = 'baz'",
		txn_mgr,
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		testing_pattern_fw.Predicate{"c", expression.Equal, "baz"},
		[]testing_pattern_fw.Assertion{{"a", 1225}, {"b", 712}, {"c", "baz"}},
		2,
	}, {
		"delete ... WHERE b = 55",
		txn_mgr,
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		testing_pattern_fw.Predicate{"b", expression.Equal, 55},
		[]testing_pattern_fw.Assertion{{"a", 99}, {"b", 55}, {"c", "bar"}},
		1,
	}, {
		"delete ... WHERE a = 20",
		txn_mgr,
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		testing_pattern_fw.Predicate{"a", expression.Equal, 20},
		[]testing_pattern_fw.Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		1,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			testing_pattern_fw.ExecuteDeleteTestCase(t, test)
		})
	}
}

func TestDeleteWithSelctInsert(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)
	// TODO: (SDB) this is a hack to get around the fact that we don't have a recovery manager
	txn.SetIsRecoveryPhase(true)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnC := column.NewColumn("c", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
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

	bpm.FlushAllPages()

	txn_mgr.Commit(nil, txn)

	cases := []testing_pattern_fw.DeleteTestCase{{
		"delete ... WHERE c = 'baz'",
		txn_mgr,
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		testing_pattern_fw.Predicate{"c", expression.Equal, "baz"},
		[]testing_pattern_fw.Assertion{{"a", 1225}, {"b", 712}, {"c", "baz"}},
		2,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			testing_pattern_fw.ExecuteDeleteTestCase(t, test)
		})
	}

	cases2 := []testing_pattern_fw.SeqScanTestCase{{
		"select a ... WHERE c = baz",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}},
		testing_pattern_fw.Predicate{"c", expression.Equal, "baz"},
		[]testing_pattern_fw.Assertion{{"a", 99}},
		0,
	}, {
		"select b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"b", types.Integer}},
		testing_pattern_fw.Predicate{"b", expression.Equal, 55},
		[]testing_pattern_fw.Assertion{{"b", 55}},
		1,
	}}

	for _, test := range cases2 {
		t.Run(test.Description, func(t *testing.T) {
			testing_pattern_fw.ExecuteSeqScanTestCase(t, test)
		})
	}

	// insert new records
	txn = txn_mgr.Begin(nil)
	row1 = make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(666))
	row1 = append(row1, types.NewInteger(777))
	row1 = append(row1, types.NewVarchar("fin"))
	rows = make([][]types.Value, 0)
	rows = append(rows, row1)

	insertPlanNode = plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine = &executors.ExecutionEngine{}
	executorContext = executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)
	bpm.FlushAllPages()
	txn_mgr.Commit(nil, txn)

	cases3 := []testing_pattern_fw.SeqScanTestCase{{
		"select a,c ... WHERE b = 777",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"c", types.Varchar}},
		testing_pattern_fw.Predicate{"b", expression.Equal, 777},
		[]testing_pattern_fw.Assertion{{"a", 666}, {"c", "fin"}},
		1,
	}}

	for _, test := range cases3 {
		t.Run(test.Description, func(t *testing.T) {
			testing_pattern_fw.ExecuteSeqScanTestCase(t, test)
		})
	}
}

func TestSimpleInsertAndUpdate(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewVarchar("hoge"))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewVarchar("foo"))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(nil, txn)

	fmt.Println("update a row...")
	txn = txn_mgr.Begin(nil)

	row1 = make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(99))
	row1 = append(row1, types.NewVarchar("updated"))

	pred := testing_pattern_fw.Predicate{"b", expression.Equal, "foo"}
	tmpColVal := new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.LeftColumn)), pred.Operator, types.Boolean)

	seqScanPlan := plans.NewSeqScanPlanNode(c, tableMetadata.Schema(), expression_, tableMetadata.OID())
	updatePlanNode := plans.NewUpdatePlanNode(row1, []int{0, 1}, seqScanPlan)
	executionEngine.Execute(updatePlanNode, executorContext)

	txn_mgr.Commit(nil, txn)

	fmt.Println("select and check value...")
	txn = txn_mgr.Begin(nil)

	outColumnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnB})

	pred = testing_pattern_fw.Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan := plans.NewSeqScanPlanNode(c, outSchema, expression_, tableMetadata.OID())
	results := executionEngine.Execute(seqPlan, executorContext)

	txn_mgr.Commit(nil, txn)

	testingpkg.Assert(t, types.NewVarchar("updated").CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 'updated'")
}

func TestInsertUpdateMix(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewVarchar("hoge"))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewVarchar("foo"))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(nil, txn)

	fmt.Println("update a row...")
	txn = txn_mgr.Begin(nil)

	row1 = make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(99))
	row1 = append(row1, types.NewVarchar("updated"))

	pred := testing_pattern_fw.Predicate{"b", expression.Equal, "foo"}
	tmpColVal := new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqScanPlan := plans.NewSeqScanPlanNode(c, tableMetadata.Schema(), expression_, tableMetadata.OID())
	updatePlanNode := plans.NewUpdatePlanNode(row1, []int{0, 1}, seqScanPlan)
	executionEngine.Execute(updatePlanNode, executorContext)

	txn_mgr.Commit(nil, txn)

	fmt.Println("select and check value...")
	txn = txn_mgr.Begin(nil)

	outColumnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnB})

	pred = testing_pattern_fw.Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan := plans.NewSeqScanPlanNode(c, outSchema, expression_, tableMetadata.OID())
	results := executionEngine.Execute(seqPlan, executorContext)

	txn_mgr.Commit(nil, txn)

	testingpkg.Assert(t, types.NewVarchar("updated").CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 'updated'")

	fmt.Println("insert after update...")

	txn = txn_mgr.Begin(nil)

	row1 = make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(77))
	row1 = append(row1, types.NewVarchar("hage"))

	row2 = make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(666))
	row2 = append(row2, types.NewVarchar("fuba"))

	rows = make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)

	insertPlanNode = plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(nil, txn)

	fmt.Println("select inserted row and check value...")
	txn = txn_mgr.Begin(nil)

	outColumnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outSchema = schema.NewSchema([]*column.Column{outColumnA})

	pred = testing_pattern_fw.Predicate{"b", expression.Equal, "hage"}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan = plans.NewSeqScanPlanNode(c, outSchema, expression_, tableMetadata.OID())
	results = executionEngine.Execute(seqPlan, executorContext)

	txn_mgr.Commit(nil, txn)

	testingpkg.Assert(t, types.NewInteger(77).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 777")
}

func TestAbortWIthDeleteUpdate(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)
	// TODO: (SDB) for avoiding crash
	txn.SetIsRecoveryPhase(true)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewVarchar("hoge"))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewVarchar("foo"))

	row3 := make([]types.Value, 0)
	row3 = append(row3, types.NewInteger(777))
	row3 = append(row3, types.NewVarchar("bar"))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)
	rows = append(rows, row3)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(nil, txn)

	fmt.Println("update and delete rows...")
	txn = txn_mgr.Begin(nil)
	// TODO: (SDB) for avoiding crash...
	txn.SetIsRecoveryPhase(true)
	executorContext.SetTransaction(txn)

	// update
	row1 = make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(99))
	row1 = append(row1, types.NewVarchar("updated"))

	pred := testing_pattern_fw.Predicate{"b", expression.Equal, "foo"}
	tmpColVal := new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqScanPlan := plans.NewSeqScanPlanNode(c, tableMetadata.Schema(), expression_, tableMetadata.OID())
	updatePlanNode := plans.NewUpdatePlanNode(row1, []int{0, 1}, seqScanPlan)
	executionEngine.Execute(updatePlanNode, executorContext)

	// delete
	pred = testing_pattern_fw.Predicate{"b", expression.Equal, "bar"}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	childSeqScanP := plans.NewSeqScanPlanNode(c, tableMetadata.Schema(), expression_, tableMetadata.OID())
	deletePlanNode := plans.NewDeletePlanNode(childSeqScanP)
	executionEngine.Execute(deletePlanNode, executorContext)

	log_mgr.DeactivateLogging()

	fmt.Println("select and check value before Abort...")

	// check updated row
	outColumnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnB})

	pred = testing_pattern_fw.Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan := plans.NewSeqScanPlanNode(c, outSchema, expression_, tableMetadata.OID())
	results := executionEngine.Execute(seqPlan, executorContext)

	testingpkg.Assert(t, types.NewVarchar("updated").CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 'updated'")

	// check deleted row
	outColumnB = column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outSchema = schema.NewSchema([]*column.Column{outColumnB})

	pred = testing_pattern_fw.Predicate{"b", expression.Equal, "bar"}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan = plans.NewSeqScanPlanNode(c, outSchema, expression_, tableMetadata.OID())
	results = executionEngine.Execute(seqPlan, executorContext)

	// TODO: this assertion is comment-outed due to temporal modification of testee for passing TestUniqSkipListPrallelTxnStrideInteger
	//       this should be reverted at appropriate timing
	testingpkg.Assert(t, len(results) == 0, "")

	txn_mgr.Abort(c, txn)

	fmt.Println("select and check value after Abort...")

	txn = txn_mgr.Begin(nil)
	// TODO: (SDB) for avoiding crash...
	txn.SetIsRecoveryPhase(true)
	executorContext.SetTransaction(txn)

	// check updated row
	outColumnB = column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outSchema = schema.NewSchema([]*column.Column{outColumnB})

	pred = testing_pattern_fw.Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan = plans.NewSeqScanPlanNode(c, outSchema, expression_, tableMetadata.OID())
	results = executionEngine.Execute(seqPlan, executorContext)

	testingpkg.Assert(t, types.NewVarchar("foo").CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 'foo'")

	// check deleted row
	outColumnB = column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outSchema = schema.NewSchema([]*column.Column{outColumnB})

	pred = testing_pattern_fw.Predicate{"b", expression.Equal, "bar"}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan = plans.NewSeqScanPlanNode(c, outSchema, expression_, tableMetadata.OID())
	results = executionEngine.Execute(seqPlan, executorContext)

	testingpkg.Assert(t, len(results) == 1, "")
}

func TestSimpleHashJoin(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)
	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)
	executorContext := executors.NewExecutorContext(c, bpm, txn)

	columnA := column.NewColumn("colA", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("colB", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnC := column.NewColumn("colC", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnD := column.NewColumn("colD", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB, columnC, columnD})
	tableMetadata1 := c.CreateTable("test_1", schema_, txn)

	column1 := column.NewColumn("col1", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	column2 := column.NewColumn("col2", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	column3 := column.NewColumn("col3", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	column4 := column.NewColumn("col3", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ = schema.NewSchema([]*column.Column{column1, column2, column3, column4})
	tableMetadata2 := c.CreateTable("test_2", schema_, txn)

	tableMeta1 := &testing_tbl_gen.TableInsertMeta{"test_1",
		100,
		[]*testing_tbl_gen.ColumnInsertMeta{
			{"colA", types.Integer, false, testing_tbl_gen.DistSerial, 0, 0, 0},
			{"colB", types.Integer, false, testing_tbl_gen.DistUniform, 0, 9, 0},
			{"colC", types.Integer, false, testing_tbl_gen.DistUniform, 0, 9999, 0},
			{"colD", types.Integer, false, testing_tbl_gen.DistUniform, 0, 99999, 0},
		}}
	tableMeta2 := &testing_tbl_gen.TableInsertMeta{"test_2",
		1000,
		[]*testing_tbl_gen.ColumnInsertMeta{
			{"col1", types.Integer, false, testing_tbl_gen.DistSerial, 0, 0, 0},
			{"col2", types.Integer, false, testing_tbl_gen.DistUniform, 0, 9, 0},
			{"col3", types.Integer, false, testing_tbl_gen.DistUniform, 0, 1024, 0},
			{"col4", types.Integer, false, testing_tbl_gen.DistUniform, 0, 2048, 0},
		}}
	testing_tbl_gen.FillTable(tableMetadata1, tableMeta1, txn)
	testing_tbl_gen.FillTable(tableMetadata2, tableMeta2, txn)

	txn_mgr.Commit(nil, txn)

	var scan_plan1 plans.Plan
	var out_schema1 *schema.Schema
	{
		table_info := executorContext.GetCatalog().GetTableByName("test_1")
		colA := column.NewColumn("test_1.colA", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		colB := column.NewColumn("test_1.colB", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		out_schema1 = schema.NewSchema([]*column.Column{colA, colB})
		scan_plan1 = plans.NewSeqScanPlanNode(c, out_schema1, nil, table_info.OID())
	}
	var scan_plan2 plans.Plan
	var out_schema2 *schema.Schema
	{
		table_info := executorContext.GetCatalog().GetTableByName("test_2")
		col1 := column.NewColumn("test_2.col1", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		col2 := column.NewColumn("test_2.col2", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		out_schema2 = schema.NewSchema([]*column.Column{col1, col2})
		scan_plan2 = plans.NewSeqScanPlanNode(c, out_schema2, nil, table_info.OID())
	}
	var join_plan *plans.HashJoinPlanNode
	var out_final *schema.Schema
	{
		// colA and colB have a tuple index of 0 because they are the left side of the join
		//var allocated_exprs []*expression.ColumnValue
		colA := expression.MakeColumnValueExpression(out_schema1, 0, "test_1.colA")
		colA_c := column.NewColumn("colA", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		colA_c.SetIsLeft(true)
		colB_c := column.NewColumn("colB", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		colB_c.SetIsLeft(true)
		// col1 and col2 have a tuple index of 1 because they are the right side of the join
		col1 := expression.MakeColumnValueExpression(out_schema2, 1, "test_2.col1")
		col1_c := column.NewColumn("col1", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		col1_c.SetIsLeft(false)
		col2_c := column.NewColumn("col2", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		col2_c.SetIsLeft(false)
		var left_keys []expression.Expression
		left_keys = append(left_keys, colA)
		var right_keys []expression.Expression
		right_keys = append(right_keys, col1)
		predicate := testing_tbl_gen.MakeComparisonExpression(colA, col1, expression.Equal)
		out_final = schema.NewSchema([]*column.Column{colA_c, colB_c, col1_c, col2_c})
		plans_ := []plans.Plan{scan_plan1, scan_plan2}
		join_plan = plans.NewHashJoinPlanNode(out_final, plans_, predicate,
			left_keys, right_keys)
	}

	executionEngine := &executors.ExecutionEngine{}
	results := executionEngine.Execute(join_plan, executorContext)

	num_tuples := len(results)
	testingpkg.Assert(t, num_tuples == 100, "")
	for ii := 0; ii < 20; ii++ {
		fmt.Println(results[ii])
	}
	fmt.Println("...")
	fmt.Printf("results length = %d\n", num_tuples)
}

func TestSimpleNestedLoopJoin(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)
	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)
	executorContext := executors.NewExecutorContext(c, bpm, txn)

	columnA := column.NewColumn("colA", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("colB", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnC := column.NewColumn("colC", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnD := column.NewColumn("colD", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB, columnC, columnD})
	tableMetadata1 := c.CreateTable("test_1", schema_, txn)

	column1 := column.NewColumn("col1", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	column2 := column.NewColumn("col2", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	column3 := column.NewColumn("col3", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	column4 := column.NewColumn("col4", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ = schema.NewSchema([]*column.Column{column1, column2, column3, column4})
	tableMetadata2 := c.CreateTable("test_2", schema_, txn)

	tableMeta1 := &testing_tbl_gen.TableInsertMeta{"test_1",
		100,
		[]*testing_tbl_gen.ColumnInsertMeta{
			{"colA", types.Integer, false, testing_tbl_gen.DistSerial, 0, 0, 0},
			{"colB", types.Integer, false, testing_tbl_gen.DistUniform, 0, 9, 0},
			{"colC", types.Integer, false, testing_tbl_gen.DistUniform, 0, 9999, 0},
			{"colD", types.Integer, false, testing_tbl_gen.DistUniform, 0, 99999, 0},
		}}
	tableMeta2 := &testing_tbl_gen.TableInsertMeta{"test_2",
		1000,
		[]*testing_tbl_gen.ColumnInsertMeta{
			{"col1", types.Integer, false, testing_tbl_gen.DistSerial, 0, 0, 0},
			{"col2", types.Integer, false, testing_tbl_gen.DistUniform, 0, 9, 0},
			{"col3", types.Integer, false, testing_tbl_gen.DistUniform, 0, 1024, 0},
			{"col4", types.Integer, false, testing_tbl_gen.DistUniform, 0, 2048, 0},
		}}
	testing_tbl_gen.FillTable(tableMetadata1, tableMeta1, txn)
	testing_tbl_gen.FillTable(tableMetadata2, tableMeta2, txn)

	txn_mgr.Commit(nil, txn)

	var scan_plan1 plans.Plan
	var out_schema1 *schema.Schema
	{
		table_info := executorContext.GetCatalog().GetTableByName("test_1")
		colA := column.NewColumn("test_1.colA", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		colB := column.NewColumn("test_1.colB", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		out_schema1 = schema.NewSchema([]*column.Column{colA, colB})
		scan_plan1 = plans.NewSeqScanPlanNode(c, out_schema1, nil, table_info.OID())
	}
	var scan_plan2 plans.Plan
	var out_schema2 *schema.Schema
	{
		table_info := executorContext.GetCatalog().GetTableByName("test_2")
		col1 := column.NewColumn("test_2.col1", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		col2 := column.NewColumn("test_2.col2", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		out_schema2 = schema.NewSchema([]*column.Column{col1, col2})
		scan_plan2 = plans.NewSeqScanPlanNode(c, out_schema2, nil, table_info.OID())
	}
	join_plan := plans.NewNestedLoopJoinPlanNode([]plans.Plan{scan_plan1, scan_plan2})

	executionEngine := &executors.ExecutionEngine{}
	results := executionEngine.Execute(join_plan, executorContext)

	num_tuples := len(results)
	testingpkg.Assert(t, num_tuples == 100000, "")
	for ii := 0; ii < 20; ii++ {
		fmt.Println(results[ii])
	}
	fmt.Println("...")
	fmt.Printf("results length = %d\n", num_tuples)
}

func TestSimpleIndexJoin(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)
	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)
	executorContext := executors.NewExecutorContext(c, bpm, txn)

	columnA := column.NewColumn("colA", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("colB", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnC := column.NewColumn("colC", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnD := column.NewColumn("colD", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB, columnC, columnD})
	tableMetadata1 := c.CreateTable("test_1", schema_, txn)

	column1 := column.NewColumn("col1", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	column2 := column.NewColumn("col2", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	column3 := column.NewColumn("col3", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	column4 := column.NewColumn("col4", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ = schema.NewSchema([]*column.Column{column1, column2, column3, column4})
	tableMetadata2 := c.CreateTable("test_2", schema_, txn)

	tableMeta1 := &testing_tbl_gen.TableInsertMeta{"test_1",
		100,
		[]*testing_tbl_gen.ColumnInsertMeta{
			{"colA", types.Integer, false, testing_tbl_gen.DistSerial, 0, 0, 0},
			{"colB", types.Integer, false, testing_tbl_gen.DistUniform, 0, 9, 0},
			{"colC", types.Integer, false, testing_tbl_gen.DistUniform, 0, 9999, 0},
			{"colD", types.Integer, false, testing_tbl_gen.DistUniform, 0, 99999, 0},
		}}
	tableMeta2 := &testing_tbl_gen.TableInsertMeta{"test_2",
		1000,
		[]*testing_tbl_gen.ColumnInsertMeta{
			{"col1", types.Integer, false, testing_tbl_gen.DistSerial, 0, 0, 0},
			{"col2", types.Integer, false, testing_tbl_gen.DistUniform, 0, 9, 0},
			{"col3", types.Integer, false, testing_tbl_gen.DistUniform, 0, 1024, 0},
			{"col4", types.Integer, false, testing_tbl_gen.DistUniform, 0, 2048, 0},
		}}
	testing_tbl_gen.FillTable(tableMetadata1, tableMeta1, txn)
	testing_tbl_gen.FillTable(tableMetadata2, tableMeta2, txn)

	txn_mgr.Commit(nil, txn)

	var scan_plan1 plans.Plan
	var out_schema1 *schema.Schema
	{
		table_info := executorContext.GetCatalog().GetTableByName("test_1")
		colA := column.NewColumn("test_1.colA", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		colB := column.NewColumn("test_1.colB", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		out_schema1 = schema.NewSchema([]*column.Column{colA, colB})
		scan_plan1 = plans.NewSeqScanPlanNode(c, out_schema1, nil, table_info.OID())
	}
	var scan_plan2 plans.Plan
	var out_schema2 *schema.Schema
	{
		table_info := executorContext.GetCatalog().GetTableByName("test_2")
		col1 := column.NewColumn("test_2.col1", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		col2 := column.NewColumn("test_2.col2", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
		out_schema2 = schema.NewSchema([]*column.Column{col1, col2})
		scan_plan2 = plans.NewSeqScanPlanNode(c, out_schema2, nil, table_info.OID())
	}

	var join_plan *plans.IndexJoinPlanNode
	{
		// colA and colB have a tuple index of 0 because they are the left side of the join
		colA := expression.MakeColumnValueExpression(out_schema1, 0, "test_1.colA")
		// col1 and col2 have a tuple index of 1 because they are the right side of the join
		col2 := expression.MakeColumnValueExpression(out_schema2, 1, "test_2.col2")
		var left_keys []expression.Expression
		left_keys = append(left_keys, colA)
		var right_keys []expression.Expression
		right_keys = append(right_keys, col2)
		join_plan = plans.NewIndexJoinPlanNode(c, scan_plan1, left_keys, scan_plan2.OutputSchema(), scan_plan2.GetTableOID(), right_keys)
	}

	executionEngine := &executors.ExecutionEngine{}
	results := executionEngine.Execute(join_plan, executorContext)

	num_tuples := len(results)
	testingpkg.Assert(t, num_tuples == 1000, "len(results) != 1000. Got %d", num_tuples)
	for ii := 0; ii < 20; ii++ {
		fmt.Println(results[ii])
	}
	fmt.Println("...")
	fmt.Printf("results length = %d\n", num_tuples)
}

func TestInsertAndSeqScanWithComplexPredicateComparison(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnC := column.NewColumn("c", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
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

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(nil, txn)

	cases := []testing_pattern_fw.SeqScanTestCase{{
		"select a ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}},
		testing_pattern_fw.Predicate{"b", expression.Equal, 55},
		[]testing_pattern_fw.Assertion{{"a", 99}},
		1,
	}, {
		"select b ... WHERE a > 20",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"b", types.Integer}},
		testing_pattern_fw.Predicate{"a", expression.GreaterThan, 20},
		[]testing_pattern_fw.Assertion{{"b", 55}},
		1,
	}, {
		"select b ... WHERE a >= 20",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}},
		testing_pattern_fw.Predicate{"a", expression.GreaterThanOrEqual, 20},
		[]testing_pattern_fw.Assertion{{"a", 20}, {"b", 22}},
		2,
	}, {
		"select a, b ... WHERE a < 99",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}},
		testing_pattern_fw.Predicate{"a", expression.LessThan, 99},
		[]testing_pattern_fw.Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b ... WHERE a <= 99",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}},
		testing_pattern_fw.Predicate{"a", expression.LessThanOrEqual, 99},
		[]testing_pattern_fw.Assertion{{"a", 20}, {"b", 22}},
		2,
	}, {
		"select a, b ... WHERE b != 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}},
		testing_pattern_fw.Predicate{"b", expression.NotEqual, 55},
		[]testing_pattern_fw.Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b, c ... WHERE a < 100",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		testing_pattern_fw.Predicate{"a", expression.LessThan, 100},
		[]testing_pattern_fw.Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		2,
	}, {
		"select a, b, c ... WHERE a <= 100",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		testing_pattern_fw.Predicate{"a", expression.LessThanOrEqual, 100},
		[]testing_pattern_fw.Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		2,
	}, {
		"select a, b, c ... WHERE a >= 10",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		testing_pattern_fw.Predicate{"b", expression.GreaterThanOrEqual, 10},
		[]testing_pattern_fw.Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		2,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			testing_pattern_fw.ExecuteSeqScanTestCase(t, test)
		})
	}
}

func rowInsertTransaction(t *testing.T, shi *samehada.SamehadaInstance, c *catalog.Catalog, tm *catalog.TableMetadata, master_ch chan int32) {
	txn := shi.GetTransactionManager().Begin(nil)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewVarchar("hoge"))
	row1 = append(row1, types.NewInteger(40))
	row1 = append(row1, types.NewVarchar("hogehoge"))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewVarchar("foo"))
	row2 = append(row2, types.NewInteger(999))
	row2 = append(row2, types.NewVarchar("foofoo"))

	row3 := make([]types.Value, 0)
	row3 = append(row3, types.NewInteger(11))
	row3 = append(row3, types.NewVarchar("bar"))
	row3 = append(row3, types.NewInteger(17))
	row3 = append(row3, types.NewVarchar("barbar"))

	row4 := make([]types.Value, 0)
	row4 = append(row4, types.NewInteger(100))
	row4 = append(row4, types.NewVarchar("piyo"))
	row4 = append(row4, types.NewInteger(1000))
	row4 = append(row4, types.NewVarchar("piyopiyo"))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)
	rows = append(rows, row3)
	rows = append(rows, row4)

	insertPlanNode := plans.NewInsertPlanNode(rows, tm.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	ret := handleFnishTxn(c, shi.GetTransactionManager(), txn)
	master_ch <- ret
}

func deleteAllRowTransaction(t *testing.T, shi *samehada.SamehadaInstance, c *catalog.Catalog, tm *catalog.TableMetadata, master_ch chan int32) {
	txn := shi.GetTransactionManager().Begin(nil)
	childSeqScanPlan := plans.NewSeqScanPlanNode(c, tm.Schema(), nil, tm.OID())
	deletePlan := plans.NewDeletePlanNode(childSeqScanPlan)

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)
	executionEngine.Execute(deletePlan, executorContext)

	ret := handleFnishTxn(c, shi.GetTransactionManager(), txn)
	master_ch <- ret
}

func selectAllRowTransaction(t *testing.T, shi *samehada.SamehadaInstance, c *catalog.Catalog, tm *catalog.TableMetadata, master_ch chan int32) {
	txn := shi.GetTransactionManager().Begin(nil)

	outColumnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnA})

	seqPlan := plans.NewSeqScanPlanNode(c, outSchema, nil, tm.OID())
	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	executionEngine.Execute(seqPlan, executorContext)

	ret := handleFnishTxn(c, shi.GetTransactionManager(), txn)
	master_ch <- ret
}

func handleFnishTxn(catalog_ *catalog.Catalog, txn_mgr *access.TransactionManager, txn *access.Transaction) int32 {
	// fmt.Println(txn.GetState())
	if txn.GetState() == access.ABORTED {
		// fmt.Println(txn.GetSharedLockSet())
		// fmt.Println(txn.GetExclusiveLockSet())
		txn_mgr.Abort(catalog_, txn)
		return 0
	} else {
		// fmt.Println(txn.GetSharedLockSet())
		// fmt.Println(txn.GetExclusiveLockSet())
		txn_mgr.Commit(nil, txn)
		return 1
	}
}

func TestSimpleAggregation(t *testing.T) {
	// SELECT COUNT(colA), SUM(colA), min(colA), max(colA) from test_1;
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
	exec_ctx := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	table_info, _ := testing_tbl_gen.GenerateTestTabls(c, exec_ctx, txn)

	var scan_plan *plans.SeqScanPlanNode
	var scan_schema *schema.Schema
	{
		schema_ := table_info.Schema()
		colA := expression.MakeColumnValueExpression(schema_, 0, "test_1.colA").(*expression.ColumnValue)
		scan_schema = testing_tbl_gen.MakeOutputSchema([]testing_tbl_gen.MakeSchemaMeta{{"test_1.colA", *colA}})
		scan_plan = plans.NewSeqScanPlanNode(c, scan_schema, nil, table_info.OID()).(*plans.SeqScanPlanNode)
	}

	var agg_plan *plans.AggregationPlanNode
	var agg_schema *schema.Schema
	{
		colA := expression.MakeColumnValueExpression(scan_schema, 0, "test_1.colA")
		countA := *testing_tbl_gen.MakeAggregateValueExpression(false, 0).(*expression.AggregateValueExpression)
		sumA := *testing_tbl_gen.MakeAggregateValueExpression(false, 1).(*expression.AggregateValueExpression)
		minA := *testing_tbl_gen.MakeAggregateValueExpression(false, 2).(*expression.AggregateValueExpression)
		maxA := *testing_tbl_gen.MakeAggregateValueExpression(false, 3).(*expression.AggregateValueExpression)

		agg_schema = testing_tbl_gen.MakeOutputSchemaAgg([]testing_tbl_gen.MakeSchemaMetaAgg{{"countA", countA}, {"sumA", sumA}, {"minA", minA}, {"maxA", maxA}})
		agg_plan = plans.NewAggregationPlanNode(
			agg_schema, scan_plan, nil, []expression.Expression{},
			[]expression.Expression{colA, colA, colA, colA},
			[]plans.AggregationType{plans.COUNT_AGGREGATE, plans.SUM_AGGREGATE,
				plans.MIN_AGGREGATE, plans.MAX_AGGREGATE})
	}

	executionEngine := &executors.ExecutionEngine{}
	executor := executionEngine.CreateExecutor(agg_plan, exec_ctx)
	executor.Init()
	tuple_, _, err := executor.Next()
	testingpkg.Assert(t, tuple_ != nil && err == nil, "first call of AggregationExecutor.Next() failed")
	countA_val := tuple_.GetValue(agg_schema, agg_schema.GetColIndex("countA")).ToInteger()
	sumA_val := tuple_.GetValue(agg_schema, agg_schema.GetColIndex("sumA")).ToInteger()
	minA_val := tuple_.GetValue(agg_schema, agg_schema.GetColIndex("minA")).ToInteger()
	maxA_val := tuple_.GetValue(agg_schema, agg_schema.GetColIndex("maxA")).ToInteger()
	// Should count all tuples
	fmt.Printf("%v %v %v %v\n", countA_val, sumA_val, minA_val, maxA_val)
	testingpkg.Assert(t, countA_val == int32(testing_tbl_gen.TEST1_SIZE), "countA_val is not expected value.")
	// Should sum from 0 to TEST1_SIZE
	testingpkg.Assert(t, sumA_val == int32(testing_tbl_gen.TEST1_SIZE*(testing_tbl_gen.TEST1_SIZE-1)/2), "sumA_val is not expected value.")
	// Minimum should be 0
	testingpkg.Assert(t, minA_val == int32(0), "minA_val is not expected value.")
	// Maximum should be TEST1_SIZE - 1
	testingpkg.Assert(t, maxA_val == int32(testing_tbl_gen.TEST1_SIZE-1), "maxA_val is not expected value.")
	tuple_, done, err := executor.Next()
	testingpkg.Assert(t, tuple_ == nil && done == true && err == nil, "second call of AggregationExecutor::Next() failed")

	txn_mgr.Commit(nil, txn)
	shi.Shutdown(true)
}

func TestSimpleGroupByAggregation(t *testing.T) {
	// SELECT count(colA), colB, sum(C) FROM test_1 Group By colB HAVING count(colA) > 100
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
	exec_ctx := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	table_info, _ := testing_tbl_gen.GenerateTestTabls(c, exec_ctx, txn)

	var scan_plan *plans.SeqScanPlanNode
	var scan_schema *schema.Schema
	{
		schema_ := table_info.Schema()
		colA := expression.MakeColumnValueExpression(schema_, 0, "colA").(*expression.ColumnValue)
		colB := expression.MakeColumnValueExpression(schema_, 0, "colB").(*expression.ColumnValue)
		colC := expression.MakeColumnValueExpression(schema_, 0, "colC").(*expression.ColumnValue)
		scan_schema = testing_tbl_gen.MakeOutputSchema([]testing_tbl_gen.MakeSchemaMeta{{"colA", *colA}, {"colB", *colB}, {"colC", *colC}})
		scan_plan = plans.NewSeqScanPlanNode(c, scan_schema, nil, table_info.OID()).(*plans.SeqScanPlanNode)
	}

	var agg_plan *plans.AggregationPlanNode
	var agg_schema *schema.Schema
	{
		colA := expression.MakeColumnValueExpression(scan_schema, 0, "colA").(*expression.ColumnValue)
		colB := expression.MakeColumnValueExpression(scan_schema, 0, "colB").(*expression.ColumnValue)
		colC := expression.MakeColumnValueExpression(scan_schema, 0, "colC").(*expression.ColumnValue)
		// Make group by
		groupbyB := *testing_tbl_gen.MakeAggregateValueExpression(true, 0).(*expression.AggregateValueExpression)
		// Make aggregates
		countA := *testing_tbl_gen.MakeAggregateValueExpression(false, 0).(*expression.AggregateValueExpression)
		sumC := *testing_tbl_gen.MakeAggregateValueExpression(false, 1).(*expression.AggregateValueExpression)
		// Make having clause
		pred_const := types.NewInteger(int32(testing_tbl_gen.TEST1_SIZE / 10))
		having := testing_tbl_gen.MakeComparisonExpression(&countA, testing_tbl_gen.MakeConstantValueExpression(&pred_const), expression.GreaterThan)

		agg_schema = testing_tbl_gen.MakeOutputSchemaAgg([]testing_tbl_gen.MakeSchemaMetaAgg{{"countA", countA}, {"colB", groupbyB}, {"sumC", sumC}})
		agg_plan = plans.NewAggregationPlanNode(
			agg_schema, scan_plan, having, []expression.Expression{colB},
			[]expression.Expression{colA, colC},
			[]plans.AggregationType{plans.COUNT_AGGREGATE, plans.SUM_AGGREGATE})
	}

	executionEngine := &executors.ExecutionEngine{}
	executor := executionEngine.CreateExecutor(agg_plan, exec_ctx)
	executor.Init()

	var encountered map[int32]int32 = make(map[int32]int32, 0)
	for tuple_, done, _ := executor.Next(); !done; tuple_, done, _ = executor.Next() {
		// Should have countA > 100
		countA := tuple_.GetValue(agg_schema, agg_schema.GetColIndex("countA")).ToInteger()
		colB := tuple_.GetValue(agg_schema, agg_schema.GetColIndex("colB")).ToInteger()
		sumC := tuple_.GetValue(agg_schema, agg_schema.GetColIndex("sumC")).ToInteger()

		fmt.Printf("%d %d %d\n", countA, colB, sumC)

		testingpkg.Assert(t, countA > int32(testing_tbl_gen.TEST1_SIZE/100), "countA result is not greater than 3")

		// should have unique colBs.
		_, ok := encountered[colB]
		testingpkg.Assert(t, !ok, "duplicated colB has been returned")
		encountered[colB] = colB
		// Sanity check: ColB should also be within [0, 10).
		testingpkg.Assert(t, 0 <= colB && colB < 10, "sanity check of colB failed")
	}

	txn_mgr.Commit(nil, txn)
	shi.Shutdown(true)
}

func TestSeqScanWithMultiItemPredicate(t *testing.T) {
	// SELECT colA, colB colC FROM test_1 WHERE (colA > 500 AND colB < 5) OR (NOT colC >= 1000)
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
	exec_ctx := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	table_info, _ := testing_tbl_gen.GenerateTestTabls(c, exec_ctx, txn)

	txn_mgr.Commit(nil, txn)
	shi.GetBufferPoolManager().FlushAllPages()

	txn = txn_mgr.Begin(nil)
	exec_ctx.SetTransaction(txn)

	var scan_plan *plans.SeqScanPlanNode
	var scan_schema *schema.Schema
	{
		// setup predicates and a execution plan
		schema_ := table_info.Schema()
		colA_val := expression.MakeColumnValueExpression(schema_, 0, "colA").(*expression.ColumnValue)
		colB_val := expression.MakeColumnValueExpression(schema_, 0, "colB").(*expression.ColumnValue)
		colC_val := expression.MakeColumnValueExpression(schema_, 0, "colC").(*expression.ColumnValue)

		pred_constA := types.NewInteger(int32(500))
		comp_predA := testing_tbl_gen.MakeComparisonExpression(colA_val, testing_tbl_gen.MakeConstantValueExpression(&pred_constA), expression.GreaterThan)

		pred_constB := types.NewInteger(int32(5))
		comp_predB := testing_tbl_gen.MakeComparisonExpression(colB_val, testing_tbl_gen.MakeConstantValueExpression(&pred_constB), expression.LessThan)

		pred_constC := types.NewInteger(int32(1000))
		comp_predC := testing_tbl_gen.MakeComparisonExpression(colC_val, testing_tbl_gen.MakeConstantValueExpression(&pred_constC), expression.GreaterThanOrEqual)

		// (colA > 500 AND colB < 5)
		left_side_pred := expression.NewLogicalOp(comp_predA, comp_predB, expression.AND, types.Boolean)
		// (NOT colC >= 1000)
		right_side_pred := expression.NewLogicalOp(comp_predC, nil, expression.NOT, types.Boolean)

		// root of predicate
		// (colA > 500 AND colB < 5) OR (NOT colC >= 1000)
		root_pred := expression.NewLogicalOp(left_side_pred, right_side_pred, expression.OR, types.Boolean)

		scan_schema = testing_tbl_gen.MakeOutputSchema([]testing_tbl_gen.MakeSchemaMeta{{"colA", *colA_val}, {"colB", *colB_val}, {"colC", *colC_val}})
		scan_plan = plans.NewSeqScanPlanNode(c, scan_schema, root_pred, table_info.OID()).(*plans.SeqScanPlanNode)
	}

	executionEngine := &executors.ExecutionEngine{}
	results := executionEngine.Execute(scan_plan, exec_ctx)
	fmt.Println(len(results))

	for _, tuple_ := range results {
		colA_val := tuple_.GetValue(scan_schema, scan_schema.GetColIndex("colA")).ToInteger()
		colB_val := tuple_.GetValue(scan_schema, scan_schema.GetColIndex("colB")).ToInteger()
		colC_val := tuple_.GetValue(scan_schema, scan_schema.GetColIndex("colC")).ToInteger()

		testingpkg.Assert(t, (colA_val > 500 && colB_val < 5) || !(colC_val >= 1000), "return tuple violates predicate!")
	}

	txn_mgr.Commit(nil, txn)

	shi.Shutdown(true)
}

func TestInsertAndSpecifiedColumnUpdate(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)

	log_mgr.ActivateLogging()
	testingpkg.Assert(t, log_mgr.IsEnabledLogging(), "")

	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	lock_mgr := access.NewLockManager(access.REGULAR, access.SS2PL_MODE)
	txn_mgr := access.NewTransactionManager(lock_mgr, log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, lock_mgr, txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewVarchar("hoge"))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(99))
	row2 = append(row2, types.NewVarchar("foo"))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(nil, txn)

	fmt.Println("update a row...")
	txn = txn_mgr.Begin(nil)
	executorContext.SetTransaction(txn)

	row1 = make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(-1))        // dummy value
	row1 = append(row1, types.NewVarchar("updated")) //target column

	pred := testing_pattern_fw.Predicate{"b", expression.Equal, "foo"}
	tmpColVal := new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.LeftColumn)), pred.Operator, types.Boolean)

	seqScanPlan := plans.NewSeqScanPlanNode(c, tableMetadata.Schema(), expression_, tableMetadata.OID())
	updatePlanNode := plans.NewUpdatePlanNode(row1, []int{1}, seqScanPlan)
	executionEngine.Execute(updatePlanNode, executorContext)

	txn_mgr.Commit(nil, txn)

	fmt.Println("select and check value...")
	txn = txn_mgr.Begin(nil)
	executorContext.SetTransaction(txn)

	outColumnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outColumnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnA, outColumnB})

	pred = testing_pattern_fw.Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan := plans.NewSeqScanPlanNode(c, outSchema, expression_, tableMetadata.OID())
	results := executionEngine.Execute(seqPlan, executorContext)

	//lock_mgr.PrintLockTables()

	txn_mgr.Commit(nil, txn)

	testingpkg.Assert(t, types.NewInteger(99).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 99")
	testingpkg.Assert(t, types.NewVarchar("updated").CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 'updated'")
}

func TestInsertAndSpecifiedColumnUpdatePageMoveCase(t *testing.T) {
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	log_mgr := shi.GetLogManager()
	txn_mgr := shi.GetTransactionManager()
	bpm := shi.GetBufferPoolManager()
	lock_mgr := shi.GetLockManager()

	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, lock_mgr, txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	// fill tuples around max amount of a page
	rows := make([][]types.Value, 0)
	for ii := 0; ii < 214; ii++ {
		row := make([]types.Value, 0)
		row = append(row, types.NewInteger(int32(ii)))
		row = append(row, types.NewVarchar("k"))

		rows = append(rows, row)
	}
	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(nil, txn)

	fmt.Println("update a row...")
	txn = txn_mgr.Begin(nil)
	executorContext.SetTransaction(txn)

	row := make([]types.Value, 0)
	row = append(row, types.NewInteger(-1))                                  // dummy value
	row = append(row, types.NewVarchar("updated_xxxxxxxxxxxxxxxxxxxxxxxxx")) //target column

	pred := testing_pattern_fw.Predicate{"a", expression.Equal, 99}
	tmpColVal := new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.LeftColumn)), pred.Operator, types.Boolean)

	seqScanPlan := plans.NewSeqScanPlanNode(c, tableMetadata.Schema(), expression_, tableMetadata.OID())
	updatePlanNode := plans.NewUpdatePlanNode(row, []int{1}, seqScanPlan)
	executionEngine.Execute(updatePlanNode, executorContext)

	txn_mgr.Commit(nil, txn)

	fmt.Println("select and check value...")
	txn = txn_mgr.Begin(nil)
	executorContext.SetTransaction(txn)

	outColumnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outColumnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnA, outColumnB})

	pred = testing_pattern_fw.Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan := plans.NewSeqScanPlanNode(c, outSchema, expression_, tableMetadata.OID())
	results := executionEngine.Execute(seqPlan, executorContext)

	txn_mgr.Commit(nil, txn)

	bpm.FlushAllPages()

	testingpkg.Assert(t, types.NewInteger(99).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 99")
	testingpkg.Assert(t, types.NewVarchar("updated_xxxxxxxxxxxxxxxxxxxxxxxxx").CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 'updated_xxxxxxxxxxxxxxxxxxxxxxxxx'")

	shi.Shutdown(true)
}

func TestInsertAndSpecifiedColumnUpdatePageMoveRecovery(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true

	// clear all state of DB
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage == true {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	log_mgr := shi.GetLogManager()
	txn_mgr := shi.GetTransactionManager()
	bpm := shi.GetBufferPoolManager()
	lock_mgr := shi.GetLockManager()

	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, lock_mgr, txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	// fill tuples around max amount of a page
	rows := make([][]types.Value, 0)
	for ii := 0; ii < 214; ii++ {
		row := make([]types.Value, 0)
		row = append(row, types.NewInteger(int32(ii)))
		row = append(row, types.NewVarchar("k"))

		rows = append(rows, row)
	}
	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(nil, txn)

	fmt.Println("update a row...")
	txn = txn_mgr.Begin(nil)
	executorContext.SetTransaction(txn)

	row := make([]types.Value, 0)
	row = append(row, types.NewInteger(300))
	row = append(row, types.NewVarchar("updated_xxxxxxxxxxxxxxxxxxxxxxxxx")) //target column

	pred := testing_pattern_fw.Predicate{"a", expression.Equal, 99}
	tmpColVal := new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.LeftColumn)), pred.Operator, types.Boolean)

	seqScanPlan := plans.NewSeqScanPlanNode(c, tableMetadata.Schema(), expression_, tableMetadata.OID())
	updatePlanNode := plans.NewUpdatePlanNode(row, []int{0, 1}, seqScanPlan)
	executionEngine.Execute(updatePlanNode, executorContext)

	// system crash before finish txn
	shi.Shutdown(false)

	// restart system
	shi = samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	log_mgr = shi.GetLogManager()
	lock_mgr = shi.GetLockManager()
	txn_mgr = shi.GetTransactionManager()
	bpm = shi.GetBufferPoolManager()

	log_recovery := log_recovery.NewLogRecovery(
		shi.GetDiskManager(),
		bpm,
		log_mgr)

	txn = txn_mgr.Begin(nil)
	c = catalog.RecoveryCatalogFromCatalogPage(bpm, log_mgr, lock_mgr, txn)
	tableMetadata = c.GetTableByName("test_1")

	executorContext = executors.NewExecutorContext(c, bpm, txn)
	executorContext.SetTransaction(txn)

	outColumnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outColumnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnA, outColumnB})

	// disable logging
	log_mgr.DeactivateLogging()
	txn.SetIsRecoveryPhase(true)

	// do recovery from Log
	log_recovery.Redo(txn)
	log_recovery.Undo(txn)

	txn.SetIsRecoveryPhase(false)
	// reactivate logging
	log_mgr.ActivateLogging()

	// check updated value does not exist (a = 300)
	fmt.Println("select and check value (2) ...")
	pred = testing_pattern_fw.Predicate{"a", expression.Equal, 300}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.LeftColumn)), pred.Operator, types.Boolean)

	seqPlan := plans.NewSeqScanPlanNode(c, outSchema, expression_, tableMetadata.OID())
	results := executionEngine.Execute(seqPlan, executorContext)
	testingpkg.Assert(t, len(results) == 0, "updated value should not be exist")

	// check updated value is rollbaced (Undo rollbacked not commited transaction)
	fmt.Println("select and check value (3) ...")
	pred = testing_pattern_fw.Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan = plans.NewSeqScanPlanNode(c, outSchema, expression_, tableMetadata.OID())
	results = executionEngine.Execute(seqPlan, executorContext)

	testingpkg.Assert(t, types.NewInteger(99).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 99")
	testingpkg.Assert(t, types.NewVarchar("k").CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 'k'")

	shi.Shutdown(true)

	common.TempSuppressOnMemStorage = false
	common.TempSuppressOnMemStorageMutex.Unlock()
}

func TestInsertAndSpecifiedColumnUpdatePageMoveOccurOnRecovery(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true

	// clear all state of DB
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage == true {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	log_mgr := shi.GetLogManager()
	txn_mgr := shi.GetTransactionManager()
	bpm := shi.GetBufferPoolManager()
	lock_mgr := shi.GetLockManager()

	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, lock_mgr, txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	// fill tuples around max amount of a page
	rows := make([][]types.Value, 0)
	for ii := 0; ii < 180; ii++ {
		row := make([]types.Value, 0)
		row = append(row, types.NewInteger(int32(ii)))
		row = append(row, types.NewVarchar("k"))

		rows = append(rows, row)
	}
	row := make([]types.Value, 0)
	row = append(row, types.NewInteger(180))
	row = append(row, types.NewVarchar("kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk"))
	rows = append(rows, row)

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(nil, txn)

	fmt.Println("a update operation which does not change data size and a update operation which changes datasize...")
	txn = txn_mgr.Begin(nil)
	executorContext.SetTransaction(txn)

	pred := testing_pattern_fw.Predicate{"a", expression.Equal, 180}
	tmpColVal := new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.LeftColumn)), pred.Operator, types.Boolean)

	row = make([]types.Value, 0)
	row = append(row, types.NewInteger(180))
	row = append(row, types.NewVarchar("kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkka")) //target column

	seqScanPlan := plans.NewSeqScanPlanNode(c, tableMetadata.Schema(), expression_, tableMetadata.OID())
	updatePlanNode := plans.NewUpdatePlanNode(row, []int{0, 1}, seqScanPlan)
	executionEngine.Execute(updatePlanNode, executorContext)

	pred = testing_pattern_fw.Predicate{"a", expression.Equal, 180}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.LeftColumn)), pred.Operator, types.Boolean)

	row = make([]types.Value, 0)
	row = append(row, types.NewInteger(300))
	row = append(row, types.NewVarchar("k")) //target column

	seqScanPlan = plans.NewSeqScanPlanNode(c, tableMetadata.Schema(), expression_, tableMetadata.OID())
	updatePlanNode = plans.NewUpdatePlanNode(row, []int{0, 1}, seqScanPlan)
	executionEngine.Execute(updatePlanNode, executorContext)

	// not commit "txn"

	fmt.Println("filling a row...")
	txn2 := txn_mgr.Begin(nil)

	rows2 := make([][]types.Value, 0)
	for ii := 0; ii < 30; ii++ {
		row := make([]types.Value, 0)
		row = append(row, types.NewInteger(int32(ii)))
		row = append(row, types.NewVarchar("k"))

		rows2 = append(rows2, row)
	}

	executionEngine = &executors.ExecutionEngine{}
	executorContext = executors.NewExecutorContext(c, bpm, txn2)
	insertPlanNode = plans.NewInsertPlanNode(rows2, tableMetadata.OID())
	executionEngine.Execute(insertPlanNode, executorContext)

	// commit filling "txn2"
	txn_mgr.Commit(nil, txn2)

	// system crash before finish "txn"
	shi.Shutdown(false)

	// restart system
	shi = samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	log_mgr = shi.GetLogManager()
	lock_mgr = shi.GetLockManager()
	txn_mgr = shi.GetTransactionManager()
	bpm = shi.GetBufferPoolManager()

	log_recovery := log_recovery.NewLogRecovery(
		shi.GetDiskManager(),
		bpm,
		log_mgr)

	txn = txn_mgr.Begin(nil)
	c = catalog.RecoveryCatalogFromCatalogPage(bpm, log_mgr, lock_mgr, txn)
	tableMetadata = c.GetTableByName("test_1")

	executorContext = executors.NewExecutorContext(c, bpm, txn)
	executorContext.SetTransaction(txn)

	outColumnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outColumnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnA, outColumnB})

	// disable logging
	log_mgr.DeactivateLogging()
	txn.SetIsRecoveryPhase(true)

	// do recovery from Log
	log_recovery.Redo(txn)
	log_recovery.Undo(txn)

	txn.SetIsRecoveryPhase(false)
	// reactivate logging
	log_mgr.ActivateLogging()

	// check updated value does not exist (a = 300)
	fmt.Println("select and check value (2) ...")
	pred = testing_pattern_fw.Predicate{"a", expression.Equal, 300}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.LeftColumn)), pred.Operator, types.Boolean)

	seqPlan := plans.NewSeqScanPlanNode(c, outSchema, expression_, tableMetadata.OID())
	results := executionEngine.Execute(seqPlan, executorContext)
	testingpkg.Assert(t, len(results) == 0, "updated value should not be exist")

	// check updated value is rollbaced (Undo rollbacked not commited transaction)
	fmt.Println("select and check value (3) ...")
	pred = testing_pattern_fw.Predicate{"a", expression.Equal, 180}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(testing_util.GetValue(pred.RightColumn), testing_util.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan = plans.NewSeqScanPlanNode(c, outSchema, expression_, tableMetadata.OID())
	results = executionEngine.Execute(seqPlan, executorContext)

	testingpkg.Assert(t, types.NewInteger(180).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 180")
	testingpkg.Assert(t, types.NewVarchar("kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk").CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 'kkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkkk'")

	shi.Shutdown(true)

	common.TempSuppressOnMemStorage = false
	common.TempSuppressOnMemStorageMutex.Unlock()
}

func TestSimpleSeqScanAndOrderBy(t *testing.T) {
	// SELECT a, b, FROM test_1 ORDER BY a, b
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
	exec_ctx := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1 = append(row1, types.NewVarchar("celemony"))

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(20))
	row2 = append(row2, types.NewVarchar("boo"))

	row3 := make([]types.Value, 0)
	row3 = append(row3, types.NewInteger(10))
	row3 = append(row3, types.NewVarchar("daylight"))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)
	rows = append(rows, row3)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())
	executionEngine := &executors.ExecutionEngine{}
	executionEngine.Execute(insertPlanNode, exec_ctx)

	txn_mgr.Commit(nil, txn)

	txn = txn_mgr.Begin(nil)
	exec_ctx.SetTransaction(txn)

	var scan_plan *plans.SeqScanPlanNode
	var scan_schema *schema.Schema
	{
		schema_ := tableMetadata.Schema()
		colA := expression.MakeColumnValueExpression(schema_, 0, "a").(*expression.ColumnValue)
		colB := expression.MakeColumnValueExpression(schema_, 0, "b").(*expression.ColumnValue)
		scan_schema = testing_tbl_gen.MakeOutputSchema([]testing_tbl_gen.MakeSchemaMeta{{"a", *colA}, {"b", *colB}})
		scan_plan = plans.NewSeqScanPlanNode(c, scan_schema, nil, tableMetadata.OID()).(*plans.SeqScanPlanNode)
	}

	orderby_plan := plans.NewOrderbyPlanNode(
		nil, scan_plan, []int{0, 1},
		[]plans.OrderbyType{plans.ASC, plans.ASC})

	results := executionEngine.Execute(orderby_plan, exec_ctx)

	fmt.Println(results[0].GetValue(scan_schema, 0).ToInteger())
	fmt.Println(results[0].GetValue(scan_schema, 1).ToVarchar())
	fmt.Println(results[1].GetValue(scan_schema, 0).ToInteger())
	fmt.Println(results[1].GetValue(scan_schema, 1).ToVarchar())
	fmt.Println(results[2].GetValue(scan_schema, 0).ToInteger())
	fmt.Println(results[2].GetValue(scan_schema, 1).ToVarchar())

	testingpkg.Assert(t, types.NewInteger(10).CompareEquals(results[0].GetValue(scan_schema, 0)), "value should be 10")
	testingpkg.Assert(t, types.NewVarchar("daylight").CompareEquals(results[0].GetValue(scan_schema, 1)), "value should be 'daylight'")

	testingpkg.Assert(t, types.NewInteger(20).CompareEquals(results[1].GetValue(scan_schema, 0)), "value should be 20")
	testingpkg.Assert(t, types.NewVarchar("boo").CompareEquals(results[1].GetValue(scan_schema, 1)), "value should be 'boo'")

	testingpkg.Assert(t, types.NewInteger(20).CompareEquals(results[2].GetValue(scan_schema, 0)), "value should be 20")
	testingpkg.Assert(t, types.NewVarchar("celemony").CompareEquals(results[2].GetValue(scan_schema, 1)), "value should be 'celemony'")

	// test other order
	orderby_plan = plans.NewOrderbyPlanNode(
		nil, scan_plan, []int{0, 1},
		[]plans.OrderbyType{plans.DESC, plans.DESC})

	results = executionEngine.Execute(orderby_plan, exec_ctx)

	fmt.Println(results[0].GetValue(scan_schema, 0).ToInteger())
	fmt.Println(results[0].GetValue(scan_schema, 1).ToVarchar())
	fmt.Println(results[1].GetValue(scan_schema, 0).ToInteger())
	fmt.Println(results[1].GetValue(scan_schema, 1).ToVarchar())
	fmt.Println(results[2].GetValue(scan_schema, 0).ToInteger())
	fmt.Println(results[2].GetValue(scan_schema, 1).ToVarchar())

	testingpkg.Assert(t, types.NewInteger(20).CompareEquals(results[0].GetValue(scan_schema, 0)), "value should be 20")
	testingpkg.Assert(t, types.NewVarchar("celemony").CompareEquals(results[0].GetValue(scan_schema, 1)), "value should be 'celemony'")

	testingpkg.Assert(t, types.NewInteger(20).CompareEquals(results[1].GetValue(scan_schema, 0)), "value should be 20")
	testingpkg.Assert(t, types.NewVarchar("boo").CompareEquals(results[1].GetValue(scan_schema, 1)), "value should be 'boo'")

	testingpkg.Assert(t, types.NewInteger(10).CompareEquals(results[2].GetValue(scan_schema, 0)), "value should be 10")
	testingpkg.Assert(t, types.NewVarchar("daylight").CompareEquals(results[2].GetValue(scan_schema, 1)), "value should be 'daylight'")

	txn_mgr.Commit(nil, txn)
	shi.Shutdown(true)
}

func TestSimpleSetNullToVarchar(t *testing.T) {
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
	exec_ctx := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1_col2 := types.NewVarchar("celemony")
	row1_col2.SetNull()
	row1 = append(row1, row1_col2)

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(20))
	row2 = append(row2, types.NewVarchar("boo"))

	row3 := make([]types.Value, 0)
	row3 = append(row3, types.NewInteger(10))
	row3 = append(row3, types.NewVarchar("daylight"))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)
	rows = append(rows, row3)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())
	executionEngine := &executors.ExecutionEngine{}
	executionEngine.Execute(insertPlanNode, exec_ctx)

	txn_mgr.Commit(nil, txn)

	txn = txn_mgr.Begin(nil)
	exec_ctx.SetTransaction(txn)

	var scan_plan *plans.SeqScanPlanNode
	var scan_schema *schema.Schema
	{
		schema_ := tableMetadata.Schema()
		colA := expression.MakeColumnValueExpression(schema_, 0, "a").(*expression.ColumnValue)
		colB := expression.MakeColumnValueExpression(schema_, 0, "b").(*expression.ColumnValue)
		scan_schema = testing_tbl_gen.MakeOutputSchema([]testing_tbl_gen.MakeSchemaMeta{{"a", *colA}, {"b", *colB}})
		scan_plan = plans.NewSeqScanPlanNode(c, scan_schema, nil, tableMetadata.OID()).(*plans.SeqScanPlanNode)
	}

	results := executionEngine.Execute(scan_plan, exec_ctx)

	fmt.Println(results[0].GetValue(scan_schema, 0).ToInteger())
	fmt.Println(results[0].GetValue(scan_schema, 1).ToVarchar())
	fmt.Println(results[1].GetValue(scan_schema, 0).ToInteger())
	fmt.Println(results[1].GetValue(scan_schema, 1).ToVarchar())
	fmt.Println(results[2].GetValue(scan_schema, 0).ToInteger())
	fmt.Println(results[2].GetValue(scan_schema, 1).ToVarchar())

	testingpkg.Assert(t, types.NewInteger(20).CompareEquals(results[0].GetValue(scan_schema, 0)), "value should be 20")
	testingpkg.Assert(t, types.NewVarchar("").CompareEquals(results[0].GetValue(scan_schema, 1)) == false, "compared result should be false")
	testingpkg.Assert(t, results[0].GetValue(scan_schema, 1).IsNull() == true, "IsNull() of column at 1 value should be true")

	testingpkg.Assert(t, types.NewInteger(20).CompareEquals(results[1].GetValue(scan_schema, 0)), "value should be 20")
	testingpkg.Assert(t, types.NewVarchar("boo").CompareEquals(results[1].GetValue(scan_schema, 1)), "value should be 'boo'")

	testingpkg.Assert(t, types.NewInteger(10).CompareEquals(results[2].GetValue(scan_schema, 0)), "value should be 10")
	testingpkg.Assert(t, types.NewVarchar("daylight").CompareEquals(results[2].GetValue(scan_schema, 1)), "value should be 'daylight'")

	txn_mgr.Commit(nil, txn)
	shi.Shutdown(true)
}

func TestInsertNullValueAndSeqScanWithNullComparison(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

	row1 := make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(20))
	row1_col2 := types.NewVarchar("celemony")
	row1_col2.SetNull()
	row1 = append(row1, row1_col2)

	row2 := make([]types.Value, 0)
	row2 = append(row2, types.NewInteger(10))
	row2 = append(row2, types.NewVarchar("boo"))

	row3 := make([]types.Value, 0)
	row3 = append(row3, types.NewInteger(30))
	row3 = append(row3, types.NewVarchar("daylight"))

	rows := make([][]types.Value, 0)
	rows = append(rows, row1)
	rows = append(rows, row2)
	rows = append(rows, row3)

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(nil, txn)

	cases := []testing_pattern_fw.SeqScanTestCase{{
		"select a, b ... WHERE b = NULL",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Varchar}},
		testing_pattern_fw.Predicate{"b", expression.Equal, types.NewVarchar("").SetNull()},
		[]testing_pattern_fw.Assertion{{"a", 20}},
		1,
	}, {
		"select a, b ... WHERE a = 20",
		executionEngine,
		executorContext,
		tableMetadata,
		[]testing_pattern_fw.Column{{"a", types.Integer}, {"b", types.Varchar}},
		testing_pattern_fw.Predicate{"a", expression.Equal, 20},
		[]testing_pattern_fw.Assertion{{"a", 20}},
		1,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			testing_pattern_fw.ExecuteSeqScanTestCase(t, test)
		})
	}
}
