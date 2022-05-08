// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
	"fmt"
	"os"
	"runtime"
	"testing"

	"github.com/devlights/gomy/output"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/disk"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/test_util"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
)

func TestSimpleInsertAndSeqScan(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, nil)
	columnB := column.NewColumn("b", types.Integer, false, nil)
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

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	outColumnA := column.NewColumn("a", types.Integer, false, nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnA})

	seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())

	results := executionEngine.Execute(seqPlan, executorContext)

	txn_mgr.Commit(txn)

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

	columnA := column.NewColumn("a", types.Float, false, nil)
	columnB := column.NewColumn("b", types.Float, false, nil)
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

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	outColumnA := column.NewColumn("a", types.Float, false, nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnA})

	seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())

	results := executionEngine.Execute(seqPlan, executorContext)

	txn_mgr.Commit(txn)

	testingpkg.Assert(t, types.NewFloat(0.5).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 0.5")
	testingpkg.Assert(t, types.NewFloat(0.99).CompareEquals(results[1].GetValue(outSchema, 0)), "value should be 0.99")
}

func TestSimpleInsertAndSeqScanWithPredicateComparison(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, nil)
	columnB := column.NewColumn("b", types.Integer, false, nil)
	columnC := column.NewColumn("c", types.Varchar, false, nil)
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

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(txn)

	cases := []SeqScanTestCase{{
		"select a ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}},
		Predicate{"b", expression.Equal, 55},
		[]Assertion{{"a", 99}},
		1,
	}, {
		"select b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"b", types.Integer}},
		Predicate{"b", expression.Equal, 55},
		[]Assertion{{"b", 55}},
		1,
	}, {
		"select a, b ... WHERE a = 20",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"a", expression.Equal, 20},
		[]Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b ... WHERE a = 99",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"a", expression.Equal, 99},
		[]Assertion{{"a", 99}, {"b", 55}},
		1,
	}, {
		"select a, b ... WHERE a = 100",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"a", expression.Equal, 100},
		[]Assertion{},
		0,
	}, {
		"select a, b ... WHERE b != 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"b", expression.NotEqual, 55},
		[]Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b, c ... WHERE c = 'foo'",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		Predicate{"c", expression.Equal, "foo"},
		[]Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		1,
	}, {
		"select a, b, c ... WHERE c != 'foo'",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		Predicate{"c", expression.NotEqual, "foo"},
		[]Assertion{{"a", 99}, {"b", 55}, {"c", "bar"}},
		1,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			ExecuteSeqScanTestCase(t, test)
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

	columnA := column.NewColumn("a", types.Integer, false, nil)
	columnB := column.NewColumn("b", types.Integer, false, nil)
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

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(txn)

	// TEST 1: select a, b ... LIMIT 1
	func() {
		a := column.NewColumn("a", types.Integer, false, nil)
		b := column.NewColumn("b", types.Integer, false, nil)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 1, 1)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 1, len(results))
		testingpkg.Assert(t, types.NewInteger(99).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 99 but was %d", results[0].GetValue(outSchema, 0).ToInteger())
		testingpkg.Assert(t, types.NewInteger(55).CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 55 but was %d", results[0].GetValue(outSchema, 1).ToInteger())
	}()

	// TEST 1: select a, b ... LIMIT 2
	func() {
		a := column.NewColumn("a", types.Integer, false, nil)
		b := column.NewColumn("b", types.Integer, false, nil)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 2, 0)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 2, len(results))
	}()

	// TEST 1: select a, b ... LIMIT 3
	func() {
		a := column.NewColumn("a", types.Integer, false, nil)
		b := column.NewColumn("b", types.Integer, false, nil)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())
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

	columnA := column.NewColumn("a", types.Integer, false, nil)
	columnB := column.NewColumn("b", types.Integer, false, nil)
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

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	// construct second table

	columnA = column.NewColumn("a", types.Integer, false, nil)
	columnB = column.NewColumn("b", types.Integer, false, nil)
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

	//executionEngine := &ExecutionEngine{}
	//executorContext := NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(txn)

	// TEST 1: select a, b ... LIMIT 1
	func() {
		a := column.NewColumn("a", types.Integer, false, nil)
		b := column.NewColumn("b", types.Integer, false, nil)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 1, 1)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 1, len(results))
		testingpkg.Assert(t, types.NewInteger(99).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 99 but was %d", results[0].GetValue(outSchema, 0).ToInteger())
		testingpkg.Assert(t, types.NewInteger(55).CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 55 but was %d", results[0].GetValue(outSchema, 1).ToInteger())
	}()

	// TEST 1: select a, b ... LIMIT 2
	func() {
		a := column.NewColumn("a", types.Integer, false, nil)
		b := column.NewColumn("b", types.Integer, false, nil)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 2, 0)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 2, len(results))
	}()

	// TEST 1: select a, b ... LIMIT 1
	func() {
		a := column.NewColumn("a", types.Integer, false, nil)
		b := column.NewColumn("b", types.Integer, false, nil)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata2.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 1, 1)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 1, len(results))
		testingpkg.Assert(t, types.NewInteger(99).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 99 but was %d", results[0].GetValue(outSchema, 0).ToInteger())
		testingpkg.Assert(t, types.NewInteger(55).CompareEquals(results[0].GetValue(outSchema, 1)), "value should be 55 but was %d", results[0].GetValue(outSchema, 1).ToInteger())
	}()

	// TEST 1: select a, b ... LIMIT 3
	func() {
		a := column.NewColumn("a", types.Integer, false, nil)
		b := column.NewColumn("b", types.Integer, false, nil)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata2.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 3, 0)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 3, len(results))
	}()
}

func TestHashTableIndex(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))

	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, true, nil)
	columnB := column.NewColumn("b", types.Integer, true, nil)
	columnC := column.NewColumn("c", types.Varchar, true, nil)
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

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(txn)

	cases := []HashIndexScanTestCase{{
		"select a ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}},
		Predicate{"b", expression.Equal, 55},
		[]Assertion{{"a", 99}},
		1,
	}, {
		"select b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"b", types.Integer}},
		Predicate{"b", expression.Equal, 55},
		[]Assertion{{"b", 55}},
		1,
	}, {
		"select a, b ... WHERE a = 20",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"a", expression.Equal, 20},
		[]Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b ... WHERE a = 99",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"a", expression.Equal, 99},
		[]Assertion{{"a", 99}, {"b", 55}},
		1,
	}, {
		"select a, b ... WHERE a = 100",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"a", expression.Equal, 100},
		[]Assertion{},
		0,
	}, {
		"select a, b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"b", expression.Equal, 55},
		[]Assertion{{"a", 99}, {"b", 55}},
		1,
	}, {
		"select a, b, c ... WHERE c = 'foo'",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		Predicate{"c", expression.Equal, "foo"},
		[]Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		1,
	}, {
		"select a, b ... WHERE c = 'baz'",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"c", expression.Equal, "baz"},
		[]Assertion{{"a", 1225}, {"b", 712}},
		2,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			ExecuteHashIndexScanTestCase(t, test)
		})
	}

}

func TestSimpleDelete(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, nil)
	columnB := column.NewColumn("b", types.Integer, false, nil)
	columnC := column.NewColumn("c", types.Varchar, false, nil)
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

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(txn)

	cases := []DeleteTestCase{{
		"delete ... WHERE c = 'baz'",
		txn_mgr,
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		Predicate{"c", expression.Equal, "baz"},
		[]Assertion{{"a", 1225}, {"b", 712}, {"c", "baz"}},
		2,
	}, {
		"delete ... WHERE b = 55",
		txn_mgr,
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		Predicate{"b", expression.Equal, 55},
		[]Assertion{{"a", 99}, {"b", 55}, {"c", "bar"}},
		1,
	}, {
		"delete ... WHERE a = 20",
		txn_mgr,
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		Predicate{"a", expression.Equal, 20},
		[]Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		1,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			ExecuteDeleteTestCase(t, test)
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

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, nil)
	columnB := column.NewColumn("b", types.Integer, false, nil)
	columnC := column.NewColumn("c", types.Varchar, false, nil)
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

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(txn)

	cases := []DeleteTestCase{{
		"delete ... WHERE c = 'baz'",
		txn_mgr,
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		Predicate{"c", expression.Equal, "baz"},
		[]Assertion{{"a", 1225}, {"b", 712}, {"c", "baz"}},
		2,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			ExecuteDeleteTestCase(t, test)
		})
	}

	cases2 := []SeqScanTestCase{{
		"select a ... WHERE c = baz",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}},
		Predicate{"c", expression.Equal, "baz"},
		[]Assertion{{"a", 99}},
		0,
	}, {
		"select b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"b", types.Integer}},
		Predicate{"b", expression.Equal, 55},
		[]Assertion{{"b", 55}},
		1,
	}}

	for _, test := range cases2 {
		t.Run(test.Description, func(t *testing.T) {
			ExecuteSeqScanTestCase(t, test)
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

	executionEngine = &ExecutionEngine{}
	executorContext = NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)
	bpm.FlushAllPages()
	txn_mgr.Commit(txn)

	cases3 := []SeqScanTestCase{{
		"select a,c ... WHERE b = 777",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"c", types.Varchar}},
		Predicate{"b", expression.Equal, 777},
		[]Assertion{{"a", 666}, {"c", "fin"}},
		1,
	}}

	for _, test := range cases3 {
		t.Run(test.Description, func(t *testing.T) {
			ExecuteSeqScanTestCase(t, test)
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

	columnA := column.NewColumn("a", types.Integer, false, nil)
	columnB := column.NewColumn("b", types.Varchar, false, nil)
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

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(txn)

	fmt.Println("update a row...")
	txn = txn_mgr.Begin(nil)

	row1 = make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(99))
	row1 = append(row1, types.NewVarchar("updated"))

	pred := Predicate{"b", expression.Equal, "foo"}
	tmpColVal := new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(pred.RightColumn), GetValueType(pred.LeftColumn)), pred.Operator, types.Boolean)

	updatePlanNode := plans.NewUpdatePlanNode(row1, &expression_, tableMetadata.OID())
	executionEngine.Execute(updatePlanNode, executorContext)

	txn_mgr.Commit(txn)

	fmt.Println("select and check value...")
	txn = txn_mgr.Begin(nil)

	outColumnB := column.NewColumn("b", types.Varchar, false, nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnB})

	pred = Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(pred.RightColumn), GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan := plans.NewSeqScanPlanNode(outSchema, &expression_, tableMetadata.OID())
	results := executionEngine.Execute(seqPlan, executorContext)

	txn_mgr.Commit(txn)

	testingpkg.Assert(t, types.NewVarchar("updated").CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 'updated'")
	//testingpkg.Assert(t, types.NewInteger(99).CompareEquals(results[1].GetValue(outSchema, 0)), "value should be 99")
}

func TestInsertUpdateMix(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, nil)
	columnB := column.NewColumn("b", types.Varchar, false, nil)
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

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(txn)

	fmt.Println("update a row...")
	txn = txn_mgr.Begin(nil)

	row1 = make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(99))
	row1 = append(row1, types.NewVarchar("updated"))

	pred := Predicate{"b", expression.Equal, "foo"}
	tmpColVal := new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(pred.RightColumn), GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	updatePlanNode := plans.NewUpdatePlanNode(row1, &expression_, tableMetadata.OID())
	executionEngine.Execute(updatePlanNode, executorContext)

	txn_mgr.Commit(txn)

	fmt.Println("select and check value...")
	txn = txn_mgr.Begin(nil)

	outColumnB := column.NewColumn("b", types.Varchar, false, nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnB})

	pred = Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(pred.RightColumn), GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan := plans.NewSeqScanPlanNode(outSchema, &expression_, tableMetadata.OID())
	results := executionEngine.Execute(seqPlan, executorContext)

	txn_mgr.Commit(txn)

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

	txn_mgr.Commit(txn)

	fmt.Println("select inserted row and check value...")
	txn = txn_mgr.Begin(nil)

	outColumnA := column.NewColumn("a", types.Integer, false, nil)
	outSchema = schema.NewSchema([]*column.Column{outColumnA})

	pred = Predicate{"b", expression.Equal, "hage"}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(pred.RightColumn), GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan = plans.NewSeqScanPlanNode(outSchema, &expression_, tableMetadata.OID())
	results = executionEngine.Execute(seqPlan, executorContext)

	txn_mgr.Commit(txn)

	testingpkg.Assert(t, types.NewInteger(77).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 777")
}

func TestAbortWIthDeleteUpdate(t *testing.T) {
	os.Stdout.Sync()
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, nil)
	columnB := column.NewColumn("b", types.Varchar, false, nil)
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

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(txn)

	fmt.Println("update and delete rows...")
	txn = txn_mgr.Begin(nil)
	executorContext.SetTransaction(txn)

	// update
	row1 = make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(99))
	row1 = append(row1, types.NewVarchar("updated"))

	pred := Predicate{"b", expression.Equal, "foo"}
	tmpColVal := new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(pred.RightColumn), GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	updatePlanNode := plans.NewUpdatePlanNode(row1, &expression_, tableMetadata.OID())
	executionEngine.Execute(updatePlanNode, executorContext)

	// delete
	pred = Predicate{"b", expression.Equal, "bar"}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(pred.RightColumn), GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	deletePlanNode := plans.NewDeletePlanNode(expression_, tableMetadata.OID())
	executionEngine.Execute(deletePlanNode, executorContext)

	common.EnableLogging = false

	fmt.Println("select and check value before Abort...")

	// check updated row
	outColumnB := column.NewColumn("b", types.Varchar, false, nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnB})

	pred = Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(pred.RightColumn), GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan := plans.NewSeqScanPlanNode(outSchema, &expression_, tableMetadata.OID())
	results := executionEngine.Execute(seqPlan, executorContext)

	testingpkg.Assert(t, types.NewVarchar("updated").CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 'updated'")

	// check deleted row
	outColumnB = column.NewColumn("b", types.Integer, false, nil)
	outSchema = schema.NewSchema([]*column.Column{outColumnB})

	pred = Predicate{"b", expression.Equal, "bar"}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(pred.RightColumn), GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan = plans.NewSeqScanPlanNode(outSchema, &expression_, tableMetadata.OID())
	results = executionEngine.Execute(seqPlan, executorContext)

	testingpkg.Assert(t, len(results) == 0, "")

	txn_mgr.Abort(txn)

	fmt.Println("select and check value after Abort...")

	txn = txn_mgr.Begin(nil)
	executorContext.SetTransaction(txn)

	// check updated row
	outColumnB = column.NewColumn("b", types.Varchar, false, nil)
	outSchema = schema.NewSchema([]*column.Column{outColumnB})

	pred = Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(pred.RightColumn), GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan = plans.NewSeqScanPlanNode(outSchema, &expression_, tableMetadata.OID())
	results = executionEngine.Execute(seqPlan, executorContext)

	testingpkg.Assert(t, types.NewVarchar("foo").CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 'foo'")

	// check deleted row
	outColumnB = column.NewColumn("b", types.Integer, false, nil)
	outSchema = schema.NewSchema([]*column.Column{outColumnB})

	pred = Predicate{"b", expression.Equal, "bar"}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(pred.RightColumn), GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	seqPlan = plans.NewSeqScanPlanNode(outSchema, &expression_, tableMetadata.OID())
	results = executionEngine.Execute(seqPlan, executorContext)

	testingpkg.Assert(t, len(results) == 1, "")
}

func TestSimpleHashJoin(t *testing.T) {
	os.Stdout.Sync()
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)
	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)
	executorContext := NewExecutorContext(c, bpm, txn)

	columnA := column.NewColumn("colA", types.Integer, false, nil)
	columnB := column.NewColumn("colB", types.Integer, false, nil)
	columnC := column.NewColumn("colC", types.Integer, false, nil)
	columnD := column.NewColumn("colD", types.Integer, false, nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB, columnC, columnD})
	tableMetadata1 := c.CreateTable("test_1", schema_, txn)

	column1 := column.NewColumn("col1", types.Integer, false, nil)
	column2 := column.NewColumn("col2", types.Integer, false, nil)
	column3 := column.NewColumn("col3", types.Integer, false, nil)
	column4 := column.NewColumn("col3", types.Integer, false, nil)
	schema_ = schema.NewSchema([]*column.Column{column1, column2, column3, column4})
	tableMetadata2 := c.CreateTable("test_2", schema_, txn)

	tableMeta1 := &TableInsertMeta{"test_1",
		100,
		[]*ColumnInsertMeta{
			{"colA", types.Integer, false, DistSerial, 0, 0, 0},
			{"colB", types.Integer, false, DistUniform, 0, 9, 0},
			{"colC", types.Integer, false, DistUniform, 0, 9999, 0},
			{"colD", types.Integer, false, DistUniform, 0, 99999, 0},
		}}
	tableMeta2 := &TableInsertMeta{"test_2",
		1000,
		[]*ColumnInsertMeta{
			{"col1", types.Integer, false, DistSerial, 0, 0, 0},
			{"col2", types.Integer, false, DistUniform, 0, 9, 0},
			{"col3", types.Integer, false, DistUniform, 0, 1024, 0},
			{"col4", types.Integer, false, DistUniform, 0, 2048, 0},
		}}
	FillTable(tableMetadata1, tableMeta1, txn)
	FillTable(tableMetadata2, tableMeta2, txn)

	txn_mgr.Commit(txn)

	var scan_plan1 plans.Plan
	var out_schema1 *schema.Schema
	{
		table_info := executorContext.GetCatalog().GetTableByName("test_1")
		//&schema := table_info.schema_
		colA := column.NewColumn("colA", types.Integer, false, nil)
		colB := column.NewColumn("colB", types.Integer, false, nil)
		out_schema1 = schema.NewSchema([]*column.Column{colA, colB})
		scan_plan1 = plans.NewSeqScanPlanNode(out_schema1, nil, table_info.OID())
	}
	var scan_plan2 plans.Plan
	var out_schema2 *schema.Schema
	{
		table_info := executorContext.GetCatalog().GetTableByName("test_2")
		//schema := table_info.schema_
		col1 := column.NewColumn("col1", types.Integer, false, nil)
		col2 := column.NewColumn("col2", types.Integer, false, nil)
		out_schema2 = schema.NewSchema([]*column.Column{col1, col2})
		scan_plan2 = plans.NewSeqScanPlanNode(out_schema2, nil, table_info.OID())
	}
	var join_plan *plans.HashJoinPlanNode
	var out_final *schema.Schema
	{
		// colA and colB have a tuple index of 0 because they are the left side of the join
		//var allocated_exprs []*expression.ColumnValue
		colA := MakeColumnValueExpression(out_schema1, 0, "colA")
		colA_c := column.NewColumn("colA", types.Integer, false, nil)
		colA_c.SetIsLeft(true)
		colB_c := column.NewColumn("colB", types.Integer, false, nil)
		colB_c.SetIsLeft(true)
		// col1 and col2 have a tuple index of 1 because they are the right side of the join
		col1 := MakeColumnValueExpression(out_schema2, 1, "col1")
		col1_c := column.NewColumn("col1", types.Integer, false, nil)
		col1_c.SetIsLeft(false)
		col2_c := column.NewColumn("col2", types.Integer, false, nil)
		col2_c.SetIsLeft(false)
		var left_keys []expression.Expression
		left_keys = append(left_keys, colA)
		var right_keys []expression.Expression
		right_keys = append(right_keys, col1)
		predicate := MakeComparisonExpression(colA, col1, expression.Equal)
		out_final = schema.NewSchema([]*column.Column{colA_c, colB_c, col1_c, col2_c})
		plans_ := []plans.Plan{scan_plan1, scan_plan2}
		join_plan = plans.NewHashJoinPlanNode(out_final, plans_, predicate,
			left_keys, right_keys)
	}

	executionEngine := &ExecutionEngine{}
	results := executionEngine.Execute(join_plan, executorContext)

	num_tuples := len(results)
	testingpkg.Assert(t, num_tuples == 100, "")
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
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false, nil)
	columnB := column.NewColumn("b", types.Integer, false, nil)
	columnC := column.NewColumn("c", types.Varchar, false, nil)
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

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	bpm.FlushAllPages()

	txn_mgr.Commit(txn)

	cases := []SeqScanTestCase{{
		"select a ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}},
		Predicate{"b", expression.Equal, 55},
		[]Assertion{{"a", 99}},
		1,
	}, {
		"select b ... WHERE a > 20",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"b", types.Integer}},
		Predicate{"a", expression.GreaterThan, 20},
		[]Assertion{{"b", 55}},
		1,
	}, {
		"select b ... WHERE a >= 20",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"a", expression.GreaterThanOrEqual, 20},
		[]Assertion{{"a", 20}, {"b", 22}},
		2,
	}, {
		"select a, b ... WHERE a < 99",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"a", expression.LessThan, 99},
		[]Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b ... WHERE a <= 99",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"a", expression.LessThanOrEqual, 99},
		[]Assertion{{"a", 20}, {"b", 22}},
		2,
	}, {
		"select a, b ... WHERE b != 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}},
		Predicate{"b", expression.NotEqual, 55},
		[]Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b, c ... WHERE a < 100",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		Predicate{"a", expression.LessThan, 100},
		[]Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		2,
	}, {
		"select a, b, c ... WHERE a <= 100",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		Predicate{"a", expression.LessThanOrEqual, 100},
		[]Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		2,
	}, {
		"select a, b, c ... WHERE a >= 10",
		executionEngine,
		executorContext,
		tableMetadata,
		[]Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		Predicate{"b", expression.GreaterThanOrEqual, 10},
		[]Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		2,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			ExecuteSeqScanTestCase(t, test)
		})
	}
}

func rowInsertTransaction(t *testing.T, shi *test_util.SamehadaInstance, c *catalog.Catalog, tm *catalog.TableMetadata, master_ch chan int32) {
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

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, shi.GetBufferPoolManager(), txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	ret := handleFnishTxn(shi.GetTransactionManager(), txn)
	master_ch <- ret
}

func deleteAllRowTransaction(t *testing.T, shi *test_util.SamehadaInstance, c *catalog.Catalog, tm *catalog.TableMetadata, master_ch chan int32) {
	txn := shi.GetTransactionManager().Begin(nil)
	deletePlan := plans.NewDeletePlanNode(nil, tm.OID())

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, shi.GetBufferPoolManager(), txn)
	executionEngine.Execute(deletePlan, executorContext)

	ret := handleFnishTxn(shi.GetTransactionManager(), txn)
	master_ch <- ret
}

func selectAllRowTransaction(t *testing.T, shi *test_util.SamehadaInstance, c *catalog.Catalog, tm *catalog.TableMetadata, master_ch chan int32) {
	txn := shi.GetTransactionManager().Begin(nil)

	outColumnA := column.NewColumn("a", types.Integer, false, nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnA})

	seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tm.OID())
	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	executionEngine.Execute(seqPlan, executorContext)

	ret := handleFnishTxn(shi.GetTransactionManager(), txn)
	master_ch <- ret
}

func handleFnishTxn(txn_mgr *access.TransactionManager, txn *access.Transaction) int32 {
	// fmt.Println(txn.GetState())
	if txn.GetState() == access.ABORTED {
		// fmt.Println(txn.GetSharedLockSet())
		// fmt.Println(txn.GetExclusiveLockSet())
		txn_mgr.Abort(txn)
		return 0
	} else {
		// fmt.Println(txn.GetSharedLockSet())
		// fmt.Println(txn.GetExclusiveLockSet())
		txn_mgr.Commit(txn)
		return 1
	}
}

// REFERENCES
//   - https://pkg.go.dev/runtime#Stack
//   - https://stackoverflow.com/questions/19094099/how-to-dump-goroutine-stacktraces
func RuntimeStack() error {
	// channels
	var (
		// chSingle = make(chan []byte, 1)
		chAll = make(chan []byte, 1)
	)

	// funcs
	var (
		getStack = func(all bool) []byte {
			// From src/runtime/debug/stack.go
			var (
				buf = make([]byte, 1024)
			)

			for {
				n := runtime.Stack(buf, all)
				if n < len(buf) {
					return buf[:n]
				}
				buf = make([]byte, 2*len(buf))
			}
		}
	)

	// current goroutin only
	// go func(ch chan<- []byte) {
	// 	defer close(ch)
	// 	ch <- getStack(false)
	// }(chSingle)

	// all goroutin
	go func(ch chan<- []byte) {
		defer close(ch)
		ch <- getStack(true)
	}(chAll)

	// // result of runtime.Stack(false)
	// for v := range chSingle {
	// 	output.Stdoutl("=== stack-single", string(v))
	// }

	// result of runtime.Stack(true)
	for v := range chAll {
		output.Stdoutl("=== stack-all   ", string(v))
	}

	return nil
}

func timeoutPanic() {
	RuntimeStack()
	os.Stdout.Sync()
	panic("timeout reached")
}

func TestConcurrentTransactionExecution(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.log")

	shi := test_util.NewSamehadaInstance()
	shi.GetLogManager().RunFlushThread()
	testingpkg.Assert(t, common.EnableLogging, "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)

	columnA := column.NewColumn("a", types.Integer, false, nil)
	columnB := column.NewColumn("b", types.Varchar, false, nil)
	columnC := column.NewColumn("c", types.Integer, false, nil)
	columnD := column.NewColumn("d", types.Varchar, false, nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB, columnC, columnD})

	tableMetadata := c.CreateTable("test_1", schema_, txn)

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

	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(c, shi.GetBufferPoolManager(), txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(txn)

	const PARALLEL_EXEC_CNT int = 100

	// // set timeout for debugging
	// time.AfterFunc(time.Duration(40)*time.Second, timeoutPanic)

	commited_cnt := int32(0)
	for i := 0; i < PARALLEL_EXEC_CNT; i++ {
		ch1 := make(chan int32)
		ch2 := make(chan int32)
		ch3 := make(chan int32)
		ch4 := make(chan int32)
		go rowInsertTransaction(t, shi, c, tableMetadata, ch1)
		go selectAllRowTransaction(t, shi, c, tableMetadata, ch2)
		go deleteAllRowTransaction(t, shi, c, tableMetadata, ch3)
		go selectAllRowTransaction(t, shi, c, tableMetadata, ch4)

		commited_cnt += <-ch1
		commited_cnt += <-ch2
		commited_cnt += <-ch3
		commited_cnt += <-ch4
		//fmt.Printf("commited_cnt: %d\n", commited_cnt)
		//shi.GetLockManager().PrintLockTables()
		//shi.GetLockManager().ClearLockTablesForDebug()
	}

	fmt.Printf("final commited_cnt: %d\n", commited_cnt)

	// remove db file and log file
	shi.Finalize(true)
}

func TestTestTableGenerator(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.log")

	shi := test_util.NewSamehadaInstance()
	shi.GetLogManager().RunFlushThread()
	testingpkg.Assert(t, common.EnableLogging, "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
	exec_ctx := NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	table_info, _ := GenerateTestTabls(c, exec_ctx, txn)

	outColumnA := column.NewColumn("colA", types.Integer, false, nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnA})

	seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, table_info.OID())

	executionEngine := &ExecutionEngine{}

	results := executionEngine.Execute(seqPlan, exec_ctx)
	fmt.Printf("len(results) => %d¥n", len(results))
	testingpkg.Assert(t, len(results) == int(TEST1_SIZE), "generated table or testcase is wrong.")

	txn_mgr.Commit(txn)
}

func TestSimpleAggregation(t *testing.T) {
	// SELECT COUNT(colA), SUM(colA), min(colA), max(colA) from test_1;
	os.Remove("test.db")
	os.Remove("test.log")

	shi := test_util.NewSamehadaInstance()
	shi.GetLogManager().RunFlushThread()
	testingpkg.Assert(t, common.EnableLogging, "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
	exec_ctx := NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	table_info, _ := GenerateTestTabls(c, exec_ctx, txn)

	var scan_plan *plans.SeqScanPlanNode
	var scan_schema *schema.Schema
	{
		//auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
		schema_ := table_info.Schema()
		colA := MakeColumnValueExpression(schema_, 0, "colA").(*expression.ColumnValue)
		scan_schema = MakeOutputSchema([]MakeSchemaMeta{{"colA", *colA}})
		scan_plan = plans.NewSeqScanPlanNode(scan_schema, nil, table_info.OID()).(*plans.SeqScanPlanNode)
	}

	var agg_plan *plans.AggregationPlanNode
	var agg_schema *schema.Schema
	{
		colA := MakeColumnValueExpression(scan_schema, 0, "colA")
		countA := *MakeAggregateValueExpression(false, 0).(*expression.AggregateValueExpression)
		sumA := *MakeAggregateValueExpression(false, 1).(*expression.AggregateValueExpression)
		minA := *MakeAggregateValueExpression(false, 2).(*expression.AggregateValueExpression)
		maxA := *MakeAggregateValueExpression(false, 3).(*expression.AggregateValueExpression)

		agg_schema = MakeOutputSchemaAgg([]MakeSchemaMetaAgg{{"countA", countA}, {"sumA", sumA}, {"minA", minA}, {"maxA", maxA}})
		agg_plan = plans.NewAggregationPlanNode(
			agg_schema, scan_plan, nil, []expression.Expression{},
			[]expression.Expression{colA, colA, colA, colA},
			[]plans.AggregationType{plans.COUNT_AGGREGATE, plans.SUM_AGGREGATE,
				plans.MIN_AGGREGATE, plans.MAX_AGGREGATE})
	}

	executionEngine := &ExecutionEngine{}
	executor := executionEngine.CreateExecutor(agg_plan, exec_ctx)
	//executor := ExecutorFactory::CreateExecutor(GetExecutorContext(), agg_plan.get())
	executor.Init()
	tuple_, _, err := executor.Next()
	testingpkg.Assert(t, tuple_ != nil && err == nil, "first call of AggregationExecutor.Next() failed")
	countA_val := tuple_.GetValue(agg_schema, agg_schema.GetColIndex("countA")).ToInteger()
	sumA_val := tuple_.GetValue(agg_schema, agg_schema.GetColIndex("sumA")).ToInteger()
	minA_val := tuple_.GetValue(agg_schema, agg_schema.GetColIndex("minA")).ToInteger()
	maxA_val := tuple_.GetValue(agg_schema, agg_schema.GetColIndex("maxA")).ToInteger()
	// Should count all tuples
	fmt.Println("")
	fmt.Printf("%v %v %v %v\n", countA_val, sumA_val, minA_val, maxA_val)
	testingpkg.Assert(t, countA_val == int32(TEST1_SIZE), "countA_val is not expected value.")
	// Should sum from 0 to TEST1_SIZE
	testingpkg.Assert(t, sumA_val == int32(TEST1_SIZE*(TEST1_SIZE-1)/2), "sumA_val is not expected value.")
	// Minimum should be 0
	testingpkg.Assert(t, minA_val == int32(0), "minA_val is not expected value.")
	// Maximum should be TEST1_SIZE - 1
	testingpkg.Assert(t, maxA_val == int32(TEST1_SIZE-1), "maxA_val is not expected value.")
	tuple_, done, err := executor.Next()
	testingpkg.Assert(t, tuple_ == nil && done == true && err == nil, "second call of AggregationExecutor::Next() failed")

	txn_mgr.Commit(txn)
}

func TestSimpleGroupByAggregation(t *testing.T) {
	// SELECT count(colA), colB, sum(C) FROM test_1 Group By colB HAVING count(colA) > 100
	os.Remove("test.db")
	os.Remove("test.log")

	shi := test_util.NewSamehadaInstance()
	shi.GetLogManager().RunFlushThread()
	testingpkg.Assert(t, common.EnableLogging, "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
	exec_ctx := NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	table_info, _ := GenerateTestTabls(c, exec_ctx, txn)

	var scan_plan *plans.SeqScanPlanNode
	var scan_schema *schema.Schema
	{
		//auto table_info = GetExecutorContext()->GetCatalog()->GetTable("test_1");
		schema_ := table_info.Schema()
		colA := MakeColumnValueExpression(schema_, 0, "colA").(*expression.ColumnValue)
		colB := MakeColumnValueExpression(schema_, 0, "colB").(*expression.ColumnValue)
		colC := MakeColumnValueExpression(schema_, 0, "colC").(*expression.ColumnValue)
		scan_schema = MakeOutputSchema([]MakeSchemaMeta{{"colA", *colA}, {"colB", *colB}, {"colC", *colC}})
		scan_plan = plans.NewSeqScanPlanNode(scan_schema, nil, table_info.OID()).(*plans.SeqScanPlanNode)
	}

	var agg_plan *plans.AggregationPlanNode
	var agg_schema *schema.Schema
	{
		colA := MakeColumnValueExpression(scan_schema, 0, "colA").(*expression.ColumnValue)
		colB := MakeColumnValueExpression(scan_schema, 0, "colB").(*expression.ColumnValue)
		colC := MakeColumnValueExpression(scan_schema, 0, "colC").(*expression.ColumnValue)
		// Make group bye
		groupbyB := *MakeAggregateValueExpression(true, 0).(*expression.AggregateValueExpression)
		// Make aggregates
		countA := *MakeAggregateValueExpression(false, 0).(*expression.AggregateValueExpression)
		sumC := *MakeAggregateValueExpression(false, 1).(*expression.AggregateValueExpression)
		// Make having clause
		// TODO: (SDB) constant value of Having clause is changed from 100 to 0 for debugging (TestSimpleGroupByAggregation)
		pred_const := types.NewInteger(3)
		//pred_const := types.NewInteger(100)
		having := MakeComparisonExpression(&countA, MakeConstantValueExpression(&pred_const), expression.GreaterThan)

		agg_schema = MakeOutputSchemaAgg([]MakeSchemaMetaAgg{{"countA", countA}, {"colB", groupbyB}, {"sumC", sumC}})
		agg_plan = plans.NewAggregationPlanNode(
			agg_schema, scan_plan, having, []expression.Expression{colB},
			[]expression.Expression{colA, colC},
			[]plans.AggregationType{plans.COUNT_AGGREGATE, plans.SUM_AGGREGATE})
	}

	executionEngine := &ExecutionEngine{}
	executor := executionEngine.CreateExecutor(agg_plan, exec_ctx)
	executor.Init()

	var encountered map[int32]int32 = make(map[int32]int32, 0)
	for tuple_, done, _ := executor.Next(); !done; tuple_, done, _ = executor.Next() {
		// Should have countA > 100
		countA := tuple_.GetValue(agg_schema, agg_schema.GetColIndex("countA")).ToInteger()
		colB := tuple_.GetValue(agg_schema, agg_schema.GetColIndex("colB")).ToInteger()
		sumC := tuple_.GetValue(agg_schema, agg_schema.GetColIndex("sumC")).ToInteger()

		fmt.Println("")
		fmt.Printf("%d %d %d\n", countA, colB, sumC)

		//TODO: (SDB) need to check validity of countA at TestSimpleGroupByAggregation
		//            and to know why value must be greater than 100 on TEST1_SIZE is 1000
		testingpkg.Assert(t, countA > 3, "countA result is not greater than 3")

		// should have unique colBs.
		_, ok := encountered[colB]
		testingpkg.Assert(t, !ok, "duplicated colB has been returned")
		encountered[colB] = colB
		// Sanity check: ColB should also be within [0, 10).
		testingpkg.Assert(t, 0 <= colB && colB < 10, "sanity check of colB failed")
	}

	txn_mgr.Commit(txn)
}
