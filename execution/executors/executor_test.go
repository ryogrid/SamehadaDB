// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
	"fmt"
	"os"
	"testing"

	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/disk"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
)

func TestSimpleInsertAndSeqScan(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false)
	columnB := column.NewColumn("b", types.Integer, false)
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

	outColumnA := column.NewColumn("a", types.Integer, false)
	outSchema := schema.NewSchema([]*column.Column{outColumnA})

	seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())

	results := executionEngine.Execute(seqPlan, executorContext)

	txn_mgr.Commit(txn)

	testingpkg.Assert(t, types.NewInteger(20).CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 20")
	testingpkg.Assert(t, types.NewInteger(99).CompareEquals(results[1].GetValue(outSchema, 0)), "value should be 99")
}

func TestSimpleInsertAndSeqScanWithPredicateComparison(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))
	txn_mgr := access.NewTransactionManager(log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false)
	columnB := column.NewColumn("b", types.Integer, false)
	columnC := column.NewColumn("c", types.Varchar, false)
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
	txn_mgr := access.NewTransactionManager(log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false)
	columnB := column.NewColumn("b", types.Integer, false)
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
		a := column.NewColumn("a", types.Integer, false)
		b := column.NewColumn("b", types.Integer, false)
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
		a := column.NewColumn("a", types.Integer, false)
		b := column.NewColumn("b", types.Integer, false)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 2, 0)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 2, len(results))
	}()

	// TEST 1: select a, b ... LIMIT 3
	func() {
		a := column.NewColumn("a", types.Integer, false)
		b := column.NewColumn("b", types.Integer, false)
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
	txn_mgr := access.NewTransactionManager(log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false)
	columnB := column.NewColumn("b", types.Integer, false)
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

	columnA = column.NewColumn("a", types.Integer, false)
	columnB = column.NewColumn("b", types.Integer, false)
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
		a := column.NewColumn("a", types.Integer, false)
		b := column.NewColumn("b", types.Integer, false)
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
		a := column.NewColumn("a", types.Integer, false)
		b := column.NewColumn("b", types.Integer, false)
		outSchema := schema.NewSchema([]*column.Column{a, b})
		seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())
		limitPlan := plans.NewLimitPlanNode(seqPlan, 2, 0)

		results := executionEngine.Execute(limitPlan, executorContext)

		testingpkg.Equals(t, 2, len(results))
	}()

	// TEST 1: select a, b ... LIMIT 1
	func() {
		a := column.NewColumn("a", types.Integer, false)
		b := column.NewColumn("b", types.Integer, false)
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
		a := column.NewColumn("a", types.Integer, false)
		b := column.NewColumn("b", types.Integer, false)
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
	// TODO: (SDB) [logging/recovery] need increment of transaction ID

	txn_mgr := access.NewTransactionManager(log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, true)
	columnB := column.NewColumn("b", types.Integer, true)
	columnC := column.NewColumn("c", types.Varchar, true)
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
	txn_mgr := access.NewTransactionManager(log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false)
	columnB := column.NewColumn("b", types.Integer, false)
	columnC := column.NewColumn("c", types.Varchar, false)
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
	txn_mgr := access.NewTransactionManager(log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false)
	columnB := column.NewColumn("b", types.Integer, false)
	columnC := column.NewColumn("c", types.Varchar, false)
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
	txn_mgr := access.NewTransactionManager(log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false)
	columnB := column.NewColumn("b", types.Varchar, false)
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
	expression_ := expression.NewComparison(*tmpColVal, expression.NewConstantValue(getValue(pred.RightColumn)), pred.Operator)

	updatePlanNode := plans.NewUpdatePlanNode(row1, &expression_, tableMetadata.OID())
	executionEngine.Execute(updatePlanNode, executorContext)

	txn_mgr.Commit(txn)

	fmt.Println("select and check value...")
	txn = txn_mgr.Begin(nil)

	outColumnB := column.NewColumn("b", types.Varchar, false)
	outSchema := schema.NewSchema([]*column.Column{outColumnB})

	pred = Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(*tmpColVal, expression.NewConstantValue(getValue(pred.RightColumn)), pred.Operator)

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
	txn_mgr := access.NewTransactionManager(log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false)
	columnB := column.NewColumn("b", types.Varchar, false)
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
	expression_ := expression.NewComparison(*tmpColVal, expression.NewConstantValue(getValue(pred.RightColumn)), pred.Operator)

	updatePlanNode := plans.NewUpdatePlanNode(row1, &expression_, tableMetadata.OID())
	executionEngine.Execute(updatePlanNode, executorContext)

	txn_mgr.Commit(txn)

	fmt.Println("select and check value...")
	txn = txn_mgr.Begin(nil)

	outColumnB := column.NewColumn("b", types.Varchar, false)
	outSchema := schema.NewSchema([]*column.Column{outColumnB})

	pred = Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(*tmpColVal, expression.NewConstantValue(getValue(pred.RightColumn)), pred.Operator)

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

	outColumnA := column.NewColumn("a", types.Integer, false)
	outSchema = schema.NewSchema([]*column.Column{outColumnA})

	pred = Predicate{"b", expression.Equal, "hage"}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(*tmpColVal, expression.NewConstantValue(getValue(pred.RightColumn)), pred.Operator)

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
	txn_mgr := access.NewTransactionManager(log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, false)
	columnB := column.NewColumn("b", types.Varchar, false)
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

	fmt.Println("update and delete rows...")
	txn = txn_mgr.Begin(nil)
	executorContext.SetTransaction(txn)

	// // update
	// row1 = make([]types.Value, 0)
	// row1 = append(row1, types.NewInteger(99))
	// row1 = append(row1, types.NewVarchar("updated"))

	// pred := Predicate{"b", expression.Equal, "foo"}
	// tmpColVal := new(expression.ColumnValue)
	// tmpColVal.SetTupleIndex(0)
	// tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	// expression_ := expression.NewComparison(*tmpColVal, expression.NewConstantValue(getValue(pred.RightColumn)), pred.Operator)

	// updatePlanNode := plans.NewUpdatePlanNode(row1, &expression_, tableMetadata.OID())
	// updated_tuple := executionEngine.Execute(updatePlanNode, executorContext)
	// fmt.Println(*updated_tuple[0])

	//txn_mgr.Commit(txn)

	// delete
	pred := Predicate{"a", expression.Equal, 20}
	tmpColVal := new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(*tmpColVal, expression.NewConstantValue(getValue(pred.RightColumn)), pred.Operator)

	deletePlanNode := plans.NewDeletePlanNode(&expression_, tableMetadata.OID())
	executionEngine.Execute(deletePlanNode, executorContext)

	fmt.Println("select and check value before Abort...")

	// check updated row
	outColumnB := column.NewColumn("b", types.Varchar, false)
	outSchema := schema.NewSchema([]*column.Column{outColumnB})

	pred = Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(*tmpColVal, expression.NewConstantValue(getValue(pred.RightColumn)), pred.Operator)

	//seqPlan := plans.NewSeqScanPlanNode(outSchema, &expression_, tableMetadata.OID())
	seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tableMetadata.OID())
	results := executionEngine.Execute(seqPlan, executorContext)

	fmt.Println(results)

	testingpkg.Assert(t, types.NewVarchar("updated").CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 'updated'")

	// // check deleted row
	// outColumnB = column.NewColumn("b", types.Integer, false)
	// outSchema = schema.NewSchema([]*column.Column{outColumnB})

	// pred = Predicate{"a", expression.Equal, 20}
	// tmpColVal = new(expression.ColumnValue)
	// tmpColVal.SetTupleIndex(0)
	// tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	// expression_ = expression.NewComparison(*tmpColVal, expression.NewConstantValue(getValue(pred.RightColumn)), pred.Operator)

	// seqPlan = plans.NewSeqScanPlanNode(outSchema, &expression_, tableMetadata.OID())
	// results = executionEngine.Execute(seqPlan, executorContext)

	// testingpkg.Assert(t, len(results) == 0, "")

	txn_mgr.Abort(txn)

	fmt.Println("select and check value after Abort...")

	txn = txn_mgr.Begin(nil)
	executorContext.SetTransaction(txn)

	// check updated row
	outColumnB = column.NewColumn("b", types.Varchar, false)
	outSchema = schema.NewSchema([]*column.Column{outColumnB})

	pred = Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(*tmpColVal, expression.NewConstantValue(getValue(pred.RightColumn)), pred.Operator)

	seqPlan = plans.NewSeqScanPlanNode(outSchema, &expression_, tableMetadata.OID())
	results = executionEngine.Execute(seqPlan, executorContext)

	testingpkg.Assert(t, types.NewVarchar("foo").CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 'foo'")

	// check deleted row
	outColumnB = column.NewColumn("b", types.Integer, false)
	outSchema = schema.NewSchema([]*column.Column{outColumnB})

	pred = Predicate{"a", expression.Equal, 20}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(*tmpColVal, expression.NewConstantValue(getValue(pred.RightColumn)), pred.Operator)

	seqPlan = plans.NewSeqScanPlanNode(outSchema, &expression_, tableMetadata.OID())
	results = executionEngine.Execute(seqPlan, executorContext)

	testingpkg.Assert(t, len(results) == 1, "")
}
