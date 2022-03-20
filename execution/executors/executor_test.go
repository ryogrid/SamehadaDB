// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
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
	// TODO: (SDB) need rewrite with SamehadaInstance class
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))
	log_mgr := recovery.NewLogManager(&diskManager)
	txn_mgr := access.NewTransactionManager(log_mgr)
	//txn := access.NewTransaction(1)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)
	// c := catalog.GetCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)
	// c.CreateTable("columns_catalog", catalog.ColumnsCatalogSchema(), txn)

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
	// TODO: (SDB) need rewrite with SamehadaInstance class
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))
	// TODO: (SDB) [logging/recovery] need increment of transaction ID
	log_mgr := recovery.NewLogManager(&diskManager)
	txn_mgr := access.NewTransactionManager(log_mgr)
	//txn := access.NewTransaction(1)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)
	// c := catalog.GetCatalog(bpm, log_manager, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)
	// c.CreateTable("columns_catalog", catalog.ColumnsCatalogSchema(), txn)

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
	// TODO: (SDB) need rewrite with SamehadaInstance class
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))
	// TODO: (SDB) [logging/recovery] need increment of transaction ID
	log_mgr := recovery.NewLogManager(&diskManager)
	txn_mgr := access.NewTransactionManager(log_mgr)
	//txn := access.NewTransaction(1)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)
	// c := catalog.GetCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)
	// c.CreateTable("columns_catalog", catalog.ColumnsCatalogSchema(), txn)

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

func TestHashTableIndex(t *testing.T) {
	testingpkg.Assert(t, false, "TestHashTableIndex is not implemented yet")

	// TODO: (SDB) need rewrite with SamehadaInstance class
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))
	// TODO: (SDB) [logging/recovery] need increment of transaction ID
	log_mgr := recovery.NewLogManager(&diskManager)
	txn_mgr := access.NewTransactionManager(log_mgr)
	//txn := access.NewTransaction(1)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)
	// c := catalog.GetCatalog(bpm, log_manager, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)
	// c.CreateTable("columns_catalog", catalog.ColumnsCatalogSchema(), txn)

	columnA := column.NewColumn("a", types.Integer, true)
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

	cases := []HashIndexScanTestCase{{
		"select a ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]ColumnIdx{{"a", types.Integer, true}},
		Predicate{"b", expression.Equal, 55},
		[]Assertion{{"a", 99}},
		1,
	}, {
		"select b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]ColumnIdx{{"b", types.Integer, true}},
		Predicate{"b", expression.Equal, 55},
		[]Assertion{{"b", 55}},
		1,
	}, {
		"select a, b ... WHERE a = 20",
		executionEngine,
		executorContext,
		tableMetadata,
		[]ColumnIdx{{"a", types.Integer, true}, {"b", types.Integer, false}},
		Predicate{"a", expression.Equal, 20},
		[]Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b ... WHERE a = 99",
		executionEngine,
		executorContext,
		tableMetadata,
		[]ColumnIdx{{"a", types.Integer, true}, {"b", types.Integer, false}},
		Predicate{"a", expression.Equal, 99},
		[]Assertion{{"a", 99}, {"b", 55}},
		1,
	}, {
		"select a, b ... WHERE a = 100",
		executionEngine,
		executorContext,
		tableMetadata,
		[]ColumnIdx{{"a", types.Integer, true}, {"b", types.Integer, false}},
		Predicate{"a", expression.Equal, 100},
		[]Assertion{},
		0,
	}, {
		"select a, b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]ColumnIdx{{"a", types.Integer, false}, {"b", types.Integer, true}},
		Predicate{"b", expression.NotEqual, 55},
		[]Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b, c ... WHERE c = 'foo'",
		executionEngine,
		executorContext,
		tableMetadata,
		[]ColumnIdx{{"a", types.Integer, false}, {"b", types.Integer, false}, {"c", types.Varchar, true}},
		Predicate{"c", expression.Equal, "foo"},
		[]Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		1,
	}, {
		"select a, b, c ... WHERE c = 'foo'",
		executionEngine,
		executorContext,
		tableMetadata,
		[]ColumnIdx{{"a", types.Integer, false}, {"b", types.Integer, false}, {"c", types.Varchar, true}},
		Predicate{"c", expression.Equal, "foo"},
		[]Assertion{{"a", 99}, {"b", 55}, {"c", "bar"}},
		1,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			ExecuteHashIndexScanTestCase(t, test)
		})
	}

}
