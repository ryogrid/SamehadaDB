package executor_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/executors"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/disk"
	"github.com/ryogrid/SamehadaDB/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"testing"
)

func TestSkipListIndexPointScan(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true

	diskManager := disk.NewDiskManagerTest()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))

	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnC := column.NewColumn("c", types.Varchar, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
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
	row4 = append(row4, types.NewInteger(1226))
	row4 = append(row4, types.NewInteger(713))
	row4 = append(row4, types.NewVarchar("bazz"))

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

	txn_mgr.Commit(txn)

	cases := []executors.IndexPointScanTestCase{{
		"select a ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"b", expression.Equal, 55},
		[]executors.Assertion{{"a", 99}},
		1,
	}, {
		"select b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"b", types.Integer}},
		executors.Predicate{"b", expression.Equal, 55},
		[]executors.Assertion{{"b", 55}},
		1,
	}, {
		"select a, b ... WHERE a = 20",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}, {"b", types.Integer}},
		executors.Predicate{"a", expression.Equal, 20},
		[]executors.Assertion{{"a", 20}, {"b", 22}},
		1,
	}, {
		"select a, b ... WHERE a = 99",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}, {"b", types.Integer}},
		executors.Predicate{"a", expression.Equal, 99},
		[]executors.Assertion{{"a", 99}, {"b", 55}},
		1,
	}, {
		"select a, b ... WHERE a = 100",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}, {"b", types.Integer}},
		executors.Predicate{"a", expression.Equal, 100},
		[]executors.Assertion{},
		0,
	}, {
		"select a, b ... WHERE b = 55",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}, {"b", types.Integer}},
		executors.Predicate{"b", expression.Equal, 55},
		[]executors.Assertion{{"a", 99}, {"b", 55}},
		1,
	}, {
		"select a, b, c ... WHERE c = 'foo'",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}, {"b", types.Integer}, {"c", types.Varchar}},
		executors.Predicate{"c", expression.Equal, "foo"},
		[]executors.Assertion{{"a", 20}, {"b", 22}, {"c", "foo"}},
		1,
	}, {
		"select a, b ... WHERE c = 'baz'",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}, {"b", types.Integer}},
		executors.Predicate{"c", expression.Equal, "baz"},
		[]executors.Assertion{{"a", 1225}, {"b", 712}},
		1,
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			executors.ExecuteIndexPointScanTestCase(t, test, index_constants.INDEX_KIND_SKIP_LIST)
		})
	}

	common.TempSuppressOnMemStorage = false
	diskManager.ShutDown()
	common.TempSuppressOnMemStorageMutex.Unlock()
}

func TestSkipListSerialIndexRangeScan(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true

	diskManager := disk.NewDiskManagerTest()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))

	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnC := column.NewColumn("c", types.Varchar, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
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
	row4 = append(row4, types.NewInteger(1226))
	row4 = append(row4, types.NewInteger(713))
	row4 = append(row4, types.NewVarchar("bazz"))

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

	txn_mgr.Commit(txn)

	cases := []executors.IndexRangeScanTestCase{{
		"select a ... WHERE a >= 20 and a <= 1225",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
		int32(tableMetadata.Schema().GetColIndex("a")),
		[]*types.Value{samehada_util.GetPonterOfValue(types.NewInteger(20)), samehada_util.GetPonterOfValue(types.NewInteger(1225))},
		3,
	}, {
		"select a ... WHERE a >= 20 and a <= 2147483647",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
		int32(tableMetadata.Schema().GetColIndex("a")),
		[]*types.Value{samehada_util.GetPonterOfValue(types.NewInteger(20)), samehada_util.GetPonterOfValue(types.NewInteger(math.MaxInt32))},
		4,
	}, {
		"select a ... WHERE a >= -2147483646 and a <= 1225",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
		int32(tableMetadata.Schema().GetColIndex("a")),
		[]*types.Value{samehada_util.GetPonterOfValue(types.NewInteger(math.MinInt32 + 1)), samehada_util.GetPonterOfValue(types.NewInteger(1225))},
		3,
	}, {
		"select a ... WHERE a >= -2147483646",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
		int32(tableMetadata.Schema().GetColIndex("a")),
		[]*types.Value{samehada_util.GetPonterOfValue(types.NewInteger(math.MinInt32 + 1)), nil},
		4,
	}, {
		"select a ... WHERE a <= 1225",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
		int32(tableMetadata.Schema().GetColIndex("a")),
		[]*types.Value{nil, samehada_util.GetPonterOfValue(types.NewInteger(1225))},
		3,
	}, {
		"select a ... ",
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
		int32(tableMetadata.Schema().GetColIndex("a")),
		[]*types.Value{nil, nil},
		4,
	}}
	/*, {
		"select a ... WHERE a >= -2147483647 and a <= 1225", // fail because restriction of current SkipList Index impl?
		executionEngine,
		executorContext,
		tableMetadata,
		[]executors.Column{{"a", types.Integer}},
		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
		int32(tableMetadata.Schema().GetColIndex("a")),
		[]types.Value{types.NewInteger(math.MinInt32), types.NewInteger(1225)},
		3,
	}}
	*/

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			executors.ExecuteIndexRangeScanTestCase(t, test, index_constants.INDEX_KIND_SKIP_LIST)
		})
	}

	common.TempSuppressOnMemStorage = false
	diskManager.ShutDown()
	common.TempSuppressOnMemStorageMutex.Unlock()
}

func TestAbortWthDeleteUpdateUsingIndexCasePointScan(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Varchar, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
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

	txn_mgr.Commit(txn)

	fmt.Println("update and delete rows...")
	txn = txn_mgr.Begin(nil)
	executorContext.SetTransaction(txn)

	// update
	row1 = make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(99))
	row1 = append(row1, types.NewVarchar("updated"))

	pred := executors.Predicate{"b", expression.Equal, "foo"}
	tmpColVal := new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(executors.GetValue(pred.RightColumn), executors.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	//seqScanPlan := plans.NewSeqScanPlanNode(tableMetadata.Schema(), expression_, tableMetadata.OID())
	skipListPointScanP := plans.NewPointScanWithIndexPlanNode(tableMetadata.Schema(), expression_.(*expression.Comparison), tableMetadata.OID())
	updatePlanNode := plans.NewUpdatePlanNode(row1, []int{0, 1}, skipListPointScanP)
	executionEngine.Execute(updatePlanNode, executorContext)

	// delete
	pred = executors.Predicate{"b", expression.Equal, "bar"}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(executors.GetValue(pred.RightColumn), executors.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	//childSeqScanPlan := plans.NewSeqScanPlanNode(tableMetadata.Schema(), expression_, tableMetadata.OID())
	skipListPointScanP = plans.NewPointScanWithIndexPlanNode(tableMetadata.Schema(), expression_.(*expression.Comparison), tableMetadata.OID())
	deletePlanNode := plans.NewDeletePlanNode(skipListPointScanP)
	executionEngine.Execute(deletePlanNode, executorContext)

	log_mgr.DeactivateLogging()

	fmt.Println("select and check value before Abort...")

	// check updated row
	outColumnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnB})

	pred = executors.Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(executors.GetValue(pred.RightColumn), executors.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	//seqPlan := plans.NewSeqScanPlanNode(outSchema, expression_, tableMetadata.OID())
	skipListPointScanP = plans.NewPointScanWithIndexPlanNode(outSchema, expression_.(*expression.Comparison), tableMetadata.OID())
	results := executionEngine.Execute(skipListPointScanP, executorContext)

	testingpkg.Assert(t, types.NewVarchar("updated").CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 'updated'")

	// check deleted row
	outColumnB = column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil)
	outSchema = schema.NewSchema([]*column.Column{outColumnB})

	pred = executors.Predicate{"b", expression.Equal, "bar"}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(executors.GetValue(pred.RightColumn), executors.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	//seqPlan = plans.NewSeqScanPlanNode(outSchema, expression_, tableMetadata.OID())
	skipListPointScanP = plans.NewPointScanWithIndexPlanNode(outSchema, expression_.(*expression.Comparison), tableMetadata.OID())
	results = executionEngine.Execute(skipListPointScanP, executorContext)

	testingpkg.Assert(t, len(results) == 0, "")

	txn_mgr.Abort(c, txn)

	fmt.Println("select and check value after Abort...")

	txn = txn_mgr.Begin(nil)
	executorContext.SetTransaction(txn)

	// check updated row
	outColumnB = column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil)
	outSchema = schema.NewSchema([]*column.Column{outColumnB})

	pred = executors.Predicate{"a", expression.Equal, 99}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(executors.GetValue(pred.RightColumn), executors.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	//seqPlan = plans.NewSeqScanPlanNode(outSchema, expression_, tableMetadata.OID())
	skipListPointScanP = plans.NewPointScanWithIndexPlanNode(outSchema, expression_.(*expression.Comparison), tableMetadata.OID())
	results = executionEngine.Execute(skipListPointScanP, executorContext)

	testingpkg.Assert(t, types.NewVarchar("foo").CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 'foo'")

	// check deleted row
	outColumnB = column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil)
	outSchema = schema.NewSchema([]*column.Column{outColumnB})

	pred = executors.Predicate{"b", expression.Equal, "bar"}
	tmpColVal = new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ = expression.NewComparison(tmpColVal, expression.NewConstantValue(executors.GetValue(pred.RightColumn), executors.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	//seqPlan = plans.NewSeqScanPlanNode(outSchema, expression_, tableMetadata.OID())
	skipListPointScanP = plans.NewPointScanWithIndexPlanNode(outSchema, expression_.(*expression.Comparison), tableMetadata.OID())
	results = executionEngine.Execute(skipListPointScanP, executorContext)

	testingpkg.Assert(t, len(results) == 1, "")
}

func TestAbortWthDeleteUpdateUsingIndexCaseRangeScan(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	bpm := buffer.NewBufferPoolManager(uint32(32), diskManager, log_mgr)
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)

	columnA := column.NewColumn("a", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnB := column.NewColumn("b", types.Varchar, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
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

	// ----------- Insert -----------------
	insertPlanNode := plans.NewInsertPlanNode(rows, tableMetadata.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(txn)

	fmt.Println("update and delete rows...")
	txn = txn_mgr.Begin(nil)
	executorContext.SetTransaction(txn)

	// ------------ Update ------------------
	row1 = make([]types.Value, 0)
	row1 = append(row1, types.NewInteger(-1)) //dummy
	row1 = append(row1, types.NewVarchar("updated"))

	skipListRangeScanP := plans.NewRangeScanWithIndexPlanNode(tableMetadata.Schema(), tableMetadata.OID(), int32(tableMetadata.Schema().GetColIndex("b")), nil, samehada_util.GetPonterOfValue(types.NewVarchar("foo")), samehada_util.GetPonterOfValue(types.NewVarchar("foo")))
	updatePlanNode := plans.NewUpdatePlanNode(row1, []int{1}, skipListRangeScanP)
	results := executionEngine.Execute(updatePlanNode, executorContext)

	testingpkg.Assert(t, len(results) == 1, "update row count should be 1.")

	// ------- Delete ---------
	pred := executors.Predicate{"b", expression.Equal, "bar"}
	tmpColVal := new(expression.ColumnValue)
	tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tableMetadata.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(executors.GetValue(pred.RightColumn), executors.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	skipListRangeScanP = plans.NewRangeScanWithIndexPlanNode(tableMetadata.Schema(), tableMetadata.OID(), int32(tableMetadata.Schema().GetColIndex("b")), expression_.(*expression.Comparison), samehada_util.GetPonterOfValue(types.NewVarchar("bar")), samehada_util.GetPonterOfValue(types.NewVarchar("bar")))

	deletePlanNode := plans.NewDeletePlanNode(skipListRangeScanP)
	results = executionEngine.Execute(deletePlanNode, executorContext)

	testingpkg.Assert(t, len(results) == 1, "deleted row count should be 1.")

	log_mgr.DeactivateLogging()

	fmt.Println("select and check value before Abort...")

	// --------- Check Updated row before rollback ----------
	outColumnB := column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnB})

	skipListRangeScanP = plans.NewRangeScanWithIndexPlanNode(outSchema, tableMetadata.OID(), int32(tableMetadata.Schema().GetColIndex("a")), nil, samehada_util.GetPonterOfValue(types.NewInteger(99)), samehada_util.GetPonterOfValue(types.NewInteger(99)))
	results = executionEngine.Execute(skipListRangeScanP, executorContext)

	testingpkg.Assert(t, len(results) == 1, "got row count should be 1.")
	testingpkg.Assert(t, types.NewVarchar("updated").CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 'updated'")

	// --------- Check Deleted row before rollback -----------
	outColumnB = column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil)
	outSchema = schema.NewSchema([]*column.Column{outColumnB})

	skipListRangeScanP = plans.NewRangeScanWithIndexPlanNode(outSchema, tableMetadata.OID(), int32(tableMetadata.Schema().GetColIndex("a")), nil, samehada_util.GetPonterOfValue(types.NewInteger(200)), nil)
	results = executionEngine.Execute(skipListRangeScanP, executorContext)

	testingpkg.Assert(t, len(results) == 0, "")

	// ---------- Abort ---------------
	txn_mgr.Abort(c, txn)

	fmt.Println("select and check value after Abort...")

	txn = txn_mgr.Begin(nil)
	executorContext.SetTransaction(txn)

	// -------- Check Rollback of Updated row -------------
	outColumnB = column.NewColumn("b", types.Varchar, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil)
	outSchema = schema.NewSchema([]*column.Column{outColumnB})

	skipListRangeScanP = plans.NewRangeScanWithIndexPlanNode(outSchema, tableMetadata.OID(), int32(tableMetadata.Schema().GetColIndex("b")), nil, samehada_util.GetPonterOfValue(types.NewVarchar("foo")), samehada_util.GetPonterOfValue(types.NewVarchar("foo")))
	results = executionEngine.Execute(skipListRangeScanP, executorContext)

	testingpkg.Assert(t, types.NewVarchar("foo").CompareEquals(results[0].GetValue(outSchema, 0)), "value should be 'foo'")

	// -------- Check Rollback of Deleted row --------------
	//outColumnB = column.NewColumn("b", types.Integer, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil)
	//outSchema = schema.NewSchema([]*column.Column{outColumnB})

	skipListRangeScanP = plans.NewRangeScanWithIndexPlanNode(tableMetadata.Schema(), tableMetadata.OID(), int32(tableMetadata.Schema().GetColIndex("b")), expression_.(*expression.Comparison), samehada_util.GetPonterOfValue(types.NewVarchar("bar")), samehada_util.GetPonterOfValue(types.NewVarchar("bar")))
	results = executionEngine.Execute(skipListRangeScanP, executorContext)

	testingpkg.Assert(t, len(results) == 1, "")
	testingpkg.Assert(t, types.NewInteger(777).CompareEquals(results[0].GetValue(tableMetadata.Schema(), 0)), "value should be 777")
	testingpkg.Assert(t, types.NewVarchar("bar").CompareEquals(results[0].GetValue(tableMetadata.Schema(), 1)), "value should be \"bar\"")
}

func getUniqRandomPrimitivVal[T int32 | float32 | string](keyType types.TypeID, checkDupMap map[T]T, checkKeyColumnDupMapMutex *sync.RWMutex) T {
	checkKeyColumnDupMapMutex.RLock()
	retVal := samehada_util.GetRandomPrimitiveVal[T](keyType)
	for _, exist := checkDupMap[retVal]; exist; _, exist = checkDupMap[retVal] {
		retVal = samehada_util.GetRandomPrimitiveVal[T](keyType)
	}
	checkKeyColumnDupMapMutex.RUnlock()
	return retVal
}

// TODO: (SDB) not implemente yet
func createBankAccountUpdatePlanNode[T int32 | float32 | string](c *catalog.Catalog, tm *catalog.TableMetadata, keyType types.TypeID, accounts []string) (createdPlan plans.Plan, moveAmount T) {
	val := samehada_util.GetRandomPrimitiveVal[T](keyType)
	return nil, val
}

// TODO: (SDB) not implemente yet
func createSpecifiedValInsertPlanNode[T int32 | float32 | string](keyColumnVal T, balance int32, c *catalog.Catalog, tm *catalog.TableMetadata, keyType types.TypeID) (createdPlan plans.Plan) {
	//val := samehada_util.GetRandomPrimitiveVal[T](keyType)
	return nil
}

// TODO: (SDB) not implemente yet
func createSpecifiedValDeletePlanNode[T int32 | float32 | string](keyColumnVal T, c *catalog.Catalog, tm *catalog.TableMetadata, keyType types.TypeID) (createdPlan plans.Plan) {
	//val := samehada_util.GetRandomPrimitiveVal[T](keyType)
	return nil
}

// TODO: (SDB) not implemente yet
func createSpecifiedValUpdatePlanNode[T int32 | float32 | string](keyColumnVal T, newBalanceVal int32, c *catalog.Catalog, tm *catalog.TableMetadata, keyType types.TypeID) (createdPlan plans.Plan, UpdateVal T) {
	val := samehada_util.GetRandomPrimitiveVal[T](keyType)
	return nil, val
}

// TODO: (SDB) not implemente yet
func createRandomRangeScanPlanNode[T int32 | float32 | string](c *catalog.Catalog, tm *catalog.TableMetadata, keyType types.TypeID) (createdPlan plans.Plan, startRange *types.Value, endRange *types.Value) {
	samehada_util.GetRandomPrimitiveVal[T](keyType)
	return nil, nil, nil
}

// TODO: (SDB) not implemente yet
func createRandomPointScanPlanNode[T int32 | float32 | string](c *catalog.Catalog, tm *catalog.TableMetadata, keyType types.TypeID, insVals *[]T, insValsMutex *sync.Mutex) (createdPlan plans.Plan, startRange *types.Value, endRange *types.Value) {
	samehada_util.GetRandomPrimitiveVal[T](keyType)
	return nil, nil, nil
}

func executePlan(c *catalog.Catalog, bpm *buffer.BufferPoolManager, txn *access.Transaction, plan plans.Plan) (results []*tuple.Tuple) {
	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, bpm, txn)
	return executionEngine.Execute(plan, executorContext)
}

func handleFnishedTxn(catalog_ *catalog.Catalog, txn_mgr *access.TransactionManager, txn *access.Transaction) bool {
	// fmt.Println(txn.GetState())
	if txn.GetState() == access.ABORTED {
		// fmt.Println(txn.GetSharedLockSet())
		// fmt.Println(txn.GetExclusiveLockSet())
		txn_mgr.Abort(catalog_, txn)
		return false
	} else {
		// fmt.Println(txn.GetSharedLockSet())
		// fmt.Println(txn.GetExclusiveLockSet())
		txn_mgr.Commit(txn)
		return true
	}
}

/*
func rowInsertTransaction_(t *testing.T, shi *samehada.SamehadaInstance, c *catalog.Catalog, tm *catalog.TableMetadata, master_ch chan int32) {
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

	ret := handleFnishTxn(shi.GetTransactionManager(), txn)
	master_ch <- ret
}

func deleteAllRowTransaction_(t *testing.T, shi *samehada.SamehadaInstance, c *catalog.Catalog, tm *catalog.TableMetadata, master_ch chan int32) {
	txn := shi.GetTransactionManager().Begin(nil)
	deletePlan := plans.NewDeletePlanNode(nil, tm.OID())

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)
	executionEngine.Execute(deletePlan, executorContext)

	ret := handleFnishTxn(shi.GetTransactionManager(), txn)
	master_ch <- ret
}

func selectAllRowTransaction_(t *testing.T, shi *samehada.SamehadaInstance, c *catalog.Catalog, tm *catalog.TableMetadata, master_ch chan int32) {
	txn := shi.GetTransactionManager().Begin(nil)

	outColumnA := column.NewColumn("a", types.Integer, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil)
	outSchema := schema.NewSchema([]*column.Column{outColumnA})

	seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, tm.OID())
	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	executionEngine.Execute(seqPlan, executorContext)

	ret := handleFnishTxn(shi.GetTransactionManager(), txn)
	master_ch <- ret
}

func TestConcurrentSkipListIndexUseTransactionExecution(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}

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

	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)
	executionEngine.Execute(insertPlanNode, executorContext)

	txn_mgr.Commit(txn)

	const PARALLEL_EXEC_CNT int = 100

	// // set timeout for debugging
	// time.AfterFunc(time.Duration(40)*time.Second, TimeoutPanic)

	commited_cnt := int32(0)
	for i := 0; i < PARALLEL_EXEC_CNT; i++ {
		ch1 := make(chan int32)
		ch2 := make(chan int32)
		ch3 := make(chan int32)
		ch4 := make(chan int32)
		go rowInsertTransaction_(t, shi, c, tableMetadata, ch1)
		go selectAllRowTransaction_(t, shi, c, tableMetadata, ch2)
		go deleteAllRowTransaction_(t, shi, c, tableMetadata, ch3)
		go selectAllRowTransaction_(t, shi, c, tableMetadata, ch4)

		commited_cnt += <-ch1
		commited_cnt += <-ch2
		commited_cnt += <-ch3
		commited_cnt += <-ch4
		//fmt.Printf("commited_cnt: %d\n", commited_cnt)
		//shi.GetLockManager().PrintLockTables()
		//shi.GetLockManager().ClearLockTablesForDebug()
	}

}
*/

// TODO: (SDB) not implemente yet
func testParallelTxnsQueryingSkipListIndexUsedColumns[T int32 | float32 | string](t *testing.T, keyType types.TypeID, stride int32, opTimes int32, seedVal int32, initialEntryNum int32, bpoolSize int32) {
	common.ShPrintf(common.DEBUG_INFO, "start of testParallelTxnsQueryingSkipListIndexUsedColumns stride=%d opTimes=%d seedVal=%d initialEntryNum=%d bpoolSize=%d ====================================================\n",
		stride, opTimes, seedVal, initialEntryNum, bpoolSize)

	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), int(bpoolSize))
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	txnMgr := shi.GetTransactionManager()
	txn := txnMgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)

	columnA := column.NewColumn("account_id", keyType, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	columnB := column.NewColumn("balance", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)
	txnMgr.Commit(txn)

	const THREAD_NUM = 20

	rand.Seed(int64(seedVal))

	insVals := make([]T, 0)
	insValsMutex := new(sync.RWMutex)
	deletedValsForSelectUpdate := make(map[T]T, 0)
	deletedValsForSelectUpdateMutex := new(sync.RWMutex)
	deletedValsForDelete := make(map[T]T, 0)
	deletedValsForDeleteMutex := new(sync.RWMutex)
	checkKeyColDupMap := make(map[T]T)
	checkKeyColDupMapMutex := new(sync.RWMutex)
	checkBalanceColDupMap := make(map[int32]int32)
	checkBalanceColDupMapMutex := new(sync.RWMutex)

	insertedTupleCnt := int32(0)
	deletedTupleCnt := int32(0)
	executedTxnCnt := int32(0)
	abortedTxnCnt := int32(0)
	commitedTxnCnt := int32(0)

	txn = txnMgr.Begin(nil)

	// insert account records
	const ACCOUNT_NUM = 4
	const BALANCE_AT_START = 1000000
	sumOfAllAccountBalanceAtStart := int32(0)
	accountIds := make([]T, 0)
	for ii := 0; ii < ACCOUNT_NUM; ii++ {
		accountId := samehada_util.GetRandomPrimitiveVal[T](keyType)
		for _, exist := checkKeyColDupMap[accountId]; exist; _, exist = checkKeyColDupMap[accountId] {
			accountId = samehada_util.GetRandomPrimitiveVal[T](keyType)
		}
		checkKeyColDupMap[accountId] = accountId
		accountIds = append(accountIds, accountId)
		// not have to duplication check of barance
		insPlan := createSpecifiedValInsertPlanNode(accountId, int32(BALANCE_AT_START+ii), c, tableMetadata, keyType)
		executePlan(c, shi.GetBufferPoolManager(), txn, insPlan)
		sumOfAllAccountBalanceAtStart += int32(BALANCE_AT_START + ii)
	}

	insertedTupleCnt += ACCOUNT_NUM

	// setup other initial entries which is not used as account
	useInitialEntryNum := int(initialEntryNum)
	for ii := 0; ii < useInitialEntryNum; ii++ {
		// avoid duplication
		keyValBase := samehada_util.GetRandomPrimitiveVal[T](keyType)
		for _, exist := checkKeyColDupMap[keyValBase]; exist; _, exist = checkKeyColDupMap[keyValBase] {
			keyValBase = samehada_util.GetRandomPrimitiveVal[T](keyType)
		}
		checkKeyColDupMap[keyValBase] = keyValBase

		for ii := int32(0); ii < stride; ii++ {
			insKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(keyValBase, stride), ii).(T)
			insBranceVal := samehada_util.GetInt32ValCorrespondToPassVal(insKeyVal)

			insPlan := createSpecifiedValInsertPlanNode(insKeyVal, insBranceVal, c, tableMetadata, keyType)
			executePlan(c, shi.GetBufferPoolManager(), txn, insPlan)
		}

		insVals = append(insVals, keyValBase)
	}

	insertedTupleCnt += initialEntryNum

	txnMgr.Commit(txn)

	ch := make(chan int32)

	finalizeRandomInsertTxn := func(txn_ *access.Transaction, insKeyValBase T) {
		txnOk := handleFnishedTxn(c, txnMgr, txn_)

		if txnOk {
			insValsMutex.Lock()
			insVals = append(insVals, insKeyValBase)
			insValsMutex.Unlock()
			atomic.AddInt32(&insertedTupleCnt, stride)
			atomic.AddInt32(&commitedTxnCnt, 1)
		} else {
			atomic.AddInt32(&abortedTxnCnt, 1)
		}
		atomic.AddInt32(&executedTxnCnt, 1)
	}

	finalizeRandomDeleteTxn := func(txn_ *access.Transaction, delKeyValBase T) {
		txnOk := handleFnishedTxn(c, txnMgr, txn_)
		if txnOk {
			deletedValsForDeleteMutex.Lock()
			deletedValsForDelete[delKeyValBase] = delKeyValBase
			deletedValsForDeleteMutex.Unlock()
			atomic.AddInt32(&deletedTupleCnt, stride)
			atomic.AddInt32(&commitedTxnCnt, 1)
		} else {
			// rollback removed element
			insValsMutex.Lock()
			insVals = append(insVals, delKeyValBase)
			insValsMutex.Unlock()
			atomic.AddInt32(&abortedTxnCnt, 1)
		}
		atomic.AddInt32(&executedTxnCnt, 1)
	}

	finalizeRandomNoSideEffectTxn := func(txn_ *access.Transaction) {
		txnOk := handleFnishedTxn(c, txnMgr, txn_)

		if txnOk {
			atomic.AddInt32(&commitedTxnCnt, 1)
		} else {
			atomic.AddInt32(&abortedTxnCnt, 1)
		}
		atomic.AddInt32(&executedTxnCnt, 1)
	}

	useOpTimes := int(opTimes)
	runningThCnt := 0
	for ii := 0; ii <= useOpTimes; ii++ {
		// wait last go routines finishes
		if ii == useOpTimes {
			for runningThCnt > 0 {
				<-ch
				runningThCnt--
				common.ShPrintf(common.DEBUGGING, "runningThCnt=%d\n", runningThCnt)
			}
			break
		}

		// wait for keeping THREAD_NUM groroutine existing
		for runningThCnt >= THREAD_NUM {
			//for runningThCnt > 0 { // serial execution
			<-ch
			runningThCnt--

			common.ShPrintf(common.DEBUGGING, "runningThCnt=%d\n", runningThCnt)
		}
		common.ShPrintf(common.DEBUGGING, "ii=%d\n", ii)
		//runningThCnt = 0

		// get 0-7
		opType := rand.Intn(8)
		switch opType {
		case 0: //Update account volume (move money)
		case 1: // Insert
			go func() {
				//checkKeyColDupMapMutex.RLock()
				insKeyValBase := getUniqRandomPrimitivVal(keyType, checkKeyColDupMap, checkKeyColDupMapMutex)
				checkKeyColDupMapMutex.Lock()
				checkKeyColDupMap[insKeyValBase] = insKeyValBase
				checkKeyColDupMapMutex.Unlock()

				txn_ := txnMgr.Begin(nil)
				for ii := int32(0); ii < stride; ii++ {
					insKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(insKeyValBase, stride), ii).(T)
					insBalanceVal := samehada_util.GetInt32ValCorrespondToPassVal(insKeyVal)

					common.ShPrintf(common.DEBUGGING, "Insert op start.")
					insPlan := createSpecifiedValInsertPlanNode(insKeyVal, insBalanceVal, c, tableMetadata, keyType)
					executePlan(c, shi.GetBufferPoolManager(), txn_, insPlan)

					if txn_.GetState() == access.ABORTED {
						break
					}
					//fmt.Printf("sl.Insert at insertRandom: ii=%d, insKeyValBase=%d len(*insVals)=%d\n", ii, insKeyValBase, len(insVals))
				}

				finalizeRandomInsertTxn(txn_, insKeyValBase)

				ch <- 1
			}()
		case 2, 3: // Delete
			// get 0-1 value
			tmpRand := rand.Intn(2)
			if tmpRand == 0 {
				// 50% is Remove to not existing entry
				go func() {
					deletedValsForDeleteMutex.RLock()
					if len(deletedValsForDelete) == 0 {
						deletedValsForDeleteMutex.RUnlock()
						ch <- 1
						return
					}
					deletedValsForDeleteMutex.RUnlock()

					txn_ := txnMgr.Begin(nil)
					deletedValsForDeleteMutex.RLock()
					delKeyValBase := samehada_util.ChoiceValFromMap(deletedValsForDelete)
					deletedValsForDeleteMutex.RUnlock()
					for ii := int32(0); ii < stride; ii++ {
						delKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(delKeyValBase, stride), ii).(T)

						common.ShPrintf(common.DEBUGGING, "Delete(fail) op start.")
						delPlan := createSpecifiedValDeletePlanNode(delKeyVal, c, tableMetadata, keyType)
						results := executePlan(c, shi.GetBufferPoolManager(), txn_, delPlan)

						if txn_.GetState() == access.ABORTED {
							break
						}

						common.SH_Assert(results != nil && len(results) == 0, "delete(fail) should not be fail!")
					}

					finalizeRandomNoSideEffectTxn(txn_)

					ch <- 1
				}()
			} else {
				// 50% is Remove to existing entry
				go func() {
					insValsMutex.Lock()
					if len(insVals)-1 < 0 {
						insValsMutex.Unlock()
						ch <- 1
						return
					}
					tmpIdx := int(rand.Intn(len(insVals)))
					delKeyValBase := insVals[tmpIdx]
					if len(insVals) == 1 {
						// make empty
						insVals = make([]T, 0)
					} else if len(insVals)-1 == tmpIdx {
						insVals = insVals[:len(insVals)-1]
					} else {
						insVals = append(insVals[:tmpIdx], insVals[tmpIdx+1:]...)
					}
					insValsMutex.Unlock()

					txn_ := txnMgr.Begin(nil)

					for ii := int32(0); ii < stride; ii++ {
						delKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(delKeyValBase, stride), ii).(T)
						//pairVal := samehada_util.GetValueForSkipListEntry(delVal)

						common.ShPrintf(common.DEBUGGING, "Delete(success) op start.")

						//// append to map before doing remove op for other point scan op thread
						//deletedValsForSelectUpdateMutex.Lock()
						//deletedValsForSelectUpdate[delKeyVal] = delKeyVal
						//deletedValsForSelectUpdateMutex.Unlock()

						//isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(delVal)), pairVal)
						delPlan := createSpecifiedValDeletePlanNode(delKeyVal, c, tableMetadata, keyType)
						results := executePlan(c, shi.GetBufferPoolManager(), txn_, delPlan)

						if txn_.GetState() == access.ABORTED {
							break
						}

						common.SH_Assert(results != nil && len(results) == 1, "Delete(success) should be fail!")

						//if results != nil && len(results) == 1 {
						//	// append to map after doing remove op for other fail remove op thread
						//	//deletedValsForDeleteMutex.Lock()
						//	//deletedValsForDelete[delKeyVal] = delKeyVal
						//	//deletedValsForDeleteMutex.Unlock()
						//} else {
						//	panic("Delete(success) should be fail!")
						//}
						//} else {
						//	deletedValsForSelectUpdateMutex.RLock()
						//	if _, ok := deletedValsForSelectUpdate[delKeyVal]; !ok {
						//		deletedValsForSelectUpdateMutex.RUnlock()
						//		panic("remove op test failed!")
						//	}
						//	deletedValsForSelectUpdateMutex.RUnlock()
						//	//panic("remove op test failed!")
						//}
					}

					finalizeRandomDeleteTxn(txn_, delKeyValBase)
					ch <- 1
					//common.SH_Assert(isDeleted == true, "remove should be success!")
				}()
			}
		case 4: // Update
			fmt.Println()
		case 5, 6: // Select (Point Scan)
			// get 0-1 value
			tmpRand := rand.Intn(2)
			if tmpRand == 0 { // 50% is Select to not existing entry
				go func() {
					insValsMutex.RLock()
					if len(insVals) == 0 {
						insValsMutex.RUnlock()
						ch <- 1
						return
					}
					tmpIdx := int(rand.Intn(len(insVals)))
					//fmt.Printf("sl.GetValue at testSkipListMix: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*deletedValsForSelectUpdate)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(deletedValsForSelectUpdate))
					getTgtBase := insVals[tmpIdx]
					insValsMutex.RUnlock()
					txn_ := txnMgr.Begin(nil)
					for ii := int32(0); ii < stride; ii++ {
						getKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(getTgtBase, stride), ii).(T)
						//getTgtVal := types.NewValue(getKeyVal)
						//correctVal := samehada_util.GetValueForSkipListEntry(getKeyVal)

						common.ShPrintf(common.DEBUGGING, "Select(fail) op start.")
						selectPlan := createSpecifiedValDeletePlanNode(getKeyVal, c, tableMetadata, keyType)
						results := executePlan(c, shi.GetBufferPoolManager(), txn_, selectPlan)

						if txn_.GetState() == access.ABORTED {
							break
						}

						common.SH_Assert(results != nil && len(results) == 1, "select(fail) should be fail!")
						collectVal := types.NewInteger(samehada_util.GetInt32ValCorrespondToPassVal(getKeyVal))
						gotVal := results[0].GetValue(tableMetadata.Schema(), 1)
						common.SH_Assert(gotVal.CompareEquals(collectVal), "value should be "+fmt.Sprintf("%d not %d", collectVal.ToInteger(), gotVal.ToInteger()))

						////gotVal := sl.GetValue(&getTgtVal)
						//if gotVal == math.MaxUint32 {
						//	deletedValsForSelectUpdateMutex.RLock()
						//	if _, ok := deletedValsForSelectUpdate[getKeyVal]; !ok {
						//		deletedValsForSelectUpdateMutex.RUnlock()
						//		panic("get op test failed!")
						//	}
						//	deletedValsForSelectUpdateMutex.RUnlock()
						//} else if gotVal != correctVal {
						//	panic("returned value of get of is wrong!")
						//}
					}
					finalizeRandomNoSideEffectTxn(txn_)
					ch <- 1
				}()
			} else { // 50% is Select to not existing entry
				go func() {
					insValsMutex.RLock()
					if len(insVals) == 0 {
						insValsMutex.RUnlock()
						ch <- 1
						//continue
						return
					}
					tmpIdx := int(rand.Intn(len(insVals)))
					//fmt.Printf("sl.GetValue at testSkipListMix: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*deletedValsForSelectUpdate)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(deletedValsForSelectUpdate))
					getTgtBase := insVals[tmpIdx]
					insValsMutex.RUnlock()
					for ii := int32(0); ii < stride; ii++ {
						getTgt := samehada_util.StrideAdd(samehada_util.StrideMul(getTgtBase, stride), ii).(T)
						getTgtVal := types.NewValue(getTgt)
						correctVal := samehada_util.GetValueForSkipListEntry(getTgt)

						common.ShPrintf(common.DEBUGGING, "Select(fail) op start.")
						gotVal := sl.GetValue(&getTgtVal)
						if gotVal == math.MaxUint32 {
							deletedValsForSelectUpdateMutex.RLock()
							if _, ok := deletedValsForSelectUpdate[getTgt]; !ok {
								deletedValsForSelectUpdateMutex.RUnlock()
								panic("get op test failed!")
							}
							deletedValsForSelectUpdateMutex.RUnlock()
						} else if gotVal != correctVal {
							panic("returned value of get of is wrong!")
						}
					}
					ch <- 1
					//common.SH_Assert(, "gotVal is not collect!")
				}()
			}
		case 7: // Select (Range Scan)
			go func() {
				insValsMutex.RLock()
				if len(insVals) == 0 {
					insValsMutex.RUnlock()
					ch <- 1
					//continue
					return
				}
				tmpIdx := int(rand.Intn(len(insVals)))
				//fmt.Printf("sl.GetValue at testSkipListMix: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*deletedValsForSelectUpdate)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(deletedValsForSelectUpdate))
				rangeStartBase := insVals[tmpIdx]
				insValsMutex.RUnlock()
				rangeStartVal := types.NewValue(rangeStartBase)
				rangeEndBase := samehada_util.StrideAdd(rangeStartBase, stride).(T)
				rangeEndVal := types.NewValue(rangeEndBase)
				itr := sl.Iterator(&rangeStartVal, &rangeEndVal)
				for done, _, _, _ := itr.Next(); !done; done, _, _, _ = itr.Next() {
				}

				ch <- 1
			}()
		}
		runningThCnt++
	}

	// TODO: (SDB) need to implement final records data validation (testParallelTxnsQueryingSkipListIndexUsedColumns)

	shi.CloseFilesForTesting()
}
