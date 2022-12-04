package executor_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/container/hash"
	"github.com/ryogrid/SamehadaDB/execution/executors"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
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

/*
import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/container/hash"
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
//, {
//		"select a ... WHERE a >= -2147483647 and a <= 1225", // fail because restriction of current SkipList Index impl?
//		executionEngine,
//		executorContext,
//		tableMetadata,
//		[]executors.Column{{"a", types.Integer}},
//		executors.Predicate{"a", expression.GreaterThanOrEqual, 20},
//		int32(tableMetadata.Schema().GetColIndex("a")),
//		[]types.Value{types.NewInteger(math.MinInt32), types.NewInteger(1225)},
//		3,
//	}}


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
*/

const (
	SERIAL_EXEC = iota
	PARALLEL_EXEC
)

// maxVal is *int32 for int32 and float32
func getUniqRandomPrimitivVal[T int32 | float32 | string](keyType types.TypeID, checkDupMap map[T]T, checkDupMapMutex *sync.RWMutex, maxVal interface{}) T {
	checkDupMapMutex.Lock()
	retVal := samehada_util.GetRandomPrimitiveVal[T](keyType, maxVal)
	for _, exist := checkDupMap[retVal]; exist; _, exist = checkDupMap[retVal] {
		retVal = samehada_util.GetRandomPrimitiveVal[T](keyType, maxVal)
	}
	checkDupMap[retVal] = retVal
	checkDupMapMutex.Unlock()
	return retVal
}

func createBalanceUpdatePlanNode[T int32 | float32 | string](keyColumnVal T, newBalance int32, c *catalog.Catalog, tm *catalog.TableMetadata, keyType types.TypeID, indexKind index_constants.IndexKind) (createdPlan plans.Plan) {
	row := make([]types.Value, 0)
	row = append(row, types.NewValue(keyColumnVal))
	row = append(row, types.NewInteger(newBalance))

	pred := executors.Predicate{"account_id", expression.Equal, keyColumnVal}
	tmpColVal := new(expression.ColumnValue)
	//tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tm.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(executors.GetValue(pred.RightColumn), executors.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	var skipListPointScanP plans.Plan
	switch indexKind {
	case index_constants.INDEX_KIND_INVAID:
		skipListPointScanP = plans.NewSeqScanPlanNode(tm.Schema(), expression_.(*expression.Comparison), tm.OID())
	case index_constants.INDEX_KIND_SKIP_LIST:
		skipListPointScanP = plans.NewPointScanWithIndexPlanNode(tm.Schema(), expression_.(*expression.Comparison), tm.OID())
	default:
		panic("not implemented!")
	}
	updatePlanNode := plans.NewUpdatePlanNode(row, []int{1}, skipListPointScanP)

	return updatePlanNode
}

func createSpecifiedValInsertPlanNode[T int32 | float32 | string](keyColumnVal T, balance int32, c *catalog.Catalog, tm *catalog.TableMetadata, keyType types.TypeID) (createdPlan plans.Plan) {
	row := make([]types.Value, 0)
	row = append(row, types.NewValue(keyColumnVal))
	row = append(row, types.NewInteger(balance))

	rows := make([][]types.Value, 0)
	rows = append(rows, row)

	insertPlanNode := plans.NewInsertPlanNode(rows, tm.OID())
	return insertPlanNode
}

func createSpecifiedValDeletePlanNode[T int32 | float32 | string](keyColumnVal T, c *catalog.Catalog, tm *catalog.TableMetadata, keyType types.TypeID, indexKind index_constants.IndexKind) (createdPlan plans.Plan) {
	pred := executors.Predicate{"account_id", expression.Equal, keyColumnVal}
	tmpColVal := new(expression.ColumnValue)
	//tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tm.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(executors.GetValue(pred.RightColumn), executors.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	var skipListPointScanP plans.Plan
	switch indexKind {
	case index_constants.INDEX_KIND_INVAID:
		skipListPointScanP = plans.NewSeqScanPlanNode(tm.Schema(), expression_.(*expression.Comparison), tm.OID())
	case index_constants.INDEX_KIND_SKIP_LIST:
		skipListPointScanP = plans.NewPointScanWithIndexPlanNode(tm.Schema(), expression_.(*expression.Comparison), tm.OID())
	default:
		panic("not implemented!")
	}
	deletePlanNode := plans.NewDeletePlanNode(skipListPointScanP)
	return deletePlanNode
}

func createAccountIdUpdatePlanNode[T int32 | float32 | string](keyColumnVal T, newKeyColumnVal T, c *catalog.Catalog, tm *catalog.TableMetadata, keyType types.TypeID, indexKind index_constants.IndexKind) (createdPlan plans.Plan) {
	row := make([]types.Value, 0)
	row = append(row, types.NewValue(newKeyColumnVal))
	row = append(row, types.NewInteger(-1))

	pred := executors.Predicate{"account_id", expression.Equal, keyColumnVal}
	tmpColVal := new(expression.ColumnValue)
	//tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tm.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(executors.GetValue(pred.RightColumn), executors.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	var skipListPointScanP plans.Plan
	switch indexKind {
	case index_constants.INDEX_KIND_INVAID:
		skipListPointScanP = plans.NewSeqScanPlanNode(tm.Schema(), expression_.(*expression.Comparison), tm.OID())
	case index_constants.INDEX_KIND_SKIP_LIST:
		skipListPointScanP = plans.NewPointScanWithIndexPlanNode(tm.Schema(), expression_.(*expression.Comparison), tm.OID())
	default:
		panic("not implemented!")
	}
	updatePlanNode := plans.NewUpdatePlanNode(row, []int{0}, skipListPointScanP)

	return updatePlanNode
}

func createSpecifiedPointScanPlanNode[T int32 | float32 | string](getKeyVal T, c *catalog.Catalog, tm *catalog.TableMetadata, keyType types.TypeID, indexKind index_constants.IndexKind) (createdPlan plans.Plan) {
	pred := executors.Predicate{"account_id", expression.Equal, getKeyVal}
	tmpColVal := new(expression.ColumnValue)
	//tmpColVal.SetTupleIndex(0)
	tmpColVal.SetColIndex(tm.Schema().GetColIndex(pred.LeftColumn))
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(executors.GetValue(pred.RightColumn), executors.GetValueType(pred.RightColumn)), pred.Operator, types.Boolean)

	var skipListPointScanP plans.Plan
	switch indexKind {
	case index_constants.INDEX_KIND_INVAID:
		skipListPointScanP = plans.NewSeqScanPlanNode(tm.Schema(), expression_.(*expression.Comparison), tm.OID())
	case index_constants.INDEX_KIND_SKIP_LIST:
		skipListPointScanP = plans.NewPointScanWithIndexPlanNode(tm.Schema(), expression_.(*expression.Comparison), tm.OID())
	default:
		panic("not implemented!")
	}
	return skipListPointScanP
}

func createSpecifiedRangeScanPlanNode[T int32 | float32 | string](c *catalog.Catalog, tm *catalog.TableMetadata, keyType types.TypeID, colIdx int32, rangeStartKey *T, rangeEndKey *T, indexKind index_constants.IndexKind) (createdPlan plans.Plan) {
	var startVal *types.Value = nil
	var endVal *types.Value = nil

	var skipListRangeScanP plans.Plan
	switch indexKind {
	case index_constants.INDEX_KIND_INVAID:
		skipListRangeScanP = plans.NewSeqScanPlanNode(tm.Schema(), nil, tm.OID())
	case index_constants.INDEX_KIND_SKIP_LIST:
		if rangeStartKey != nil {
			startVal = samehada_util.GetPonterOfValue(types.NewValue(*rangeStartKey))
		}
		if rangeEndKey != nil {
			endVal = samehada_util.GetPonterOfValue(types.NewValue(*rangeEndKey))
		}
		skipListRangeScanP = plans.NewRangeScanWithIndexPlanNode(tm.Schema(), tm.OID(), colIdx, nil, startVal, endVal)
	default:
		panic("not implemented!")
	}

	return skipListRangeScanP
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

func testParallelTxnsQueryingSkipListIndexUsedColumns[T int32 | float32 | string](t *testing.T, keyType types.TypeID, stride int32, opTimes int32, seedVal int32, initialEntryNum int32, bpoolSize int32, indexKind index_constants.IndexKind, execType int32, threadNum int) {
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

	var columnA *column.Column
	var columnB *column.Column
	switch indexKind {
	case index_constants.INDEX_KIND_INVAID:
		columnA = column.NewColumn("account_id", keyType, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil)
		columnB = column.NewColumn("balance", types.Integer, false, index_constants.INDEX_KIND_INVAID, types.PageID(-1), nil)
	case index_constants.INDEX_KIND_SKIP_LIST:
		columnA = column.NewColumn("account_id", keyType, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
		columnB = column.NewColumn("balance", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
	default:
		panic("not implemented!")
	}
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB})

	tableMetadata := c.CreateTable("test_1", schema_, txn)
	txnMgr.Commit(txn)

	// ignored when execType is SERIAL_EXEC
	//const THREAD_NUM = 20 //1 // 10 //20 // 2

	rand.Seed(int64(seedVal))

	insVals := make([]T, 0)
	insValsMutex := new(sync.RWMutex)
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
	const ACCOUNT_NUM = 10 //20 //4
	const BALANCE_AT_START = 1000
	sumOfAllAccountBalanceAtStart := int32(0)
	accountIds := make([]T, 0)
	for ii := 0; ii < ACCOUNT_NUM; ii++ {
		accountId := samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		for _, exist := checkKeyColDupMap[accountId]; exist; _, exist = checkKeyColDupMap[accountId] {
			accountId = samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		}
		checkKeyColDupMap[accountId] = accountId
		checkBalanceColDupMap[int32(BALANCE_AT_START+ii)] = int32(BALANCE_AT_START + ii)
		accountIds = append(accountIds, accountId)
		// not have to duplication check of barance
		insPlan := createSpecifiedValInsertPlanNode(accountId, int32(BALANCE_AT_START+ii), c, tableMetadata, keyType)
		executePlan(c, shi.GetBufferPoolManager(), txn, insPlan)
		sumOfAllAccountBalanceAtStart += int32(BALANCE_AT_START + ii)
	}
	txnMgr.Commit(txn)

	txn = txnMgr.Begin(nil)

	insertedTupleCnt += ACCOUNT_NUM

	getInt32ValCorrespondToPassVal := func(val interface{}) int32 {
		switch val.(type) {
		case int32:
			return val.(int32)
		case float32:
			return int32(val.(float32))
		case string:
			casted := val.(string)
			byteArr := make([]byte, len(casted))
			copy(byteArr, casted)
			//return int32(hash.GenHashMurMur(byteArr)) % (math.MaxInt32 / stride)
			return int32(hash.GenHashMurMur(byteArr)) //% (math.MaxInt32 / stride)
		default:
			panic("unsupported type!")
		}
	}

	// setup other initial entries which is not used as account
	useInitialEntryNum := int(initialEntryNum)
	for ii := 0; ii < useInitialEntryNum; ii++ {
	retry0:
		keyValBase := getUniqRandomPrimitivVal(keyType, checkKeyColDupMap, checkKeyColDupMapMutex, nil)
		balanceVal := getInt32ValCorrespondToPassVal(keyValBase)
		if _, exist := checkBalanceColDupMap[balanceVal]; exist || (balanceVal >= 0 && balanceVal > sumOfAllAccountBalanceAtStart) {
			delete(checkBalanceColDupMap, balanceVal)
			goto retry0
		}
		checkBalanceColDupMap[balanceVal] = balanceVal
		checkKeyColDupMap[keyValBase] = keyValBase

		for ii := int32(0); ii < stride; ii++ {
			insKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(keyValBase, stride), ii).(T)
			insBranceVal := getInt32ValCorrespondToPassVal(insKeyVal)

			insPlan := createSpecifiedValInsertPlanNode(insKeyVal, insBranceVal, c, tableMetadata, keyType)
			executePlan(c, shi.GetBufferPoolManager(), txn, insPlan)
		}

		insVals = append(insVals, keyValBase)
	}

	insertedTupleCnt += initialEntryNum * stride

	txnMgr.Commit(txn)

	ch := make(chan int32)

	abortTxnAndUpdateCounter := func(txn_ *access.Transaction) {
		handleFnishedTxn(c, txnMgr, txn_)
		atomic.AddInt32(&executedTxnCnt, 1)
		atomic.AddInt32(&abortedTxnCnt, 1)
		if execType == PARALLEL_EXEC {
			ch <- 1
		}
	}

	insValsAppendWithLock := func(keyVal T) {
		insValsMutex.Lock()
		insVals = append(insVals, keyVal)
		insValsMutex.Unlock()
	}

	checkKeyColDupMapDeleteWithLock := func(keyVal T) {
		checkKeyColDupMapMutex.Lock()
		delete(checkKeyColDupMap, keyVal)
		checkKeyColDupMapMutex.Unlock()
	}

	//checkKeyColDupMapSetWithLock := func(keyVal T) {
	//	checkKeyColDupMapMutex.Lock()
	//	checkKeyColDupMap[keyVal] = keyVal
	//	checkKeyColDupMapMutex.Unlock()
	//}

	checkBalanceColDupMapDeleteWithLock := func(balanceVal int32) {
		checkBalanceColDupMapMutex.Lock()
		delete(checkBalanceColDupMap, balanceVal)
		checkBalanceColDupMapMutex.Unlock()
	}

	checkBalanceColDupMapSetWithLock := func(balanceVal int32) {
		checkBalanceColDupMapMutex.Lock()
		checkBalanceColDupMap[balanceVal] = balanceVal
		checkBalanceColDupMapMutex.Unlock()
	}

	deleteCheckMapEntriesWithLock := func(keyValBase T) {
		checkKeyColDupMapDeleteWithLock(keyValBase)
		balanceVal := getInt32ValCorrespondToPassVal(keyValBase)
		checkBalanceColDupMapDeleteWithLock(balanceVal)
	}

	deleteCheckBalanceColDupMapBalances := func(balance1_ int32, balance2_ int32) {
		checkBalanceColDupMapMutex.Lock()
		delete(checkBalanceColDupMap, balance1_)
		delete(checkBalanceColDupMap, balance2_)
		checkBalanceColDupMapMutex.Unlock()
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

	// internally, issue new transaction
	checkTotalBalanceNoChange := func() {
		txn_ := txnMgr.Begin(nil)
		txn_.SetDebugInfo("checkTotalBrance-Op")
		sumOfAllAccountBalanceAfterTest := int32(0)
		for ii := 0; ii < ACCOUNT_NUM; ii++ {
			selPlan := createSpecifiedPointScanPlanNode(accountIds[ii], c, tableMetadata, keyType, indexKind)
			results := executePlan(c, shi.GetBufferPoolManager(), txn_, selPlan)
			//common.SH_Assert(txn_.GetState() != access.ABORTED, "txn state should not be ABORTED!")
			if txn_.GetState() == access.ABORTED {
				handleFnishedTxn(c, txnMgr, txn_)
				return
			}
			common.SH_Assert(results != nil && len(results) == 1, fmt.Sprintf("point scan result count is not 1 (%d)!\n", len(results)))
			sumOfAllAccountBalanceAfterTest += results[0].GetValue(tableMetadata.Schema(), 1).ToInteger()
		}
		common.SH_Assert(sumOfAllAccountBalanceAfterTest == sumOfAllAccountBalanceAtStart, fmt.Sprintf("total account volume is changed! %d != %d\n", sumOfAllAccountBalanceAfterTest, sumOfAllAccountBalanceAtStart))
		finalizeRandomNoSideEffectTxn(txn_)
	}

	finalizeAccountUpdateTxn := func(txn_ *access.Transaction, oldBalance1 int32, oldBalance2 int32, newBalance1 int32, newBalance2 int32) {
		//if rand.Intn(3) == 0 {
		//	txn_.SetState(access.ABORTED)
		//}

		txnOk := handleFnishedTxn(c, txnMgr, txn_)

		if txnOk {
			checkTotalBalanceNoChange()
			atomic.AddInt32(&commitedTxnCnt, 1)
			deleteCheckBalanceColDupMapBalances(oldBalance1, oldBalance2)
		} else {
			checkTotalBalanceNoChange()
			atomic.AddInt32(&abortedTxnCnt, 1)
			// rollback
			deleteCheckBalanceColDupMapBalances(newBalance1, newBalance2)
		}
		atomic.AddInt32(&executedTxnCnt, 1)
	}

	finalizeRandomInsertTxn := func(txn_ *access.Transaction, insKeyValBase T) {
		txnOk := handleFnishedTxn(c, txnMgr, txn_)
		if txnOk {
			insValsAppendWithLock(insKeyValBase)
			//putCheckMapEntriesWithLock(insKeyValBase)
			atomic.AddInt32(&insertedTupleCnt, stride)
			atomic.AddInt32(&commitedTxnCnt, 1)
		} else {
			// rollback
			deleteCheckMapEntriesWithLock(insKeyValBase)
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
			deleteCheckMapEntriesWithLock(delKeyValBase)
			atomic.AddInt32(&deletedTupleCnt, stride)
			atomic.AddInt32(&commitedTxnCnt, 1)
		} else {
			// rollback removed element
			insValsAppendWithLock(delKeyValBase)
			atomic.AddInt32(&abortedTxnCnt, 1)
		}
		atomic.AddInt32(&executedTxnCnt, 1)
	}

	finalizeRandomUpdateTxn := func(txn_ *access.Transaction, oldKeyValBase T, newKeyValBase T) {
		txnOk := handleFnishedTxn(c, txnMgr, txn_)
		if txnOk {
			// append new base value
			insValsAppendWithLock(newKeyValBase)
			deleteCheckMapEntriesWithLock(oldKeyValBase)
			atomic.AddInt32(&commitedTxnCnt, 1)
		} else {
			// rollback removed element
			insValsAppendWithLock(oldKeyValBase)
			deleteCheckMapEntriesWithLock(newKeyValBase)
			atomic.AddInt32(&abortedTxnCnt, 1)
		}
		atomic.AddInt32(&executedTxnCnt, 1)
	}

	useOpTimes := int(opTimes)
	runningThCnt := 0
	for ii := 0; ii <= useOpTimes; ii++ {
		// wait last go routines finishes
		if ii == useOpTimes && execType == PARALLEL_EXEC {
			for runningThCnt > 0 {
				<-ch
				runningThCnt--
				common.ShPrintf(common.DEBUGGING, "runningThCnt=%d\n", runningThCnt)
			}
			break
		}

		// wait for keeping THREAD_NUM groroutine existing
		for runningThCnt >= threadNum && execType == PARALLEL_EXEC {
			//for runningThCnt > 0 { // serial execution
			<-ch
			runningThCnt--

			common.ShPrintf(common.DEBUGGING, "runningThCnt=%d\n", runningThCnt)
		}
		common.ShPrintf(common.DEBUGGING, "ii=%d\n", ii)

		//// get 0-7
		//opType := rand.Intn(8)

		//// move money, random Insert, Delete
		//opType := rand.Intn(4)

		//// move money, Random Insert, Randome Delete, Random Update
		//opType := rand.Intn(5)

		//// move money, Random Insert, Randome Delete, Random Update, Random Point Scan
		//opType := rand.Intn(7)

		//// Random Insert only
		//opType := 1

		//// Random Isert and Random Delete only
		//opType := rand.Intn(3)
		//opType += 1

		//// move money, Random Insert, Randome Delete, Random Update, Random Point Scan, Random Range Scan
		//opType := rand.Intn(8)

		// move money, Random Insert, Randome Delete, ,Random Point Scan, Random Range Scan
		opType := rand.Intn(7)
		if opType >= 4 {
			opType += 1
		}

		//// move money only
		//opType := 0

		//// move money, Random Insert, Random Update, Random Point Scan
		//opType := rand.Intn(5)
		//if opType >= 2 {
		//	opType = opType + 2
		//}
		switch opType {
		case 0: // Update two account balance (move money)
			moveMoneyOpFunc := func() {
				txn_ := txnMgr.Begin(nil)
				txn_.SetDebugInfo("MoneyMove-Op")

				// decide accounts
				idx1 := rand.Intn(ACCOUNT_NUM)
				idx2 := idx1 + 1
				if idx2 == ACCOUNT_NUM {
					idx2 = 0
				}

				// get current volume of money move accounts
				selPlan1 := createSpecifiedPointScanPlanNode(accountIds[idx1], c, tableMetadata, keyType, indexKind)
				results1 := executePlan(c, shi.GetBufferPoolManager(), txn_, selPlan1)
				if txn_.GetState() == access.ABORTED {
					abortTxnAndUpdateCounter(txn_)
					return
				}
				if results1 == nil || len(results1) != 1 {
					//time.Sleep(time.Second * 120)
					panic("balance check failed(1).")
				}
				balance1 := results1[0].GetValue(tableMetadata.Schema(), 1).ToInteger()

				selPlan2 := createSpecifiedPointScanPlanNode(accountIds[idx2], c, tableMetadata, keyType, indexKind)
				results2 := executePlan(c, shi.GetBufferPoolManager(), txn_, selPlan2)
				if txn_.GetState() == access.ABORTED {
					abortTxnAndUpdateCounter(txn_)
					return
				}
				if results2 == nil || len(results2) != 1 {
					//time.Sleep(time.Second * 120)
					panic("balance check failed(2).")
				}
				balance2 := results2[0].GetValue(tableMetadata.Schema(), 1).ToInteger()

				// utility func
				putCheckBalanceColDupMapNewBalance := func(newBalance1_ int32, newBalance2_ int32) {
					checkBalanceColDupMapMutex.Lock()
					checkBalanceColDupMap[newBalance1_] = newBalance1_
					checkBalanceColDupMap[newBalance2_] = newBalance2_
					checkBalanceColDupMapMutex.Unlock()
				}

				// decide move ammount

				var newBalance1 int32
				var newBalance2 int32
				if balance1 > balance2 {
				retry1_1:
					newBalance1 = getUniqRandomPrimitivVal(types.Integer, checkBalanceColDupMap, checkBalanceColDupMapMutex, &balance1)
					newBalance2 = balance2 + (balance1 - newBalance1)
					checkBalanceColDupMapMutex.Lock()
					if _, exist := checkBalanceColDupMap[newBalance2]; exist {
						delete(checkBalanceColDupMap, newBalance1)
						checkBalanceColDupMapMutex.Unlock()
						goto retry1_1
					}
					checkBalanceColDupMapMutex.Unlock()
					putCheckBalanceColDupMapNewBalance(newBalance1, newBalance2)
				} else {
				retry1_2:
					newBalance2 = getUniqRandomPrimitivVal(types.Integer, checkBalanceColDupMap, checkBalanceColDupMapMutex, &balance2)
					newBalance1 = balance1 + (balance2 - newBalance2)
					checkBalanceColDupMapMutex.Lock()
					if _, exist := checkBalanceColDupMap[newBalance1]; exist {
						delete(checkBalanceColDupMap, newBalance2)
						checkBalanceColDupMapMutex.Unlock()
						goto retry1_2
					}
					//putCheckBalanceColDupMapNewBalance(balance1, balance2, newBalance1, newBalance2)
					checkBalanceColDupMapMutex.Unlock()
					putCheckBalanceColDupMapNewBalance(newBalance1, newBalance2)
				}

				// create plans and execute these

				common.ShPrintf(common.DEBUGGING, "Update account op start.\n")

				updatePlan1 := createBalanceUpdatePlanNode(accountIds[idx1], newBalance1, c, tableMetadata, keyType, indexKind)
				updateRslt1 := executePlan(c, shi.GetBufferPoolManager(), txn_, updatePlan1)

				if txn_.GetState() == access.ABORTED {
					finalizeAccountUpdateTxn(txn_, balance1, balance2, newBalance1, newBalance2)
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
					return
				}

				common.SH_Assert(len(updateRslt1) == 1 && txn_.GetState() != access.ABORTED, fmt.Sprintf("account update fails!(1) txn_.txn_id:%v", txn_.GetTransactionId()))

				updatePlan2 := createBalanceUpdatePlanNode(accountIds[idx2], newBalance2, c, tableMetadata, keyType, indexKind)
				updateRslt2 := executePlan(c, shi.GetBufferPoolManager(), txn_, updatePlan2)

				if txn_.GetState() == access.ABORTED {
					finalizeAccountUpdateTxn(txn_, balance1, balance2, newBalance1, newBalance2)
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
					return
				}

				common.SH_Assert(len(updateRslt2) == 1 && txn_.GetState() != access.ABORTED, fmt.Sprintf("account update fails!(2) txn_.txn_id:%v", txn_.GetTransactionId()))

				finalizeAccountUpdateTxn(txn_, balance1, balance2, newBalance1, newBalance2)
				if execType == PARALLEL_EXEC {
					ch <- 1
				}
			}
			if execType == PARALLEL_EXEC {
				go moveMoneyOpFunc()
			} else {
				moveMoneyOpFunc()
			}
		case 1: // Insert
			randomInsertOpFunc := func() {
			retry2:
				tmpMax := math.MaxInt32 / stride
				insKeyValBase := getUniqRandomPrimitivVal(keyType, checkKeyColDupMap, checkKeyColDupMapMutex, &tmpMax)
				balanceVal := getInt32ValCorrespondToPassVal(insKeyValBase)
				checkBalanceColDupMapMutex.RLock()
				if _, exist := checkBalanceColDupMap[balanceVal]; exist || (balanceVal >= 0 && balanceVal < sumOfAllAccountBalanceAtStart) {
					checkBalanceColDupMapMutex.RUnlock()
					checkKeyColDupMapDeleteWithLock(insKeyValBase)
					goto retry2
				}
				checkBalanceColDupMapMutex.RUnlock()
				checkBalanceColDupMapSetWithLock(balanceVal)

				txn_ := txnMgr.Begin(nil)
				txn_.SetDebugInfo("Insert(random)-Op")
				common.ShPrintf(common.DEBUGGING, fmt.Sprintf("Insert op start. txnId:%v ii:%d\n", txn_.GetTransactionId(), ii))
				for jj := int32(0); jj < stride; jj++ {
					insKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(insKeyValBase, stride), jj).(T)
					insBalanceVal := getInt32ValCorrespondToPassVal(insKeyVal)

					common.ShPrintf(common.DEBUGGING, fmt.Sprintf("Insert op start. txnId:%v ii:%d jj:%d\n", txn_.GetTransactionId(), ii, jj))
					insPlan := createSpecifiedValInsertPlanNode(insKeyVal, insBalanceVal, c, tableMetadata, keyType)
					executePlan(c, shi.GetBufferPoolManager(), txn_, insPlan)

					if txn_.GetState() == access.ABORTED {
						break
					}
					//fmt.Printf("sl.Insert at insertRandom: jj=%d, insKeyValBase=%d len(*insVals)=%d\n", jj, insKeyValBase, len(insVals))
				}

				finalizeRandomInsertTxn(txn_, insKeyValBase)
				if execType == PARALLEL_EXEC {
					ch <- 1
				}
			}
			if execType == PARALLEL_EXEC {
				go randomInsertOpFunc()
			} else {
				randomInsertOpFunc()
			}
		case 2, 3: // Delete
			// get 0-1 value
			tmpRand := rand.Intn(2)
			//tmpRand := 1
			if tmpRand == 0 {
				// 50% is Delete to not existing entry
				randomDeleteFailOpFunc := func() {
					deletedValsForDeleteMutex.RLock()
					if len(deletedValsForDelete) == 0 {
						deletedValsForDeleteMutex.RUnlock()
						if execType == PARALLEL_EXEC {
							ch <- 1
						}
						return
					}
					deletedValsForDeleteMutex.RUnlock()

					txn_ := txnMgr.Begin(nil)
					txn_.SetDebugInfo("Delete(fail)-Op")
					deletedValsForDeleteMutex.RLock()
					delKeyValBase := samehada_util.ChoiceValFromMap(deletedValsForDelete)
					deletedValsForDeleteMutex.RUnlock()
					for jj := int32(0); jj < stride; jj++ {
						delKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(delKeyValBase, stride), jj).(T)

						common.ShPrintf(common.DEBUGGING, "Delete(fail) op start.\n")
						delPlan := createSpecifiedValDeletePlanNode(delKeyVal, c, tableMetadata, keyType, indexKind)
						results := executePlan(c, shi.GetBufferPoolManager(), txn_, delPlan)

						if txn_.GetState() == access.ABORTED {
							break
						}

						common.SH_Assert(results != nil && len(results) == 0, "delete(fail) should not be fail!")
					}

					finalizeRandomNoSideEffectTxn(txn_)

					if execType == PARALLEL_EXEC {
						ch <- 1
					}
				}
				if execType == PARALLEL_EXEC {
					go randomDeleteFailOpFunc()
				} else {
					randomDeleteFailOpFunc()
				}
			} else {
				// 50% is Delete to existing entry
				randomDeleteSuccessOpFunc := func() {
					insValsMutex.Lock()
					if len(insVals)-1 < 0 {
						insValsMutex.Unlock()
						if execType == PARALLEL_EXEC {
							ch <- 1
						}
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
					txn_.SetDebugInfo("Delete(success)-Op")

					for jj := int32(0); jj < stride; jj++ {
						delKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(delKeyValBase, stride), jj).(T)

						common.ShPrintf(common.DEBUGGING, "Delete(success) op start.\n")

						delPlan := createSpecifiedValDeletePlanNode(delKeyVal, c, tableMetadata, keyType, indexKind)
						results := executePlan(c, shi.GetBufferPoolManager(), txn_, delPlan)

						if txn_.GetState() == access.ABORTED {
							break
						}

						common.SH_Assert(results != nil && len(results) == 1, "Delete(success) failed!")
					}

					finalizeRandomDeleteTxn(txn_, delKeyValBase)
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
				}
				if execType == PARALLEL_EXEC {
					go randomDeleteSuccessOpFunc()
				} else {
					randomDeleteSuccessOpFunc()
				}
			}
		case 4: // Random Update
			randomUpdateOpFunc := func() {
				insValsMutex.Lock()
				if len(insVals) == 0 {
					insValsMutex.Unlock()
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
					return
				}
				tmpIdx := int(rand.Intn(len(insVals)))
				updateKeyValBase := insVals[tmpIdx]
				if len(insVals) == 1 {
					// make empty
					insVals = make([]T, 0)
				} else if len(insVals)-1 == tmpIdx {
					insVals = insVals[:len(insVals)-1]
				} else {
					insVals = append(insVals[:tmpIdx], insVals[tmpIdx+1:]...)
				}
				insValsMutex.Unlock()
			retry3:
				tmpMax := math.MaxInt32 / stride
				updateNewKeyValBase := getUniqRandomPrimitivVal(keyType, checkKeyColDupMap, checkKeyColDupMapMutex, &tmpMax)
				newBalanceVal := getInt32ValCorrespondToPassVal(updateNewKeyValBase)
				checkBalanceColDupMapMutex.RLock()
				if _, exist := checkBalanceColDupMap[newBalanceVal]; exist || (newBalanceVal >= 0 && newBalanceVal <= sumOfAllAccountBalanceAtStart) {
					checkBalanceColDupMapMutex.RUnlock()
					checkKeyColDupMapDeleteWithLock(updateNewKeyValBase)
					goto retry3
				}
				checkBalanceColDupMapMutex.RUnlock()
				checkBalanceColDupMapSetWithLock(newBalanceVal)

				txn_ := txnMgr.Begin(nil)
				txn_.SetDebugInfo("Update(random)-Op")

				for jj := int32(0); jj < stride; jj++ {
					updateKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(updateKeyValBase, stride), jj).(T)
					updateNewKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(updateNewKeyValBase, stride), jj).(T)
					//newBalanceVal := samehada_util.getInt32ValCorrespondToPassVal(updateKeyVal)

					common.ShPrintf(common.DEBUGGING, "Update (random) op start.")

					updatePlan1 := createAccountIdUpdatePlanNode(updateKeyVal, updateNewKeyVal, c, tableMetadata, keyType, indexKind)
					results1 := executePlan(c, shi.GetBufferPoolManager(), txn_, updatePlan1)

					if txn_.GetState() == access.ABORTED {
						break
					}

					common.SH_Assert(results1 != nil && len(results1) == 1, "Update failed!")

					updatePlan2 := createBalanceUpdatePlanNode(updateNewKeyVal, getInt32ValCorrespondToPassVal(updateNewKeyVal), c, tableMetadata, keyType, indexKind)
					results2 := executePlan(c, shi.GetBufferPoolManager(), txn_, updatePlan2)

					if txn_.GetState() == access.ABORTED {
						break
					}

					common.SH_Assert(results2 != nil && len(results2) == 1, "Update failed!")
				}

				finalizeRandomUpdateTxn(txn_, updateKeyValBase, updateNewKeyValBase)
				if execType == PARALLEL_EXEC {
					ch <- 1
				}
			}
			if execType == PARALLEL_EXEC {
				go randomUpdateOpFunc()
			} else {
				randomUpdateOpFunc()
			}
		case 5, 6: // Select (Point Scan)
			// get 0-1 value
			tmpRand := rand.Intn(2)
			if tmpRand == 0 { // 50% is Select to not existing entry
				randomPointScanFailOpFunc := func() {
					deletedValsForDeleteMutex.RLock()
					if len(deletedValsForDelete) == 0 {
						deletedValsForDeleteMutex.RUnlock()
						if execType == PARALLEL_EXEC {
							ch <- 1
						}
						return
					}
					getTgtBase := samehada_util.ChoiceValFromMap(deletedValsForDelete)
					deletedValsForDeleteMutex.RUnlock()
					txn_ := txnMgr.Begin(nil)
					txn_.SetDebugInfo("Select(point|fail)-Op")
					for jj := int32(0); jj < stride; jj++ {
						getKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(getTgtBase, stride), jj).(T)

						common.ShPrintf(common.DEBUGGING, "Select(fail) op start.")
						selectPlan := createSpecifiedPointScanPlanNode(getKeyVal, c, tableMetadata, keyType, indexKind)
						results := executePlan(c, shi.GetBufferPoolManager(), txn_, selectPlan)

						if txn_.GetState() == access.ABORTED {
							break
						}

						common.SH_Assert(results != nil && len(results) == 0, "Select(fail) should be fail!")
					}
					finalizeRandomNoSideEffectTxn(txn_)
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
				}
				if execType == PARALLEL_EXEC {
					go randomPointScanFailOpFunc()
				} else {
					randomPointScanFailOpFunc()
				}
			} else { // 50% is Select to existing entry
				randomPointScanSuccessOpFunc := func() {
					insValsMutex.RLock()
					if len(insVals) == 0 {
						insValsMutex.RUnlock()
						if execType == PARALLEL_EXEC {
							ch <- 1
						}
						return
					}
					tmpIdx := int(rand.Intn(len(insVals)))
					//fmt.Printf("sl.GetValue at testSkipListMix: jj=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*deletedValsForSelectUpdate)=%d\n", jj, tmpIdx, insVals[tmpIdx], len(insVals), len(deletedValsForSelectUpdate))
					getKeyValBase := insVals[tmpIdx]
					insValsMutex.RUnlock()
					txn_ := txnMgr.Begin(nil)
					txn_.SetDebugInfo("Select(point|success)-Op")
					for jj := int32(0); jj < stride; jj++ {
						getKeyVal := samehada_util.StrideAdd(samehada_util.StrideMul(getKeyValBase, stride), jj).(T)

						common.ShPrintf(common.DEBUGGING, "Select(success) op start.")
						selectPlan := createSpecifiedPointScanPlanNode(getKeyVal, c, tableMetadata, keyType, indexKind)
						results := executePlan(c, shi.GetBufferPoolManager(), txn_, selectPlan)

						if txn_.GetState() == access.ABORTED {
							break
						}

						common.SH_Assert(results != nil && len(results) == 1, "Select(success) should not be fail!")
						collectVal := types.NewInteger(getInt32ValCorrespondToPassVal(getKeyVal))
						gotVal := results[0].GetValue(tableMetadata.Schema(), 1)
						common.SH_Assert(gotVal.CompareEquals(collectVal), "value should be "+fmt.Sprintf("%d not %d", collectVal.ToInteger(), gotVal.ToInteger()))
					}
					finalizeRandomNoSideEffectTxn(txn_)
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
				}
				if execType == PARALLEL_EXEC {
					go randomPointScanSuccessOpFunc()
				} else {
					randomPointScanSuccessOpFunc()
				}
			}
		case 7: // Select (Range Scan)
			randomRangeScanOpFunc := func() {
				insValsMutex.RLock()
				if len(insVals) < 2 {
					insValsMutex.RUnlock()
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
					return
				}
				common.ShPrintf(common.DEBUGGING, "Select(success) op start.\n")
				tmpIdx1 := int(rand.Intn(len(insVals)))
				tmpIdx2 := int(rand.Intn(len(insVals)))
				diffToMakeNoExist := int32(10)
				rangeStartKey := insVals[tmpIdx1]
				rangeEndKey := insVals[tmpIdx2]
				insValsMutex.RUnlock()
				// get 0-8 value
				tmpRand := rand.Intn(9)
				var rangeScanPlan plans.Plan
				switch tmpRand {
				case 0: // start only
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, &rangeStartKey, nil, indexKind)
				case 1: // end only
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, nil, &rangeEndKey, indexKind)
				case 2: // start and end
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, &rangeStartKey, &rangeEndKey, indexKind)
				case 3: // not specified both
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, nil, nil, indexKind)
				case 4: // start only (not exisiting val)
					tmpStartKey := samehada_util.StrideAdd(rangeStartKey, diffToMakeNoExist).(T)
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, &tmpStartKey, nil, indexKind)
				case 5: // end only (not existing val)
					tmpEndKey := samehada_util.StrideAdd(rangeEndKey, diffToMakeNoExist).(T)
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, nil, &tmpEndKey, indexKind)
				case 6: // start and end (start val is not existing one)
					tmpStartKey := samehada_util.StrideAdd(rangeStartKey, diffToMakeNoExist).(T)
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, &tmpStartKey, &rangeEndKey, indexKind)
				case 7: // start and end (start val is not existing one)
					tmpEndKey := samehada_util.StrideAdd(rangeEndKey, diffToMakeNoExist).(T)
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, &rangeStartKey, &tmpEndKey, indexKind)
				case 8: // start and end (end val is not existing one)
					tmpStartKey := samehada_util.StrideAdd(rangeStartKey, diffToMakeNoExist).(T)
					tmpEndKey := samehada_util.StrideAdd(rangeEndKey, diffToMakeNoExist).(T)
					rangeScanPlan = createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, &tmpStartKey, &tmpEndKey, indexKind)
				}

				txn_ := txnMgr.Begin(nil)
				txn_.SetDebugInfo("Select(Range)-Op")
				results := executePlan(c, shi.GetBufferPoolManager(), txn_, rangeScanPlan)

				if txn_.GetState() == access.ABORTED {
					finalizeRandomNoSideEffectTxn(txn_)
					if execType == PARALLEL_EXEC {
						ch <- 1
					}
					return
				}

				if indexKind == index_constants.INDEX_KIND_SKIP_LIST {
					resultsLen := len(results)
					var prevVal *types.Value = nil
					for jj := 0; jj < resultsLen; jj++ {
						curVal := results[jj].GetValue(tableMetadata.Schema(), 0)

						if prevVal != nil {
							common.SH_Assert(curVal.CompareGreaterThan(*prevVal), "values should be "+fmt.Sprintf("%v > %v", curVal.ToIFValue(), (*prevVal).ToIFValue()))
						}
						prevVal = &curVal
					}
				}
				finalizeRandomNoSideEffectTxn(txn_)
				if execType == PARALLEL_EXEC {
					ch <- 1
				}
			}
			if execType == PARALLEL_EXEC {
				go randomRangeScanOpFunc()
			} else {
				randomRangeScanOpFunc()
			}
		}
		runningThCnt++
	}

	// final checking of DB stored data
	// below, txns are execurted serial. so, txn abort due to CC protocol doesn't occur

	// check total volume of accounts
	checkTotalBalanceNoChange()

	// check counts and order of all record got with index used full scan

	// col1 ---------------------------------
	txn_ := txnMgr.Begin(nil)

	// check record num (index of col1 is used)
	collectNumMaybe := insertedTupleCnt - deletedTupleCnt

	rangeScanPlan1 := createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 0, nil, nil, indexKind)
	results1 := executePlan(c, shi.GetBufferPoolManager(), txn_, rangeScanPlan1)
	resultsLen1 := len(results1)
	common.SH_Assert(collectNumMaybe == int32(resultsLen1), "records count is not matched with assumed num "+fmt.Sprintf("%d != %d", collectNumMaybe, resultsLen1))

	if indexKind == index_constants.INDEX_KIND_SKIP_LIST {
		// check order (col1 when index of it is used)
		var prevVal1 *types.Value = nil
		for jj := 0; jj < resultsLen1; jj++ {
			curVal1 := results1[jj].GetValue(tableMetadata.Schema(), 0)
			if prevVal1 != nil {
				common.SH_Assert(curVal1.CompareGreaterThan(*prevVal1), "values should be "+fmt.Sprintf("%v > %v", curVal1.ToIFValue(), (*prevVal1).ToIFValue()))
			}
			prevVal1 = &curVal1
		}
		finalizeRandomNoSideEffectTxn(txn_)
	}
	// ---------------------------------------

	// col2 ----------------------------------
	txn_ = txnMgr.Begin(nil)

	//check record num (index of col2 is used)
	rangeScanPlan2 := createSpecifiedRangeScanPlanNode[T](c, tableMetadata, keyType, 1, nil, nil, indexKind)
	results2 := executePlan(c, shi.GetBufferPoolManager(), txn_, rangeScanPlan2)
	resultsLen2 := len(results2)
	common.SH_Assert(collectNumMaybe == int32(resultsLen2), "records count is not matched with assumed num "+fmt.Sprintf("%d != %d", collectNumMaybe, resultsLen2))

	if indexKind == index_constants.INDEX_KIND_SKIP_LIST {
		// check order (col2 when index of it is used)
		var prevVal2 *types.Value = nil
		for jj := 0; jj < resultsLen2; jj++ {
			curVal2 := results2[jj].GetValue(tableMetadata.Schema(), 1)
			if prevVal2 != nil {
				common.SH_Assert(curVal2.CompareGreaterThan(*prevVal2), "values should be "+fmt.Sprintf("%v > %v", curVal2.ToIFValue(), (*prevVal2).ToIFValue()))
			}
			prevVal2 = &curVal2
		}
		finalizeRandomNoSideEffectTxn(txn_)
	}
	// --------------------------------------

	// check txn finished state and print these statistics
	common.SH_Assert(commitedTxnCnt+abortedTxnCnt == executedTxnCnt, "txn counting has bug!")
	fmt.Printf("commited: %d aborted: %d all: %d\n", commitedTxnCnt, abortedTxnCnt, executedTxnCnt)

	shi.CloseFilesForTesting()
}

func testSkipListParallelTxnStrideRoot[T int32 | float32 | string](t *testing.T, keyType types.TypeID) {
	//bpoolSize := int32(500)
	//bpoolSize := int32(50)
	bpoolSize := int32(100)
	//bpoolSize := int32(200)
	//bpoolSize := int32(30)

	switch keyType {
	case types.Integer:
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 50, 13, 0, bpoolSize, index_constants.INDEX_KIND_INVAID, SERIAL_EXEC)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 500, 13, 0, bpoolSize, index_constants.INDEX_KIND_INVAID, PARALLEL_EXEC)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 500, 18, 0, bpoolSize, index_constants.INDEX_KIND_INVAID, PARALLEL_EXEC)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 500, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 2000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 4000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 4000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 8000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
		testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 32000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 16000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 4000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, SERIAL_EXEC, 20)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 800, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC)

		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 2000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, SERIAL_EXEC)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 2000, 13, 0, bpoolSize, index_constants.INDEX_KIND_INVAID, SERIAL_EXEC)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 500, 13, 0, bpoolSize, index_constants.INDEX_KIND_INVAID, SERIAL_EXEC)

		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 500, 19, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 500, 100, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, SERIAL_EXEC)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 500, 100, 13, 100, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 500, 1000, 13, 100, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST)
	case types.Varchar:
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 32000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 2000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 8000, 13, 0, bpoolSize, index_constants.INDEX_KIND_SKIP_LIST, PARALLEL_EXEC, 20)
		//testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 500, 13, 0, bpoolSize, index_constants.INDEX_KIND_INVAID, PARALLEL_EXEC, 20)
		testParallelTxnsQueryingSkipListIndexUsedColumns[T](t, keyType, 400, 1500, 13, 0, bpoolSize, index_constants.INDEX_KIND_INVAID, PARALLEL_EXEC, 20)
	default:
		panic("not implemented!")
	}

}

//func TestSkipListPrallelTxnStrideInteger(t *testing.T) {
//	//t.Parallel()
//	//if testing.Short() {
//	//	t.Skip("skip this in short mode.")
//	//}
//	testSkipListParallelTxnStrideRoot[int32](t, types.Integer)
//}

func TestSkipListPrallelTxnStrideVarchar(t *testing.T) {
	//t.Parallel()
	//if testing.Short() {
	//	t.Skip("skip this in short mode.")
	//}
	testSkipListParallelTxnStrideRoot[string](t, types.Varchar)
}

//func TestSkipListPrallelTxnStrideVarchar(t *testing.T) {
//	//t.Parallel()
//	//if testing.Short() {
//	//	t.Skip("skip this in short mode.")
//	//}
//	testSkipListParallelTxnStrideRoot[string](t, types.Varchar)
//}
