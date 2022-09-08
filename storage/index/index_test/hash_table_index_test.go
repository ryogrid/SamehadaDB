package index_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/executors"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/recovery/log_recovery"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
	"os"
	"testing"

	"github.com/ryogrid/SamehadaDB/storage/page"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
)

func TestPackAndUnpackRID(t *testing.T) {
	rid := new(page.RID)
	rid.PageId = 55
	rid.SlotNum = 1027

	packed_val := samehada_util.PackRIDtoUint32(rid)
	fmt.Println(packed_val)
	unpacked_val := samehada_util.UnpackUint32toRID(packed_val)

	testingpkg.Assert(t, unpacked_val.PageId == 55, "")
	testingpkg.Assert(t, unpacked_val.SlotNum == 1027, "")
}

func TestRecounstructionOfHashIndex(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.log")

	shi := samehada.NewSamehadaInstanceForTesting()
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, common.EnableLogging, "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	bpm := shi.GetBufferPoolManager()
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(bpm, shi.GetLogManager(), shi.GetLockManager(), txn)
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

	//bpm.FlushAllPages()
	txn_mgr.Commit(txn)

	txn = shi.GetTransactionManager().Begin(nil)

	cases := []executors.HashIndexScanTestCase{{
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
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			executors.ExecuteHashIndexScanTestCase(t, test)
		})
	}

	shi.GetTransactionManager().Commit(txn)
	shi.Shutdown(false)

	// ----------- check recovery includes index data ----------

	// recovery catalog data and tuple datas
	shi = samehada.NewSamehadaInstanceForTesting()
	shi.GetLogManager().DeactivateLogging()
	txn = shi.GetTransactionManager().Begin(nil)

	log_recovery := log_recovery.NewLogRecovery(
		shi.GetDiskManager(),
		shi.GetBufferPoolManager())
	greatestLSN, _ := log_recovery.Redo()
	log_recovery.Undo()

	dman := shi.GetDiskManager()
	dman.GCLogFile()
	shi.GetLogManager().SetNextLSN(greatestLSN + 1)
	c = catalog.RecoveryCatalogFromCatalogPage(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)

	// reconstruct all index data of all column
	tableMetadata = c.GetTableByName("test_1")
	samehada.ReconstructAllIndexData(c, dman, txn)
	shi.GetTransactionManager().Commit(txn)

	// checking reconstruction result of index data by getting tuples using index
	txn = shi.GetTransactionManager().Begin(nil)

	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, common.EnableLogging, "")
	fmt.Println("System logging is active.")

	executionEngine = &executors.ExecutionEngine{}
	executorContext = executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	cases = []executors.HashIndexScanTestCase{{
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
	}}

	for _, test := range cases {
		t.Run(test.Description, func(t *testing.T) {
			executors.ExecuteHashIndexScanTestCase(t, test)
		})
	}

	shi.GetTransactionManager().Commit(txn)
	shi.Shutdown(false)
}
