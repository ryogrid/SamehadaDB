package index_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/execution/executors"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/recovery/log_recovery"
	"github.com/ryogrid/SamehadaDB/lib/samehada"
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	testingpkg "github.com/ryogrid/SamehadaDB/lib/testing/testing_assert"
	"github.com/ryogrid/SamehadaDB/lib/testing/testing_pattern_fw"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"os"
	"testing"

	"github.com/ryogrid/SamehadaDB/lib/storage/page"
)

func TestPackAndUnpackRID32(t *testing.T) {
	rid := new(page.RID)
	rid.PageId = 55
	rid.SlotNum = 1027

	packed_val := samehada_util.PackRIDtoUint32(rid)
	unpacked_val := samehada_util.UnpackUint32toRID(packed_val)

	testingpkg.Assert(t, unpacked_val.PageId == 55, "")
	testingpkg.Assert(t, unpacked_val.SlotNum == 1027, "")
}

func TestPackAndUnpackRID64(t *testing.T) {
	rid := new(page.RID)
	rid.PageId = 55
	rid.SlotNum = 1027

	packed_val := samehada_util.PackRIDtoUint64(rid)
	unpacked_val := samehada_util.UnpackUint64toRID(packed_val)

	testingpkg.Assert(t, unpacked_val.PageId == 55, "")
	testingpkg.Assert(t, unpacked_val.SlotNum == 1027, "")
}

func TestRecounstructionOfHashIndex(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true

	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
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
			testing_pattern_fw.ExecuteIndexPointScanTestCase(t, test, index_constants.INDEX_KIND_HASH)
		})
	}

	shi.GetTransactionManager().Commit(nil, txn)
	shi.Shutdown(samehada.ShutdownPatternCloseFiles)

	// ----------- check recovery includes index data ----------

	// recovery catalog data and tuple datas
	shi = samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi.GetLogManager().DeactivateLogging()
	txn = shi.GetTransactionManager().Begin(nil)
	txn.SetIsRecoveryPhase(true)

	log_recovery := log_recovery.NewLogRecovery(
		shi.GetDiskManager(),
		shi.GetBufferPoolManager(),
		shi.GetLogManager())
	greatestLSN, _, _ := log_recovery.Redo(txn)
	log_recovery.Undo(txn)

	dman := shi.GetDiskManager()
	dman.GCLogFile()
	shi.GetLogManager().SetNextLSN(greatestLSN + 1)
	c = catalog.RecoveryCatalogFromCatalogPage(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn, true)

	// reconstruct all index data of all column
	tableMetadata = c.GetTableByName("test_1")
	samehada.ReconstructAllIndexData(c, dman, txn)
	shi.GetTransactionManager().Commit(nil, txn)

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
			testing_pattern_fw.ExecuteIndexPointScanTestCase(t, test, index_constants.INDEX_KIND_HASH)
		})
	}

	shi.GetTransactionManager().Commit(nil, txn)

	common.TempSuppressOnMemStorage = false
	shi.Shutdown(samehada.ShutdownPatternCloseFiles)
	common.TempSuppressOnMemStorageMutex.Unlock()
}
