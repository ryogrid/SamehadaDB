package optimizer

import (
	"fmt"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/executors"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/recovery"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/disk"
	"github.com/ryogrid/SamehadaDB/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	testingpkg "github.com/ryogrid/SamehadaDB/testing/testing_assert"
	"github.com/ryogrid/SamehadaDB/types"
	"strconv"
	"testing"
)

type ColumnMeta struct {
	Name       string
	ColumnType types.TypeID
	IdxKind    index_constants.IndexKind
}

type ColValGenFunc func(idx int) interface{}

type SetupTableMeta struct {
	TableName      string
	EntriesNum     int64
	Columns        []*ColumnMeta
	ColValGenFuncs []ColValGenFunc
}

func SetupTableWithMetadata(exec_ctx *executors.ExecutorContext, tableMeta *SetupTableMeta) *catalog.TableMetadata {
	c := exec_ctx.GetCatalog()
	txn := exec_ctx.GetTransaction()

	cols := make([]*column.Column, 0)
	for _, colMeta := range tableMeta.Columns {
		if colMeta.IdxKind != index_constants.INDEX_KIND_INVALID {
			col := column.NewColumn(colMeta.Name, colMeta.ColumnType, true, colMeta.IdxKind, types.PageID(-1), nil)
			cols = append(cols, col)
		} else {
			col := column.NewColumn(colMeta.Name, colMeta.ColumnType, false, colMeta.IdxKind, types.PageID(-1), nil)
			cols = append(cols, col)
		}
	}
	schema_ := schema.NewSchema(cols)
	tm := c.CreateTable(tableMeta.TableName, schema_, txn)

	for ii := 0; ii < int(tableMeta.EntriesNum); ii++ {
		vals := make([]types.Value, 0)
		for _, genFunc := range tableMeta.ColValGenFuncs {
			vals = append(vals, types.NewValue(genFunc(ii)))
		}
		tuple_ := tuple.NewTupleFromSchema(vals, schema_)
		rid, _ := tm.Table().InsertTuple(tuple_, false, txn, tm.OID())
		for jj, colMeta := range tableMeta.Columns {
			if colMeta.IdxKind != index_constants.INDEX_KIND_INVALID {
				tm.GetIndex(jj).InsertEntry(tuple_, *rid, txn)
			}
		}
	}

	return tm
}

func setupTablesAndStatisticsDataForTesting(exec_ctx *executors.ExecutorContext) (*catalog.TableMetadata, *catalog.TableMetadata, *catalog.TableMetadata, *catalog.TableMetadata) {
	Sc1Meta := &SetupTableMeta{
		"Sc1",
		100,
		[]*ColumnMeta{
			{"c1", types.Integer, index_constants.INDEX_KIND_INVALID},
			{"c2", types.Varchar, index_constants.INDEX_KIND_SKIP_LIST},
			{"c3", types.Float, index_constants.INDEX_KIND_INVALID},
		},
		[]ColValGenFunc{
			func(idx int) interface{} { return int32(idx) },
			func(idx int) interface{} { return "c2-" + strconv.Itoa(idx) },
			func(idx int) interface{} { return float32(idx) + 9.9 },
		},
	}
	tm1 := SetupTableWithMetadata(exec_ctx, Sc1Meta)

	Sc2Meta := &SetupTableMeta{
		"Sc2",
		200,
		[]*ColumnMeta{
			{"d1", types.Integer, index_constants.INDEX_KIND_INVALID},
			{"d2", types.Float, index_constants.INDEX_KIND_INVALID},
			{"d3", types.Varchar, index_constants.INDEX_KIND_SKIP_LIST},
			{"d4", types.Integer, index_constants.INDEX_KIND_INVALID},
		},
		[]ColValGenFunc{
			func(idx int) interface{} { return int32(idx) },
			func(idx int) interface{} { return float32(idx) + 0.2 },
			func(idx int) interface{} { return "d3-" + strconv.Itoa(idx%10) },
			func(idx int) interface{} { return int32(16) },
		},
	}
	tm2 := SetupTableWithMetadata(exec_ctx, Sc2Meta)

	Sc3Meta := &SetupTableMeta{
		"Sc3",
		20,
		[]*ColumnMeta{
			{"e1", types.Integer, index_constants.INDEX_KIND_INVALID},
			{"e2", types.Float, index_constants.INDEX_KIND_INVALID},
		},
		[]ColValGenFunc{
			func(idx int) interface{} { return int32(idx + 1) },
			func(idx int) interface{} { return float32(idx+1) + 53.4 },
		},
	}
	tm3 := SetupTableWithMetadata(exec_ctx, Sc3Meta)

	Sc4Meta := &SetupTableMeta{
		"Sc4",
		100,
		[]*ColumnMeta{
			{"c1", types.Integer, index_constants.INDEX_KIND_INVALID},
			{"c2", types.Varchar, index_constants.INDEX_KIND_SKIP_LIST},
		},
		[]ColValGenFunc{
			func(idx int) interface{} { return int32(idx + 1) },
			func(idx int) interface{} { return strconv.Itoa((idx + 1) % 4) },
		},
	}
	tm4 := SetupTableWithMetadata(exec_ctx, Sc4Meta)

	txn := exec_ctx.GetTransaction()

	stat1 := tm1.GetStatistics()
	stat1.Update(tm1, txn)

	stat2 := tm2.GetStatistics()
	stat2.Update(tm2, txn)

	stat3 := tm3.GetStatistics()
	stat3.Update(tm3, txn)

	stat4 := tm4.GetStatistics()
	stat4.Update(tm4, txn)

	return tm1, tm2, tm3, tm4
}

func TestSetupedTableAndStatistcsContents(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	log_mgr.ActivateLogging()
	testingpkg.Assert(t, log_mgr.IsEnabledLogging(), "")
	fmt.Println("System logging is active.")
	bpm := buffer.NewBufferPoolManager(common.BufferPoolMaxFrameNumForTest, diskManager, log_mgr) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))
	lock_mgr := access.NewLockManager(access.REGULAR, access.DETECTION)
	txn_mgr := access.NewTransactionManager(lock_mgr, log_mgr)

	txn := txn_mgr.Begin(nil)
	c := catalog.BootstrapCatalog(bpm, log_mgr, lock_mgr, txn)
	exec_ctx := executors.NewExecutorContext(c, bpm, txn)

	tm1, tm2, tm3, tm4 := setupTablesAndStatisticsDataForTesting(exec_ctx)
	txn_mgr.Commit(c, txn)

	txn = txn_mgr.Begin(nil)

	// Sc1
	it := tm1.Table().Iterator(txn)
	rows := 0
	schema_ := tm1.Schema()
	for tuple_ := it.Current(); !it.End(); tuple_ = it.Next() {
		colVal1 := tuple_.GetValue(schema_, uint32(0))
		testingpkg.Assert(t, colVal1.ToInteger() == int32(rows), "colVal1.ToInteger() != int32(rows)")
		colVal2 := tuple_.GetValue(schema_, uint32(1))
		testingpkg.Assert(t, colVal2.ToVarchar() == "c2-"+strconv.Itoa(rows), "colVal2.ToVarchar() != 'c2-' + strconv.Itoa(rows)")
		colVal3 := tuple_.GetValue(schema_, uint32(2))
		testingpkg.Assert(t, colVal3.ToFloat() == float32(rows)+9.9, "colVal3.ToFloat() != float32(rows) + 9.9")
		rows++
	}
	testingpkg.Assert(t, rows == 100, "rows != 100")

	idx1 := tm1.GetIndex(1)
	idxIt1 := idx1.GetRangeScanIterator(nil, nil, txn)
	rows = 0
	for done, _, _, _ := idxIt1.Next(); !done; done, _, _, _ = idxIt1.Next() {
		rows++
	}
	testingpkg.Assert(t, rows == 100, "rows != 100")

	stat1 := tm1.GetStatistics()
	testingpkg.Assert(t, stat1.Rows() == 100, "stat1.Rows() != 100")
	testingpkg.Assert(t, stat1.ColumnNum() == 3, "stat1.ColumnNum() != 3")
	testingpkg.Assert(t, stat1.EstimateCount(0, types.NewInteger(0).SetInfMin(), types.NewInteger(0).SetInfMax()) == 99, "EstimateCount should be 99.")
	testingpkg.Assert(t, stat1.EstimateCount(1, types.NewVarchar("").SetInfMin(), types.NewVarchar("").SetInfMax()) == 2, "EstimateCount should be 2.")
	testingpkg.Assert(t, stat1.EstimateCount(2, types.NewFloat(0).SetInfMin(), types.NewFloat(0).SetInfMax()) == 99, "EstimateCount should be 99.")

	// Sc1 table only check ReductionFactor
	predStr := "Sc1.c1 = 1"
	testingpkg.Assert(t, stat1.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) == 100, "stat1.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) != 100")
	predStr = "'a' = Sc1.c2"
	testingpkg.Assert(t, stat1.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) == 100, "stat1.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) != 100")
	predStr = "Sc1.c3 = 1.1"
	testingpkg.Assert(t, stat1.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) == 100, "stat1.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) != 100")
	predStr = "Sc1.c1 = 1 and Sc1.c2 = 'a' and Sc1.c3 = 1.1"
	testingpkg.Assert(t, stat1.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) == 100*100*100, "stat1.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) != 100*100*100")

	// test TableStatistics::GetDeepCopy method here
	stat1_2 := stat1.GetDeepCopy()
	testingpkg.Assert(t, stat1_2.Rows() == 100, "stat1_2.Rows() != 100")
	testingpkg.Assert(t, stat1_2.ColumnNum() == 3, "stat1_2.ColumnNum() != 3")
	testingpkg.Assert(t, stat1_2.EstimateCount(0, types.NewInteger(0).SetInfMin(), types.NewInteger(0).SetInfMax()) == 99, "EstimateCount should be 99.")
	testingpkg.Assert(t, stat1_2.EstimateCount(1, types.NewVarchar("").SetInfMin(), types.NewVarchar("").SetInfMax()) == 2, "EstimateCount should be 2.")
	testingpkg.Assert(t, stat1_2.EstimateCount(2, types.NewFloat(0).SetInfMin(), types.NewFloat(0).SetInfMax()) == 99, "EstimateCount should be 99.")
	predStr = "Sc1.c1 = 1"
	testingpkg.Assert(t, stat1_2.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) == 100, "stat1_2.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) != 100")
	predStr = "'a' = Sc1.c2"
	testingpkg.Assert(t, stat1_2.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) == 100, "stat1_2.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) != 100")
	predStr = "Sc1.c3 = 1.1"
	testingpkg.Assert(t, stat1_2.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) == 100, "stat1_2.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) != 100")
	predStr = "Sc1.c1 = 1 and Sc1.c2 = 'a' and Sc1.c3 = 1.1"
	testingpkg.Assert(t, stat1_2.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) == 100*100*100, "stat1_2.ReductionFactor(schema_, parser.GetPredicateExprFromStr(schema_, &predStr)) != 100*100*100")

	// Sc2
	it = tm2.Table().Iterator(txn)
	rows = 0
	schema_ = tm2.Schema()
	for tuple_ := it.Current(); !it.End(); tuple_ = it.Next() {
		colVal1 := tuple_.GetValue(schema_, uint32(0))
		testingpkg.Assert(t, colVal1.ToInteger() == int32(rows), "colVal1.ToInteger() != int32(rows)")
		colVal2 := tuple_.GetValue(schema_, uint32(1))
		testingpkg.Assert(t, colVal2.ToFloat() == float32(rows)+0.2, "colVal2.ToFloat() != float32(rows) + 0.2")
		colVal3 := tuple_.GetValue(schema_, uint32(2))
		testingpkg.Assert(t, colVal3.ToVarchar() == "d3-"+strconv.Itoa(rows%10), "colVal3.ToVarchar() != 'd3-' + strconv.Itoa(rows%10)")
		colVal4 := tuple_.GetValue(schema_, uint32(3))
		testingpkg.Assert(t, colVal4.ToInteger() == int32(16), "colVal4.ToInteger() != int32(16)")
		rows++
	}
	testingpkg.Assert(t, rows == 200, "rows != 200")

	idx2 := tm2.GetIndex(2)
	idxIt2 := idx2.GetRangeScanIterator(nil, nil, txn)
	rows = 0
	for done, _, _, _ := idxIt2.Next(); !done; done, _, _, _ = idxIt2.Next() {
		rows++
	}
	testingpkg.Assert(t, rows == 200, "rows != 200")

	stat2 := tm2.GetStatistics()
	testingpkg.Assert(t, stat2.Rows() == 200, "stat2.Rows() != 200")
	testingpkg.Assert(t, stat2.ColumnNum() == 4, "stat2.ColumnNum() != 4")
	testingpkg.Assert(t, stat2.EstimateCount(0, types.NewInteger(0).SetInfMin(), types.NewInteger(0).SetInfMax()) == 199, "EstimateCount should be 199.")
	testingpkg.Assert(t, stat2.EstimateCount(1, types.NewFloat(0).SetInfMin(), types.NewFloat(0).SetInfMax()) == 199, "EstimateCount should be 199.")
	testingpkg.Assert(t, stat2.EstimateCount(2, types.NewVarchar("").SetInfMin(), types.NewVarchar("").SetInfMax()) == 2, "EstimateCount should be 2.")
	testingpkg.Assert(t, stat2.EstimateCount(3, types.NewInteger(0).SetInfMin(), types.NewInteger(0).SetInfMax()) == 0, "EstimateCount should be 0.")

	// Sc3
	it = tm3.Table().Iterator(txn)
	rows = 0
	schema_ = tm3.Schema()
	for tuple_ := it.Current(); !it.End(); tuple_ = it.Next() {
		colVal1 := tuple_.GetValue(schema_, uint32(0))
		testingpkg.Assert(t, colVal1.ToInteger() == int32(rows+1), "colVal1.ToInteger() != int32(rows) + 1")
		colVal2 := tuple_.GetValue(schema_, uint32(1))
		testingpkg.Assert(t, colVal2.ToFloat() == float32(rows+1)+53.4, "colVal2.ToFloat() != float32(rows) + 53.4")
		rows++
	}
	testingpkg.Assert(t, rows == 20, "rows != 20")

	stat3 := tm3.GetStatistics()
	testingpkg.Assert(t, stat3.Rows() == 20, "stat3.Rows() != 20")
	testingpkg.Assert(t, stat3.ColumnNum() == 2, "stat3.ColumnNum() != 2")
	testingpkg.Assert(t, stat3.EstimateCount(0, types.NewInteger(0).SetInfMin(), types.NewInteger(0).SetInfMax()) == 19, "EstimateCount should be 19.")
	testingpkg.Assert(t, stat3.EstimateCount(1, types.NewFloat(0).SetInfMin(), types.NewFloat(0).SetInfMax()) == 19, "EstimateCount should be 19.")

	// Sc4
	it = tm4.Table().Iterator(txn)
	rows = 0
	schema_ = tm4.Schema()
	for tuple_ := it.Current(); !it.End(); tuple_ = it.Next() {
		colVal1 := tuple_.GetValue(schema_, uint32(0))
		testingpkg.Assert(t, colVal1.ToInteger() == int32(rows+1), "colVal1.ToInteger() != int32(rows) + 1")
		colVal2 := tuple_.GetValue(schema_, uint32(1))
		testingpkg.Assert(t, colVal2.ToVarchar() == strconv.Itoa((rows+1)%4), "colVal2.ToVarchar() != strconv.Itoa((rows + 1) % 4)")
		rows++
	}
	testingpkg.Assert(t, rows == 100, "rows != 100")

	idx4 := tm4.GetIndex(1)
	idxIt4 := idx4.GetRangeScanIterator(nil, nil, txn)
	rows = 0
	for done, _, _, _ := idxIt4.Next(); !done; done, _, _, _ = idxIt4.Next() {
		rows++
	}
	testingpkg.Assert(t, rows == 100, "rows != 100")

	stat4 := tm4.GetStatistics()
	testingpkg.Assert(t, stat4.Rows() == 100, "stat4.Rows() != 100")
	testingpkg.Assert(t, stat4.ColumnNum() == 2, "stat3.ColumnNum() != 2")
	testingpkg.Assert(t, stat4.EstimateCount(0, types.NewInteger(0).SetInfMin(), types.NewInteger(0).SetInfMax()) == 99, "EstimateCount should be 99.")
	testingpkg.Assert(t, stat4.EstimateCount(1, types.NewVarchar("").SetInfMin(), types.NewVarchar("").SetInfMax()) == 2, "EstimateCount should be 2.")
}

func TestFindBestScans(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	log_mgr.ActivateLogging()
	testingpkg.Assert(t, log_mgr.IsEnabledLogging(), "")
	fmt.Println("System logging is active.")
	bpm := buffer.NewBufferPoolManager(common.BufferPoolMaxFrameNumForTest, diskManager, log_mgr) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))
	lock_mgr := access.NewLockManager(access.REGULAR, access.DETECTION)
	txn_mgr := access.NewTransactionManager(lock_mgr, log_mgr)

	txn := txn_mgr.Begin(nil)
	c := catalog.BootstrapCatalog(bpm, log_mgr, lock_mgr, txn)
	exec_ctx := executors.NewExecutorContext(c, bpm, txn)

	setupTablesAndStatisticsDataForTesting(exec_ctx)
	txn_mgr.Commit(c, txn)

	var queryStr string
	var queryInfo *parser.QueryInfo
	var optimalPlans map[mapset.Set[string]]CostAndPlan

	//queryStr = "select Sc1.c1 from Sc1 where Sc1.c1 = 2;" // Simple(SequentialScan)
	//queryInfo = parser.ProcessSQLStr(&queryStr)
	//optimalPlans = NewSelingerOptimizer(queryInfo, c).findBestScans()
	//testingpkg.Assert(t, len(optimalPlans) == len(queryInfo.JoinTables_), "len(optimalPlans) != len(query.JoinTables_) (1)")

	//queryStr = "select Sc1.c1, Sc1.c3 from Sc1 where Sc1.c2 = 'c2-32';" // IndexScan
	//queryInfo = parser.ProcessSQLStr(&queryStr)
	//optimalPlans = NewSelingerOptimizer(queryInfo, c).findBestScans()
	//testingpkg.Assert(t, len(optimalPlans) == len(queryInfo.JoinTables_), "len(optimalPlans) != len(query.JoinTables_) (2)")

	//queryStr = "select Sc2.d1, Sc2.d2, Sc2.d3, Sc2.d4 from Sc2 where Sc2.d3 >= 'd3-3' and Sc2.d3 <= 'd3-5';" // IndexScanInclude(1)
	//queryInfo = parser.ProcessSQLStr(&queryStr)
	//optimalPlans = NewSelingerOptimizer(queryInfo, c).findBestScans()
	//testingpkg.Assert(t, len(optimalPlans) == len(queryInfo.JoinTables_), "len(optimalPlans) != len(query.JoinTables_) (3)")

	//queryStr = "select Sc2.d1, Sc2.d2, Sc2.d3, Sc2.d4 from Sc2 where Sc2.d3 >= 'd3-3' and Sc2.d3 < 'd3-5';" // IndexScanInclude(2)
	//queryInfo = parser.ProcessSQLStr(&queryStr)
	//optimalPlans = NewSelingerOptimizer(queryInfo, c).findBestScans()
	//testingpkg.Assert(t, len(optimalPlans) == len(queryInfo.JoinTables_), "len(optimalPlans) != len(query.JoinTables_) (4)")

	queryStr = "select Sc1.c2, Sc2.d1, Sc2.d3 from Sc1, Sc2 where Sc1.c1 = Sc2.d1;" // Join(HashJoin)
	queryInfo = parser.ProcessSQLStr(&queryStr)
	optimalPlans = NewSelingerOptimizer(queryInfo, c).findBestScans()
	testingpkg.Assert(t, len(optimalPlans) == len(queryInfo.JoinTables_), "len(optimalPlans) != len(query.JoinTables_) (5)")
	PrintOptimalPlans("Join(HashJoin)", optimalPlans)

	queryStr = "select Sc1.c2, Sc4.c1, Sc4.c2 from Sc1, Sc2 where Sc1.c1 = Sc4.c2 and Sc4.c2 = '1';" // Join(IndexJoin)
	queryInfo = parser.ProcessSQLStr(&queryStr)
	optimalPlans = NewSelingerOptimizer(queryInfo, c).findBestScans()
	testingpkg.Assert(t, len(optimalPlans) == len(queryInfo.JoinTables_), "len(optimalPlans) != len(query.JoinTables_) (6)")
	PrintOptimalPlans("Join(IndexJoin)", optimalPlans)

	//queryStr := "select Sc1.c2, Sc2.d1, Sc3.e2, Sc3.c1 from Sc1, Sc2, Sc3 where Sc1.c1 = Sc2.d1 and Sc2.d1 = Sc3.e1;" // ThreeJoin(HashJoin)
	//queryStr := "select Sc1.c1, Sc1.c2, Sc2.d1, Sc2.d2, Sc2.d3 from Sc1, Sc2 where Sc1.c1 = 2;" // JonWhere(NestedLoopJoin)
	//queryStr := "select Sc1.c1, Sc1.c2, Sc1.c3, Sc4.c1, Sc4.c2 from Sc1, Sc4 where Sc1.c1 = Sc4.c1 and Sc4.c1 = 2;" // SameNameColumn
	//queryStr := "select * from Sc1, Sc4 where Sc1.c1 = Sc4.c1 and Sc4.c1 = 2;" // Asterisk
}

func PrintOptimalPlans(title string, optimalPlans map[mapset.Set[string]]CostAndPlan) {
	fmt.Println("")
	fmt.Println("Pattern: " + title)
	fmt.Println("==================================================")
	isFirst := true
	for _, costPlan := range optimalPlans {
		if isFirst {
			isFirst = false
		} else {
			fmt.Println("--------------------------------------------------")
		}
		plan_ := costPlan.plan
		fmt.Println("Cost: " + strconv.Itoa(int(costPlan.cost)))
		plans.PrintPlanTree(plan_, 0)

	}
	fmt.Println("==================================================")
}

/*
func TestSimplePlanOptimization(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	log_mgr.ActivateLogging()
	testingpkg.Assert(t, log_mgr.IsEnabledLogging(), "")
	fmt.Println("System logging is active.")
	bpm := buffer.NewBufferPoolManager(common.BufferPoolMaxFrameNumForTest, diskManager, log_mgr) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))
	lock_mgr := access.NewLockManager(access.REGULAR, access.DETECTION)
	txn_mgr := access.NewTransactionManager(lock_mgr, log_mgr)

	txn := txn_mgr.Begin(nil)
	c := catalog.BootstrapCatalog(bpm, log_mgr, lock_mgr, txn)
	exec_ctx := executors.NewExecutorContext(c, bpm, txn)

	setupTablesAndStatisticsDataForTesting(exec_ctx)
	txn_mgr.Commit(c, txn)

	queryStr := "TO BE WRITTEN"
	queryInfo := parser.ProcessSQLStr(&queryStr)

	optimizer := NewSelingerOptimizer(queryInfo, c)
	solution, err := optimizer.Optimize()
	if err != nil {
		fmt.Println(err)
	}
	testingpkg.Assert(t, err == nil, "err != nil")
	fmt.Println(solution)
}
*/
