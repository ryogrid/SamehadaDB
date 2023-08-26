package optimizer

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/executors"
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
		for jj, genFunc := range tableMeta.ColValGenFuncs {
			vals = append(vals, types.NewValue(genFunc(jj)))
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

func setupTablesAndStatisticsDataForTesting(exec_ctx *executors.ExecutorContext) {
	/*	c := exec_ctx.GetCatalog()
		txn := exec_ctx.GetTransaction()
		//bpm := exec_ctx.GetBufferPoolManager()

		colC1 := column.NewColumn("c1", types.Integer, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
		colC2 := column.NewColumn("c2", types.Varchar, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
		colC3 := column.NewColumn("c3", types.Float, true, index_constants.INDEX_KIND_SKIP_LIST, types.PageID(-1), nil)
		schemaSc1 := schema.NewSchema([]*column.Column{colC1, colC2, colC3})
		tmSc1 := c.CreateTable("Sc1", schemaSc1, txn)

		idxC1 := tmSc1.GetIndex(0)
		idxC2 := tmSc1.GetIndex(1)
		idxC3 := tmSc1.GetIndex(2)
		for ii := 0; ii < 100; ii++ {
			valCol1 := types.NewInteger(int32(ii))
			valCol2 := types.NewVarchar("c2-" + strconv.Itoa(ii))
			valCol3 := types.NewFloat(float32(ii) + 9.9)
			tuple_ := tuple.NewTupleFromSchema([]types.Value{valCol1, valCol2, valCol3}, schemaSc1)
			rid, _ := tmSc1.Table().InsertTuple(tuple_, false, txn, tmSc1.OID())

			idxC1.InsertEntry(tuple_, *rid, txn)
			idxC2.InsertEntry(tuple_, *rid, txn)
			idxC3.InsertEntry(tuple_, *rid, txn)
		}*/

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
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)

	txn := txn_mgr.Begin(nil)
	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)
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

func TestFindBestScans(t *testing.T) {
	diskManager := disk.NewDiskManagerTest()
	defer diskManager.ShutDown()
	log_mgr := recovery.NewLogManager(&diskManager)
	log_mgr.ActivateLogging()
	testingpkg.Assert(t, log_mgr.IsEnabledLogging(), "")
	fmt.Println("System logging is active.")
	bpm := buffer.NewBufferPoolManager(common.BufferPoolMaxFrameNumForTest, diskManager, log_mgr) //, recovery.NewLogManager(diskManager), access.NewLockManager(access.REGULAR, access.PREVENTION))
	txn_mgr := access.NewTransactionManager(access.NewLockManager(access.REGULAR, access.DETECTION), log_mgr)

	txn := txn_mgr.Begin(nil)
	c := catalog.BootstrapCatalog(bpm, log_mgr, access.NewLockManager(access.REGULAR, access.PREVENTION), txn)
	exec_ctx := executors.NewExecutorContext(c, bpm, txn)

	setupTablesAndStatisticsDataForTesting(exec_ctx)
	txn_mgr.Commit(c, txn)

	queryStr := "select Sc1.c1 from Sc1 where Sc1.c1 = 2;" // Simple(SequentialScan)
	//queryStr := "select Sc1.c1, Sc1.c3 from Sc1 where Sc1.c2 = 'c2-32';" // IndexScan
	//queryStr := "select Sc2.d1, Sc2.d2, Sc2.d3, Sc2.d4 from Sc2 where Sc2.d3 >= 'd3-3' and Sc2.d3 <= 'd3-5';" // IndexScanInclude
	//queryStr := "select Sc1.c2, Sc2.d1, Sc2.d3 from Sc1, Sc2 where Sc1.c1 = Sc2.d1;" // Join(HashJoin)
	//queryStr := "select Sc1.c2, Sc4.c1, Sc4.c2 from Sc1, Sc2 where Sc1.c1 = Sc4.c2 and Sc4.c2 = '1';" // Join(IndexJoin)
	//queryStr := "select Sc1.c2, Sc2.d1, Sc3.e2, Sc3.c1 from Sc1, Sc2, Sc3 where Sc1.c1 = Sc2.d1 and Sc2.d1 = Sc3.e1;" // ThreeJoin(HashJoin)
	//queryStr := "select Sc1.c1, Sc1.c2, Sc2.d1, Sc2.d2, Sc2.d3 from Sc1, Sc2 where Sc1.c1 = 2;" // JonWhere(NestedLoopJoin)
	//queryStr := "select Sc1.c1, Sc1.c2, Sc1.c3, Sc4.c1, Sc4.c2 from Sc1, Sc4 where Sc1.c1 = Sc4.c1 and Sc4.c1 = 2;" // SameNameColumn
	//queryStr := "select * from Sc1, Sc4 where Sc1.c1 = Sc4.c1 and Sc4.c1 = 2;" // Asterisk
	queryInfo := parser.ProcessSQLStr(&queryStr)

	optimalPlans := NewSelingerOptimizer(queryInfo, c).findBestScans()
	testingpkg.Assert(t, len(optimalPlans) == len(queryInfo.JoinTables_), "len(optimalPlans) != len(query.JoinTables_)")
}
