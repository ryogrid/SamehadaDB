package optimizer

import (
	"fmt"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/executors"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"

	//"strings"
	"testing"
)

func TestSimplePlanOptimization(t *testing.T) {
	// TODO: (SDB) not implemented yet
}

func containsAny(map1 mapset.Set[string], map2 mapset.Set[string]) bool {
	interSet := map1.Intersect(map2)
	if interSet.Cardinality() > 0 {
		return true
	} else {
		return false
	}
}

func makeSet[T comparable](from []*T) mapset.Set[T] {
	joined := mapset.NewSet[T]()
	for _, f := range from {
		joined.Add(*f)
	}
	return joined
}

func testBestScanInner(t *testing.T, query *parser.QueryInfo, exec_ctx *executors.ExecutorContext, c *catalog.Catalog, txn *access.Transaction) {
	// TODO: (SDB) not implemented yet (testBestScanInner)

	optimalPlans := make(map[mapset.Set[string]]CostAndPlan)

	// 1. Initialize every single tables to start.
	touchedColumns := query.WhereExpression_.TouchedColumns()
	for _, item := range query.SelectFields_ {
		touchedColumns = touchedColumns.Union(item.TouchedColumns())
	}
	for _, from := range query.JoinTables_ {
		tbl := c.GetTableByName(*from)
		stats := c.GetTableByName(*from).GetStatistics()
		projectTarget := make([]*column.Column, 0)
		// Push down all selection & projection.
		for ii := 0; ii < int(tbl.GetColumnNum()); ii++ {
			for _, touchedCol := range touchedColumns.ToSlice() {
				tableCol := tbl.Schema().GetColumn(uint32(ii))
				// TODO: (SDB) GetColumnName() value should contain table name. if not,  rewrite of this comparison code is needed
				if tableCol.GetColumnName() == touchedCol.GetColumnName() {
					projectTarget = append(projectTarget, tableCol)
				}
			}
		}
		scan, _ := NewSelingerOptimizer().bestScan(query.SelectFields_, query.WhereExpression_, tbl, c, stats)
		optimalPlans[makeSet([]*string{from})] = CostAndPlan{scan.AccessRowCount(), scan}
	}
	samehada_util.SHAssert(len(optimalPlans) == len(query.JoinTables_), "len(optimalPlans) != len(query.JoinTables_)")
}

func testBestJoinInner(t *testing.T, query *parser.QueryInfo, exec_ctx *executors.ExecutorContext, c *catalog.Catalog, txn *access.Transaction) {
	// TODO: (SDB) need to setup of optimalPlans which is needed at BestJoin
	// TODO: (SDB) need to setup of statistics data of tables related to query
	optimalPlans := make(map[mapset.Set[string]]CostAndPlan)

	for ii := 0; ii < len(query.JoinTables_); ii += 1 {
		for baseTableFrom, baseTableCP := range optimalPlans {
			for joinTableFrom, joinTableCP := range optimalPlans {
				if containsAny(baseTableFrom, joinTableFrom) {
					continue
				}
				// TODO: (SDB) (len(baseTable) + len(joinTable) == ii + 1) should be checked?
				//             and (len(baseTable) == 1 or len(joinTable) == 1) should be checked?
				bestJoinPlan, _ := NewSelingerOptimizer().bestJoin(query.WhereExpression_, baseTableCP.plan, joinTableCP.plan)
				fmt.Println(bestJoinPlan)

				joinedTables := baseTableFrom.Union(joinTableFrom)
				common.SH_Assert(1 < joinedTables.Cardinality(), "joinedTables.Cardinality() is illegal!")
				cost := bestJoinPlan.AccessRowCount()

				// TODO: (SDB) update target should be changed to tempolal table?
				//             (its scope is same ii value loop and it is merged to optimalPlans at end of the ii loop)
				if existedPlan, ok := optimalPlans[joinedTables]; ok {
					optimalPlans[joinedTables] = CostAndPlan{cost, bestJoinPlan}
				} else if cost < existedPlan.cost {
					optimalPlans[joinedTables] = CostAndPlan{cost, bestJoinPlan}
				}
			}
		}
	}
	optimalPlan, ok := optimalPlans[makeSet(query.JoinTables_)]
	testingpkg.Assert(t, ok, "plan which includes all tables is not found")

	// Attach final projection and emit the result
	solution := optimalPlan.plan
	solution = plans.NewProjectionPlanNode(solution, samehada_util.ConvParsedSelectionExprToExpIFOne(query.SelectFields_))

	fmt.Println(solution)
}

func TestBestScan(t *testing.T) {
	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
	exec_ctx := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	// TODO: (SDB) need to setup tables for use in query
	// TODO: (SDB) need to setup of statistics data of created tables and query which uses the tables for testing BestJoin func
	//table_info, _ := executors.GenerateTestTabls(c, exec_ctx, txn)
	query := new(parser.QueryInfo)

	testBestScanInner(t, query, exec_ctx, c, txn)
}

func TestBestJoin(t *testing.T) {
	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
	exec_ctx := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	// TODO: (SDB) need to create tables for use in query
	// TODO: (SDB) need to setup of statistics data of created tables and query which uses the tables for testing BestJoin func
	//table_info, _ := executors.GenerateTestTabls(c, exec_ctx, txn)
	query := new(parser.QueryInfo)

	testBestJoinInner(t, query, exec_ctx, c, txn)
}
