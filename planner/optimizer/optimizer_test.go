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

	/*
	  if (query.from_.empty()) {
	    throw std::runtime_error("No table specified");
	  }
	  std::unordered_map<std::unordered_set<std::string>, CostAndPlan>
	      optimal_plans;

	  // 1. Initialize every single tables to start.
	  std::unordered_set<ColumnName> touched_columns =
	      query.where_->TouchedColumns();
	  for (const auto& sel : query.select_) {
	    touched_columns.merge(sel.expression->TouchedColumns());
	  }
	  for (const auto& from : query.from_) {
	    ASSIGN_OR_RETURN(std::shared_ptr<Table>, tbl, ctx.GetTable(from));
	    ASSIGN_OR_RETURN(std::shared_ptr<TableStatistics>, stats,
	                     ctx.GetStats(from));

	    // Push down all selection & projection.
	    std::vector<NamedExpression> project_target;
	    for (size_t i = 0; i < tbl->GetSchema().ColumnCount(); ++i) {
	      for (const auto& touched_col : touched_columns) {
	        const Column& table_col = tbl->GetSchema().GetColumn(i);
	        if (table_col.Name().name == touched_col.name &&
	            (touched_col.schema.empty() ||
	             touched_col.schema == tbl->GetSchema().Name())) {
	          project_target.emplace_back(table_col.Name());
	        }
	      }
	    }
	    Plan scan = BestScan(project_target, *tbl, query.where_, *stats);
	    optimal_plans.emplace(std::unordered_set({from}),
	                          CostAndPlan{scan->AccessRowCount(), scan});
	  }
	  assert(optimal_plans.size() == query.from_.size());
	*/
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
				bestJoinPlan := new(SelingerOptimizer).bestJoin(query.WhereExpression_, baseTableCP.plan, joinTableCP.plan)
				fmt.Println(bestJoinPlan)

				joinedTables := baseTableFrom.Union(joinTableFrom)
				common.SH_Assert(1 < joinedTables.Cardinality(), "joinedTables.Cardinality() is illegal!")
				cost := bestJoinPlan.AccessRowCount()

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

	// TODO: (SDB) need to create tables for use in query
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
