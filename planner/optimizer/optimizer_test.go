package optimizer

import (
	"fmt"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/executors"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/samehada"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"

	//"strings"
	"testing"
)

func TestSimplePlanOptimization(t *testing.T) {
	// TODO: (SDB) not implemented yet
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

	optimalPlans := findBestScans(query, exec_ctx, c, txn)
	testingpkg.Assert(t, len(optimalPlans) == len(query.JoinTables_), "len(optimalPlans) != len(query.JoinTables_)")
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

	// TODO: (SDB) need to setup of optimalPlans (TestBestJoin)
	optimalPlans := make(map[mapset.Set[string]]CostAndPlan, 0)

	solution := findBestJoin(optimalPlans, query, exec_ctx, c, txn)
	fmt.Println(solution)
}
