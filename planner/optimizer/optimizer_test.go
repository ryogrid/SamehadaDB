package optimizer

import (
	"testing"
)

func TestSimplePlanOptimization(t *testing.T) {
	// TODO: (SDB) [OPT] not implemented yet (TestSimplePlanOptimization)
}

/*
func TestBestScan(t *testing.T) {
	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi.GetLogManager().ActivateLogging()
	testingpkg.Assert(t, shi.GetLogManager().IsEnabledLogging(), "")
	fmt.Println("System logging is active.")

	txn_mgr := shi.GetTransactionManager()
	txn := txn_mgr.Begin(nil)

	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
	exec_ctx := executors.NewExecutorContext(c, shi.GetBufferPoolManager(), txn)

	// TODO: (SDB) [OPT] need to setup tables for use in query (TestBestScan)
	// TODO: (SDB) [OPT] need to setup of statistics data of created tables and query which uses the tables for testing BestJoin func (TestBestScan)
	//table_info, _ := executors.GenerateTestTabls(c, exec_ctx, txn)
	query := new(parser.QueryInfo)

	optimalPlans := NewSelingerOptimizer().findBestScans(query, exec_ctx, c, txn)
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

	// TODO: (SDB) [OPT] need to create tables for use in query (TestBestJoin)
	// TODO: (SDB) [OPT] need to setup of statistics data of created tables and query which uses the tables for testing BestJoin func (TestBestJoin)
	//table_info, _ := executors.GenerateTestTabls(c, exec_ctx, txn)
	query := new(parser.QueryInfo)

	// TODO: (SDB) [OPT] need to setup of optimalPlans (TestBestJoin)
	optimalPlans := make(map[mapset.Set[string]]CostAndPlan, 0)

	solution := NewSelingerOptimizer().findBestJoin(optimalPlans, query, exec_ctx, c, txn)
	fmt.Println(solution)
}
*/
