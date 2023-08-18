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
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"testing"
)

func setupTablesAndStatisticsDataForTesting(c *catalog.Catalog, exec_ctx *executors.ExecutorContext, txn *access.Transaction) {
	// TODO: (SDB) [OPT] not implemented yet (setupTablesAndStatisticsDataForTesting)
}

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

	setupTablesAndStatisticsDataForTesting(c, exec_ctx, txn)

	// TODO: (SDB) [OPT] need to write query for testing BestJoin func (TestSimplePlanOptimization)
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

	setupTablesAndStatisticsDataForTesting(c, exec_ctx, txn)

	// TODO: (SDB) [OPT] need to write query for testing BestJoin func (TestFindBestScans)
	queryStr := "TO BE WRITTEN"
	queryInfo := parser.ProcessSQLStr(&queryStr)

	optimalPlans := NewSelingerOptimizer(queryInfo, c).findBestScans()
	testingpkg.Assert(t, len(optimalPlans) == len(queryInfo.JoinTables_), "len(optimalPlans) != len(query.JoinTables_)")
}
