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
	testingpkg "github.com/ryogrid/SamehadaDB/testing/testing_assert"
	"testing"
)

func setupTablesAndStatisticsDataForTesting(c *catalog.Catalog, exec_ctx *executors.ExecutorContext, txn *access.Transaction) {
	// TODO: (SDB) [OPT] not implemented yet (setupTablesAndStatisticsDataForTesting)
	/*
		     prefix_ = "optimizer_test-" + RandomString();
			 rs_->CreateTable(ctx,
							  Schema("Sc1", {Column("c1", ValueType::kInt64),
											 Column("c2", ValueType::kVarChar),
											 Column("c3", ValueType::kDouble)}));
		     for (int i = 0; i < 100; ++i) {
		           tbl.Insert(ctx.txn_,
		                      Row({Value(i), Value("c2-" + std::to_string(i)),
		                           Value(i + 9.9)}));
		     }

			 rs_->CreateTable(ctx,
							  Schema("Sc2", {Column("d1", ValueType::kInt64),
											 Column("d2", ValueType::kDouble),
											 Column("d3", ValueType::kVarChar),
											 Column("d4", ValueType::kInt64)}));
		     for (int i = 0; i < 200; ++i) {
		           tbl.Insert(ctx.txn_,
		                      Row({Value(i), Value(i + 0.2),
		                           Value("d3-" + std::to_string(i % 10)), Value(16)}));
		     }


			 rs_->CreateTable(ctx,
							  Schema("Sc3", {Column("e1", ValueType::kInt64),
											 Column("e2", ValueType::kDouble)}));
		     for (int i = 20; 0 < i; --i) {
		           tbl.Insert(ctx.txn_, Row({Value(i), Value(i + 53.4)}));
		     }

			 rs_->CreateTable(ctx,
							  Schema("Sc4", {Column("c1", ValueType::kInt64),
											 Column("c2", ValueType::kVarChar)}));
		     for (int i = 100; 0 < i; --i) {
		           tbl.Insert(ctx.txn_, Row({Value(i), Value(std::to_string(i % 4))}));
		     }

		     IndexSchema idx_sc("SampleIndex", {1, 2});
		     rs_->CreateIndex(ctx, "Sc1", IndexSchema("KeyIdx", {1, 2}));
		     rs_->CreateIndex(ctx, "Sc1", IndexSchema("Sc1PK", {0}));
		     rs_->CreateIndex(ctx, "Sc2", IndexSchema("Sc2PK", {0}));
		     rs_->CreateIndex(ctx, "Sc2",IndexSchema("NameIdx", {2, 3}, {0, 1}, IndexMode::kNonUnique));
		     rs_->CreateIndex(ctx, "Sc4", IndexSchema("Sc4_IDX", {1}, {}, IndexMode::kNonUnique));
		     ctx.txn_.PreCommit();

		     auto stat_tx = rs_->BeginContext();
		     rs_->RefreshStatistics(stat_tx, "Sc1");
		     rs_->RefreshStatistics(stat_tx, "Sc2");
		     rs_->RefreshStatistics(stat_tx, "Sc3");
		     rs_->RefreshStatistics(stat_tx, "Sc4");
		     stat_tx.PreCommit();
	*/
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
