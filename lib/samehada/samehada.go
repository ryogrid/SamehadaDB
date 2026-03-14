package samehada

import (
	"errors"
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/concurrency"
	"github.com/ryogrid/SamehadaDB/lib/execution/executors"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/parser"
	"github.com/ryogrid/SamehadaDB/lib/planner"
	"github.com/ryogrid/SamehadaDB/lib/planner/optimizer"
	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/recovery/log_recovery"
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/disk"
	"github.com/ryogrid/SamehadaDB/lib/storage/index"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"math"
	"unsafe"
)

type SamehadaDB struct {
	shi            *SamehadaInstance
	cat            *catalog.Catalog
	execEngine     *executors.ExecutionEngine
	statisticsUpdr *concurrency.StatisticsUpdater
	requestMgr     *RequestManager
}

type reqResult struct {
	err      error
	result   [][]interface{}
	reqID    *uint64
	query    *string
	callerCh *chan *reqResult
}

// return internal object (for testing)
func (sdb *SamehadaDB) GetSamehadaInstance() *SamehadaInstance {
	return sdb.shi
}

func reconstructIndexDataOfATbl(t *catalog.TableMetadata, c *catalog.Catalog, dman disk.DiskManager, txn *access.Transaction) {
	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, t.Table().GetBufferPoolManager(), txn)

	zeroClearedBuf := make([]byte, common.PageSize)
	//zeroClearedBuf := directio.AlignedBlock(common.PageSize)
	bpm := t.Table().GetBufferPoolManager()

	for colIdx, idx := range t.Indexes() {
		if idx != nil {
			col := t.Schema().GetColumn(uint32(colIdx))
			switch col.IndexKind() {
			case index_constants.IndexKindHash:
				// clear pages for HashTableBlockPage for avoiding conflict with reconstruction
				// due to there may be pages (on disk) which has old index entries data in current design...
				// note: when this method is called, the pages are not fetched yet (= are not in memory)

				indexHeaderPageID := col.IndexHeaderPageID()

				hPageData := bpm.FetchPage(indexHeaderPageID).Data()
				headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(hPageData))
				for ii := uint64(0); ii < headerPage.NumBlocks(); ii++ {
					blockPageID := headerPage.GetBlockPageID(ii)
					// zero clear specifed space of db file
					dman.WritePage(blockPageID, zeroClearedBuf)
				}
			case index_constants.IndexKindUniqSkipList:
				// do nothing here
				// (Since SkipList index can't reuse past allocated pages, data clear of allocated pages
				//  are not needed...)
			case index_constants.IndexKindSkipList:
				// do nothing here
				// (Since SkipList index can't reuse past allocated pages, data clear of allocated pages
				//  are not needed...)
			case index_constants.IndexKindBtree:
				// do nothing here
				// (Since BTree index can't reuse past allocated pages, data clear of allocated pages
				//  are not needed...)
			default:
				panic("invalid index kind!")
			}
		}
	}

	var allTuples []*tuple.Tuple = nil

	// insert index entries correspond to each tuple and column to each index objects
	for _, idx := range t.Indexes() {
		if idx != nil {
			if allTuples == nil {
				// get all tuples once
				outSchema := t.Schema()
				seqPlan := plans.NewSeqScanPlanNode(c, outSchema, nil, t.OID())
				allTuples = executionEngine.Execute(seqPlan, executorContext)
			}
			for _, tpl := range allTuples {
				rid := tpl.GetRID()
				idx.InsertEntry(tpl, *rid, txn)
			}
		}
	}
}

func ReconstructAllIndexData(c *catalog.Catalog, dman disk.DiskManager, txn *access.Transaction) {
	allTables := c.GetAllTables()
	for ii := 0; ii < len(allTables); ii++ {
		reconstructIndexDataOfATbl(allTables[ii], c, dman, txn)
	}
}

func NewSamehadaDB(dbName string, memKBytes int) *SamehadaDB {
	isExistingDB := false

	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage {
		isExistingDB = samehada_util.FileExists(dbName + ".db")
	}

	bpoolSize := math.Floor(float64(memKBytes*1024) / float64(common.PageSize))
	shi := NewSamehadaInstance(dbName, int(bpoolSize))
	txn := shi.GetTransactionManager().Begin(nil)

	shi.GetLogManager().DeactivateLogging()
	txn.SetIsRecoveryPhase(true)

	var c *catalog.Catalog
	if isExistingDB {
		logRecov := log_recovery.NewLogRecovery(
			shi.GetDiskManager(),
			shi.GetBufferPoolManager(),
			shi.GetLogManager())
		greatestLSN, isUndoNeeded, isGracefulShutdown := logRecov.Redo(txn)
		if isUndoNeeded {
			logRecov.Undo(txn)
		}

		dman := shi.GetDiskManager()
		dman.GCLogFile()
		shi.GetLogManager().SetNextLSN(greatestLSN + 1)

		// rewrite reusable page id log because it is not wrote to log file after launch
		// but the information is needed next launch also
		reusablePageIDs := shi.bpm.GetReusablePageIDs()
		if len(reusablePageIDs) > 0 {
			for _, pageID := range reusablePageIDs {
				logRecord := recovery.NewLogRecordDeallocatePage(pageID)
				shi.logManager.AppendLogRecord(logRecord)
			}
		}
		shi.logManager.Flush()

		c = catalog.RecoveryCatalogFromCatalogPage(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn, isGracefulShutdown)

		// if last shutdown is not gracefully done, all index data should be reconstructed
		if !isGracefulShutdown {
			// index date reloading and recovery is not implemented yet
			// so when db did not exit graceful, all index data should be reconstructed
			// (hash index uses already allocated pages but skip list index deserts these...)
			ReconstructAllIndexData(c, shi.GetDiskManager(), txn)
		}
	} else {
		c = catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
	}

	shi.bpm.FlushAllPages()
	shi.transactionManager.Commit(c, txn)

	shi.GetLogManager().ActivateLogging()

	execEngine := &executors.ExecutionEngine{}

	shi.GetCheckpointManager().StartCheckpointTh()

	// statics data is updated periodically by this thread with full scan of all tables
	// this may be not good implementation of statistics, but it is enough for now...
	statUpdater := concurrency.NewStatisticsUpdater(shi.GetTransactionManager(), c)
	statUpdater.StartStaticsUpdaterTh()

	ret := &SamehadaDB{shi, c, execEngine, statUpdater, nil}
	tmpReqMgr := NewRequestManager(ret)
	ret.requestMgr = tmpReqMgr
	ret.requestMgr.StartTh()

	return ret
}

func (sdb *SamehadaDB) ExecuteSQLForTxnTh(ch *chan *reqResult, qr *queryRequest) {
	err, results := sdb.ExecuteSQLRetValues(*qr.queryStr)
	if err != nil {
		*ch <- &reqResult{err, nil, qr.reqID, qr.queryStr, qr.callerCh}
		return
	}
	*ch <- &reqResult{nil, samehada_util.ConvValueListToIFs(results), qr.reqID, qr.queryStr, qr.callerCh}
}

func (sdb *SamehadaDB) ExecuteSQL(sqlStr string) (error, [][]interface{}) {
	ch := sdb.requestMgr.AppendRequest(&sqlStr)
	ret := <-*ch
	return ret.err, ret.result
}

var PlanCreationErr = errors.New("plan creation error")

// temporal error
var QueryAbortedErr = errors.New("query aborted")

func (sdb *SamehadaDB) ExecuteSQLRetValues(sqlStr string) (error, [][]*types.Value) {
	qi, err := parser.ProcessSQLStr(&sqlStr)
	if err != nil {
		return err, nil
	}
	qi, err = optimizer.RewriteQueryInfo(sdb.cat, qi)
	if err != nil {
		return err, nil
	}
	txn := sdb.shi.transactionManager.Begin(nil)
	err, plan := planner.NewSimplePlanner(sdb.cat, sdb.shi.bpm).MakePlan(qi, txn)

	if err == nil && plan == nil {
		// some problem exists on SQL string
		sdb.shi.GetTransactionManager().Commit(sdb.cat, txn)
		if *qi.QueryType == parser.CreateTable {
			return nil, nil
		} else {
			return PlanCreationErr, nil
		}
	} else if err != nil {
		// already table exist case
		sdb.shi.GetTransactionManager().Commit(sdb.cat, txn)
		return err, nil
	}

	context := executors.NewExecutorContext(sdb.cat, sdb.shi.GetBufferPoolManager(), txn)
	result := sdb.execEngine.Execute(plan, context)

	if txn.GetState() == access.ABORTED {
		sdb.shi.GetTransactionManager().Abort(sdb.cat, txn)
		// temporal impl
		return QueryAbortedErr, nil
	} else {
		sdb.shi.GetTransactionManager().Commit(sdb.cat, txn)
	}

	outSchema := plan.OutputSchema()
	if outSchema == nil { // when DELETE etc...
		return nil, nil
	}

	//fmt.Println(result, outSchema)
	retVals := samehada_util.ConvTupleListToValues(outSchema, result)

	return nil, retVals
}

// use this when shutdown DB
// and before flush of page buffer
func (si *SamehadaDB) finalizeIndexesInternalState() {
	allTables := si.cat.GetAllTables()
	for _, table := range allTables {
		for _, idx := range table.Indexes() {
			if idx != nil {
				switch idx.(type) {
				case *index.BTreeIndex:
					// serialize internal page mapping entries to pages
					idx.(*index.BTreeIndex).WriteOutContainerStateToBPM()
				default:
					//do nothing
				}
			}
		}
	}
}

func (sdb *SamehadaDB) Shutdown() {
	// set a flag which is checked by checkpointing thread
	sdb.statisticsUpdr.StopStatsUpdateTh()
	sdb.shi.GetCheckpointManager().StopCheckpointTh()
	sdb.requestMgr.StopTh()
	sdb.finalizeIndexesInternalState()
	logRecord := recovery.NewLogRecordGracefulShutdown()
	sdb.shi.logManager.AppendLogRecord(logRecord)
	sdb.shi.Shutdown(ShutdownPatternCloseFiles)
}

// no flush of page buffer
func (sdb *SamehadaDB) ShutdownForTescase() {
	// set a flag which is checked by checkpointing thread
	sdb.shi.GetCheckpointManager().StopCheckpointTh()
	sdb.statisticsUpdr.StopStatsUpdateTh()
	sdb.requestMgr.StopTh()
	sdb.finalizeIndexesInternalState()
	sdb.shi.CloseFilesForTesting()
}

func (sdb *SamehadaDB) ForceCheckpointingForTestcase() {
	sdb.shi.GetCheckpointManager().BeginCheckpoint()
	sdb.shi.GetCheckpointManager().EndCheckpoint()
}

// for internal unit testing
func (sdb *SamehadaDB) GetCatalogForTesting() *catalog.Catalog {
	return sdb.cat
}
