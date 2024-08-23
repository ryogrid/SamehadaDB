package samehada

import (
	"errors"
	"fmt"
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
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"math"
	"unsafe"
)

type SamehadaDB struct {
	shi_               *SamehadaInstance
	catalog_           *catalog.Catalog
	exec_engine_       *executors.ExecutionEngine
	statistics_updator *concurrency.StatisticsUpdater
	request_manager    *RequestManager
}

type reqResult struct {
	err      error
	result   [][]interface{}
	reqId    *uint64
	query    *string
	callerCh *chan *reqResult
}

func reconstructIndexDataOfATbl(t *catalog.TableMetadata, c *catalog.Catalog, dman disk.DiskManager, txn *access.Transaction) {
	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, t.Table().GetBufferPoolManager(), txn)

	zeroClearedBuf := make([]byte, common.PageSize)
	//zeroClearedBuf := directio.AlignedBlock(common.PageSize)
	bpm := t.Table().GetBufferPoolManager()

	for colIdx, index_ := range t.Indexes() {
		if index_ != nil {
			column_ := t.Schema().GetColumn(uint32(colIdx))
			switch column_.IndexKind() {
			case index_constants.INDEX_KIND_HASH:
				// clear pages for HashTableBlockPage for avoiding conflict with reconstruction
				// due to there may be pages (on disk) which has old index entries data in current design...
				// note: when this method is called, the pages are not fetched yet (= are not in memory)

				indexHeaderPageId := column_.IndexHeaderPageId()

				hPageData := bpm.FetchPage(indexHeaderPageId).Data()
				headerPage := (*page.HashTableHeaderPage)(unsafe.Pointer(hPageData))
				for ii := uint64(0); ii < headerPage.NumBlocks(); ii++ {
					blockPageId := headerPage.GetBlockPageId(ii)
					// zero clear specifed space of db file
					dman.WritePage(blockPageId, zeroClearedBuf)
				}
			case index_constants.INDEX_KIND_UNIQ_SKIP_LIST:
				// do nothing here
				// (Since SkipList index can't reuse past allocated pages, data clear of allocated pages
				//  are not needed...)
			case index_constants.INDEX_KIND_SKIP_LIST:
				// do nothing here
				// (Since SkipList index can't reuse past allocated pages, data clear of allocated pages
				//  are not needed...)
			case index_constants.INDEX_KIND_BTREE:
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
	for _, index_ := range t.Indexes() {
		if index_ != nil {
			if allTuples == nil {
				// get all tuples once
				outSchema := t.Schema()
				seqPlan := plans.NewSeqScanPlanNode(c, outSchema, nil, t.OID())
				allTuples = executionEngine.Execute(seqPlan, executorContext)
			}
			for _, tuple_ := range allTuples {
				rid := tuple_.GetRID()
				index_.InsertEntry(tuple_, *rid, txn)
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
		log_recovery := log_recovery.NewLogRecovery(
			shi.GetDiskManager(),
			shi.GetBufferPoolManager(),
			shi.GetLogManager())
		greatestLSN, isUndoNeeded, isGracefulShutdown := log_recovery.Redo(txn)
		if isUndoNeeded {
			log_recovery.Undo(txn)
		}

		dman := shi.GetDiskManager()
		dman.GCLogFile()
		shi.GetLogManager().SetNextLSN(greatestLSN + 1)

		// rewrite reusable page id log because it is not wrote to log file after launch
		// but the information is needed next launch also
		reusablePageIds := shi.bpm.GetReusablePageIds()
		if len(reusablePageIds) > 0 {
			for _, pageId := range reusablePageIds {
				logRecord := recovery.NewLogRecordDeallocatePage(pageId)
				shi.log_manager.AppendLogRecord(logRecord)
			}
		}
		shi.log_manager.Flush()

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
	shi.transaction_manager.Commit(c, txn)

	shi.GetLogManager().ActivateLogging()

	exec_engine := &executors.ExecutionEngine{}

	shi.GetCheckpointManager().StartCheckpointTh()

	// statics data is updated periodically by this thread with full scan of all tables
	// this may be not good implementation of statistics, but it is enough for now...
	statUpdater := concurrency.NewStatisticsUpdater(shi.GetTransactionManager(), c)
	statUpdater.StartStaticsUpdaterTh()

	ret := &SamehadaDB{shi, c, exec_engine, statUpdater, nil}
	tmpReqMgr := NewRequestManager(ret)
	ret.request_manager = tmpReqMgr
	ret.request_manager.StartTh()

	return ret
}

func (sdb *SamehadaDB) ExecuteSQLForTxnTh(ch *chan *reqResult, qr *queryRequest) {
	err, results := sdb.ExecuteSQLRetValues(*qr.queryStr)
	if err != nil {
		*ch <- &reqResult{err, nil, qr.reqId, qr.queryStr, qr.callerCh}
		return
	}
	*ch <- &reqResult{nil, ConvValueListToIFs(results), qr.reqId, qr.queryStr, qr.callerCh}
}

func (sdb *SamehadaDB) ExecuteSQL(sqlStr string) (error, [][]interface{}) {
	ch := sdb.request_manager.AppendRequest(&sqlStr)
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
	qi, err = optimizer.RewriteQueryInfo(sdb.catalog_, qi)
	if err != nil {
		return err, nil
	}
	txn := sdb.shi_.transaction_manager.Begin(nil)
	err, plan := planner.NewSimplePlanner(sdb.catalog_, sdb.shi_.bpm).MakePlan(qi, txn)

	if err == nil && plan == nil {
		// some problem exists on SQL string
		sdb.shi_.GetTransactionManager().Commit(sdb.catalog_, txn)
		if *qi.QueryType_ == parser.CREATE_TABLE {
			return nil, nil
		} else {
			return PlanCreationErr, nil
		}
	} else if err != nil {
		// already table exist case
		sdb.shi_.GetTransactionManager().Commit(sdb.catalog_, txn)
		return err, nil
	}

	context := executors.NewExecutorContext(sdb.catalog_, sdb.shi_.GetBufferPoolManager(), txn)
	result := sdb.exec_engine_.Execute(plan, context)

	if txn.GetState() == access.ABORTED {
		sdb.shi_.GetTransactionManager().Abort(sdb.catalog_, txn)
		// temporal impl
		return QueryAbortedErr, nil
	} else {
		sdb.shi_.GetTransactionManager().Commit(sdb.catalog_, txn)
	}

	outSchema := plan.OutputSchema()
	if outSchema == nil { // when DELETE etc...
		return nil, nil
	}

	//fmt.Println(result, outSchema)
	retVals := ConvTupleListToValues(outSchema, result)

	return nil, retVals
}

func (sdb *SamehadaDB) Shutdown() {
	// set a flag which is checked by checkpointing thread
	sdb.statistics_updator.StopStatsUpdateTh()
	sdb.shi_.GetCheckpointManager().StopCheckpointTh()
	sdb.request_manager.StopTh()
	logRecord := recovery.NewLogRecordGracefulShutdown()
	sdb.shi_.log_manager.AppendLogRecord(logRecord)
	sdb.shi_.Shutdown(ShutdownPatternCloseFiles)
}

// no flush of page buffer
func (sdb *SamehadaDB) ShutdownForTescase() {
	// set a flag which is checked by checkpointing thread
	sdb.shi_.GetCheckpointManager().StopCheckpointTh()
	sdb.statistics_updator.StopStatsUpdateTh()
	sdb.request_manager.StopTh()
	sdb.shi_.CloseFilesForTesting()
}

func (sdb *SamehadaDB) ForceCheckpointingForTestcase() {
	sdb.shi_.GetCheckpointManager().BeginCheckpoint()
	sdb.shi_.GetCheckpointManager().EndCheckpoint()
}

func ConvTupleListToValues(schema_ *schema.Schema, result []*tuple.Tuple) [][]*types.Value {
	retVals := make([][]*types.Value, 0)
	for _, tuple_ := range result {
		rowVals := make([]*types.Value, 0)
		colNum := int(schema_.GetColumnCount())
		for idx := 0; idx < colNum; idx++ {
			val := tuple_.GetValue(schema_, uint32(idx))
			rowVals = append(rowVals, &val)
		}
		retVals = append(retVals, rowVals)
	}
	return retVals
}

func ConvValueListToIFs(vals [][]*types.Value) [][]interface{} {
	retVals := make([][]interface{}, 0)
	for _, valsRow := range vals {
		ifsList := make([]interface{}, 0)
		for _, val := range valsRow {
			if val.IsNull() {
				ifsList = append(ifsList, nil)
			} else {
				switch val.ValueType() {
				case types.Integer:
					ifsList = append(ifsList, val.ToInteger())
				case types.Float:
					ifsList = append(ifsList, val.ToFloat())
				case types.Varchar:
					ifsList = append(ifsList, val.ToString())
				default:
					panic("not supported Value object")
				}
			}
		}
		retVals = append(retVals, ifsList)
	}
	return retVals
}

func PrintExecuteResults(results [][]*types.Value) {
	fmt.Println("----")
	for _, valList := range results {
		for _, val := range valList {
			fmt.Printf("%s ", val.ToString())
		}
		fmt.Println("")
	}
}
