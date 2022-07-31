package samehada

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/concurrency"
	"github.com/ryogrid/SamehadaDB/execution/executors"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/planner"
	"github.com/ryogrid/SamehadaDB/recovery/log_recovery"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/disk"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"time"
)

type SamehadaDB struct {
	shi_         *SamehadaInstance
	catalog_     *catalog.Catalog
	exec_engine_ *executors.ExecutionEngine
	planner_     planner.Planner
}

func StartCheckpointThread(cpMngr *concurrency.CheckpointManager) {
	go func() {
		for {
			time.Sleep(time.Minute * 5)
			cpMngr.BeginCheckpoint()
			cpMngr.EndCheckpoint()
		}
	}()
}

func reconstructIndexDataOfEachCol(t *catalog.TableMetadata, c *catalog.Catalog, txn *access.Transaction) {
	executionEngine := &executors.ExecutionEngine{}
	executorContext := executors.NewExecutorContext(c, t.Table().GetBufferPoolManager(), txn)

	// get all tuples
	outSchema := t.Schema()
	seqPlan := plans.NewSeqScanPlanNode(outSchema, nil, t.OID())
	results := executionEngine.Execute(seqPlan, executorContext)

	// insert index entries correspond to each tuple and column to each index objects
	for _, index_ := range t.Indexes() {
		if index_ != nil {
			for _, tuple_ := range results {
				rid := tuple_.GetRID()
				index_.InsertEntry(tuple_, *rid, txn)
			}
		}
	}
}

func ReconstructAllIndexData(c *catalog.Catalog, txn *access.Transaction) {
	allTables := c.GetAllTables()
	for ii := 0; ii < len(allTables); ii++ {
		reconstructIndexDataOfEachCol(allTables[ii], c, txn)
	}
}

func NewSamehadaDB(dbName string, memKBytes int) *SamehadaDB {
	isExisitingDB := samehada_util.FileExists(dbName + ".db")

	bpoolSize := math.Floor(float64(memKBytes*1024) / float64(common.PageSize))
	shi := NewSamehadaInstance(dbName, int(bpoolSize))
	txn := shi.GetTransactionManager().Begin(nil)

	shi.GetLogManager().DeactivateLogging()

	if isExisitingDB {
		log_recovery := log_recovery.NewLogRecovery(
			shi.GetDiskManager(),
			shi.GetBufferPoolManager())
		greatestLSN := log_recovery.Redo()
		log_recovery.Undo()

		dman := shi.GetDiskManager().(*disk.DiskManagerImpl)
		dman.GCLogFile()
		shi.GetLogManager().SetNextLSN(greatestLSN + 1)
	}

	var c *catalog.Catalog
	if isExisitingDB {
		c = catalog.RecoveryCatalogFromCatalogPage(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
		// index date reloading/recovery is not implemented yet
		// so all index data should be recounstruct using already allocated pages
		ReconstructAllIndexData(c, txn)
	} else {
		c = catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
	}

	shi.bpm.FlushAllPages()
	shi.transaction_manager.Commit(txn)

	shi.GetLogManager().ActivateLogging()

	exec_engine := &executors.ExecutionEngine{}
	pnner := planner.NewSimplePlanner(c, shi.GetBufferPoolManager())

	StartCheckpointThread(concurrency.NewCheckpointManager(shi.GetTransactionManager(), shi.GetLogManager(), shi.GetBufferPoolManager()))

	return &SamehadaDB{shi, c, exec_engine, pnner}
}

func (sdb *SamehadaDB) ExecuteSQL(sqlStr string) (error, [][]interface{}) {
	err, results := sdb.ExecuteSQLRetValues(sqlStr)
	return err, ConvValueListToIFs(results)
}

func (sdb *SamehadaDB) ExecuteSQLRetValues(sqlStr string) (error, [][]*types.Value) {
	qi := parser.ProcessSQLStr(&sqlStr)
	txn := sdb.shi_.transaction_manager.Begin(nil)
	err, plan := sdb.planner_.MakePlan(qi, txn)

	if err == nil && plan == nil {
		// CREATE_TABLE is scceeded
		sdb.shi_.GetTransactionManager().Commit(txn)
		return nil, nil
	} else if err != nil {
		return err, nil
	}

	context := executors.NewExecutorContext(sdb.catalog_, sdb.shi_.GetBufferPoolManager(), txn)
	result := sdb.exec_engine_.Execute(plan, context)

	if txn.GetState() == access.ABORTED {
		sdb.shi_.GetTransactionManager().Abort(txn)
		// TODO: (SDB) when concurrent execution of transaction is activated, appropriate handling of aborted transactions is needed
	} else {
		sdb.shi_.GetTransactionManager().Commit(txn)
	}

	outSchema := plan.OutputSchema()
	if outSchema == nil { // when DELETE etc...
		return nil, nil
	}

	//fmt.Println(result, outSchema)
	retVals := ConvTupleListToValues(outSchema, result)

	return nil, retVals
}

func (sdb *SamehadaDB) Finalize() {
	//sdb.shi_.GetBufferPoolManager().FlushAllPages()
	sdb.shi_.Finalize(false)
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
