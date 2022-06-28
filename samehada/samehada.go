package samehada

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/executors"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/planner"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

type SamehadaDB struct {
	shi_         *SamehadaInstance
	catalog_     *catalog.Catalog
	exec_engine_ *executors.ExecutionEngine
	planner_     planner.Planner
}

// TODO: (SDB) need to add argument which tells usable memory amount for DB
//             and set max frame num to BufferPoolManager for the amount info
func NewSamehadaDB(dbName string) *SamehadaDB {
	// TODO: (SDB) check existance of db file corresponding dbName and set result to flag variable

	shi := NewSamehadaInstance(dbName)
	txn := shi.GetTransactionManager().Begin(nil)

	// TODO: (SDB) if db has db file when calling NewSamehadaDB func, Rede/Undo process should be done here

	// TODO: (SDB) if db has db file when calling NewSamehadaDB func, get Catalog object with calling catalog.RecoveryCatalogFromCatalogPage
	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)

	shi.transaction_manager.Commit(txn)
	exec_engine := &executors.ExecutionEngine{}
	pnner := planner.NewSimplePlanner(c, shi.GetBufferPoolManager())

	// TODO: (SDB) need to spawn thread which do snapshot periodically

	return &SamehadaDB{shi, c, exec_engine, pnner}
}

func (sdb *SamehadaDB) ExecuteSQL(sqlStr string) (error, [][]*types.Value) {
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

func PrintExecuteResults(results [][]*types.Value) {
	fmt.Println("----")
	for _, valList := range results {
		for _, val := range valList {
			fmt.Printf("%s ", val.ToString())
		}
		fmt.Println("")
	}
}
