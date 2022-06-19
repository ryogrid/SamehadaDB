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

func NewSamehadaDB(dbName string) *SamehadaDB {
	shi := NewSamehadaInstance(dbName)
	txn := shi.GetTransactionManager().Begin(nil)
	c := catalog.BootstrapCatalog(shi.GetBufferPoolManager(), shi.GetLogManager(), shi.GetLockManager(), txn)
	shi.transaction_manager.Commit(txn)
	exec_engine := &executors.ExecutionEngine{}
	pnner := planner.NewSimplePlanner(c, shi.GetBufferPoolManager())
	return &SamehadaDB{shi, c, exec_engine, pnner}
}

// TODO: (SDB) need to implement ExecuteSQL of SamehadaDB class
func (sdb *SamehadaDB) ExecuteSQL(sqlStr string) (error, [][]*types.Value) {
	qi := parser.ProcessSQLStr(&sqlStr)
	txn := sdb.shi_.transaction_manager.Begin(nil)
	_, plan := sdb.planner_.MakePlan(qi, txn)

	context := executors.NewExecutorContext(sdb.catalog_, sdb.shi_.GetBufferPoolManager(), txn)
	result := sdb.exec_engine_.Execute(plan, context)

	if txn.GetState() == access.ABORTED {
		sdb.shi_.GetTransactionManager().Abort(txn)
		// TODO: (SDB) when concurrent execution of transaction is activated, appropriate handling of aborted transactions is needed
	} else {
		sdb.shi_.GetTransactionManager().Commit(txn)
	}

	outSchema := plan.OutputSchema()
	fmt.Println(result, outSchema)
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
