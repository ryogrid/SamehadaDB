// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
)

/**
 * InsertExecutor executes an insert into a table.
 * Inserted values can either be embedded in the plan itself ("raw insert") or come from a child executor.
 */
type InsertExecutor struct {
	context       *ExecutorContext
	plan          *plans.InsertPlanNode
	tableMetadata *catalog.TableMetadata
}

func NewInsertExecutor(context *ExecutorContext, plan *plans.InsertPlanNode) Executor {
	tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())
	//catalog := context.GetCatalog()

	return &InsertExecutor{context, plan, tableMetadata}
}

func (e *InsertExecutor) Init() {

}

// Next inserts the tuples into the tables
// Note that Insert does not return any tuple
// We return an error if the insert failed for any reason, and return nil if all inserts succeeded.
func (e *InsertExecutor) Next() (*tuple.Tuple, Done, error) {
	// let's assume it is raw insert

	for _, values := range e.plan.GetRawValues() {
		tuple := tuple.NewTupleFromSchema(values, e.tableMetadata.Schema())
		tableHeap := e.tableMetadata.Table()
		_, err := tableHeap.InsertTuple(tuple, e.context.txn)
		// TODO: (SDB) insert index entry if needed
		if err != nil {
			return nil, true, err
		}
	}

	return nil, true, nil
}
