package executors

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
)

/**
 * UpdateExecutor executes a sequential scan over a table and update tuples according to predicate.
 */
type UpdateExecutor struct {
	context       *ExecutorContext
	plan          *plans.DeletePlanNode
	tableMetadata *catalog.TableMetadata
	it            *access.TableHeapIterator
	txn           *access.Transaction
}

func NewUpdateExecutor(context *ExecutorContext, plan *plans.DeletePlanNode) Executor {
	tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())

	return &UpdateExecutor{context, plan, tableMetadata, nil, context.GetTransaction()}
}

func (e *UpdateExecutor) Init() {
	e.it = e.tableMetadata.Table().Iterator(e.txn)
}

// Next implements the next method for the sequential scan operator
// It uses the table heap iterator to iterate through the table heap
// tyring to find a tuple to be updated. It performs selection on-the-fly
func (e *UpdateExecutor) Next() (*tuple.Tuple, Done, error) {

	// iterates through the table heap trying to select a tuple that matches the predicate
	for t := e.it.Current(); !e.it.End(); t = e.it.Next() {
		if e.selects(t, e.plan.GetPredicate()) {
			// change e.it.Current() value for subsequent call
			if !e.it.End() {
				defer e.it.Next()
			}
			rid := e.it.Current().GetRID()
			e.tableMetadata.Table().MarkDelete(rid, e.txn)

			// DeleteEntry is called once at most because a table can have one index only
			colNum := e.tableMetadata.GetColumnNum()
			for ii := 0; ii < int(colNum); ii++ {
				ret := e.tableMetadata.GetIndex(ii)
				if ret == nil {
					continue
				} else {
					index_ := *ret
					index_.DeleteEntry(e.it.Current(), *rid, e.txn)
				}
			}

			return e.it.Current(), false, nil
		}
	}

	return nil, true, nil
}

// select evaluates an expression on the tuple
func (e *UpdateExecutor) selects(tuple *tuple.Tuple, predicate *expression.Expression) bool {
	return predicate == nil || (*predicate).Evaluate(tuple, e.tableMetadata.Schema()).ToBoolean()
}
