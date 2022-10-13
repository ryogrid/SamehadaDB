package executors

import (
	"errors"
	"fmt"
	"github.com/ryogrid/SamehadaDB/storage/page"

	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
)

/**
 * UpdateExecutor executes a sequential scan over a table and update tuples according to predicate.
 */
type UpdateExecutor struct {
	context       *ExecutorContext
	plan          *plans.UpdatePlanNode
	tableMetadata *catalog.TableMetadata
	it            *access.TableHeapIterator
	txn           *access.Transaction
}

func NewUpdateExecutor(context *ExecutorContext, plan *plans.UpdatePlanNode) Executor {
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
		if t == nil {
			err := errors.New("e.it.Next returned nil")
			return nil, true, err
		}
		if e.selects(t, e.plan.GetPredicate()) {
			// change e.it.Current() value for subsequent call
			if !e.it.End() {
				defer e.it.Next()
			}
			rid := e.it.Current().GetRID()
			values := e.plan.GetRawValues()
			new_tuple := tuple.NewTupleFromSchema(values, e.tableMetadata.Schema())

			var is_updated bool = false
			var new_rid *page.RID = nil
			if e.plan.GetUpdateColIdxs() == nil {
				is_updated, new_rid = e.tableMetadata.Table().UpdateTuple(new_tuple, nil, nil, e.tableMetadata.OID(), *rid, e.txn)
			} else {
				is_updated, new_rid = e.tableMetadata.Table().UpdateTuple(new_tuple, e.plan.GetUpdateColIdxs(), e.tableMetadata.Schema(), e.tableMetadata.OID(), *rid, e.txn)
			}

			if !is_updated {
				err := errors.New("tuple update failed. PageId:SlotNum = " + string(rid.GetPageId()) + ":" + fmt.Sprint(rid.GetSlotNum()))
				return nil, false, err
			}

			colNum := e.tableMetadata.GetColumnNum()
			for ii := 0; ii < int(colNum); ii++ {
				ret := e.tableMetadata.GetIndex(ii)
				if ret == nil {
					continue
				} else {
					index_ := ret
					index_.DeleteEntry(e.it.Current(), *rid, e.txn)
					if new_rid != nil {
						// when tuple is moved page location on update, RID is changed to new value
						fmt.Println("UpdateExecuter: index entry insert with new_rid.")
						index_.InsertEntry(new_tuple, *new_rid, e.txn)
					} else {
						index_.InsertEntry(new_tuple, *rid, e.txn)
					}

				}
			}

			return new_tuple, false, nil
		}
	}

	return nil, true, nil
}

// select evaluates an expression on the tuple
func (e *UpdateExecutor) selects(tuple *tuple.Tuple, predicate expression.Expression) bool {
	return predicate == nil || predicate.Evaluate(tuple, e.tableMetadata.Schema()).ToBoolean()
}

func (e *UpdateExecutor) GetOutputSchema() *schema.Schema {
	return e.plan.OutputSchema()
}
