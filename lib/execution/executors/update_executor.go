package executors

import (
	"errors"
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"

	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
)

/**
 * UpdateExecutor executes a sequential scan over a table and update tuples according to predicate.
 */
type UpdateExecutor struct {
	context *ExecutorContext
	plan    *plans.UpdatePlanNode
	child   Executor
	txn     *access.Transaction
}

func NewUpdateExecutor(context *ExecutorContext, plan *plans.UpdatePlanNode, child Executor) Executor {
	return &UpdateExecutor{context, plan, child, context.GetTransaction()}
}

func (e *UpdateExecutor) Init() {
	e.child.Init()
}

// Next implements the next method for the sequential scan operator
// It uses the table heap iterator to iterate through the table heap
// tyring to find a tuple to be updated. It performs selection on-the-fly
func (e *UpdateExecutor) Next() (*tuple.Tuple, Done, error) {

	// t is tuple before update
	for t, done, err := e.child.Next(); !done; t, done, err = e.child.Next() {
		if t == nil {
			wrapErr := errors.New("e.it.Next returned nil")
			e.txn.SetState(access.ABORTED)
			return nil, true, wrapErr
		}
		if err != nil {
			e.txn.SetState(access.ABORTED)
			return nil, true, err
		}
		if e.txn.GetState() == access.ABORTED {
			return nil, true, err
		}

		rid := t.GetRID()
		values := e.plan.GetRawValues()
		newTuple := tuple.NewTupleFromSchema(values, e.child.GetTableMetaData().Schema())

		var isUpdated = false
		var newRID *page.RID = nil
		var updateErr error = nil
		var updateTuple *tuple.Tuple
		if e.plan.GetUpdateColIdxs() == nil {
			isUpdated, newRID, updateErr, updateTuple, _ = e.child.GetTableMetaData().Table().UpdateTuple(newTuple, nil, nil, e.child.GetTableMetaData().OID(), *rid, e.txn, false)
		} else {
			isUpdated, newRID, updateErr, updateTuple, _ = e.child.GetTableMetaData().Table().UpdateTuple(newTuple, e.plan.GetUpdateColIdxs(), e.child.GetTableMetaData().Schema(), e.child.GetTableMetaData().OID(), *rid, e.txn, false)
		}

		if newRID != nil {
			//fmt.Printf("UpdateTuple at UpdateExecuter::Next moved record position! oldRID:%v newRID:%v\n", *rid, *newRID)
			common.NewRIDAtNormal = true
		}

		if !isUpdated || updateErr != nil {
			wrapErr := errors.New("tuple update failed. PageID:SlotNum = " + string(rid.GetPageID()) + ":" + fmt.Sprint(rid.GetSlotNum()))
			e.txn.SetState(access.ABORTED)
			return nil, false, wrapErr
		}

		colNum := e.child.GetTableMetaData().GetColumnNum()
		updateIdxs := e.plan.GetUpdateColIdxs()
		for ii := 0; ii < int(colNum); ii++ {
			ret := e.child.GetTableMetaData().GetIndex(ii)
			if ret == nil {
				continue
			} else {
				idx := ret
				if updateIdxs == nil || samehada_util.IsContainList[int](updateIdxs, ii) {
					if newRID != nil {
						idx.UpdateEntry(t, *rid, updateTuple, *newRID, e.txn)
					} else {
						idx.UpdateEntry(t, *rid, updateTuple, *rid, e.txn)
					}
				} else {
					if newRID != nil {
						idx.UpdateEntry(t, *rid, updateTuple, *newRID, e.txn)
					} else {
						// do nothing
					}
				}
			}
		}

		return newTuple, false, updateErr
	}

	return nil, true, nil
}

func (e *UpdateExecutor) GetOutputSchema() *schema.Schema {
	return e.plan.OutputSchema()
}

func (e *UpdateExecutor) GetTableMetaData() *catalog.TableMetadata {
	return e.child.GetTableMetaData()
}
