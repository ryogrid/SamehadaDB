package executors

import (
	"errors"
	"fmt"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/page"

	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
)

/**
 * UpdateExecutor executes a sequential scan over a table and update tuples according to predicate.
 */
type UpdateExecutor struct {
	context *ExecutorContext
	plan    *plans.UpdatePlanNode
	//tableMetadata *catalog.TableMetadata
	//it            *access.TableHeapIterator
	child Executor
	txn   *access.Transaction
}

func NewUpdateExecutor(context *ExecutorContext, plan *plans.UpdatePlanNode, child Executor) Executor {
	//tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())

	//return &UpdateExecutor{context, plan, tableMetadata, nil, context.GetTransaction()}
	return &UpdateExecutor{context, plan, child, context.GetTransaction()}
}

func (e *UpdateExecutor) Init() {
	//e.it = e.tableMetadata.Table().Iterator(e.txn)
	e.child.Init()
}

// Next implements the next method for the sequential scan operator
// It uses the table heap iterator to iterate through the table heap
// tyring to find a tuple to be updated. It performs selection on-the-fly
func (e *UpdateExecutor) Next() (*tuple.Tuple, Done, error) {

	// t is tuple before update
	for t, done, err := e.child.Next(); !done; t, done, err = e.child.Next() {
		if t == nil {
			err_ := errors.New("e.it.Next returned nil")
			return nil, true, err_
		}
		if err != nil {
			return nil, true, err
		}
		if e.txn.GetState() == access.ABORTED {
			return nil, true, err
		}

		rid := t.GetRID()
		values := e.plan.GetRawValues()
		new_tuple := tuple.NewTupleFromSchema(values, e.child.GetTableMetaData().Schema())

		var is_updated bool = false
		var new_rid *page.RID = nil
		var updateErr error = nil
		var updateTuple *tuple.Tuple
		if e.plan.GetUpdateColIdxs() == nil {
			is_updated, new_rid, updateErr, updateTuple = e.child.GetTableMetaData().Table().UpdateTuple(new_tuple, nil, nil, e.child.GetTableMetaData().OID(), *rid, e.txn)
		} else {
			is_updated, new_rid, updateErr, updateTuple = e.child.GetTableMetaData().Table().UpdateTuple(new_tuple, e.plan.GetUpdateColIdxs(), e.child.GetTableMetaData().Schema(), e.child.GetTableMetaData().OID(), *rid, e.txn)
		}

		if !is_updated && updateErr != access.ErrPartialUpdate {
			err_ := errors.New("tuple update failed. PageId:SlotNum = " + string(rid.GetPageId()) + ":" + fmt.Sprint(rid.GetSlotNum()))
			return nil, false, err_
		}

		colNum := e.child.GetTableMetaData().GetColumnNum()
		updateIdxs := e.plan.GetUpdateColIdxs()
		for ii := 0; ii < int(colNum); ii++ {
			ret := e.child.GetTableMetaData().GetIndex(ii)
			if ret == nil {
				continue
			} else {
				index_ := ret
				if updateIdxs == nil || samehada_util.IsContainList[int](updateIdxs, ii) {
					if new_rid != nil {
						// when tuple is moved page location on update, RID is changed to new value
						// removing index entry is done at commit phase because delete operation uses marking technique

						//index_.DeleteEntry(t, *rid, e.txn)
						if updateErr != access.ErrPartialUpdate {
							fmt.Println("UpdateExecuter: index entry insert with new_rid.")
							//index_.InsertEntry(new_tuple, *new_rid, e.txn)
							index_.InsertEntry(updateTuple, *new_rid, e.txn)
						}
					} else {
						index_.DeleteEntry(t, *rid, e.txn)
						//index_.InsertEntry(new_tuple, *rid, e.txn)
						index_.InsertEntry(updateTuple, *rid, e.txn)
					}
				} else {
					if new_rid != nil {
						// when tuple is moved page location on update, RID is changed to new value
						// removing index entry is done at commit phase because delete operation uses marking technique

						//index_.DeleteEntry(t, *rid, e.txn)
						if updateErr != access.ErrPartialUpdate {
							fmt.Println("UpdateExecuter: index entry insert with new_rid. value update of index entry occurs.")
							//index_.InsertEntry(t, *new_rid, e.txn)
							index_.InsertEntry(updateTuple, *new_rid, e.txn)
						}
					} else {
						// update is not needed
					}
				}
			}
		}

		return new_tuple, false, updateErr
	}

	return nil, true, nil
}

//// select evaluates an expression on the tuple
//func (e *UpdateExecutor) selects(tuple *tuple.Tuple, predicate expression.Expression) bool {
//	return predicate == nil || predicate.Evaluate(tuple, e.child.GetTableMetaData().Schema()).ToBoolean()
//}

func (e *UpdateExecutor) GetOutputSchema() *schema.Schema {
	return e.plan.OutputSchema()
}

func (e *UpdateExecutor) GetTableMetaData() *catalog.TableMetadata {
	//return e.tableMetadata
	return e.child.GetTableMetaData()
}
