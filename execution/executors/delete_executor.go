package executors

import (
	"errors"
	"fmt"

	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
)

/**
 * DeleteExecutor executes a sequential scan over a table and delete tuples according to predicate.
 */
type DeleteExecutor struct {
	context *ExecutorContext
	plan    *plans.DeletePlanNode
	//tableMetadata *catalog.TableMetadata
	//it            *access.TableHeapIterator
	child Executor
	txn   *access.Transaction
}

func NewDeleteExecutor(context *ExecutorContext, plan *plans.DeletePlanNode, child Executor) Executor {
	//tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())

	return &DeleteExecutor{context, plan, child, context.GetTransaction()}
}

func (e *DeleteExecutor) Init() {
	//e.it = e.tableMetadata.Table().Iterator(e.txn)
	e.child.Init()
}

// Next implements the next method for the sequential scan operator
// It uses the table heap iterator to iterate through the table heap
// tyring to find a tuple to be deleted. It performs selection on-the-fly
// if find tuple to be delete, mark it to be deleted at commit and return value
func (e *DeleteExecutor) Next() (*tuple.Tuple, Done, error) {

	// iterates through the table heap trying to select a tuple that matches the predicate
	for t, done, err := e.child.Next(); !done; t, done, err = e.child.Next() {
		if t == nil {
			err_ := errors.New("e.it.Next returned nil")
			e.txn.SetState(access.ABORTED)
			return nil, true, err_
		}
		if err != nil {
			e.txn.SetState(access.ABORTED)
			return nil, true, err
		}

		rid := t.GetRID()
		tableMetadata := e.child.GetTableMetaData()
		is_marked := tableMetadata.Table().MarkDelete(rid, tableMetadata.OID(), e.txn)
		if !is_marked {
			err := errors.New("marking tuple deleted failed. PageId:SlotNum = " + string(rid.GetPageId()) + ":" + fmt.Sprint(rid.GetSlotNum()))
			e.txn.SetState(access.ABORTED)
			return nil, false, err
		}

		// removing index entry is done at commit phase because delete operation uses marking technique

		colNum := tableMetadata.GetColumnNum()
		for ii := 0; ii < int(colNum); ii++ {
			ret := tableMetadata.GetIndex(ii)
			if ret == nil {
				continue
			} else {
				index_ := ret
				//index_.DeleteEntry(e.it.Current(), *rid, e.txn)
				index_.DeleteEntry(t, *rid, e.txn)
			}
		}

		return t, false, nil
	}

	return nil, true, nil
}

//// select evaluates an expression on the tuple
//func (e *DeleteExecutor) selects(tuple *tuple.Tuple, predicate expression.Expression) bool {
//	return predicate == nil || predicate.Evaluate(tuple, e.tableMetadata.Schema()).ToBoolean()
//}

func (e *DeleteExecutor) GetOutputSchema() *schema.Schema { return e.plan.OutputSchema() }

func (e *DeleteExecutor) GetTableMetaData() *catalog.TableMetadata { return e.child.GetTableMetaData() }
