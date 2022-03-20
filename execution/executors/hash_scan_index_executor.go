// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/index"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * HashScanIndexNode executes scan with hash index to filter rows matches predicate.
 */
type HashScanIndexExecutor struct {
	context       *ExecutorContext
	plan          *plans.HashScanIndexPlanNode
	tableMetadata *catalog.TableMetadata
	//it            *access.TableHeapIterator
	txn         *access.Transaction
	foundTuples []*tuple.Tuple
}

// TODO: (SDB) not implemented yet (HashScanIndexExecutor)

func NewHashScanIndexExecutor(context *ExecutorContext, plan *plans.HashScanIndexPlanNode) Executor {
	tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())
	//txn := access.NewTransaction(1)
	//catalog := context.GetCatalog()

	return &HashScanIndexExecutor{context, plan, tableMetadata, context.GetTransaction(), make([]*tuple.Tuple, 0)}
}

func (e *HashScanIndexExecutor) Init() {
	//e.it = e.tableMetadata.Table().Iterator(e.txn)

	comparison := e.plan.GetPredicate()
	schema_ := e.tableMetadata.Schema()
	colName := comparison.GetLeftSideValue(nil, schema_).ToVarchar()
	colIdxOfPred := schema_.GetColIndex(colName)

	colNum := int(e.tableMetadata.GetColumnNum())
	var index_ index.Index = nil
	var indexColNum int = -1
	for ii := 0; ii < colNum; ii++ {
		ret := e.tableMetadata.GetIndex(ii)
		if ret == nil {
			continue
		} else {
			index_ = *ret
			indexColNum = ii
			break
		}
	}

	if index_ == nil || indexColNum == -1 {
		panic("HashScanIndexExecutor assumes that table which has index are passed.")
	}
	if colIdxOfPred != uint32(indexColNum) {
		panic("HashScanIndexExecutor assumes that column which has index matches one specified on predicate.")
	}

	dummyTuple := 
	rids := index_.ScanKey()
	//e.tableMetadata.Table().GetTuple(e.context.txn)
	//comparison.
}

/*
// GetTuple reads a tuple from the table
func (t *TableHeap) GetTuplesWithIndexKey(key []byte, table_metadata *catalog.TableMetadata, txn *Transaction) *tuple.Tuple {
	// TODO: (SDB) need implment GetTuplesWithIndexKey
	panic("not implmented yet")
	// get Index class from table_metadata and get RIDs with it. then call GetTuple.
	page := CastPageAsTablePage(t.bpm.FetchPage(rid.GetPageId()))
	defer t.bpm.UnpinPage(page.ID(), false)
	return page.GetTuple(rid, t.log_manager, t.lock_manager, txn)
}
*/

func (e *HashScanIndexExecutor) Next() (*tuple.Tuple, Done, error) {
	/*
		// iterates through the table heap trying to select a tuple that matches the predicate
		for t := e.it.Current(); !e.it.End(); t = e.it.Next() {
			if e.selects(t, e.plan.GetPredicate()) {
				break
			}
		}

		// if the iterator is not in the end, projects the current tuple into the output schema
		if !e.it.End() {
			defer e.it.Next() // advances the iterator after projection
			return e.projects(e.it.Current()), false, nil
		}
	*/

	return nil, true, nil
}

// // select evaluates an expression on the tuple
// func (e *HashScanIndexExecutor) selects(tuple *tuple.Tuple, predicate *expression.Expression) bool {
// 	return predicate == nil || (*predicate).Evaluate(tuple, e.tableMetadata.Schema()).ToBoolean()
// }

// project applies the projection operator defined by the output schema
// It transform the tuple into a new tuple that corresponds to the output schema
func (e *HashScanIndexExecutor) projects(tuple_ *tuple.Tuple) *tuple.Tuple {
	outputSchema := e.plan.OutputSchema()

	values := []types.Value{}
	for i := uint32(0); i < outputSchema.GetColumnCount(); i++ {
		colIndex := e.tableMetadata.Schema().GetColIndex(outputSchema.GetColumns()[i].GetColumnName())
		values = append(values, tuple_.GetValue(e.tableMetadata.Schema(), colIndex))
	}

	return tuple.NewTupleFromSchema(values, outputSchema)
}
