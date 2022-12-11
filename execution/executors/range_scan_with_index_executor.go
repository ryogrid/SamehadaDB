package executors

import (
	"errors"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/index"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * RangeScanWithIndexExecutor executes scan with hash index to filter rows matches predicate.
 */
type RangeScanWithIndexExecutor struct {
	context       *ExecutorContext
	plan          *plans.RangeScanWithIndexPlanNode
	tableMetadata *catalog.TableMetadata
	//it            *access.TableHeapIterator
	txn    *access.Transaction
	ridItr index.IndexRangeScanIterator
	//foundTuples []*tuple.Tuple
}

func NewRangeScanWithIndexExecutor(context *ExecutorContext, plan *plans.RangeScanWithIndexPlanNode) Executor {
	tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())

	return &RangeScanWithIndexExecutor{context, plan, tableMetadata, context.GetTransaction(), nil} //, make([]*tuple.Tuple, 0)}
}

func (e *RangeScanWithIndexExecutor) Init() {
	schema_ := e.tableMetadata.Schema()

	//colIdxOfPred := comparison.GetLeftSideColIdx()
	//colNum := int(e.tableMetadata.GetColumnNum())
	//var index_ index.Index = nil
	//var indexColNum int = -1
	//for {
	//	index_ = nil
	//	for ii := indexColNum + 1; ii < colNum; ii++ {
	//		ret := e.tableMetadata.GetIndex(ii)
	//		if ret == nil {
	//			continue
	//		} else {
	//			index_ = ret
	//			indexColNum = ii
	//			break
	//		}
	//	}
	//
	//	if index_ == nil || indexColNum == -1 {
	//		fmt.Printf("colIdxOfPred=%d,indexColNum=%d\n", colIdxOfPred, indexColNum)
	//		panic("RangeScanWithIndexExecutor assumes that table which has index are passed.")
	//	}
	//	if colIdxOfPred != uint32(indexColNum) {
	//		// find next index having column
	//		continue
	//	}
	//	break
	//}

	indexColNum := int(e.plan.GetColIdx())
	index_ := e.tableMetadata.GetIndex(indexColNum)

	dummyTupleStart := tuple.GenTupleForIndexSearch(schema_, uint32(indexColNum), e.plan.GetStartRange())
	dummyTupleEnd := tuple.GenTupleForIndexSearch(schema_, uint32(indexColNum), e.plan.GetEndRange())
	e.ridItr = index_.GetRangeScanIterator(dummyTupleStart, dummyTupleEnd, e.txn)

	// currently result num is one
	//rids := index_.ScanKey(dummyTuple, e.txn)
	//for _, rid := range rids {
	//	tuple_ := e.tableMetadata.Table().GetTuple(&rid, e.txn)
	//	if tuple_ == nil {
	//		e.foundTuples = make([]*tuple.Tuple, 0)
	//		return
	//	}
	//	e.foundTuples = append(e.foundTuples, tuple_)
	//}
}

//func (e *RangeScanWithIndexExecutor) Next() (*tuple.Tuple, Done, error) {
//	if len(e.foundTuples) > 0 {
//		tuple_ := e.foundTuples[0]
//		e.foundTuples = e.foundTuples[1:]
//		return e.projects(tuple_), false, nil
//	}
//
//	return nil, true, nil
//}

// Next implements the next method for the sequential scan operator
// It uses the table heap iterator to iterate through the table heap
// tyring to find a tuple. It performs selection and projection on-the-fly
func (e *RangeScanWithIndexExecutor) Next() (*tuple.Tuple, Done, error) {
	// iterates through the RIDs got from index
	var tuple_ *tuple.Tuple = nil
	var err error = nil
	for done, _, key, rid := e.ridItr.Next(); !done; done, _, key, rid = e.ridItr.Next() {
		tuple_, err = e.tableMetadata.Table().GetTuple(rid, e.txn)
		if tuple_ == nil && (err == nil || err == access.ErrGeneral) {
			err := errors.New("e.ridItr.Next returned nil")
			e.txn.SetState(access.ABORTED)
			return nil, true, err
		}

		if err == access.ErrSelfDeletedCase {
			continue
		}

		//// TODO: (SDB) temporal impl  due to other temporal code for handle self deleted tuple
		////             this impl must be modified with the temporal code modificatoin
		//if tuple_.Size() == 0 {
		//	// tuple has been deleted by other txn
		//	e.txn.SetState(access.ABORTED)
		//	return nil, true, errors.New("detect value delete after iterator created. changes transaction state to aborted.")
		//}

		// check value update after getting iterator which contains snapshot of RIDs and Keys which were stored in Index
		curKeyVal := tuple_.GetValue(e.tableMetadata.Schema(), uint32(e.plan.GetColIdx()))
		if !curKeyVal.CompareEquals(*key) {
			// column value corresponding index key is updated
			e.txn.SetState(access.ABORTED)
			return nil, true, errors.New("detect value update after iterator created. changes transaction state to aborted.")
		}
		// check predicate
		if e.selects(tuple_, e.plan.GetPredicate()) {
			tuple_.SetRID(rid)
			break
		}
	}

	// roop above passed because done is true
	if tuple_ == nil {
		return nil, true, nil
	}

	// tuple_ is projected to OutputSchema
	ret := e.projects(tuple_)
	ret.SetRID(tuple_.GetRID())

	return ret, false, nil
}

// select evaluates an expression on the tuple
func (e *RangeScanWithIndexExecutor) selects(tuple *tuple.Tuple, predicate expression.Expression) bool {
	return predicate == nil || predicate.Evaluate(tuple, e.tableMetadata.Schema()).ToBoolean()
}

// project applies the projection operator defined by the output schema
// It transform the tuple into a new tuple that corresponds to the output schema
func (e *RangeScanWithIndexExecutor) projects(tuple_ *tuple.Tuple) *tuple.Tuple {
	outputSchema := e.plan.OutputSchema()

	values := []types.Value{}
	for i := uint32(0); i < outputSchema.GetColumnCount(); i++ {
		colIndex := e.tableMetadata.Schema().GetColIndex(outputSchema.GetColumns()[i].GetColumnName())
		values = append(values, tuple_.GetValue(e.tableMetadata.Schema(), colIndex))
	}

	ret := tuple.NewTupleFromSchema(values, outputSchema)
	ret.SetRID(tuple_.GetRID())
	return ret
}

func (e *RangeScanWithIndexExecutor) GetOutputSchema() *schema.Schema {
	return e.plan.OutputSchema()
}

func (e *RangeScanWithIndexExecutor) GetTableMetaData() *catalog.TableMetadata {
	return e.tableMetadata
}
