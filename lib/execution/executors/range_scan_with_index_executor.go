package executors

import (
	"errors"
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/index"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

/**
 * RangeScanWithIndexExecutor executes scan with hash index to filter rows matches predicate.
 */
type RangeScanWithIndexExecutor struct {
	context       *ExecutorContext
	plan          *plans.RangeScanWithIndexPlanNode
	tableMetadata *catalog.TableMetadata
	txn           *access.Transaction
	ridItr        index.IndexRangeScanIterator
}

func NewRangeScanWithIndexExecutor(context *ExecutorContext, plan *plans.RangeScanWithIndexPlanNode) Executor {
	tableMetadata := context.GetCatalog().GetTableByOID(plan.GetTableOID())

	return &RangeScanWithIndexExecutor{context, plan, tableMetadata, context.GetTransaction(), nil}
}

func (e *RangeScanWithIndexExecutor) Init() {
	sch := e.tableMetadata.Schema()

	indexColNum := int(e.plan.GetColIdx())
	idx := e.tableMetadata.GetIndex(indexColNum)

	dummyTupleStart := tuple.GenTupleForIndexSearch(sch, uint32(indexColNum), e.plan.GetStartRange())
	dummyTupleEnd := tuple.GenTupleForIndexSearch(sch, uint32(indexColNum), e.plan.GetEndRange())
	e.ridItr = idx.GetRangeScanIterator(dummyTupleStart, dummyTupleEnd, e.txn)
}

// Next implements the next method for the sequential scan operator
// It uses the table heap iterator to iterate through the table heap
// tyring to find a tuple. It performs selection and projection on-the-fly
func (e *RangeScanWithIndexExecutor) Next() (*tuple.Tuple, Done, error) {
	// iterates through the RIDs got from index
	var tpl *tuple.Tuple = nil
	var err error = nil

	indexColNum := int(e.plan.GetColIdx())
	idx := e.tableMetadata.GetIndex(indexColNum)
	orgKeyType := idx.GetMetadata().GetTupleSchema().GetColumn(uint32(indexColNum)).GetType()

	for done, _, key, rid := e.ridItr.Next(); !done; done, _, key, rid = e.ridItr.Next() {
		tpl, err = e.tableMetadata.Table().GetTuple(rid, e.txn)
		if tpl == nil && (err == nil || err == access.ErrGeneral) {
			err := errors.New("e.ridItr.Next returned nil")
			e.txn.SetState(access.ABORTED)
			return nil, true, err
		}

		if err == access.ErrSelfDeletedCase {
			fmt.Println("RangeScanWithIndexExecutor:Next ErrSelfDeletedCase!")
			continue
		}

		if e.txn.GetState() == access.ABORTED {
			return nil, true, access.ErrGeneral
		}

		// check value update after getting iterator which contains snapshot of RIDs and Keys which were stored in Index
		curKeyVal := tpl.GetValue(e.tableMetadata.Schema(), uint32(e.plan.GetColIdx()))
		switch idx.(type) {
		case *index.UniqSkipListIndex:
			if !curKeyVal.CompareEquals(*key) {
				// column value corresponding index key is updated
				e.txn.SetState(access.ABORTED)
				return nil, true, errors.New("detect value update after iterator created. changes transaction state to aborted.")
			}
		case *index.SkipListIndex:
			orgKey := samehada_util.ExtractOrgKeyFromDicOrderComparableEncodedVarchar(key, orgKeyType)

			if !curKeyVal.CompareEquals(*orgKey) {
				// column value corresponding index key is updated
				e.txn.SetState(access.ABORTED)
				return nil, true, errors.New("detect value update after iterator created. changes transaction state to aborted.")
			}
		case *index.BTreeIndex:
			//orgKey := samehada_util.ExtractOrgKeyFromDicOrderComparableEncodedVarchar(key, orgKeyType)

			//if !curKeyVal.CompareEquals(*orgKey) {

			// when BTreeIndex is used, key is original key
			if !curKeyVal.CompareEquals(*key) {
				// column value corresponding index key is updated
				e.txn.SetState(access.ABORTED)
				return nil, true, errors.New("detect value update after iterator created. changes transaction state to aborted.")
			}
		}

		// check predicate
		if e.selects(tpl, e.plan.GetPredicate()) {
			tpl.SetRID(rid)
			break
		}
	}

	// roop above passed because done is true
	if tpl == nil {
		return nil, true, nil
	}

	// tpl is projected to OutputSchema
	ret := e.projects(tpl)
	ret.SetRID(tpl.GetRID())

	return ret, false, nil
}

// select evaluates an expression on the tuple
func (e *RangeScanWithIndexExecutor) selects(tuple *tuple.Tuple, predicate expression.Expression) bool {
	return predicate == nil || predicate.Evaluate(tuple, e.tableMetadata.Schema()).ToBoolean()
}

// project applies the projection operator defined by the output schema
func (e *RangeScanWithIndexExecutor) projects(tpl *tuple.Tuple) *tuple.Tuple {
	outputSchema := e.plan.OutputSchema()

	values := []types.Value{}
	for i := uint32(0); i < outputSchema.GetColumnCount(); i++ {
		colIndex := e.tableMetadata.Schema().GetColIndex(outputSchema.GetColumns()[i].GetColumnName())
		values = append(values, tpl.GetValue(e.tableMetadata.Schema(), colIndex))
	}

	ret := tuple.NewTupleFromSchema(values, outputSchema)
	ret.SetRID(tpl.GetRID())
	return ret
}

func (e *RangeScanWithIndexExecutor) GetOutputSchema() *schema.Schema {
	return e.plan.OutputSchema()
}

func (e *RangeScanWithIndexExecutor) GetTableMetaData() *catalog.TableMetadata {
	return e.tableMetadata
}
