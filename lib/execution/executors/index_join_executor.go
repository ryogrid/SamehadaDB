package executors

import (
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

func makePointScanPlanNodeForJoin(c *catalog.Catalog, getKeyVal *types.Value, scanTblSchema *schema.Schema, keyColIdx uint32, scanTblOID uint32) (createdPlan plans.Plan) {
	tmpColVal := new(expression.ColumnValue)
	tmpColVal.SetColIndex(keyColIdx)
	expression_ := expression.NewComparison(tmpColVal, expression.NewConstantValue(*getKeyVal, getKeyVal.ValueType()), expression.Equal, types.Boolean)
	return plans.NewPointScanWithIndexPlanNode(c, scanTblSchema, expression_.(*expression.Comparison), scanTblOID)
}

type IndexJoinExecutor struct {
	context       *ExecutorContext
	plan_         *plans.IndexJoinPlanNode
	left_         Executor
	left_expr_    expression.Expression
	right_expr_   expression.Expression
	retTuples     []*tuple.Tuple
	curIdx        int32
	output_exprs_ []expression.Expression
}

func NewIndexJoinExecutor(exec_ctx *ExecutorContext, plan *plans.IndexJoinPlanNode, left Executor) *IndexJoinExecutor {
	ret := new(IndexJoinExecutor)
	ret.plan_ = plan
	ret.context = exec_ctx
	ret.left_ = left
	ret.retTuples = make([]*tuple.Tuple, 0)
	return ret
}

func (e *IndexJoinExecutor) GetOutputSchema() *schema.Schema { return e.plan_.OutputSchema() }

func (e *IndexJoinExecutor) Init() {
	rightTblOID := e.plan_.GetRightTableOID()
	rightTblMetadata := e.context.catalog.GetTableByOID(rightTblOID)
	// this schema is from table definition. not from child plan's output schema.
	rightTblSchema := rightTblMetadata.Schema()
	rightTblColIdx := e.plan_.OnPredicate().GetChildAt(1).(*expression.ColumnValue).GetColIndex()

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(e.context.catalog, e.context.bpm, e.context.txn)

	// get exprs to evaluate to output result
	output_column_cnt := int(e.GetOutputSchema().GetColumnCount())
	for i := 0; i < output_column_cnt; i++ {
		column_ := e.GetOutputSchema().GetColumn(uint32(i))
		var colVal expression.Expression
		if column_.IsLeft() {
			colname := column_.GetColumnName()
			colIndex := e.plan_.GetLeftPlan().OutputSchema().GetColIndex(colname)
			colVal = expression.NewColumnValue(0, colIndex, types.Invalid)
		} else {
			colname := column_.GetColumnName()
			colIndex := rightTblSchema.GetColIndex(colname)
			colVal = expression.NewColumnValue(1, colIndex, types.Invalid)
		}

		e.output_exprs_ = append(e.output_exprs_, colVal)
	}
	e.left_.Init()
	e.left_expr_ = e.plan_.OnPredicate().GetChildAt(0)
	e.right_expr_ = e.plan_.OnPredicate().GetChildAt(1)

	// use value of Value::ToIFValue() as key
	rightTuplesCache := make(map[interface{}]*[]*tuple.Tuple, 0)
	for left_tuple, done, _ := e.left_.Next(); !done; left_tuple, done, _ = e.left_.Next() {
		if left_tuple == nil {
			return
		}
		leftValueAsKey := e.left_expr_.Evaluate(left_tuple, e.left_.GetOutputSchema())

		// find matching tuples from right table using point scan

		var foundTuples []*tuple.Tuple
		cachedTuples, ok := rightTuplesCache[leftValueAsKey.ToIFValue()]
		if ok {
			// already same key has been lookup
			foundTuples = *cachedTuples
		} else {
			pointScanPlan := makePointScanPlanNodeForJoin(e.context.catalog, &leftValueAsKey, rightTblSchema, rightTblColIdx, rightTblOID)
			foundTuplesTmp := executionEngine.Execute(pointScanPlan, executorContext)
			if e.context.txn.GetState() == access.ABORTED {
				return
			}
			if foundTuplesTmp == nil {
				return
			}
			if len(foundTuplesTmp) == 0 {
				continue
			}
			// cache point scaned tuples
			rightTuplesCache[leftValueAsKey.ToIFValue()] = &foundTuplesTmp
			foundTuples = foundTuplesTmp
		}

		// make joined tuples and store them
		for _, right_tuple := range foundTuples {
			// TODO: SDB [OPT] should be removed after debugging (on IndexJoinExecutor::Init)
			if !e.IsValidCombination(left_tuple, right_tuple, rightTblSchema) {
				panic("Invalid combination!")
			}
			e.retTuples = append(e.retTuples, e.MakeOutputTuple(left_tuple, right_tuple, rightTblSchema))
		}
	}
}

func (e *IndexJoinExecutor) Next() (*tuple.Tuple, Done, error) {
	if e.curIdx >= int32(len(e.retTuples)) {
		return nil, true, nil
	}
	ret := e.retTuples[e.curIdx]
	e.curIdx++
	return ret, false, nil
}

func (e *IndexJoinExecutor) IsValidCombination(left_tuple *tuple.Tuple, right_tuple *tuple.Tuple, right_org_schema *schema.Schema) bool {
	return e.plan_.OnPredicate().EvaluateJoin(left_tuple, e.left_.GetOutputSchema(), right_tuple, right_org_schema).ToBoolean()
}

func (e *IndexJoinExecutor) MakeOutputTuple(left_tuple *tuple.Tuple, right_tuple *tuple.Tuple, right_org_schema *schema.Schema) *tuple.Tuple {
	output_column_cnt := int(e.GetOutputSchema().GetColumnCount())
	values := make([]types.Value, output_column_cnt)
	for i := 0; i < output_column_cnt; i++ {
		values[i] =
			e.output_exprs_[i].EvaluateJoin(left_tuple, e.left_.GetOutputSchema(), right_tuple, right_org_schema)
	}
	return tuple.NewTupleFromSchema(values, e.GetOutputSchema())
}

// can not be used
func (e *IndexJoinExecutor) GetTableMetaData() *catalog.TableMetadata {
	panic("IndexJoinExecutor::GetTableMetaData() should not be called")
}
