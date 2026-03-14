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
	expr := expression.NewComparison(tmpColVal, expression.NewConstantValue(*getKeyVal, getKeyVal.ValueType()), expression.Equal, types.Boolean)
	return plans.NewPointScanWithIndexPlanNode(c, scanTblSchema, expr.(*expression.Comparison), scanTblOID)
}

type IndexJoinExecutor struct {
	context       *ExecutorContext
	plan         *plans.IndexJoinPlanNode
	left         Executor
	leftExpr    expression.Expression
	rightExpr   expression.Expression
	retTuples     []*tuple.Tuple
	curIdx        int32
	outputExprs []expression.Expression
}

func NewIndexJoinExecutor(execCtx *ExecutorContext, plan *plans.IndexJoinPlanNode, left Executor) *IndexJoinExecutor {
	ret := new(IndexJoinExecutor)
	ret.plan = plan
	ret.context = execCtx
	ret.left = left
	ret.retTuples = make([]*tuple.Tuple, 0)
	return ret
}

func (e *IndexJoinExecutor) GetOutputSchema() *schema.Schema { return e.plan.OutputSchema() }

func (e *IndexJoinExecutor) Init() {
	rightTblOID := e.plan.GetRightTableOID()
	rightTblMetadata := e.context.catalog.GetTableByOID(rightTblOID)
	// this schema is from table definition. not from child plan's output schema.
	rightTblSchema := rightTblMetadata.Schema()
	rightTblColIdx := e.plan.OnPredicate().GetChildAt(1).(*expression.ColumnValue).GetColIndex()

	executionEngine := &ExecutionEngine{}
	executorContext := NewExecutorContext(e.context.catalog, e.context.bpm, e.context.txn)

	// get exprs to evaluate to output result
	outputColumnCnt := int(e.GetOutputSchema().GetColumnCount())
	for i := 0; i < outputColumnCnt; i++ {
		col := e.GetOutputSchema().GetColumn(uint32(i))
		var colVal expression.Expression
		if col.IsLeft() {
			colname := col.GetColumnName()
			colIndex := e.plan.GetLeftPlan().OutputSchema().GetColIndex(colname)
			colVal = expression.NewColumnValue(0, colIndex, types.Invalid)
		} else {
			colname := col.GetColumnName()
			colIndex := rightTblSchema.GetColIndex(colname)
			colVal = expression.NewColumnValue(1, colIndex, types.Invalid)
		}

		e.outputExprs = append(e.outputExprs, colVal)
	}
	e.left.Init()
	e.leftExpr = e.plan.OnPredicate().GetChildAt(0)
	e.rightExpr = e.plan.OnPredicate().GetChildAt(1)

	// use value of Value::ToIFValue() as key
	rightTuplesCache := make(map[interface{}]*[]*tuple.Tuple, 0)
	for leftTuple, done, _ := e.left.Next(); !done; leftTuple, done, _ = e.left.Next() {
		if leftTuple == nil {
			return
		}
		leftValueAsKey := e.leftExpr.Evaluate(leftTuple, e.left.GetOutputSchema())

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
		for _, rightTuple := range foundTuples {
			// TODO: SDB [OPT] should be removed after debugging (on IndexJoinExecutor::Init)
			if !e.IsValidCombination(leftTuple, rightTuple, rightTblSchema) {
				panic("Invalid combination!")
			}
			e.retTuples = append(e.retTuples, e.MakeOutputTuple(leftTuple, rightTuple, rightTblSchema))
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

func (e *IndexJoinExecutor) IsValidCombination(leftTuple *tuple.Tuple, rightTuple *tuple.Tuple, rightOrgSchema *schema.Schema) bool {
	return e.plan.OnPredicate().EvaluateJoin(leftTuple, e.left.GetOutputSchema(), rightTuple, rightOrgSchema).ToBoolean()
}

func (e *IndexJoinExecutor) MakeOutputTuple(leftTuple *tuple.Tuple, rightTuple *tuple.Tuple, rightOrgSchema *schema.Schema) *tuple.Tuple {
	outputColumnCnt := int(e.GetOutputSchema().GetColumnCount())
	values := make([]types.Value, outputColumnCnt)
	for i := 0; i < outputColumnCnt; i++ {
		values[i] =
			e.outputExprs[i].EvaluateJoin(leftTuple, e.left.GetOutputSchema(), rightTuple, rightOrgSchema)
	}
	return tuple.NewTupleFromSchema(values, e.GetOutputSchema())
}

// can not be used
func (e *IndexJoinExecutor) GetTableMetaData() *catalog.TableMetadata {
	panic("IndexJoinExecutor::GetTableMetaData() should not be called")
}
