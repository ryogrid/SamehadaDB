package executors

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

// TODO: (SDB) [OPT] not implmented yet (index_join_executor.go)

type IndexJoinExecutor struct {
	context       *ExecutorContext
	plan_         *plans.IndexJoinPlanNode
	left_         Executor
	right_        Executor
	left_expr_    expression.Expression
	right_expr_   expression.Expression
	retTuples     []*tuple.Tuple
	curIdx        int32
	output_exprs_ []expression.Expression
}

func NewIndexJoinExecutor(exec_ctx *ExecutorContext, plan *plans.IndexJoinPlanNode, left Executor,
	right Executor) *IndexJoinExecutor {
	ret := new(IndexJoinExecutor)
	ret.plan_ = plan
	ret.context = exec_ctx
	ret.left_ = left
	ret.right_ = right
	ret.retTuples = make([]*tuple.Tuple, 0)
	return ret
}

func (e *IndexJoinExecutor) GetOutputSchema() *schema.Schema { return e.plan_.OutputSchema() }

func (e *IndexJoinExecutor) Init() {
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
			colIndex := e.plan_.GetRightPlan().OutputSchema().GetColIndex(colname)
			colVal = expression.NewColumnValue(1, colIndex, types.Invalid)
		}

		e.output_exprs_ = append(e.output_exprs_, colVal)
	}
	e.left_.Init()
	e.right_.Init()
	e.left_expr_ = e.plan_.OnPredicate().GetChildAt(0)
	e.right_expr_ = e.plan_.OnPredicate().GetChildAt(1)
	// use value of Value::ToIFValue() as key
	rightTuplesCache := make(map[interface{}]*[]*tuple.Tuple, 0)
	for left_tuple, done, _ := e.left_.Next(); !done; left_tuple, done, _ = e.left_.Next() {
		if left_tuple == nil {
			return
		}
		leftValueAsKey := e.left_expr_.Evaluate(left_tuple, e.left_.GetOutputSchema())

		// TODO: (SDB) [OPT] need to create joined records using point scan of right table (IndexJoinExecutor::Init)
	}
}

// TODO: (SDB) need to refactor IndexJoinExecutor::Next method to use GetExpr method of Column class
func (e *IndexJoinExecutor) Next() (*tuple.Tuple, Done, error) {
	if e.curIdx >= int32(len(e.retTuples)) {
		return nil, true, nil
	}
	ret := e.retTuples[e.curIdx]
	e.curIdx++
	return ret, false, nil
}

func (e *IndexJoinExecutor) IsValidCombination(left_tuple *tuple.Tuple, right_tuple *tuple.Tuple) bool {
	return e.plan_.OnPredicate().EvaluateJoin(left_tuple, e.left_.GetOutputSchema(), right_tuple, e.right_.GetOutputSchema()).ToBoolean()
}

func (e *IndexJoinExecutor) MakeOutputTuple(left_tuple *tuple.Tuple, right_tuple *tuple.Tuple) *tuple.Tuple {
	output_column_cnt := int(e.GetOutputSchema().GetColumnCount())
	values := make([]types.Value, output_column_cnt)
	for i := 0; i < output_column_cnt; i++ {
		values[i] =
			e.output_exprs_[i].EvaluateJoin(left_tuple, e.left_.GetOutputSchema(), right_tuple, e.right_.GetOutputSchema())
	}
	return tuple.NewTupleFromSchema(values, e.GetOutputSchema())
}

// can not be used
func (e *IndexJoinExecutor) GetTableMetaData() *catalog.TableMetadata {
	panic("IndexJoinExecutor::GetTableMetaData() should not be called")
}
