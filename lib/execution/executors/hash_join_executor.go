package executors

import (
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/container/hash"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/materialization"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

/**
* HashJoinExecutor executes hash join operations (inner join).
 */
type HashJoinExecutor struct {
	context *ExecutorContext
	/** The hash join plan node. */
	plan_ *plans.HashJoinPlanNode
	/** The hash table that we are using. */
	jht_ *SimpleHashJoinHashTable
	/** The number of buckets in the hash table. */
	jht_num_buckets_ uint32 //= 2
	left_            Executor
	right_           Executor
	left_expr_       expression.Expression
	right_expr_      expression.Expression
	tmp_tuples_      []materialization.TmpTuple
	index_           int32
	output_exprs_    []expression.Expression
	tmp_page_ids_    []types.PageID
	right_tuple_     tuple.Tuple
}

/**
* Creates a new hash join executor.
* @param exec_ctx the context that the hash join should be performed in
* @param plan the hash join plan node
* @param left the left child, used by convention to build the hash table
* @param right the right child, used by convention to probe the hash table
 */
func NewHashJoinExecutor(exec_ctx *ExecutorContext, plan *plans.HashJoinPlanNode, left Executor,
	right Executor) *HashJoinExecutor {
	ret := new(HashJoinExecutor)
	ret.plan_ = plan
	ret.context = exec_ctx
	ret.left_ = left
	ret.right_ = right
	// about 200k entry can be stored
	ret.jht_num_buckets_ = 100
	ret.jht_ = NewSimpleHashJoinHashTable()
	return ret
}

func (e *HashJoinExecutor) GetJHT() *SimpleHashJoinHashTable { return e.jht_ }

func (e *HashJoinExecutor) GetOutputSchema() *schema.Schema { return e.plan_.OutputSchema() }

func (e *HashJoinExecutor) Init() {
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
	// build hash table from left
	e.left_.Init()
	e.right_.Init()
	e.left_expr_ = e.plan_.OnPredicate().GetChildAt(0)
	e.right_expr_ = e.plan_.OnPredicate().GetChildAt(1)
	// store all the left tuples in tmp pages in that it can not fit in memory
	// use tmp tuple as the value of the hash table kv pair
	var tmp_page *materialization.TmpTuplePage = nil
	var tmp_page_id types.PageID = common.InvalidPageID
	var tmp_tuple materialization.TmpTuple
	for left_tuple, done, _ := e.left_.Next(); !done; left_tuple, done, _ = e.left_.Next() {
		if left_tuple == nil {
			return
		}
		if tmp_page == nil || !tmp_page.Insert(left_tuple, &tmp_tuple) {
			// unpin the last full tmp page
			if tmp_page_id != common.InvalidPageID {
				e.context.GetBufferPoolManager().UnpinPage(tmp_page_id, true)
			}
			// create new tmp page
			page := e.context.GetBufferPoolManager().NewPage()
			tmp_page = materialization.CastPageAsTmpTuplePage(page)
			if tmp_page == nil {
				panic("fail to create new tmp page when doing hash join")
			}
			tmp_page.Init(tmp_page.GetPageId(), common.PageSize)
			tmp_page_id = tmp_page.GetPageId()
			e.tmp_page_ids_ = append(e.tmp_page_ids_, tmp_page_id)
			// reinsert the tuple
			tmp_page.Insert(left_tuple, &tmp_tuple)
		}
		valueAsKey := e.left_expr_.Evaluate(left_tuple, e.left_.GetOutputSchema())
		if !valueAsKey.IsNull() {
			e.jht_.Insert(hash.HashValue(&valueAsKey), &tmp_tuple)
		}
	}
}

// TODO: (SDB) need to refactor HashJoinExecutor::Next method to use GetExpr method of Column class
//
//	current impl is avoiding the method because it does not exist when this code was wrote
func (e *HashJoinExecutor) Next() (*tuple.Tuple, Done, error) {
	inner_next_cnt := 0
	for {
		for int(e.index_) == len(e.tmp_tuples_) {
			// we have traversed all possible join combination of the current right tuple
			// move to the next right tuple
			e.tmp_tuples_ = []materialization.TmpTuple{}
			e.index_ = 0
			var done Done = false
			var tmp_tuple *tuple.Tuple
			if tmp_tuple, done, _ = e.right_.Next(); done {
				// hash join finished, delete all the tmp page we created
				for _, tmp_page_id := range e.tmp_page_ids_ {
					e.context.GetBufferPoolManager().DeallocatePage(tmp_page_id, true)
				}
				// tmp_tuple should be nil
				return tmp_tuple, true, nil
			}
			inner_next_cnt++
			e.right_tuple_ = *tmp_tuple
			value := e.right_expr_.Evaluate(&e.right_tuple_, e.right_.GetOutputSchema())
			if value.IsNull() {
				continue
			}
			e.tmp_tuples_ = e.jht_.GetValue(hash.HashValue(&value))
		}
		// traverse corresponding left tuples stored in the tmp pages until we find a tuple which satisfies the predicate with current right tuple
		left_tmp_tuple := e.tmp_tuples_[e.index_]
		var left_tuple tuple.Tuple
		materialization.FetchTupleFromTmpTuplePage(e.context.bpm, &left_tuple, &left_tmp_tuple)
		for !e.IsValidCombination(&left_tuple, &e.right_tuple_) {
			e.index_++
			if int(e.index_) == len(e.tmp_tuples_) {
				break
			}
			left_tmp_tuple = e.tmp_tuples_[e.index_]
			materialization.FetchTupleFromTmpTuplePage(e.context.bpm, &left_tuple, &left_tmp_tuple)
		}
		if int(e.index_) < len(e.tmp_tuples_) {
			// valid combination found
			ret_tuple := e.MakeOutputTuple(&left_tuple, &e.right_tuple_)
			e.index_++
			return ret_tuple, false, nil
		}
		// no valid combination, turn to the next right tuple by for loop
	}
}

func (e *HashJoinExecutor) IsValidCombination(left_tuple *tuple.Tuple, right_tuple *tuple.Tuple) bool {
	return e.plan_.OnPredicate().EvaluateJoin(left_tuple, e.left_.GetOutputSchema(), right_tuple, e.right_.GetOutputSchema()).ToBoolean()
}

func (e *HashJoinExecutor) MakeOutputTuple(left_tuple *tuple.Tuple, right_tuple *tuple.Tuple) *tuple.Tuple {
	output_column_cnt := int(e.GetOutputSchema().GetColumnCount())
	values := make([]types.Value, output_column_cnt)
	for i := 0; i < output_column_cnt; i++ {
		values[i] =
			e.output_exprs_[i].EvaluateJoin(left_tuple, e.left_.GetOutputSchema(), right_tuple, e.right_.GetOutputSchema())
	}
	return tuple.NewTupleFromSchema(values, e.GetOutputSchema())
}

// can not be used
func (e *HashJoinExecutor) GetTableMetaData() *catalog.TableMetadata { return nil }

type SimpleHashJoinHashTable struct {
	hash_table_ map[uint32][]materialization.TmpTuple
}

func NewSimpleHashJoinHashTable() *SimpleHashJoinHashTable {
	return &SimpleHashJoinHashTable{hash_table_: make(map[uint32][]materialization.TmpTuple)}
}

/**
 * Inserts a (hash key, tuple) pair into the hash table.
 * @param txn the transaction that we execute in
 * @param h the hash key
 * @param t the tuple to associate with the key
 * @return true if the insert succeeded
 */
func (jht *SimpleHashJoinHashTable) Insert(h uint32, t *materialization.TmpTuple) bool {
	if jht.hash_table_[h] == nil {
		vals := make([]materialization.TmpTuple, 0)
		vals = append(vals, *t)
		jht.hash_table_[h] = vals
	} else {
		jht.hash_table_[h] = append(jht.hash_table_[h], *t)
	}

	return true
}

/**
 * Gets the values in the hash table that match the given hash key.
 * @param txn the transaction that we execute in
 * @param h the hash key
 * @param[out] t the list of tuples that matched the key
 */
func (jht *SimpleHashJoinHashTable) GetValue(h uint32) []materialization.TmpTuple {
	return jht.hash_table_[h]
}
