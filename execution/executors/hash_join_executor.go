package executors

import (
	"github.com/ryogrid/SamehadaDB/container/hash"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

// TODO(student): when you are ready to attempt task 3, replace the using declaration!
// using HT = SimpleHashJoinHashTable

// using HashJoinKeyType = Value
// using HashJoinValType = TmpTuple
// using HT = LinearProbeHashTable<HashJoinKeyType, HashJoinValType, ValueComparator>

/**
* HashJoinExecutor executes hash join operations.
 */
type HashJoinExecutor struct {
	context *ExecutorContext
	/** The hash join plan node. */
	plan_ *plans.HashJoinPlanNode
	/** The comparator is used to compare hashes. */
	//ValueComparator jht_comp
	/** The identity hash function. */
	//HashFunction<Value> jht_hash_fn_{}

	/** The hash table that we are using. */
	jht_ *hash.LinearProbeHashTable
	/** The number of buckets in the hash table. */
	jht_num_buckets_ uint32 //= 2
	left_            *Executor
	right_           *Executor
	left_expr_       *expression.Expression
	right_expr_      *expression.Expression
	tmp_tuples_      []TmpTuple
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
func NewHashJoinExecutor(exec_ctx *ExecutorContext, plan *plans.HashJoinPlanNode, left *Executor,
	right *Executor) *HashJoinExecutor {
	//retun &HashJoinExecutor{exec_ctx, plan, }
	ret := new(HashJoinExecutor)
	ret.context = exec_ctx
	ret.left_ = left
	ret.right_ = right
	// about 200k entry can be stored
	ret.jht_num_buckets_ = 100
	ret.jht_ = hash.NewLinearProbeHashTable(exec_ctx.GetBufferPoolManager(), ret.jht_num_buckets_)
	return ret
}

/** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
func (e *HashJoinExecutor) GetJHT() *hash.LinearProbeHashTable { return jht_ }

func (e *HashJoinExecutor) GetOutputSchema() *schema.Schema { return e.plan_.OutputSchema() }

func (e *HashJoinExecutor) Init() {
	// get exprs to evaluate to output result
	output_column_cnt := GetOutputSchema().GetColumnCount()
	e.output_exprs_.resize(output_column_cnt)
	for i := 0; i < output_column_cnt; i++ {
		e.output_exprs_[i] = GetOutputSchema().GetColumn(i).GetExpr()
	}
	// build hash table from left
	e.left_.Init()
	e.right_.Init()
	e.left_expr_ = e.plan_.Predicate().GetChildAt(0)
	e.right_expr_ = e.plan_.Predicate().GetChildAt(1)
	var left_tuple tuple.Tuple
	// store all the left tuples in tmp pages in that it can not fit in memory
	// use tmp tuple as the value of the hash table kv pair
	var tmp_page *TmpTuplePage = nil
	tmp_page_id := INVALID_PAGE_ID
	var tmp_tuple TmpTuple
	for e.left_.Next(&left_tuple) {
		if !tmp_page || !tmp_page.Insert(left_tuple, &tmp_tuple) {
			// unpin the last full tmp page
			if tmp_page_id != INVALID_PAGE_ID {
				e.context.GetBufferPoolManager().UnpinPage(tmp_page_id, true)
			}
			// create new tmp page
			tmp_page = (*TmpTuplePage)(e.context.GetBufferPoolManager().NewPage(&tmp_page_id))
			if !tmp_page {
				panic("fail to create new tmp page when doing hash join")
			}
			tmp_page.Init(tmp_page_id, PAGE_SIZE)
			e.tmp_page_ids_ = append(e.tmp_page_ids_, (tmp_page_id))
			// reinsert the tuple
			//assert(tmp_page.Insert(left_tuple, &tmp_tuple))
			tmp_page.Insert(left_tuple, &tmp_tuple)
		}
		value := e.left_expr_.Evaluate(&left_tuple, e.left_.GetOutputSchema())
		e.jht_.Insert(e.context.GetTransaction(), value, tmp_tuple)
	}
}

func (e *HashJoinExecutor) Next(tuple_ *tuple.Tuple) bool {
	for true {
		for e.index_ == e.tmp_tuples_.size() {
			// we have traversed all possible join combination of the current right tuple
			// move to the next right tuple
			e.tmp_tuples_.clear()
			e.index_ = 0
			if !e.right_.Next(e.right_tuple_) {
				// hash join finished, delete all the tmp page we created
				for tmp_page_id := range e.tmp_page_ids_ {
					e.context.GetBufferPoolManager().DeletePage(tmp_page_id)
				}
				return false
			}
			value := e.right_expr_.Evaluate(&e.right_tuple_, e.right_.GetOutputSchema())
			e.jht_.GetValue(e.context.GetTransaction(), value, &e.tmp_tuples_)
		}
		// traverse corresponding left tuples stored in the tmp pages util we find one satisfying the predicate with current right tuple
		left_tmp_tuple := e.tmp_tuples_[e.index_]
		var left_tuple tuple.Tuple
		FetchTupleFromTmpTuplePage(&left_tuple, left_tmp_tuple)
		for !IsValidCombination(left_tuple, e.right_tuple_) {
			e.index_++
			if e.index_ == e.tmp_tuples_.size() {
				break
			}
			left_tmp_tuple = e.tmp_tuples_[e.index_]
			FetchTupleFromTmpTuplePage(&left_tuple, left_tmp_tuple)
		}
		if e.index_ < e.tmp_tuples_.size() {
			// valid combination found
			MakeOutputTuple(tuple_, &left_tuple, &e.right_tuple_)
			e.index_++
			return true
		}
		// no valid combination, turn to the next right tuple by for loop
	}
}

func (e *HashJoinExecutor) FetchTupleFromTmpTuplePage(tuple_ *tuple.Tuple, tmp_tuple *TmpTuple) {
	tmp_page := (*TmpTuplePage)(e.context.GetBufferPoolManager().FetchPage(tmp_tuple.GetPageId()))
	if !tmp_page { /*throw std::runtime_error("fail to fetch tmp page when doing hash join")*/
	}
	tmp_page.Get(tuple_, tmp_tuple.GetOffset())
	e.context.GetBufferPoolManager().UnpinPage(tmp_tuple.GetPageId(), false)
}

func (e *HashJoinExecutor) IsValidCombination(left_tuple *tuple.Tuple, right_tuple *tuple.Tuple) bool {
	return e.plan_.Predicate().EvaluateJoin(&left_tuple, e.left_.GetOutputSchema(), &right_tuple, e.right_.GetOutputSchema())
	//.GetAs>()
}

func (e *HashJoinExecutor) MakeOutputTuple(output_tuple *tuple.Tuple, left_tuple *tuple.Tuple, tuple_ *tuple.Tuple, right_tuple *tuple.Tuple) {
	output_column_cnt := GetOutputSchema().GetColumnCount()
	values := make([]types.Value, output_column_cnt)
	for i := 0; i < output_column_cnt; i++ {
		values[i] =
			e.output_exprs_[i].EvaluateJoin(left_tuple, e.left_.GetOutputSchema(), right_tuple, e.right_.GetOutputSchema())
	}
	output_tuple = tuple.NewTupleFromSchema(values, GetOutputSchema())
}

/**
* Hashes a tuple by evaluating it against every expression on the given schema, combining all non-null hashes.
* @param tuple tuple to be hashed
* @param schema schema to evaluate the tuple on
* @param exprs expressions to evaluate the tuple with
* @return the hashed tuple
 */
func (e *HashJoinExecutor) HashValues(tuple_ *tuple.Tuple, schema_ *schema.Schema, exprs []*expression.Expression) uint32 {
	var curr_hash uint32 = 0
	// For every expression,
	for expr := range exprs {
		// We evaluate the tuple on the expression and schema.
		val := expr.Evaluate(tuple_, schema_)
		// If this produces a value,
		if !val.IsNull() {
			// We combine the hash of that value into our current hash.
			curr_hash = hash.CombineHashes(curr_hash, hash.HashValue(&val))
		}
	}
	return curr_hash
}
