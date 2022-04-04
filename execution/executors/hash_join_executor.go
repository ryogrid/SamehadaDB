package executors

import "github.com/ryogrid/SamehadaDB/storage/tuple"

// TODO(student): when you are ready to attempt task 3, replace the using declaration!
// using HT = SimpleHashJoinHashTable

// using HashJoinKeyType = Value
// using HashJoinValType = TmpTuple
// using HT = LinearProbeHashTable<HashJoinKeyType, HashJoinValType, ValueComparator>

/**
* HashJoinExecutor executes hash join operations.
 */
type HashJoinExecutor struct {
	/** The hash join plan node. */
	plan_ *plans.HashJoinPlanNode
	/** The comparator is used to compare hashes. */
	//ValueComparator jht_comp
	/** The identity hash function. */
	//HashFunction<Value> jht_hash_fn_{}

	/** The hash table that we are using. */
	jht_ HT
	/** The number of buckets in the hash table. */
	jht_num_buckets_ uint32 //= 2
	left_ *expression.Expression
	right_ *expression.Expression
	left_expr_ *expression.Expression
	right_expr_ *expression.Expression
	tmp_tuples_ []TmpTuple
	index_ int32
	output_exprs_ *expression.Expressions
	tmp_page_ids_ []types.PageID
	right_tuple_ tuple.Tuple
}

/**
* Creates a new hash join executor.
* @param exec_ctx the context that the hash join should be performed in
* @param plan the hash join plan node
* @param left the left child, used by convention to build the hash table
* @param right the right child, used by convention to probe the hash table
*/
func NewHashJoinExecutor(exec_ctx *ExecutorContext,  plan *plans.HashJoinPlanNode, left *Executor,
				right *Executor) *HashJoinExecutor {
	AbstractExecutor := exec_ctx
	plan_ := plan
	jht_ := "hash join", exec_ctx_.GetBufferPoolManager(), jht_comp_, jht_num_buckets_, jht_hash_fn_
	left_ := left
	right_ := right 
}
   
/** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
func (e *HashJoinExecutor) GetJHT() *HT { return &jht_ }

func (e *HashJoinExecutor)  GetOutputSchema() *schema.Schema { return plan_.OutputSchema() }

func (e *HashJoinExecutor) Init()  {
	// get exprs to evaluate to output result
	 output_column_cnt = GetOutputSchema().GetColumnCount()
	output_exprs_.resize(output_column_cnt)
	for (size_t i = 0; i < output_column_cnt; i++) {
		output_exprs_[i] = GetOutputSchema().GetColumn(i).GetExpr()
	}
	// build hash table from left
	left_.Init()
	right_.Init()
	left_expr_ = plan_.Predicate().GetChildAt(0)
	right_expr_ = plan_.Predicate().GetChildAt(1)
	Tuple left_tuple
	// store all the left tuples in tmp pages in that it can not fit in memory
	// use tmp tuple as the value of the hash table kv pair
	TmpTuplePage *tmp_page = nullptr
	page_id_t tmp_page_id = INVALID_PAGE_ID
	TmpTuple tmp_tuple
	for (left_.Next(&left_tuple)) {
		if (!tmp_page || !tmp_page.Insert(left_tuple,&tmp_tuple)) {
		// unpin the last full tmp page
		if (tmp_page_id != INVALID_PAGE_ID) exec_ctx_.GetBufferPoolManager().UnpinPage(tmp_page_id,true)
		// create new tmp page
		tmp_page = reinterpret_cast<TmpTuplePage *>(exec_ctx_.GetBufferPoolManager().NewPage(&tmp_page_id))
		if (!tmp_page) throw std::runtime_error("fail to create new tmp page when doing hash join")
		tmp_page.Init(tmp_page_id, PAGE_SIZE)
		tmp_page_ids_.push_back(tmp_page_id)
		// reinsert the tuple
		assert(tmp_page.Insert(left_tuple, &tmp_tuple))
		}
		Value value = left_expr_.Evaluate(&left_tuple, left_.GetOutputSchema())
		jht_.Insert(exec_ctx_.GetTransaction(), value, tmp_tuple)
	}
}

func (e *HashJoinExecutor) Next(tuple_ *tuple.Tuple) bool {
	for (true) {
		for (index_ == tmp_tuples_.size()) {
		// we have traversed all possible join combination of the current right tuple
		// move to the next right tuple
		tmp_tuples_.clear()
		index_ = 0
		if (!right_.Next(&right_tuple_)) {
			// hash join finished, delete all the tmp page we created
			for ( tmp_page_id : tmp_page_ids_) {
				exec_ctx_.GetBufferPoolManager().DeletePage(tmp_page_id)
			}
			return false
		}
		 value = right_expr_.Evaluate(&right_tuple_, right_.GetOutputSchema())
		jht_.GetValue(exec_ctx_.GetTransaction(), value, &tmp_tuples_)
		}
		// traverse corresponding left tuples stored in the tmp pages util we find one satisfying the predicate with current right tuple
		 left_tmp_tuple = tmp_tuples_[index_]
		Tuple left_tuple
		FetchTupleFromTmpTuplePage(&left_tuple, left_tmp_tuple)
		for (!IsValidCombination(left_tuple, right_tuple_)) {
			index_++
			if (index_ == tmp_tuples_.size()) break
			left_tmp_tuple = tmp_tuples_[index_]
			FetchTupleFromTmpTuplePage(&left_tuple, left_tmp_tuple)
		}
		if (index_ < tmp_tuples_.size()) {
			// valid combination found
			MakeOutputTuple(tuple,&left_tuple,&right_tuple_)
			index_++
			return true
		}
		// no valid combination, turn to the next right tuple by for loop
	}
}

func (e *HashJoinExecutor) FetchTupleFromTmpTuplePage(tuple_ *tuple.Tuple, tmp_tuple *TmpTuple) {
	 tmp_page := reinterpret_cast<TmpTuplePage*>(exec_ctx_.GetBufferPoolManager().FetchPage(tmp_tuple.GetPageId()))
	if (!tmp_page) { /*throw std::runtime_error("fail to fetch tmp page when doing hash join")*/ }
	tmp_page.Get(tuple,tmp_tuple.GetOffset())
	exec_ctx_.GetBufferPoolManager().UnpinPage(tmp_tuple.GetPageId(),false)
}

func (e *HashJoinExecutor) IsValidCombination(left_tuple *tuple.Tuple, right_tuple *tuple.Tuple) bool {
	return plan_.Predicate()
		.EvaluateJoin(&left_tuple, left_.GetOutputSchema(), &right_tuple, right_.GetOutputSchema())
		//.GetAs>()
}

func (e *HashJoinExecutor) MakeOutputTuple(output_tuple *tuple.Tuple, left_tuple *tuple Tuple, right_tuple *tuple.Tuple) {
	 output_column_cnt = GetOutputSchema().GetColumnCount()
	values := make([]column.Value output_column_cnt);
	for (size_t i = 0; i < output_column_cnt; i++) {
		values[i] =
			output_exprs_[i].EvaluateJoin(left_tuple, left_.GetOutputSchema(), right_tuple, right_.GetOutputSchema())
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
func (e *HashJoinExecutor) HashValues(tuple_ *tuple.Tuple, schema_ *schema.Schema, exprs []*expression.Expression) hash_t {
	var curr_hash hash_t = 0
	// For every expression,
	for (  &expr : exprs) {
		// We evaluate the tuple on the expression and schema.
		val := expr.Evaluate(tuple, schema)
		// If this produces a value,
		if (!val.IsNull()) {
			// We combine the hash of that value into our current hash.
			curr_hash = HashUtil::CombineHashes(curr_hash, HashUtil::HashValue(&val))
		}
	}
	return curr_hash
}
