package executors

import (
	"fmt"

	"github.com/ryogrid/SamehadaDB/common"
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
	jht_ *SimpleHashJoinHashTable //*hash.LinearProbeHashTable
	/** The number of buckets in the hash table. */
	jht_num_buckets_ uint32 //= 2
	left_            Executor
	right_           Executor
	left_expr_       expression.Expression
	right_expr_      expression.Expression
	tmp_tuples_      []*hash.TmpTuple
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
	//retun &HashJoinExecutor{exec_ctx, plan, }
	ret := new(HashJoinExecutor)
	ret.plan_ = plan
	ret.context = exec_ctx
	ret.left_ = left
	ret.right_ = right
	// about 200k entry can be stored
	ret.jht_num_buckets_ = 100
	//ret.jht_ = hash.NewLinearProbeHashTable(exec_ctx.GetBufferPoolManager(), int(ret.jht_num_buckets_))
	ret.jht_ = NewSimpleHashJoinHashTable()
	return ret
}

/** @return the JHT in use. Do not modify this function, otherwise you will get a zero. */
func (e *HashJoinExecutor) GetJHT() *SimpleHashJoinHashTable { return e.jht_ }

func (e *HashJoinExecutor) GetOutputSchema() *schema.Schema { return e.plan_.OutputSchema() }

func (e *HashJoinExecutor) Init() {
	// get exprs to evaluate to output result
	output_column_cnt := int(e.GetOutputSchema().GetColumnCount())
	//e.output_exprs_.resize(output_column_cnt)
	for i := 0; i < output_column_cnt; i++ {
		//e.output_exprs_[i] = e.GetOutputSchema().GetColumn(uint32(i)).GetExpr().(expression.Expression)
		column_ := e.GetOutputSchema().GetColumn(uint32(i))
		var colVal expression.Expression
		if column_.IsLeft() {
			colVal = expression.NewColumnValue(0, uint32(i), types.Invalid)
		} else {
			colVal = expression.NewColumnValue(1, uint32(i), types.Invalid)
		}

		e.output_exprs_ = append(e.output_exprs_, colVal)
	}
	// build hash table from left
	e.left_.Init()
	e.right_.Init()
	e.left_expr_ = e.plan_.Predicate().GetChildAt(0)
	e.right_expr_ = e.plan_.Predicate().GetChildAt(1)
	//var left_tuple tuple.Tuple
	// store all the left tuples in tmp pages in that it can not fit in memory
	// use tmp tuple as the value of the hash table kv pair
	var tmp_page *hash.TmpTuplePage = nil
	var tmp_page_id types.PageID = common.InvalidPageID
	var tmp_tuple hash.TmpTuple
	var loop_cnt int32 = 0
	for left_tuple, done, _ := e.left_.Next(); !done; left_tuple, done, _ = e.left_.Next() {
		if tmp_page == nil || !tmp_page.Insert(left_tuple, &tmp_tuple) {
			// unpin the last full tmp page
			if tmp_page_id != common.InvalidPageID {
				e.context.GetBufferPoolManager().UnpinPage(tmp_page_id, true)
			}
			// create new tmp page
			tmp_page = hash.CastPageAsTmpTuplePage(e.context.GetBufferPoolManager().NewPage())
			if tmp_page == nil {
				panic("fail to create new tmp page when doing hash join")
			}
			fmt.Println("create new tmp page")
			tmp_page.Init(tmp_page.GetPageId(), common.PageSize)
			tmp_page_id = tmp_page.GetPageId()
			e.tmp_page_ids_ = append(e.tmp_page_ids_, tmp_page_id)
			// reinsert the tuple
			//assert(tmp_page.Insert(left_tuple, &tmp_tuple))
			tmp_page.Insert(left_tuple, &tmp_tuple)
			loop_cnt++
			if loop_cnt > 100 {
				panic("loop_cnt > 100")
			}
		}
		valueAsKey := e.left_expr_.Evaluate(left_tuple, e.left_.GetOutputSchema())
		e.jht_.Insert(hash.HashValue(&valueAsKey), &tmp_tuple)
		fmt.Println("insert left tuple into hash table")
	}
}

func (e *HashJoinExecutor) Next() (*tuple.Tuple, Done, error) {
	for {
		for int(e.index_) == len(e.tmp_tuples_) {
			// we have traversed all possible join combination of the current right tuple
			// move to the next right tuple
			e.tmp_tuples_ = []*hash.TmpTuple{}
			e.index_ = 0
			if _, done, _ := e.right_.Next(); !done {
				// hash join finished, delete all the tmp page we created
				for _, tmp_page_id := range e.tmp_page_ids_ {
					e.context.GetBufferPoolManager().DeletePage(tmp_page_id)
				}
				return nil, false, nil
			}
			value := e.right_expr_.Evaluate(&e.right_tuple_, e.right_.GetOutputSchema())
			e.tmp_tuples_ = e.jht_.GetValue(hash.HashValue(&value))
		}
		// traverse corresponding left tuples stored in the tmp pages util we find one satisfying the predicate with current right tuple
		left_tmp_tuple := e.tmp_tuples_[e.index_]
		var left_tuple tuple.Tuple
		e.FetchTupleFromTmpTuplePage(&left_tuple, left_tmp_tuple)
		for !e.IsValidCombination(&left_tuple, &e.right_tuple_) {
			e.index_++
			if int(e.index_) == len(e.tmp_tuples_) {
				break
			}
			left_tmp_tuple = e.tmp_tuples_[e.index_]
			e.FetchTupleFromTmpTuplePage(&left_tuple, left_tmp_tuple)
		}
		if int(e.index_) < len(e.tmp_tuples_) {
			// valid combination found
			ret_tuple := e.MakeOutputTuple(&left_tuple, &e.right_tuple_)
			e.index_++
			return ret_tuple, true, nil
		}
		// no valid combination, turn to the next right tuple by for loop
	}
	//return nil, false, nil
}

func (e *HashJoinExecutor) FetchTupleFromTmpTuplePage(tuple_ *tuple.Tuple, tmp_tuple *hash.TmpTuple) {
	tmp_page := hash.CastPageAsTmpTuplePage(e.context.GetBufferPoolManager().FetchPage(tmp_tuple.GetPageId()))
	if tmp_page == nil {
		panic("fail to fetch tmp page when doing hash join")
	}
	tmp_page.Get(tuple_, tmp_tuple.GetOffset())
	e.context.GetBufferPoolManager().UnpinPage(tmp_tuple.GetPageId(), false)
}

// TODO: (SDB) need port codes of Boolean type support for hash join
func (e *HashJoinExecutor) IsValidCombination(left_tuple *tuple.Tuple, right_tuple *tuple.Tuple) bool {
	return e.plan_.Predicate().EvaluateJoin(left_tuple, e.left_.GetOutputSchema(), right_tuple, e.right_.GetOutputSchema()).ToBoolean()
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
	for _, expr := range exprs {
		// We evaluate the tuple on the expression and schema.
		val := (*expr).Evaluate(tuple_, schema_)
		// If this produces a value,
		if !val.IsNull() {
			// We combine the hash of that value into our current hash.
			curr_hash = hash.CombineHashes(curr_hash, hash.HashValue(&val))
		}
	}
	return curr_hash
}

type SimpleHashJoinHashTable struct {
	hash_table_ map[uint32][]*hash.TmpTuple
}

func NewSimpleHashJoinHashTable() *SimpleHashJoinHashTable {
	return &SimpleHashJoinHashTable{hash_table_: make(map[uint32][]*hash.TmpTuple)}
}

/**
 * Inserts a (hash key, tuple) pair into the hash table.
 * @param txn the transaction that we execute in
 * @param h the hash key
 * @param t the tuple to associate with the key
 * @return true if the insert succeeded
 */
func (jht *SimpleHashJoinHashTable) Insert(h uint32, t *hash.TmpTuple) bool {
	if jht.hash_table_[h] == nil {
		vals := make([]*hash.TmpTuple, 0)
		vals = append(vals, t)
		jht.hash_table_[h] = vals
	} else {
		jht.hash_table_[h] = append(jht.hash_table_[h], t)
	}

	return true
}

/**
 * Gets the values in the hash table that match the given hash key.
 * @param txn the transaction that we execute in
 * @param h the hash key
 * @param[out] t the list of tuples that matched the key
 */
func (jht *SimpleHashJoinHashTable) GetValue(h uint32) []*hash.TmpTuple { return jht.hash_table_[h] }
