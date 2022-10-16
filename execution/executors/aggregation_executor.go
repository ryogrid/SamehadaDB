package executors

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
	"math"

	"github.com/ryogrid/SamehadaDB/container/hash"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * An iterator through the simplified aggregation hash table.
 */
type AggregateHTIterator struct {
	/** Aggregates map. */
	keys   []*plans.AggregateKey
	values []*plans.AggregateValue
	index  int32
}

/** Creates an iterator for the aggregate map. */
func NewAggregateHTIteratorIterator(keys []*plans.AggregateKey, values []*plans.AggregateValue) *AggregateHTIterator {
	ret := new(AggregateHTIterator)
	ret.keys = keys
	ret.values = values
	ret.index = 0
	return ret
}

func (it *AggregateHTIterator) Next() bool {
	key_len := int32(len(it.keys))
	it.index++
	return it.index >= key_len
}

func (it *AggregateHTIterator) Key() *plans.AggregateKey {
	if it.index >= int32(len(it.keys)) {
		return nil
	}
	return it.keys[it.index]
}

func (it *AggregateHTIterator) Val() *plans.AggregateValue {
	if it.index >= int32(len(it.values)) {
		return nil
	}
	return it.values[it.index]
}

func (it *AggregateHTIterator) IsEnd() bool {
	return it.index >= int32(len(it.keys))
}

// return whether iterator is End state if Next method is called
func (it *AggregateHTIterator) IsNextEnd() bool {
	return it.index+1 >= int32(len(it.keys))
}

/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
type SimpleAggregationHashTable struct {
	/** The hash table is just a map from hash val of aggregate keys to aggregate values. */
	ht_val map[uint32]*plans.AggregateValue
	/** The hash table is just a map from hash val of aggregate keys to aggregate keys. */
	ht_key map[uint32]*plans.AggregateKey
	/** The aggregate expressions that we have. */
	agg_exprs_ []expression.Expression
	/** The types of aggregations that we have. */
	agg_types_ []plans.AggregationType
}

/**
 * Create a new simplified aggregation hash table.
 * @param agg_exprs the aggregation expressions
 * @param agg_types the types of aggregations
 */
func NewSimpleAggregationHashTable(agg_exprs []expression.Expression, agg_types []plans.AggregationType) *SimpleAggregationHashTable {
	ret := new(SimpleAggregationHashTable)
	ret.ht_val = make(map[uint32]*plans.AggregateValue)
	ret.ht_key = make(map[uint32]*plans.AggregateKey)
	ret.agg_exprs_ = agg_exprs
	ret.agg_types_ = agg_types
	return ret
}

// geneate a hash value from types.Value objs plans.AggregateKey has
func HashValuesOnAggregateKey(key *plans.AggregateKey) uint32 {
	input_bytes := make([]byte, 0)
	for _, val := range key.Group_bys_ {
		input_bytes = append(input_bytes, val.Serialize()...)
	}
	return hash.GenHashMurMur(input_bytes)
}

/** @return the initial aggregrate value for this aggregation executor */
func (ht *SimpleAggregationHashTable) GenerateInitialAggregateValue() *plans.AggregateValue {
	var values []*types.Value
	for _, agg_type := range ht.agg_types_ {
		switch agg_type {
		case plans.COUNT_AGGREGATE:
			// Count starts at zero.
			new_elem := types.NewInteger(0)
			values = append(values, &new_elem)
		case plans.SUM_AGGREGATE:
			// Sum starts at zero.
			new_elem := types.NewInteger(0)
			values = append(values, &new_elem)
		case plans.MIN_AGGREGATE:
			// Min starts at INT_MAX.
			new_elem := types.NewInteger(math.MaxInt32)
			values = append(values, &new_elem)
		case plans.MAX_AGGREGATE:
			// Max starts at INT_MIN.
			new_elem := types.NewInteger(math.MinInt32)
			values = append(values, &new_elem)
		}
	}
	return &plans.AggregateValue{Aggregates_: values}
}

/** Combines the input into the aggregation result. */
func (aht *SimpleAggregationHashTable) CombineAggregateValues(result *plans.AggregateValue, input *plans.AggregateValue) {
	for i := 0; i < len(aht.agg_exprs_); i++ {
		switch aht.agg_types_[i] {
		case plans.COUNT_AGGREGATE:
			// Count increases by one.
			add_val := types.NewInteger(1)
			result.Aggregates_[i] = result.Aggregates_[i].Add(&add_val)
		case plans.SUM_AGGREGATE:
			// Sum increases by addition.
			result.Aggregates_[i] = result.Aggregates_[i].Add(input.Aggregates_[i])
		case plans.MIN_AGGREGATE:
			// Min is just the min.
			result.Aggregates_[i] = result.Aggregates_[i].Min(input.Aggregates_[i])
		case plans.MAX_AGGREGATE:
			// Max is just the max.
			result.Aggregates_[i] = result.Aggregates_[i].Max(input.Aggregates_[i])
		}
	}
}

/**
 * Inserts a value into the hash table and then combines it with the current aggregation.
 * @param agg_key the key to be inserted
 * @param agg_val the value to be inserted
 */
func (aht *SimpleAggregationHashTable) InsertCombine(agg_key *plans.AggregateKey, agg_val *plans.AggregateValue) {
	hashval_of_aggkey := HashValuesOnAggregateKey(agg_key)
	//fmt.Printf("%v ", hashval_of_aggkey)
	if _, ok := aht.ht_val[hashval_of_aggkey]; !ok {
		aht.ht_val[hashval_of_aggkey] = aht.GenerateInitialAggregateValue()
		//aht.ht.insert({agg_key, GenerateInitialAggregateValue()})
	}
	cur_val := aht.ht_val[hashval_of_aggkey]
	aht.CombineAggregateValues(cur_val, agg_val)

	// additional data store for realize iterator
	if _, ok := aht.ht_key[hashval_of_aggkey]; !ok {
		aht.ht_key[hashval_of_aggkey] = agg_key
	}
}

/** @return iterator to the start of the hash table */
func (aht *SimpleAggregationHashTable) Begin() *AggregateHTIterator {
	var agg_key_list []*plans.AggregateKey = make([]*plans.AggregateKey, 0)
	var agg_val_list []*plans.AggregateValue = make([]*plans.AggregateValue, 0)
	for hval, val := range aht.ht_val {
		agg_key_list = append(agg_key_list, aht.ht_key[hval])
		agg_val_list = append(agg_val_list, val)
	}
	//return GetRangeScanIterator{ht.cbegin()}
	return NewAggregateHTIteratorIterator(agg_key_list, agg_val_list)
}

/**
* AggregationExecutor executes an aggregation operation (e.g. COUNT, SUM, MIN, MAX) on the tuples of a child executor.
 */
type AggregationExecutor struct {
	context *ExecutorContext
	/** The aggregation plan node. */
	plan_ *plans.AggregationPlanNode
	/** The child executor whose tuples we are aggregating. */
	child_ []Executor
	/** Simple aggregation hash table. */
	aht_ *SimpleAggregationHashTable
	/** Simple aggregation hash table iterator. */
	aht_iterator_ *AggregateHTIterator
	exprs_        []expression.Expression
}

/**
 * Creates a new aggregation executor.
 * @param exec_ctx the context that the aggregation should be performed in
 * @param plan the aggregation plan node
 * @param child the child executor
 */
func NewAggregationExecutor(exec_ctx *ExecutorContext, plan *plans.AggregationPlanNode,
	child Executor) *AggregationExecutor {
	aht := NewSimpleAggregationHashTable(plan.GetAggregates(), plan.GetAggregateTypes())
	return &AggregationExecutor{exec_ctx, plan, []Executor{child}, aht, nil, []expression.Expression{}}
}

func (e *AggregationExecutor) GetOutputSchema() *schema.Schema { return e.plan_.OutputSchema() }

func (e *AggregationExecutor) Init() {
	//Tuple tuple
	e.child_[0].Init()
	child_exec := e.child_[0]
	output_column_cnt := int(e.GetOutputSchema().GetColumnCount())
	for i := 0; i < output_column_cnt; i++ {
		agg_expr := e.GetOutputSchema().GetColumn(uint32(i)).GetExpr().(expression.AggregateValueExpression)
		e.exprs_ = append(e.exprs_, &agg_expr)
	}
	insert_call_cnt := 0
	for {
		tuple_, done, err := child_exec.Next()
		if err != nil || done {
			if err != nil {
				fmt.Println(err)
			}
			break
		}

		if tuple_ != nil {
			e.aht_.InsertCombine(e.MakeKey(tuple_), e.MakeVal(tuple_))
			insert_call_cnt++
		}
	}
	fmt.Printf("insert_call_cnt %d\n", insert_call_cnt)
	e.aht_iterator_ = e.aht_.Begin()
}

func (e *AggregationExecutor) Next() (*tuple.Tuple, Done, error) {
	for !e.aht_iterator_.IsNextEnd() && e.plan_.GetHaving() != nil && !e.plan_.GetHaving().EvaluateAggregate(e.aht_iterator_.Key().Group_bys_, e.aht_iterator_.Val().Aggregates_).ToBoolean() {
		e.aht_iterator_.Next()
	}
	if e.aht_iterator_.IsEnd() {
		return nil, true, nil
	}
	var values []types.Value = make([]types.Value, 0)
	for i := 0; i < len(e.exprs_); i++ {
		values = append(values, e.exprs_[i].EvaluateAggregate(e.aht_iterator_.Key().Group_bys_, e.aht_iterator_.Val().Aggregates_))
	}
	tuple_ := tuple.NewTupleFromSchema(values, e.GetOutputSchema())
	e.aht_iterator_.Next()
	return tuple_, false, nil
}

/** @return the tuple as an AggregateKey */
func (e *AggregationExecutor) MakeKey(tuple_ *tuple.Tuple) *plans.AggregateKey {
	var keys []*types.Value = make([]*types.Value, 0)
	for _, expr := range e.plan_.GetGroupBys() {
		tmp_val := expr.Evaluate(tuple_, e.child_[0].GetOutputSchema())
		keys = append(keys, &tmp_val)
	}
	return &plans.AggregateKey{Group_bys_: keys}
}

/** @return the tuple as an AggregateValue */
func (e *AggregationExecutor) MakeVal(tuple_ *tuple.Tuple) *plans.AggregateValue {
	var vals []*types.Value = make([]*types.Value, 0)
	//for (  &ex	pr : plan_.GetAggregates()) {
	for _, expr := range e.plan_.GetAggregates() {
		tmp_val := expr.Evaluate(tuple_, e.child_[0].GetOutputSchema())
		vals = append(vals, &tmp_val)
	}
	return &plans.AggregateValue{Aggregates_: vals}
}

func (e *AggregationExecutor) GetTableMetaData() *catalog.TableMetadata {
	return e.child_[0].GetTableMetaData()
}
