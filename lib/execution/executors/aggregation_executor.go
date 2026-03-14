package executors

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"math"

	"github.com/ryogrid/SamehadaDB/lib/container/hash"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
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
	keyLen := int32(len(it.keys))
	it.index++
	return it.index >= keyLen
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
	htVal map[uint32]*plans.AggregateValue
	/** The hash table is just a map from hash val of aggregate keys to aggregate keys. */
	htKey map[uint32]*plans.AggregateKey
	/** The aggregate expressions that we have. */
	aggExprs []expression.Expression
	/** The types of aggregations that we have. */
	aggTypes []plans.AggregationType
}

/**
 * Create a new simplified aggregation hash table.
 * @param aggExprs the aggregation expressions
 * @param aggTypes the types of aggregations
 */
func NewSimpleAggregationHashTable(aggExprs []expression.Expression, aggTypes []plans.AggregationType) *SimpleAggregationHashTable {
	ret := new(SimpleAggregationHashTable)
	ret.htVal = make(map[uint32]*plans.AggregateValue)
	ret.htKey = make(map[uint32]*plans.AggregateKey)
	ret.aggExprs = aggExprs
	ret.aggTypes = aggTypes
	return ret
}

// geneate a hash value from types.Value objs plans.AggregateKey has
func HashValuesOnAggregateKey(key *plans.AggregateKey) uint32 {
	inputBytes := make([]byte, 0)
	for _, val := range key.GroupBys {
		inputBytes = append(inputBytes, val.Serialize()...)
	}
	return hash.GenHashMurMur(inputBytes)
}

/** @return the initial aggregrate value for this aggregation executor */
func (ht *SimpleAggregationHashTable) GenerateInitialAggregateValue() *plans.AggregateValue {
	var values []*types.Value
	for _, aggType := range ht.aggTypes {
		switch aggType {
		case plans.CountAggregate:
			// count starts at zero.
			newElem := types.NewInteger(0)
			values = append(values, &newElem)
		case plans.SumAggregate:
			// Sum starts at zero.
			newElem := types.NewInteger(0)
			values = append(values, &newElem)
		case plans.MinAggregate:
			// min starts at INT_MAX.
			newElem := types.NewInteger(math.MaxInt32)
			values = append(values, &newElem)
		case plans.MaxAggregate:
			// max starts at INT_MIN.
			newElem := types.NewInteger(math.MinInt32)
			values = append(values, &newElem)
		}
	}
	return &plans.AggregateValue{Aggregates: values}
}

/** Combines the input into the aggregation result. */
func (aht *SimpleAggregationHashTable) CombineAggregateValues(result *plans.AggregateValue, input *plans.AggregateValue) {
	for i := 0; i < len(aht.aggExprs); i++ {
		switch aht.aggTypes[i] {
		case plans.CountAggregate:
			// count increases by one.
			addVal := types.NewInteger(1)
			result.Aggregates[i] = result.Aggregates[i].Add(&addVal)
		case plans.SumAggregate:
			// Sum increases by addition.
			result.Aggregates[i] = result.Aggregates[i].Add(input.Aggregates[i])
		case plans.MinAggregate:
			// min is just the min.
			result.Aggregates[i] = result.Aggregates[i].Min(input.Aggregates[i])
		case plans.MaxAggregate:
			// max is just the max.
			result.Aggregates[i] = result.Aggregates[i].Max(input.Aggregates[i])
		}
	}
}

/**
 * Inserts a value into the hash table and then combines it with the current aggregation.
 * @param aggKey the key to be inserted
 * @param aggVal the value to be inserted
 */
func (aht *SimpleAggregationHashTable) InsertCombine(aggKey *plans.AggregateKey, aggVal *plans.AggregateValue) {
	hashvalOfAggkey := HashValuesOnAggregateKey(aggKey)
	//fmt.Printf("%v ", hashvalOfAggkey)
	if _, ok := aht.htVal[hashvalOfAggkey]; !ok {
		aht.htVal[hashvalOfAggkey] = aht.GenerateInitialAggregateValue()
	}
	curVal := aht.htVal[hashvalOfAggkey]
	aht.CombineAggregateValues(curVal, aggVal)

	// additional data store for realize iterator
	if _, ok := aht.htKey[hashvalOfAggkey]; !ok {
		aht.htKey[hashvalOfAggkey] = aggKey
	}
}

/* return iterator to the start of the hash table */
func (aht *SimpleAggregationHashTable) Begin() *AggregateHTIterator {
	var aggKeyList = make([]*plans.AggregateKey, 0)
	var aggValList = make([]*plans.AggregateValue, 0)
	for hval, val := range aht.htVal {
		aggKeyList = append(aggKeyList, aht.htKey[hval])
		aggValList = append(aggValList, val)
	}
	return NewAggregateHTIteratorIterator(aggKeyList, aggValList)
}

/**
* AggregationExecutor executes an aggregation operation (e.g. COUNT, SUM, MIN, MAX) on the tuples of a child executor.
 */
type AggregationExecutor struct {
	context *ExecutorContext
	/** The aggregation plan node. */
	plan *plans.AggregationPlanNode
	/** The child executor whose tuples we are aggregating. */
	child []Executor
	/** Simple aggregation hash table. */
	aht *SimpleAggregationHashTable
	/** Simple aggregation hash table iterator. */
	ahtIterator *AggregateHTIterator
	exprs        []expression.Expression
}

/**
 * Creates a new aggregation executor.
 * @param execCtx the context that the aggregation should be performed in
 * @param plan the aggregation plan node
 * @param child the child executor
 */
func NewAggregationExecutor(execCtx *ExecutorContext, plan *plans.AggregationPlanNode,
	child Executor) *AggregationExecutor {
	aht := NewSimpleAggregationHashTable(plan.GetAggregates(), plan.GetAggregateTypes())
	return &AggregationExecutor{execCtx, plan, []Executor{child}, aht, nil, []expression.Expression{}}
}

func (e *AggregationExecutor) GetOutputSchema() *schema.Schema { return e.plan.OutputSchema() }

func (e *AggregationExecutor) Init() {
	e.child[0].Init()
	childExec := e.child[0]
	outputColumnCnt := int(e.GetOutputSchema().GetColumnCount())
	for i := 0; i < outputColumnCnt; i++ {
		aggExpr := e.GetOutputSchema().GetColumn(uint32(i)).GetExpr().(expression.AggregateValueExpression)
		e.exprs = append(e.exprs, &aggExpr)
	}
	insertCallCnt := 0
	for {
		tpl, done, err := childExec.Next()
		if err != nil || done {
			if err != nil {
				fmt.Println(err)
			}
			break
		}

		if tpl != nil {
			e.aht.InsertCombine(e.MakeKey(tpl), e.MakeVal(tpl))
			insertCallCnt++
		}
	}
	fmt.Printf("insertCallCnt %d\n", insertCallCnt)
	e.ahtIterator = e.aht.Begin()
}

func (e *AggregationExecutor) Next() (*tuple.Tuple, Done, error) {
	for !e.ahtIterator.IsNextEnd() && e.plan.GetHaving() != nil && !e.plan.GetHaving().EvaluateAggregate(e.ahtIterator.Key().GroupBys, e.ahtIterator.Val().Aggregates).ToBoolean() {
		e.ahtIterator.Next()
	}
	if e.ahtIterator.IsEnd() {
		return nil, true, nil
	}
	var values = make([]types.Value, 0)
	for i := 0; i < len(e.exprs); i++ {
		values = append(values, e.exprs[i].EvaluateAggregate(e.ahtIterator.Key().GroupBys, e.ahtIterator.Val().Aggregates))
	}
	tpl := tuple.NewTupleFromSchema(values, e.GetOutputSchema())
	e.ahtIterator.Next()
	return tpl, false, nil
}

/** return the tuple as an AggregateKey */
func (e *AggregationExecutor) MakeKey(tpl *tuple.Tuple) *plans.AggregateKey {
	var keys = make([]*types.Value, 0)
	for _, expr := range e.plan.GetGroupBys() {
		tmpVal := expr.Evaluate(tpl, e.child[0].GetOutputSchema())
		keys = append(keys, &tmpVal)
	}
	return &plans.AggregateKey{GroupBys: keys}
}

/** return the tuple as an AggregateValue */
func (e *AggregationExecutor) MakeVal(tpl *tuple.Tuple) *plans.AggregateValue {
	var vals = make([]*types.Value, 0)
	for _, expr := range e.plan.GetAggregates() {
		tmpVal := expr.Evaluate(tpl, e.child[0].GetOutputSchema())
		vals = append(vals, &tmpVal)
	}
	return &plans.AggregateValue{Aggregates: vals}
}

func (e *AggregationExecutor) GetTableMetaData() *catalog.TableMetadata {
	return e.child[0].GetTableMetaData()
}
