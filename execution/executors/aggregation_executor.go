package executors

import (
	"math"

	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * An iterator through the simplified aggregation hash table.
 */
type AggregateHTIterator struct {
	/** Aggregates map. */
	keys   []plans.AggregateKey
	values []plans.AggregateValue
	index  int32
}

/** Creates an iterator for the aggregate map. */
func NewAggregateHTIteratorIterator(keys []plans.AggregateKey, values []plans.AggregateValue) *AggregateHTIterator {
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
	return &it.keys[it.index]
}

func (it *AggregateHTIterator) Value() *plans.AggregateValue {
	if it.index >= int32(len(it.values)) {
		return nil
	}
	return &it.values[it.index]
}

func (it *AggregateHTIterator) IsEnd() bool {
	return it.index >= int32(len(it.keys))
}

//    /** @return the key of the iterator */
//    const AggregateKey &Key() { return iter_->first; }

//    /** @return the value of the iterator */
//    const AggregateValue &Val() { return iter_->second; }

//    /** @return the iterator before it is incremented */
//    Iterator &operator++() {
// 	 ++iter_;
// 	 return *this;
//    }

//    /** @return true if both iterators are identical */
//    bool operator==(const Iterator &other) { return this->iter_ == other.iter_; }

//    /** @return true if both iterators are different */
//    bool operator!=(const Iterator &other) { return this->iter_ != other.iter_; }

// TODO: (SDB) need port SimpleAggregateHashTable and AggregationExecutor class

/**
 * A simplified hash table that has all the necessary functionality for aggregations.
 */
type SimpleAggregationHashTable struct {
	/** The hash table is just a map from aggregate keys to aggregate values. */
	ht map[plans.AggregateKey]*plans.AggregateValue
	/** The aggregate expressions that we have. */
	agg_exprs_ []expression.Expression
	/** The types of aggregations that we have. */
	agg_types_ []*plans.AggregationType
}

/**
 * Create a new simplified aggregation hash table.
 * @param agg_exprs the aggregation expressions
 * @param agg_types the types of aggregations
 */
//  SimpleAggregationHashTable(const std::vector<const AbstractExpression *> &agg_exprs,
// 							const std::vector<AggregationType> &agg_types)
// 	 : agg_exprs_{agg_exprs}, agg_types_{agg_types} {}

/** @return the initial aggregrate value for this aggregation executor */
func GenerateInitialAggregateValue() *plans.AggregateValue {
	var values []types.Value
	for agg_type, _ := range agg_types_ {
		switch agg_type {
		case plans.COUNT_AGGREGATE:
			// Count starts at zero.
			values.emplace_back(types.NewInteger(0))
		case plans.SUM_AGGREGATE:
			// Sum starts at zero.
			values.emplace_back(types.NewInteger(0))
		case plans.MIN_AGGREGATE:
			// Min starts at INT_MAX.
			values.emplace_back(types.NewInteger(math.MaxInt32))
		case plans.MAX_AGGREGATE:
			// Max starts at INT_MIN.
			values.emplace_back(types.NewInteger(math.MinInt32))
		}
	}
	return values
}

/** Combines the input into the aggregation result. */
func CombineAggregateValues(result *plans.AggregateValue, input *plans.AggregateValue) {
	for i := 0; i < len(agg_exprs_); i++ {
		switch agg_types_[i] {
		case plans.COUNT_AGGREGATE:
			// Count increases by one.
			result.aggregates_[i] = result.aggregates_[i].Add(types.NewInteger(0))
		case plans.SUM_AGGREGATE:
			// Sum increases by addition.
			result.aggregates_[i] = result.aggregates_[i].Add(input.aggregates_[i])
		case plans.MIN_AGGREGATE:
			// Min is just the min.
			result.aggregates_[i] = result.aggregates_[i].Min(input.aggregates_[i])
		case plans.MAX_AGGREGATE:
			// Max is just the max.
			result.aggregates_[i] = result.aggregates_[i].Max(input.aggregates_[i])
		}
	}
}

/**
 * Inserts a value into the hash table and then combines it with the current aggregation.
 * @param agg_key the key to be inserted
 * @param agg_val the value to be inserted
 */
func InsertCombine(agg_key plans.AggregateKey, agg_val *plans.AggregateValue) {
	//    if ht.count(agg_key) == 0 {
	// 	 	ht.insert({agg_key, GenerateInitialAggregateValue()})
	//    }
	//    CombineAggregateValues(&ht[agg_key], agg_val);
}

/** @return iterator to the start of the hash table */
func (ht *SimpleAggregateHashTable) Begin() *AggregateHTIterator {
	//return Iterator{ht.cbegin()}
	return nil
}

//  /** @return iterator to the end of the hash table */
//  Iterator End() { return Iterator{ht.cend()}; }

//    /**
// 	* AggregationExecutor executes an aggregation operation (e.g. COUNT, SUM, MIN, MAX) on the tuples of a child executor.
// 	*/
//    class AggregationExecutor : public AbstractExecutor {
// 	public:
// 	 /**
// 	  * Creates a new aggregation executor.
// 	  * @param exec_ctx the context that the aggregation should be performed in
// 	  * @param plan the aggregation plan node
// 	  * @param child the child executor
// 	  */
// 	 AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
// 						 std::unique_ptr<AbstractExecutor> &&child)
// 		 : AbstractExecutor(exec_ctx),
// 		   aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
// 		   aht_iterator_(aht_.Begin()) {
// 	   plan_ = plan;
// 	   child_ = std::move(child);
// 	 }

// 	 /** Do not use or remove this function, otherwise you will get zero points. */
// 	 const AbstractExecutor *GetChildExecutor() const { return child_.get(); }

// 	 const Schema *GetOutputSchema() override { return plan_->OutputSchema(); }

// 	 void Init() override {
// 	   Tuple tuple;
// 	   child_->Init();
// 	   while (child_->Next(&tuple)) {
// 		 aht_.InsertCombine(MakeKey(&tuple), MakeVal(&tuple));
// 	   }
// 	   aht_iterator_ = aht_.Begin();
// 	 }

// 	 bool Next(Tuple *tuple) override {
// 	   if (aht_iterator_ == aht_.End()) {
// 		 return false;
// 	   }
// 	   while (aht_iterator_ != aht_.End()) {
// 		 if (plan_->GetHaving() != nullptr) {
// 		   if (!plan_->GetHaving()
// 					->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_)
// 					.GetAs<bool>()) {
// 			 aht_iterator_.operator++();
// 			 continue;
// 		   }
// 		 }
// 		 std::vector<Value> values;
// 		 for (auto col : plan_->OutputSchema()->GetColumns()) {
// 		   values.push_back(
// 			   col.GetExpr()->EvaluateAggregate(aht_iterator_.Key().group_bys_, aht_iterator_.Val().aggregates_));
// 		 }
// 		 aht_iterator_.operator++();
// 		 Tuple tuple1(values, plan_->OutputSchema());
// 		 *tuple = tuple1;
// 		 return true;
// 	   }
// 	   return false;
// 	 }

// 	 /** @return the tuple as an AggregateKey */
// 	 AggregateKey MakeKey(const Tuple *tuple) {
// 	   std::vector<Value> keys;
// 	   for (const auto &expr : plan_->GetGroupBys()) {
// 		 keys.emplace_back(expr->Evaluate(tuple, child_->GetOutputSchema()));
// 	   }
// 	   return {keys};
// 	 }

// 	 /** @return the tuple as an AggregateValue */
// 	 AggregateValue MakeVal(const Tuple *tuple) {
// 	   std::vector<Value> vals;
// 	   for (const auto &expr : plan_->GetAggregates()) {
// 		 vals.emplace_back(expr->Evaluate(tuple, child_->GetOutputSchema()));
// 	   }
// 	   return {vals};
// 	 }

// 	private:
// 	 /** The aggregation plan node. */
// 	 const AggregationPlanNode *plan_;
// 	 /** The child executor whose tuples we are aggregating. */
// 	 std::unique_ptr<AbstractExecutor> child_;
// 	 /** Simple aggregation hash table. */
// 	 SimpleAggregationHashTable aht_;
// 	 /** Simple aggregation hash table iterator. */
// 	 SimpleAggregationHashTable::Iterator aht_iterator_;
//    };
