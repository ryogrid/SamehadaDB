package plans

import "github.com/ryogrid/SamehadaDB/types"

// /** AggregationType enumerates all the possible aggregation functions in our system. */
type AggregationType int32

/** The type of the log record. */
const (
	COUNT_AGGREGATE AggregationType = iota
	SUM_AGGREGATE
	MIN_AGGREGATE
	MAX_AGGREGATE
)

//TODO: (SDB) need port AggregationPlan class and etc

// /**
//  * AggregationPlanNode represents the various SQL aggregation functions.
//  * For example, COUNT(), SUM(), MIN() and MAX().
//  * To simplfiy this project, AggregationPlanNode must always have exactly one child.
//  */
// class AggregationPlanNode : public AbstractPlanNode {
//  public:
//   /**
//    * Creates a new AggregationPlanNode.
//    * @param output_schema the output format of this plan node
//    * @param child the child plan to aggregate data over
//    * @param having the having clause of the aggregation
//    * @param group_bys the group by clause of the aggregation
//    * @param aggregates the expressions that we are aggregating
//    * @param agg_types the types that we are aggregating
//    */
//   AggregationPlanNode(const Schema *output_schema, const AbstractPlanNode *child, const AbstractExpression *having,
//                       std::vector<const AbstractExpression *> &&group_bys,
//                       std::vector<const AbstractExpression *> &&aggregates, std::vector<AggregationType> &&agg_types)
//       : AbstractPlanNode(output_schema, {child}),
//         having_(having),
//         group_bys_(std::move(group_bys)),
//         aggregates_(std::move(aggregates)),
//         agg_types_(std::move(agg_types)) {}

//   PlanType GetType() const override { return PlanType::Aggregation; }

//   /** @return the child of this aggregation plan node */
//   const AbstractPlanNode *GetChildPlan() const {
//     BUSTUB_ASSERT(GetChildren().size() == 1, "Aggregation expected to only have one child.");
//     return GetChildAt(0);
//   }

//   /** @return the having clause */
//   const AbstractExpression *GetHaving() const { return having_; }

//   /** @return the idx'th group by expression */
//   const AbstractExpression *GetGroupByAt(uint32_t idx) const { return group_bys_[idx]; }

//   /** @return the group by expressions */
//   const std::vector<const AbstractExpression *> &GetGroupBys() const { return group_bys_; }

//   /** @return the idx'th aggregate expression */
//   const AbstractExpression *GetAggregateAt(uint32_t idx) const { return aggregates_[idx]; }

//   /** @return the aggregate expressions */
//   const std::vector<const AbstractExpression *> &GetAggregates() const { return aggregates_; }

//   /** @return the aggregate types */
//   const std::vector<AggregationType> &GetAggregateTypes() const { return agg_types_; }

//  private:
//   const AbstractExpression *having_;
//   std::vector<const AbstractExpression *> group_bys_;
//   std::vector<const AbstractExpression *> aggregates_;
//   std::vector<AggregationType> agg_types_;
// };

type ValueHasNoPointer struct {
	valueType types.TypeID
	integer   int32
	boolean   bool
	varchar   string
	float     float32
}

type AggregateKey struct {
	//Group_bys_ [10]types.Value
	Group_bys_ [10]ValueHasNoPointer
}

/**
 * Compares two aggregate keys for equality.
 * @param other the other aggregate key to be compared with
 * @return true if both aggregate keys have equivalent group-by expressions, false otherwise
 */
func (key AggregateKey) CompareEquals(other AggregateKey) bool {
	for i := 0; i < len(other.Group_bys_); i++ {
		if !key.Group_bys_[i].CompareEquals(other.Group_bys_[i]) {
			return false
		}
	}
	return true
}

type AggregateValue struct {
	Aggregates_ []*types.Value
}

// namespace std {

// /**
//  * Implements std::hash on AggregateKey.
//  */
// template <>
// struct hash<bustub::AggregateKey> {
//   std::size_t operator()(const bustub::AggregateKey &agg_key) const {
//     size_t curr_hash = 0;
//     for (const auto &key : agg_key.group_bys_) {
//       if (!key.IsNull()) {
//         curr_hash = bustub::HashUtil::CombineHashes(curr_hash, bustub::HashUtil::HashValue(&key));
//       }
//     }
//     return curr_hash;
//   }
// };
