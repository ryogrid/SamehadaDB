package plans

import (
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"math"
)

type AggregationType int32

/** The type of the log record. */
const (
	CountAggregate AggregationType = iota
	SumAggregate
	MinAggregate
	MaxAggregate
)

/**
 * AggregationPlanNode represents the various SQL aggregation functions.
 * For example, COUNT(), SUM(), MIN() and MAX().
 * To simplfiy this project, AggregationPlanNode must always have exactly one child.
 */
type AggregationPlanNode struct {
	*AbstractPlanNode
	having     expression.Expression
	groupBys  []expression.Expression
	aggregates []expression.Expression
	aggTypes  []AggregationType
	stats      *catalog.TableStatistics
}

/**
 * Creates a new AggregationPlanNode.
 * @param outputSchema the output format of this plan node
 * @param child the child plan to aggregate data over
 * @param having the having clause of the aggregation
 * @param groupBys the group by clause of the aggregation
 * @param aggregates the expressions that we are aggregating
 * @param aggTypes the types that we are aggregating
 */
func NewAggregationPlanNode(outputSchema *schema.Schema, child Plan, having expression.Expression,
	groupBys []expression.Expression,
	aggregates []expression.Expression, aggTypes []AggregationType) *AggregationPlanNode {
	return &AggregationPlanNode{&AbstractPlanNode{outputSchema, []Plan{child}}, having, groupBys, aggregates, aggTypes, child.GetStatistics().GetDeepCopy()}
}

func (p *AggregationPlanNode) GetType() PlanType { return Aggregation }

/** @return the child of this aggregation plan node */
func (p *AggregationPlanNode) GetChildPlan() Plan {
	common.SHAssert(len(p.GetChildren()) == 1, "Aggregation expected to only have one child.")
	return p.GetChildAt(0)
}

func (p *AggregationPlanNode) GetChildAt(childIndex uint32) Plan {
	return p.children[childIndex]
}

func (p *AggregationPlanNode) GetChildren() []Plan {
	return p.children
}

/** @return the having clause */
func (p *AggregationPlanNode) GetHaving() expression.Expression { return p.having }

/** @return the idx'th group by expression */
func (p *AggregationPlanNode) GetGroupByAt(idx uint32) expression.Expression {
	return p.groupBys[idx]
}

/** @return the group by expressions */
func (p *AggregationPlanNode) GetGroupBys() []expression.Expression { return p.groupBys }

/** @return the idx'th aggregate expression */
func (p *AggregationPlanNode) GetAggregateAt(idx uint32) expression.Expression {
	return p.aggregates[idx]
}

/** @return the aggregate expressions */
func (p *AggregationPlanNode) GetAggregates() []expression.Expression { return p.aggregates }

/** @return the aggregate types */
func (p *AggregationPlanNode) GetAggregateTypes() []AggregationType { return p.aggTypes }

func (p *AggregationPlanNode) GetTableOID() uint32 {
	return math.MaxInt32
}

func (p *AggregationPlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	return p.children[0].AccessRowCount(c)
}

func (p *AggregationPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	return 1
}

func (p *AggregationPlanNode) GetDebugStr() string {
	// TODO: (SDB) [OPT] not implemented yet (AggregationPlanNode::GetDebugStr)
	panic("not implemented yet")
}

func (p *AggregationPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats
}

type AggregateKey struct {
	GroupBys []*types.Value
}

type AggregateValue struct {
	Aggregates []*types.Value
}
