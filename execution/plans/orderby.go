package plans

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
)

// /** OrderbyType enumerates all the possible aggregation functions in our system. */
type OrderbyType int32

/** The type of the log record. */
const (
	ASC OrderbyType = iota
	DESC
)

/**
 * AggregationPlanNode represents the various SQL aggregation functions.
 * For example, COUNT(), SUM(), MIN() and MAX().
 * To simplfiy this project, AggregationPlanNode must always have exactly one child.
 */
type OrderbyPlanNode struct {
	*AbstractPlanNode
	order_bys_     []expression.Expression
	orderby_types_ []OrderbyType
}

/**
 * Creates a new OrderbyPlanNode.
 * @param output_schema the output format of this plan node
 * @param child the child plan to aggregate data over
 * @param having the having clause of the aggregation
 * @param group_bys the group by clause of the aggregation
 * @param aggregates the expressions that we are aggregating
 * @param agg_types the types that we are aggregating
 */
func NewOrderbyPlanNode(output_schema *schema.Schema, child Plan, having expression.Expression,
	group_bys []expression.Expression,
	aggregates []expression.Expression, agg_types []AggregationType) *OrderbyPlanNode {
	return &OrderbyPlanNode{&AbstractPlanNode{output_schema, []Plan{child}}, having, group_bys, aggregates, agg_types}
}

func (p *OrderbyPlanNode) GetType() PlanType { return Orderby }

/** @return the child of this aggregation plan node */
func (p *OrderbyPlanNode) GetChildPlan() Plan {
	common.SH_Assert(len(p.GetChildren()) == 1, "OrderBy expected to only have one child.")
	return p.GetChildAt(0)
}

func (p *OrderbyPlanNode) GetChildAt(childIndex uint32) Plan {
	return p.children[childIndex]
}

func (p *OrderbyPlanNode) GetChildren() []Plan {
	return p.children
}

/** @return the idx'th group by expression */
func (p *OrderbyPlanNode) GetOrderByAt(idx uint32) expression.Expression {
	return p.order_bys_[idx]
}

/** @return the group by expressions */
func (p *OrderbyPlanNode) GetOrderBys() []expression.Expression { return p.order_bys_ }

/** @return the aggregate types */
func (p *OrderbyPlanNode) GetAggregateTypes() []OrderbyType { return p.orderby_types_ }
