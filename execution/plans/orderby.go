package plans

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
)

// /** OrderbyType enumerates all the possible aggregation functions in our system. */
type OrderbyType int32

/** The type of the sort order. */
const (
	ASC OrderbyType = iota
	DESC
)

/**
 * OrderbyPlanNode represents the ORDER BY clause of SQL.
 */
type OrderbyPlanNode struct {
	*AbstractPlanNode
	col_idxs_      []int
	orderby_types_ []OrderbyType
}

/**
 * Creates a new OrderbyPlanNode.
 * @param output_schema the output format of this plan node. it is same with output schema of child
 * @param child the child plan to sort data over
 * @param col_idxs the specified columns idx at ORDER BY clause
 * @param order_types the order types of sorting with specifed columns
 */
func NewOrderbyPlanNode(child_schema *schema.Schema, child Plan, col_idxs []int,
	order_types []OrderbyType) *OrderbyPlanNode {
	return &OrderbyPlanNode{&AbstractPlanNode{child_schema, []Plan{child}}, col_idxs, order_types}
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
func (p *OrderbyPlanNode) GetColIdxAt(idx uint32) int {
	return p.col_idxs_[idx]
}

/** @return column indexes to deside sort order */
func (p *OrderbyPlanNode) GetColIdxs() []int { return p.col_idxs_ }

/** @return the Order type ASC or DESC */
func (p *OrderbyPlanNode) GetOrderbyTypes() []OrderbyType { return p.orderby_types_ }

func (p *OrderbyPlanNode) GetTableOID() uint32 {
	return p.children[0].GetTableOID()
}

func (p *OrderbyPlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	// TODO: (SDB) [OPT] not implemented yet (OrderbyPlanNode::AccessRowCount)
	return 0
}

func (p *OrderbyPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	// TODO: (SDB) [OPT] not implemented yet (OrderbyPlanNode::EmitRowCount)
	return 0
}
