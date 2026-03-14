package plans

import (
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
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
	colIdxs      []int
	orderbyTypes []OrderbyType
	stats         *catalog.TableStatistics
}

/**
 * Creates a new OrderbyPlanNode.
 * @param output_schema the output format of this plan node. it is same with output schema of child
 * @param child the child plan to sort data over
 * @param colIdxs the specified columns idx at ORDER BY clause
 * @param orderTypes the order types of sorting with specifed columns
 */
func NewOrderbyPlanNode(childSchema *schema.Schema, child Plan, colIdxs []int,
	orderTypes []OrderbyType) *OrderbyPlanNode {
	return &OrderbyPlanNode{&AbstractPlanNode{childSchema, []Plan{child}}, colIdxs, orderTypes, child.GetStatistics().GetDeepCopy()}
}

func (p *OrderbyPlanNode) GetType() PlanType { return Orderby }

/** @return the child of this aggregation plan node */
func (p *OrderbyPlanNode) GetChildPlan() Plan {
	common.SHAssert(len(p.GetChildren()) == 1, "OrderBy expected to only have one child.")
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
	return p.colIdxs[idx]
}

/** @return column indexes to deside sort order */
func (p *OrderbyPlanNode) GetColIdxs() []int { return p.colIdxs }

/** @return the Order type ASC or DESC */
func (p *OrderbyPlanNode) GetOrderbyTypes() []OrderbyType { return p.orderbyTypes }

func (p *OrderbyPlanNode) GetTableOID() uint32 {
	return p.children[0].GetTableOID()
}

func (p *OrderbyPlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	return p.children[0].AccessRowCount(c)
}

func (p *OrderbyPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	return p.children[0].EmitRowCount(c)
}

func (p *OrderbyPlanNode) GetDebugStr() string {
	// TODO: (SDB) [OPT] not implemented yet (OrderbyPlanNode::GetDebugStr)
	panic("not implemented yet")
}

func (p *OrderbyPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats
}
