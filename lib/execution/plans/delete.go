package plans

import (
	"github.com/ryogrid/SamehadaDB/lib/catalog"
)

/**
 * DeletePlanNode identifies a table and conditions specify record to be deleted.
 */
type DeletePlanNode struct {
	*AbstractPlanNode
	stats_ *catalog.TableStatistics
}

func NewDeletePlanNode(child Plan) Plan {
	return &DeletePlanNode{&AbstractPlanNode{nil, []Plan{child}}, child.GetStatistics().GetDeepCopy()}
}

func (p *DeletePlanNode) GetTableOID() uint32 {
	return p.children[0].GetTableOID()
}

func (p *DeletePlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	return p.children[0].EmitRowCount(c)
}

func (p *DeletePlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	return p.children[0].EmitRowCount(c)
}

func (p *DeletePlanNode) GetDebugStr() string {
	// TODO: (SDB) [OPT] not implemented yet (DeletePlanNode::GetDebugStr)
	panic("not implemented yet")
}

func (p *DeletePlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}

func (p *DeletePlanNode) GetType() PlanType {
	return Delete
}
