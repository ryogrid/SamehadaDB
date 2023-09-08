// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SamehadaDB/catalog"
)

type LimitPlanNode struct {
	*AbstractPlanNode
	limit  uint32
	offset uint32
	stats_ *catalog.TableStatistics
}

func NewLimitPlanNode(child Plan, limit uint32, offset uint32) Plan {
	return &LimitPlanNode{&AbstractPlanNode{nil, []Plan{child}}, limit, offset, child.GetStatistics().GetDeepCopy()}
}

func (p *LimitPlanNode) GetLimit() uint32 {
	return p.limit
}

func (p *LimitPlanNode) GetOffset() uint32 {
	return p.offset
}

func (p *LimitPlanNode) GetType() PlanType {
	return Limit
}

func (p *LimitPlanNode) GetTableOID() uint32 {
	return p.children[0].GetTableOID()
}

func (p *LimitPlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	return p.children[0].AccessRowCount(c)
}

func (p *LimitPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	return uint64(p.limit)
}

func (p *LimitPlanNode) GetDebugStr() string {
	// TODO: (SDB) [OPT] not implemented yet (LimitPlanNode::GetDebugStr)
	panic("not implemented yet")
}

func (p *LimitPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}
