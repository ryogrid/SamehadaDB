package plans

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"math"
)

type NestedLoopJoinPlanNode struct {
	*AbstractPlanNode
	stats_ *catalog.TableStatistics
}

func GenNestedLoopJoinStats(leftPlan Plan, rightPlan Plan) *catalog.TableStatistics {
	leftStats := leftPlan.GetStatistics().GetDeepCopy()
	leftStats.Multiply(float64(leftStats.Rows()))
	rightStats := rightPlan.GetStatistics().GetDeepCopy()
	rightStats.Multiply(float64(rightStats.Rows()))
	leftStats.Concat(rightStats)
	return leftStats
}

// used only for Cross Join
func NewNestedLoopJoinPlanNode(children []Plan) *NestedLoopJoinPlanNode {
	return &NestedLoopJoinPlanNode{
		&AbstractPlanNode{makeMergedOutputSchema(children[0].OutputSchema(), children[1].OutputSchema()), children},
		GenNestedLoopJoinStats(children[0], children[1])}
}

func (p *NestedLoopJoinPlanNode) GetType() PlanType { return NestedLoopJoin }

func (p *NestedLoopJoinPlanNode) GetLeftPlan() Plan {
	common.SH_Assert(len(p.GetChildren()) == 2, "nested loop joins should have exactly two children plans.")
	return p.GetChildAt(0)
}

func (p *NestedLoopJoinPlanNode) GetRightPlan() Plan {
	common.SH_Assert(len(p.GetChildren()) == 2, "nested loop joins should have exactly two children plans.")
	return p.GetChildAt(1)
}

// can not be used
func (p *NestedLoopJoinPlanNode) GetTableOID() uint32 {
	return math.MaxUint32
}

func (p *NestedLoopJoinPlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	return p.GetLeftPlan().AccessRowCount(c) +
		(1 + p.GetLeftPlan().EmitRowCount(c)*p.GetRightPlan().AccessRowCount(c))
}

func (p *NestedLoopJoinPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	return p.GetLeftPlan().EmitRowCount(c) * p.GetRightPlan().EmitRowCount(c)
}

func (p *NestedLoopJoinPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}

func (p *NestedLoopJoinPlanNode) GetDebugStr() string {
	// TODO: (SDB) [OPT] not implemented yet (NestedLoopJoinPlanNode::GetDebugStr)
	panic("not implemented yet")
}
