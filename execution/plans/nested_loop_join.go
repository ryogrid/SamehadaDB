package plans

import (
	"github.com/ryogrid/SamehadaDB/common"
	"math"
)

type NestedLoopJoinPlanNode struct {
	*AbstractPlanNode
}

// used only for Cross Join
func NewNestedLoopJoinPlanNode(children []Plan) *NestedLoopJoinPlanNode {
	return &NestedLoopJoinPlanNode{&AbstractPlanNode{makeMergedOutputSchema(children[0].OutputSchema(), children[1].OutputSchema()), children}}
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

func (p *NestedLoopJoinPlanNode) AccessRowCount() uint64 {
	// TODO: (SDB) [OPT] not implemented yet (NestedLoopJoinPlanNode::AccessRowCount)
	return 0
}
