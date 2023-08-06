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
	/*
	  if (left_cols_.empty() && right_cols_.empty()) {
	    return left_src_->AccessRowCount() +
	           (1 + left_src_->EmitRowCount() * right_src_->AccessRowCount());
	  }
	  if (right_tbl_ != nullptr) {
	    // IndexJoin.
	    return left_src_->AccessRowCount() * 3;
	  }
	  // Cost of hash join.
	  return left_src_->AccessRowCount() + right_src_->AccessRowCount();
	*/
	return 0
}

func (p *NestedLoopJoinPlanNode) EmitRowCount() uint64 {
	// TODO: (SDB) [OPT] not implemented yet (NestedLoopJoinPlanNode::EmitRowCount)
	/*
	  if (left_cols_.empty() && right_cols_.empty()) {  // CrossJoin.
	    return left_src_->EmitRowCount() * right_src_->EmitRowCount();
	  }
	  if (right_tbl_ != nullptr) {  // IndexJoin
	    return std::min(left_src_->EmitRowCount(), right_ts_->Rows());
	  }
	  return std::min(left_src_->EmitRowCount(), right_src_->EmitRowCount());
	*/
	return 1
}
