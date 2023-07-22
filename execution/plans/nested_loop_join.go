package plans

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"math"
)

type NestedLoopJoinPlanNode struct {
	*AbstractPlanNode
}

func makeOutputSchema(left_schema *schema.Schema, right_schema *schema.Schema) *schema.Schema {
	var ret *schema.Schema
	columns := make([]*column.Column, 0)
	for _, col := range left_schema.GetColumns() {
		columns = append(columns, col)
	}
	for _, col := range right_schema.GetColumns() {
		columns = append(columns, col)
	}
	ret = schema.NewSchema(columns)
	return ret
}

// used only for Cross Join
func NewNestedLoopJoinPlanNode(children []Plan) *NestedLoopJoinPlanNode {
	return &NestedLoopJoinPlanNode{&AbstractPlanNode{makeOutputSchema(children[0].OutputSchema(), children[1].OutputSchema()), children}}
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
