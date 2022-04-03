package plans

import (
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * UpdatePlanNode identifies a table and conditions specify record to be deleted.
 */
type UpdatePlanNode struct {
	*AbstractPlanNode
	rawValues []types.Value
	predicate *expression.Expression
	tableOID  uint32
}

func NewUpdatePlanNode(rawValues []types.Value, predicate *expression.Expression, oid uint32) Plan {
	return &UpdatePlanNode{&AbstractPlanNode{nil, nil}, rawValues, predicate, oid}
}

func (p *UpdatePlanNode) GetTableOID() uint32 {
	return p.tableOID
}

func (p *UpdatePlanNode) GetPredicate() *expression.Expression {
	return p.predicate
}

func (p *UpdatePlanNode) GetType() PlanType {
	return Delete
}

// GetRawValues returns the raw values to be overwrite data
func (p *UpdatePlanNode) GetRawValues() []types.Value {
	return p.rawValues
}
