package plans

import "github.com/ryogrid/SamehadaDB/execution/expression"

/**
 * DeletePlanNode identifies a table and conditions specify record to be deleted.
 */
type DeletePlanNode struct {
	*AbstractPlanNode
	predicate expression.Expression
	tableOID  uint32
}

func NewDeletePlanNode(predicate expression.Expression, oid uint32) Plan {
	return &DeletePlanNode{&AbstractPlanNode{nil, nil}, predicate, oid}
}

func (p *DeletePlanNode) GetTableOID() uint32 {
	return p.tableOID
}

func (p *DeletePlanNode) GetPredicate() expression.Expression {
	return p.predicate
}

func (p *DeletePlanNode) GetType() PlanType {
	return Delete
}
