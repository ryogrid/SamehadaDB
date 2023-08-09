package plans

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
)

// do selection according to WHERE clause for Plan(Executor) which has no selection functionality

type SelectionPlanNode struct {
	*AbstractPlanNode
	predicate expression.Expression
}

func NewSelectionPlanNode(child Plan, predicate expression.Expression) Plan {
	childOutSchema := child.OutputSchema()
	return &SelectionPlanNode{&AbstractPlanNode{childOutSchema, []Plan{child}}, predicate}
}

func (p *SelectionPlanNode) GetType() PlanType {
	return Selection
}

func (p *SelectionPlanNode) GetPredicate() expression.Expression {
	return p.predicate
}

func (p *SelectionPlanNode) GetTableOID() uint32 {
	return p.children[0].GetTableOID()
}

func (p *SelectionPlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	// TODO: (SDB) [OPT] not implemented yet (SelectionPlanNode::AccessRowCount)
	/*
		return src_->EmitRowCount();
	*/
	return 0
}

func (p *SelectionPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	// TODO: (SDB) [OPT] not implemented yet (SelectionPlanNode::EmitRowCount)
	/*
	  return std::ceil(static_cast<double>(src_->EmitRowCount()) /
	                   stats_.ReductionFactor(GetSchema(), exp_));
	*/
	return 1
}
