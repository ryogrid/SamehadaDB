package plans

import (
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
)

// do selection according to WHERE clause for Plan(Executor) which has no selection feature

type SelectionPlanNode struct {
	*AbstractPlanNode
	// TODO: (SDB) [OPT] SelectionPlanNode::selectColumns should be removed
	selectColumns *schema.Schema
	predicate     expression.Expression
}

func NewSelectionPlanNode(child Plan, selectColumns *schema.Schema, predicate expression.Expression) Plan {
	childOutSchema := child.OutputSchema()
	return &SelectionPlanNode{&AbstractPlanNode{childOutSchema, []Plan{child}}, selectColumns, predicate}
}

func (p *SelectionPlanNode) GetType() PlanType {
	return Selection
}

func (p *SelectionPlanNode) GetPredicate() expression.Expression {
	return p.predicate
}

func (p *SelectionPlanNode) GetSelectColumns() *schema.Schema {
	return p.selectColumns
}

func (p *SelectionPlanNode) GetTableOID() uint32 {
	return p.children[0].GetTableOID()
}

func (p *SelectionPlanNode) AccessRowCount() uint64 {
	// TODO: (SDB) [OPT] not implemented yet (SelectionPlanNode::AccessRowCount)
	return 0
}
