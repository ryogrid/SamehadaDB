package plans

import (
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
)

// do filtering according to WHERE clause for Plan(Executor) which has no filtering feature

type FilterPlanNode struct {
	*AbstractPlanNode
	selectColumns *schema.Schema
	predicate     expression.Expression
}

func NewFilterPlanNode(child Plan, selectColumns *schema.Schema, predicate expression.Expression) Plan {
	childOutSchema := child.OutputSchema()
	return &FilterPlanNode{&AbstractPlanNode{childOutSchema, []Plan{child}}, selectColumns, predicate}
}

func (p *FilterPlanNode) GetType() PlanType {
	return Filter
}

func (p *FilterPlanNode) GetPredicate() expression.Expression {
	return p.predicate
}

func (p *FilterPlanNode) GetSelectColumns() *schema.Schema {
	return p.selectColumns
}
