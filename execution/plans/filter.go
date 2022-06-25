package plans

import "github.com/ryogrid/SamehadaDB/execution/expression"

// do filtering according to WHERE clause for Plan(Executor) which has no filtering feature

type FilterPlanNode struct {
	*AbstractPlanNode
	predicate expression.Expression
}

func NewFilterPlanNode(child Plan, predicate expression.Expression) Plan {
	childOutSchema := child.OutputSchema()
	return &FilterPlanNode{&AbstractPlanNode{childOutSchema, []Plan{child}}, predicate}
}

func (p *FilterPlanNode) GetType() PlanType {
	return Filter
}

func (p *FilterPlanNode) GetPredicate() expression.Expression {
	return p.predicate
}
