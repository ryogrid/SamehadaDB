package plans

import (
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"math"
)

// do selection according to WHERE clause for Plan(Executor) which has no selection functionality

type SelectionPlanNode struct {
	*AbstractPlanNode
	predicate expression.Expression
	stats_    *catalog.TableStatistics
}

func NewSelectionPlanNode(child Plan, predicate expression.Expression) Plan {
	childOutSchema := child.OutputSchema()
	return &SelectionPlanNode{&AbstractPlanNode{childOutSchema, []Plan{child}}, predicate, child.GetStatistics().GetDeepCopy()}
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
	return p.children[0].AccessRowCount(c)
}

func (p *SelectionPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	return uint64(math.Ceil(float64(
		p.children[0].EmitRowCount(c)) /
		p.children[0].GetStatistics().ReductionFactor(p.children[0].OutputSchema(), p.predicate)))
}

func (p *SelectionPlanNode) GetDebugStr() string {
	return "SelectionPlanNode [ " + expression.GetExpTreeStr(p.predicate) + "]"
}

func (p *SelectionPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}
