package plans

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
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
	var tmpStats *catalog.TableStatistics
	samehada_util.DeepCopy(tmpStats, child.GetStatistics())
	return &SelectionPlanNode{&AbstractPlanNode{childOutSchema, []Plan{child}}, predicate, tmpStats}
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
	// 	  return std::ceil(static_cast<double>(src_->EmitRowCount()) /
	//	                   stats_.ReductionFactor(GetSchema(), exp_));
	return uint64(math.Ceil(float64(
		p.children[0].EmitRowCount(c)) /
		p.children[0].GetStatistics().ReductionFactor(*p.children[0].OutputSchema(), p.predicate)))
	//c.GetTableByOID(p.GetTableOID()).GetStatistics().ReductionFactor(*p.children[0].OutputSchema(), p.predicate)))
}

func (p *SelectionPlanNode) GetTreeInfoStr() string {
	// TODO: (SDB) [OPT] not implemented yet (SelectionPlanNode::GetTreeInfoStr)
	panic("not implemented yet")
}

func (p *SelectionPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}
