package plans

import (
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"math"
)

type ProjectionPlanNode struct {
	*AbstractPlanNode
	stats_ *catalog.TableStatistics
}

func NewProjectionPlanNode(child Plan, projectColumns *schema.Schema) Plan {
	return &ProjectionPlanNode{&AbstractPlanNode{projectColumns, []Plan{child}}, child.GetStatistics().GetDeepCopy()}
}

func (p *ProjectionPlanNode) GetType() PlanType {
	return Projection
}

func (p *ProjectionPlanNode) GetTableOID() uint32 {
	return math.MaxInt32
}

func (p *ProjectionPlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	return p.children[0].AccessRowCount(c)
}

func (p *ProjectionPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	return p.children[0].EmitRowCount(c)
}

func (p *ProjectionPlanNode) GetDebugStr() string {
	projColNames := "["
	for _, col := range p.outputSchema.GetColumns() {
		projColNames += col.GetColumnName() + ", "
	}
	return "ProjectionPlanNode " + projColNames + "]"
}

func (p *ProjectionPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}
