package plans

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"math"
)

type ProjectionPlanNode struct {
	*AbstractPlanNode
	stats_ *catalog.TableStatistics
}

func NewProjectionPlanNode(child Plan, projectColumns *schema.Schema) Plan {
	var tmpStats *catalog.TableStatistics
	samehada_util.DeepCopy(tmpStats, child.GetStatistics())
	return &ProjectionPlanNode{&AbstractPlanNode{projectColumns, []Plan{child}}, tmpStats}
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

func (p *ProjectionPlanNode) GetTreeInfoStr() string {
	// TODO: (SDB) [OPT] not implemented yet (ProjectionPlanNode::GetTreeInfoStr)
	panic("not implemented yet")
}

func (p *ProjectionPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}
