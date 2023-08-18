package plans

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"math"
)

type ProjectionPlanNode struct {
	*AbstractPlanNode
}

func NewProjectionPlanNode(child Plan, projectColumns *schema.Schema) Plan {
	return &ProjectionPlanNode{&AbstractPlanNode{projectColumns, []Plan{child}}}
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
