package plans

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
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
	return p.children[0].GetTableOID()
}

func (p *ProjectionPlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	// TODO: (SDB) [OPT] not implemented yet (ProjectionPlanNode::AccessRowCount)
	/*
		return src_->AccessRowCount();
	*/
	return 0
}

func (p *ProjectionPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	// TODO: (SDB) [OPT] not implemented yet (ProjectionPlanNode::EmitRowCount)
	/*
		return src_->EmitRowCount();
	*/
	return 0
}
