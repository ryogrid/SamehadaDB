package plans

import (
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
)

// TODO: (SDB) [OPT] not implemented yet (projection.go)

type ProjectionPlanNode struct {
	*AbstractPlanNode
	//projectionColumns []*parser.SelectFieldExpression
	projectionColumns *schema.Schema
}

func NewProjectionPlanNode(child Plan, selectColumns *schema.Schema) Plan {
	childOutSchema := child.OutputSchema()
	return &ProjectionPlanNode{&AbstractPlanNode{childOutSchema, []Plan{child}}, selectColumns}
}

func (p *ProjectionPlanNode) GetType() PlanType {
	return Projection
}

func (p *ProjectionPlanNode) GetProjectionColumns() *schema.Schema {
	return p.projectionColumns
}

func (p *ProjectionPlanNode) GetTableOID() uint32 {
	return p.children[0].GetTableOID()
}

func (p *ProjectionPlanNode) AccessRowCount() uint64 {
	// TODO: (SDB) [OPT] not implemented yet (ProjectionPlanNode::AccessRowCount)
	return 0
}
