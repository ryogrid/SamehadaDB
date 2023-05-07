package plans

import (
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
)

// TODO: (SDB) not implemented yet (projection.go)

type ProjectionPlanNode struct {
	*AbstractPlanNode
	projectionColumns *schema.Schema
}

func NewProjectionPlanNode(child Plan, selectColumns *schema.Schema, predicate expression.Expression) Plan {
	childOutSchema := child.OutputSchema()
	return &ProjectionPlanNode{&AbstractPlanNode{childOutSchema, []Plan{child}}, selectColumns}
}

func (p *ProjectionPlanNode) GetType() PlanType {
	return Filter
}

func (p *ProjectionPlanNode) GetProjectionColumns() *schema.Schema {
	return p.projectionColumns
}

func (p *ProjectionPlanNode) GetTableOID() uint32 {
	return p.children[0].GetTableOID()
}
