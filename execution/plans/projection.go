package plans

import (
	"github.com/ryogrid/SamehadaDB/parser"
)

// TODO: (SDB) not implemented yet (projection.go)

type ProjectionPlanNode struct {
	*AbstractPlanNode
	projectionColumns []*parser.SelectFieldExpression
}

func NewProjectionPlanNode(child Plan, selectColumns []*parser.SelectFieldExpression) Plan {
	childOutSchema := child.OutputSchema()
	return &ProjectionPlanNode{&AbstractPlanNode{childOutSchema, []Plan{child}}, selectColumns}
}

func (p *ProjectionPlanNode) GetType() PlanType {
	return Projection
}

func (p *ProjectionPlanNode) GetProjectionColumns() []*parser.SelectFieldExpression {
	return p.projectionColumns
}

func (p *ProjectionPlanNode) GetTableOID() uint32 {
	return p.children[0].GetTableOID()
}

func (p *ProjectionPlanNode) AccessRowCount() uint64 {
	// TODO: (SDB) not implemented yet
	return 0
}
