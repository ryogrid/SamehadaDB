package plans

import (
	"github.com/brunocalza/go-bustub/storage/table"
	"github.com/brunocalza/go-bustub/types"
)

type InsertPlanNode struct {
	rawValues    [][]types.Value
	tableOID     uint32
	children     []Plan
	outputSchema *table.Schema
}

func NewInsertPlanNode(rawValues [][]types.Value, oid uint32) Plan {
	return &InsertPlanNode{rawValues, oid, nil, nil}
}

func (p *InsertPlanNode) GetTableOID() uint32 {
	return p.tableOID
}

func (p *InsertPlanNode) GetRawValues() [][]types.Value {
	return p.rawValues
}

func (p *InsertPlanNode) GetChildAt(childIndex uint32) Plan {
	return p.children[childIndex]
}

func (p *InsertPlanNode) GetChildren() []Plan {
	return p.children
}

func (p *InsertPlanNode) OutputSchema() *table.Schema {
	return p.outputSchema
}

func (p *InsertPlanNode) GetType() PlanType {
	return Insert
}
