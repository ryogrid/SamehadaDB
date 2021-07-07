package plans

import (
	"github.com/brunocalza/go-bustub/storage/table"
	"github.com/brunocalza/go-bustub/types"
)

// InsertPlanNode identifies a table that should be inserted into
// The values to be inserted are embedded into the InsertPlanNode itself
type InsertPlanNode struct {
	rawValues    [][]types.Value
	tableOID     uint32
	children     []Plan
	outputSchema *table.Schema
}

// NewInsertPlanNode creates a new insert plan node for inserting raw values
func NewInsertPlanNode(rawValues [][]types.Value, oid uint32) Plan {
	return &InsertPlanNode{rawValues, oid, nil, nil}
}

// GetTableOID returns the identifier of the table that should be inserted into
func (p *InsertPlanNode) GetTableOID() uint32 {
	return p.tableOID
}

// GetRawValues returns the raw values to be inserted
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
