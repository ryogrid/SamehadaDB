// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * InsertPlanNode identifies a table that should be inserted into.
 * The values to be inserted are either embedded into the InsertPlanNode itself, i.e. a "raw insert",
 * or will come from the child of the InsertPlanNode. To simplify the assignment, InsertPlanNode has at most one child.
 */
type InsertPlanNode struct {
	*AbstractPlanNode
	rawValues [][]types.Value
	tableOID  uint32
}

// NewInsertPlanNode creates a new insert plan node for inserting raw values
func NewInsertPlanNode(rawValues [][]types.Value, oid uint32) Plan {
	return &InsertPlanNode{&AbstractPlanNode{nil, nil}, rawValues, oid}
}

// GetTableOID returns the identifier of the table that should be inserted into
func (p *InsertPlanNode) GetTableOID() uint32 {
	return p.tableOID
}

// GetRawValues returns the raw values to be inserted
func (p *InsertPlanNode) GetRawValues() [][]types.Value {
	return p.rawValues
}

func (p *InsertPlanNode) GetType() PlanType {
	return Insert
}

func (p *InsertPlanNode) AccessRowCount() uint64 {
	// TODO: (SDB) [OPT] not implemented yet (InsertPlanNode::AccessRowCount)
	return 0
}
