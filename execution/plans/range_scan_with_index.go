// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * RangeScanWithIndexPlanNode use hash index to filter rows matches predicate.
 */
type RangeScanWithIndexPlanNode struct {
	*AbstractPlanNode
	//predicate *expression.Comparison
	tableOID   uint32
	startRange *types.Value
	endRange   *types.Value
}

func NewRangeScanWithIndexPlanNode(schema *schema.Schema, tableOID uint32, startRange *types.Value, endRange *types.Value) Plan {
	return &RangeScanWithIndexPlanNode{&AbstractPlanNode{schema, nil}, tableOID, startRange, endRange}
}

func (p *RangeScanWithIndexPlanNode) GetStartRange() *types.Value {
	return p.startRange
}

func (p *RangeScanWithIndexPlanNode) GetEndRange() *types.Value {
	return p.endRange
}

func (p *RangeScanWithIndexPlanNode) GetTableOID() uint32 {
	return p.tableOID
}

func (p *RangeScanWithIndexPlanNode) GetType() PlanType {
	return IndexRangeScan
}
