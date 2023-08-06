// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
)

/**
 * RangeScanWithIndexPlanNode use hash index to filter rows matches predicate.
 */
type RangeScanWithIndexPlanNode struct {
	*AbstractPlanNode
	predicate  expression.Expression
	tableOID   uint32
	colIdx     int32 // column idx which has index to be used
	startRange *types.Value
	endRange   *types.Value
}

func NewRangeScanWithIndexPlanNode(schema *schema.Schema, tableOID uint32, colIdx int32, predicate expression.Expression, startRange *types.Value, endRange *types.Value) Plan {
	return &RangeScanWithIndexPlanNode{&AbstractPlanNode{schema, nil}, predicate, tableOID, colIdx, startRange, endRange}
}

func (p *RangeScanWithIndexPlanNode) GetPredicate() expression.Expression {
	return p.predicate
}

func (p *RangeScanWithIndexPlanNode) GetTableOID() uint32 {
	return p.tableOID
}

func (p *RangeScanWithIndexPlanNode) GetColIdx() int32 {
	return p.colIdx
}

func (p *RangeScanWithIndexPlanNode) GetStartRange() *types.Value {
	return p.startRange
}

func (p *RangeScanWithIndexPlanNode) GetEndRange() *types.Value {
	return p.endRange
}

func (p *RangeScanWithIndexPlanNode) GetType() PlanType {
	return IndexRangeScan
}

func (p *RangeScanWithIndexPlanNode) EmitRowCount() uint64 {
	// TODO: (SDB) [OPT] not implemented yet (RangeScanWithIndexPlanNode::EmitRowCount)
	/*
	   	if (index_.IsUnique() && begin_ == end_) {
	   return 1;
	   }
	   return std::ceil(stats_.EstimateCount(index_.sc_.key_[0], begin_, end_));
	*/
	return 1
}

func (p *RangeScanWithIndexPlanNode) AccessRowCount() uint64 {
	return p.EmitRowCount()
}
