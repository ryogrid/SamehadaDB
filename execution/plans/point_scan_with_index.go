// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
)

/**
 * PointScanWithIndexPlanNode use hash index to filter rows matches predicate.
 */
type PointScanWithIndexPlanNode struct {
	*AbstractPlanNode
	predicate *expression.Comparison
	tableOID  uint32
}

func NewPointScanWithIndexPlanNode(schema *schema.Schema, predicate *expression.Comparison, tableOID uint32) Plan {
	return &PointScanWithIndexPlanNode{&AbstractPlanNode{schema, nil}, predicate, tableOID}
}

func (p *PointScanWithIndexPlanNode) GetPredicate() *expression.Comparison {
	return p.predicate
}

func (p *PointScanWithIndexPlanNode) GetTableOID() uint32 {
	return p.tableOID
}

func (p *PointScanWithIndexPlanNode) GetType() PlanType {
	return IndexPointScan
}

func (p *PointScanWithIndexPlanNode) AccessRowCount() uint64 {
	// TODO: (SDB) [SDB] not implemented yet (PointScanWithIndexPlanNode::AccessRowCount)
	return 0
}
