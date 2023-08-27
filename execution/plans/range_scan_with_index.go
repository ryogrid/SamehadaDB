// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
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
	stats_     *catalog.TableStatistics
}

func NewRangeScanWithIndexPlanNode(c *catalog.Catalog, schema *schema.Schema, tableOID uint32, colIdx int32, predicate expression.Expression, startRange *types.Value, endRange *types.Value) Plan {
	var tmpStats *catalog.TableStatistics
	tm := c.GetTableByOID(tableOID)
	samehada_util.DeepCopy(tmpStats, tm.GetStatistics())
	return &RangeScanWithIndexPlanNode{&AbstractPlanNode{schema, nil}, predicate, tableOID, colIdx, startRange, endRange, tmpStats}
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

func (p *RangeScanWithIndexPlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	return p.EmitRowCount(c)
}

func (p *RangeScanWithIndexPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	// 	TODO: (SDB) if (index_.IsUnique() && begin_ == end_) { return 1; } (RangeScanWithIndexPlanNode::EmitRowCount)
	return uint64(math.Ceil(c.GetTableByOID(p.tableOID).GetStatistics().EstimateCount(p.colIdx, p.startRange, p.endRange)))
}

func (p *RangeScanWithIndexPlanNode) GetTreeInfoStr() string {
	// TODO: (SDB) [OPT] not implemented yet (RangeScanWithIndexPlanNode::GetTreeInfoStr)
	panic("not implemented yet")
}

func (p *RangeScanWithIndexPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}
