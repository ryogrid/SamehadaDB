// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"math"
)

/**
 * PointScanWithIndexPlanNode use hash index to filter rows matches predicate.
 */
type PointScanWithIndexPlanNode struct {
	*AbstractPlanNode
	predicate *expression.Comparison
	tableOID  uint32
	stats_    *catalog.TableStatistics
}

func NewPointScanWithIndexPlanNode(c *catalog.Catalog, schema *schema.Schema, predicate *expression.Comparison, tableOID uint32) Plan {
	tm := c.GetTableByOID(tableOID)
	return &PointScanWithIndexPlanNode{&AbstractPlanNode{schema, nil}, predicate, tableOID, tm.GetStatistics().GetDeepCopy()}
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

func (p *PointScanWithIndexPlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	return p.EmitRowCount(c)
}

func (p *PointScanWithIndexPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	tm := c.GetTableByOID(p.tableOID)
	return uint64(math.Ceil(
		float64(tm.GetStatistics().Rows()) /
			tm.GetStatistics().ReductionFactor(tm.Schema(), p.predicate)))
}

func (p *PointScanWithIndexPlanNode) GetDebugStr() string {
	// TODO: (SDB) [OPT] not implemented yet (PointScanWithIndexPlanNode::GetDebugStr)

	outColNames := "["
	for _, col := range p.OutputSchema().GetColumns() {
		outColNames += col.GetColumnName() + ", "
	}
	return "PointScanWithIndexPlanNode " + outColNames + "]"
}

func (p *PointScanWithIndexPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}
