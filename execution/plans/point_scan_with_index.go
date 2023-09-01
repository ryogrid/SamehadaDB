// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
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
	var tmpStats *catalog.TableStatistics
	tm := c.GetTableByOID(tableOID)
	samehada_util.DeepCopy(tmpStats, tm.GetStatistics())
	return &PointScanWithIndexPlanNode{&AbstractPlanNode{schema, nil}, predicate, tableOID, tmpStats}
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

func (p *PointScanWithIndexPlanNode) GetTreeInfoStr() string {
	// TODO: (SDB) [OPT] not implemented yet (PointScanWithIndexPlanNode::GetTreeInfoStr)
	panic("not implemented yet")
}

func (p *PointScanWithIndexPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}
