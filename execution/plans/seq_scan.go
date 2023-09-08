// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
)

/**
 * SeqScanPlanNode identifies a table that should be scanned with an optional predicate.
 */
type SeqScanPlanNode struct {
	*AbstractPlanNode
	predicate expression.Expression
	tableOID  uint32
	stats_    *catalog.TableStatistics
}

func NewSeqScanPlanNode(c *catalog.Catalog, schema *schema.Schema, predicate expression.Expression, tableOID uint32) Plan {
	tm := c.GetTableByOID(tableOID)
	return &SeqScanPlanNode{&AbstractPlanNode{schema, nil}, predicate, tableOID, tm.GetStatistics().GetDeepCopy()}
}

func (p *SeqScanPlanNode) GetPredicate() expression.Expression {
	return p.predicate
}

func (p *SeqScanPlanNode) GetTableOID() uint32 {
	return p.tableOID
}

func (p *SeqScanPlanNode) GetType() PlanType {
	return SeqScan
}

func (p *SeqScanPlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	// return stats_.Rows();
	return c.GetTableByOID(p.GetTableOID()).GetStatistics().Rows()
}

func (p *SeqScanPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	// assumption: selection with predicate is not used
	return p.AccessRowCount(c)
}

func (p *SeqScanPlanNode) GetDebugStr() string {
	// TODO: (SDB) [OPT] not implemented yet (SeqScanPlanNode::GetDebugStr)
	return "SeqScanPlanNode"
}

func (p *SeqScanPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}
