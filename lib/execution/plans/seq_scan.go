// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
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
	return c.GetTableByOID(p.GetTableOID()).GetStatistics().Rows()
}

func (p *SeqScanPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	// assumption: selection with predicate is not used
	return p.AccessRowCount(c)
}

func (p *SeqScanPlanNode) GetDebugStr() string {
	outColNames := "["
	for _, col := range p.OutputSchema().GetColumns() {
		outColNames += col.GetColumnName() + ", "
	}

	return "SeqScanPlanNode " + outColNames + "]"
}

func (p *SeqScanPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}
