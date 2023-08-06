// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
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
}

func NewSeqScanPlanNode(schema *schema.Schema, predicate expression.Expression, tableOID uint32) Plan {
	return &SeqScanPlanNode{&AbstractPlanNode{schema, nil}, predicate, tableOID}
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

func (p *SeqScanPlanNode) AccessRowCount() uint64 {
	// TODO: (SDB) [OPT] not implemented yet (SeqScanPlanNode::AccessRowCount)
	/*
		return stats_.Rows();
	*/
	return 0
}

func (p *SeqScanPlanNode) EmitRowCount() uint64 {
	// TODO: (SDB) [OPT] not implemented yet (SeqScanPlanNode::EmitRowCount)
	return 0
}
