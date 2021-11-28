// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SaitomDB/execution/expression"
	"github.com/ryogrid/SaitomDB/storage/table"
)

type SeqScanPlanNode struct {
	*AbstractPlanNode
	predicate *expression.Expression
	tableOID  uint32
}

func NewSeqScanPlanNode(schema *table.Schema, predicate *expression.Expression, tableOID uint32) Plan {
	return &SeqScanPlanNode{&AbstractPlanNode{schema, nil}, predicate, tableOID}
}

func (p *SeqScanPlanNode) GetPredicate() *expression.Expression {
	return p.predicate
}

func (p *SeqScanPlanNode) GetTableOID() uint32 {
	return p.tableOID
}

func (p *SeqScanPlanNode) GetType() PlanType {
	return SeqScan
}
