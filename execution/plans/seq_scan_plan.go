package plans

import (
	"github.com/brunocalza/go-bustub/execution/expression"
	"github.com/brunocalza/go-bustub/storage/table"
)

type SeqScanPlanNode struct {
	outputSchema *table.Schema
	predicate    *expression.Expression
	tableOID     uint32
	children     []Plan
}

func NewSeqScanPlanNode(schema *table.Schema, predicate *expression.Expression, tableOID uint32, outputSchema *table.Schema) *SeqScanPlanNode {
	return &SeqScanPlanNode{schema, predicate, tableOID, nil}
}

func (p *SeqScanPlanNode) GetPredicate() *expression.Expression {
	return p.predicate
}

func (p *SeqScanPlanNode) GetTableOID() uint32 {
	return p.tableOID
}

func (p *SeqScanPlanNode) GetChildAt(childIndex uint32) Plan {
	return p.children[childIndex]
}

func (p *SeqScanPlanNode) GetChildren() []Plan {
	return p.children
}

func (p *SeqScanPlanNode) OutputSchema() *table.Schema {
	return p.outputSchema
}

func (p *SeqScanPlanNode) GetType() PlanType {
	return SeqScan
}
