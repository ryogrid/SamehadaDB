// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
)

/* TODO: (SDB) not implemented yet (HashScanIndexNode) */
/**
 * HashScanIndexNode use hash index to filter rows matches predicate.
 */
type HashScanIndexPlanNode struct {
	*AbstractPlanNode
	predicate *expression.Expression
	tableOID  uint32
}

func NewHashScanIndexPlanNode(schema *schema.Schema, predicate *expression.Expression, tableOID uint32) Plan {
	return &HashScanIndexPlanNode{&AbstractPlanNode{schema, nil}, predicate, tableOID}
}

func (p *HashScanIndexPlanNode) GetPredicate() *expression.Expression {
	return p.predicate
}

func (p *HashScanIndexPlanNode) GetTableOID() uint32 {
	return p.tableOID
}

func (p *HashScanIndexPlanNode) GetType() PlanType {
	return HashScanIndex
}
