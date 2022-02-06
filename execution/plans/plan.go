// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
)

type PlanType int

const (
	SeqScan PlanType = iota
	Insert
	Limit
)

type Plan interface {
	OutputSchema() *schema.Schema
	GetChildAt(childIndex uint32) Plan
	GetChildren() []Plan
	GetType() PlanType
}

type AbstractPlanNode struct {
	outputSchema *schema.Schema
	children     []Plan
}

func (p *AbstractPlanNode) GetChildAt(childIndex uint32) Plan {
	return p.children[childIndex]
}

func (p *AbstractPlanNode) GetChildren() []Plan {
	return p.children
}

func (p *AbstractPlanNode) OutputSchema() *schema.Schema {
	return p.outputSchema
}
