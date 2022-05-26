// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package plans

import (
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
)

type PlanType int

/** PlanType represents the types of plans that we have in our system. */
const (
	SeqScan PlanType = iota
	Insert
	Delete
	Limit
	HashScanIndex
	HashJoin
	Aggregation
	Orderby
)

type Plan interface {
	OutputSchema() *schema.Schema
	GetChildAt(childIndex uint32) Plan
	GetChildren() []Plan
	GetType() PlanType
}

/**
 * AbstractPlanNode represents all the possible types of plan nodes in our system.
 * Plan nodes are modeled as trees, so each plan node can have a variable number of children.
 * Per the Volcano model, the plan node receives the tuples of its children.
 * The ordering of the children may matter.
 */
type AbstractPlanNode struct {
	/**
	 * The schema for the output of this plan node. In the volcano model, every plan node will spit out tuples,
	 * and this tells you what schema this plan node's tuples will have.
	 */
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
