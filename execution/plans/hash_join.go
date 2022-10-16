package plans

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"math"
)

/**
 * HashJoinPlanNode is used to represent performing a hash join between two children plan nodes.
 * By convention, the left child (index 0) is used to build the hash table,
 * and the right child (index 1) is used in probing the hash table.
 */
type HashJoinPlanNode struct {
	*AbstractPlanNode
	/** The hash join predicate. */
	onPredicate expression.Expression
	/** The left child's hash keys. */
	left_hash_keys []expression.Expression
	/** The right child's hash keys. */
	right_hash_keys []expression.Expression
}

func NewHashJoinPlanNode(output_schema *schema.Schema, children []Plan,
	onPredicate expression.Expression, left_hash_keys []expression.Expression,
	right_hash_keys []expression.Expression) *HashJoinPlanNode {
	return &HashJoinPlanNode{&AbstractPlanNode{output_schema, children}, onPredicate, left_hash_keys, right_hash_keys}
}

func (p *HashJoinPlanNode) GetType() PlanType { return HashJoin }

/** @return the onPredicate to be used in the hash join */
func (p *HashJoinPlanNode) OnPredicate() expression.Expression { return p.onPredicate }

/** @return the left plan node of the hash join, by convention this is used to build the table */
func (p *HashJoinPlanNode) GetLeftPlan() Plan {
	common.SH_Assert(len(p.GetChildren()) == 2, "Hash joins should have exactly two children plans.")
	return p.GetChildAt(0)
}

/** @return the right plan node of the hash join */
func (p *HashJoinPlanNode) GetRightPlan() Plan {
	common.SH_Assert(len(p.GetChildren()) == 2, "Hash joins should have exactly two children plans.")
	return p.GetChildAt(1)
}

/** @return the left key at the given index */
func (p *HashJoinPlanNode) GetLeftKeyAt(idx uint32) expression.Expression {
	return p.left_hash_keys[idx]
}

/** @return the left keys */
func (p *HashJoinPlanNode) GetLeftKeys() []expression.Expression { return p.left_hash_keys }

/** @return the right key at the given index */
func (p *HashJoinPlanNode) GetRightKeyAt(idx uint32) expression.Expression {
	return p.right_hash_keys[idx]
}

/** @return the right keys */
func (p *HashJoinPlanNode) GetRightKeys() []expression.Expression { return p.right_hash_keys }

// can not be used
func (p *HashJoinPlanNode) GetTableOID() uint32 {
	return math.MaxUint32
}
