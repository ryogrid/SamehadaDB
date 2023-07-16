package plans

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"math"
)

// TODO: (SDB) [OPT] not implemented yet (nested_loop_join.go)

type NestedLoopJoinPlanNode struct {
	*AbstractPlanNode
	/** The hash join predicate. */
	onPredicate expression.Expression
	/** The left child's hash keys. */
	left_hash_keys []expression.Expression
	/** The right child's hash keys. */
	right_hash_keys []expression.Expression
}

func NewNestedLoopJoinPlanNode(output_schema *schema.Schema, children []Plan,
	onPredicate expression.Expression, left_hash_keys []expression.Expression,
	right_hash_keys []expression.Expression) *NestedLoopJoinPlanNode {
	return &NestedLoopJoinPlanNode{&AbstractPlanNode{output_schema, children}, onPredicate, left_hash_keys, right_hash_keys}
}

func NewNestedLoopJoinPlanNodeWithPredicate(left_child Plan, right_child Plan, pred expression.Expression) *NestedLoopJoinPlanNode {
	// TODO: (SDB) [OPT] not implemented yet (NewNestedLoopJoinPlanNodeWithChilds)
	return nil
}

func (p *NestedLoopJoinPlanNode) GetType() PlanType { return NestedLoopJoin }

/** @return the onPredicate to be used in the hash join */
func (p *NestedLoopJoinPlanNode) OnPredicate() expression.Expression { return p.onPredicate }

/** @return the left plan node of the hash join, by convention this is used to build the table */
func (p *NestedLoopJoinPlanNode) GetLeftPlan() Plan {
	common.SH_Assert(len(p.GetChildren()) == 2, "Hash joins should have exactly two children plans.")
	return p.GetChildAt(0)
}

/** @return the right plan node of the hash join */
func (p *NestedLoopJoinPlanNode) GetRightPlan() Plan {
	common.SH_Assert(len(p.GetChildren()) == 2, "Hash joins should have exactly two children plans.")
	return p.GetChildAt(1)
}

/** @return the left key at the given index */
func (p *NestedLoopJoinPlanNode) GetLeftKeyAt(idx uint32) expression.Expression {
	return p.left_hash_keys[idx]
}

/** @return the left keys */
func (p *NestedLoopJoinPlanNode) GetLeftKeys() []expression.Expression { return p.left_hash_keys }

/** @return the right key at the given index */
func (p *NestedLoopJoinPlanNode) GetRightKeyAt(idx uint32) expression.Expression {
	return p.right_hash_keys[idx]
}

/** @return the right keys */
func (p *NestedLoopJoinPlanNode) GetRightKeys() []expression.Expression { return p.right_hash_keys }

// can not be used
func (p *NestedLoopJoinPlanNode) GetTableOID() uint32 {
	return math.MaxUint32
}

func (p *NestedLoopJoinPlanNode) AccessRowCount() uint64 {
	// TODO: (SDB) [OPT] not implemented yet (NestedLoopJoinPlanNode::AccessRowCount)
	return 0
}
