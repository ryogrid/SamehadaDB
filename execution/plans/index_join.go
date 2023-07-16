package plans

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"math"
)

// TODO: (SDB) not implemented yet (index_join.go)

type IndexJoinPlanNode struct {
	*AbstractPlanNode
	/** The hash join predicate. */
	onPredicate expression.Expression
	/** The left child's hash keys. */
	left_hash_keys []expression.Expression
	/** The right child's hash keys. */
	right_hash_keys []expression.Expression
}

func NewIndexJoinPlanNode(output_schema *schema.Schema, children []Plan,
	onPredicate expression.Expression, left_hash_keys []expression.Expression,
	right_hash_keys []expression.Expression) *IndexJoinPlanNode {
	return &IndexJoinPlanNode{&AbstractPlanNode{output_schema, children}, onPredicate, left_hash_keys, right_hash_keys}
}

func NewIndexJoinPlanNodeWithChilds(left_child Plan, left_keys []expression.Expression, right_child Plan, right_keys []expression.Expression) *IndexJoinPlanNode {
	// TODO: (SDB) not implemented yet (NewIndexJoinPlanNodeWithChilds)
	return nil
}

func (p *IndexJoinPlanNode) GetType() PlanType { return IndexJoin }

/** @return the onPredicate to be used in the hash join */
func (p *IndexJoinPlanNode) OnPredicate() expression.Expression { return p.onPredicate }

/** @return the left plan node of the hash join, by convention this is used to build the table */
func (p *IndexJoinPlanNode) GetLeftPlan() Plan {
	common.SH_Assert(len(p.GetChildren()) == 2, "Hash joins should have exactly two children plans.")
	return p.GetChildAt(0)
}

/** @return the right plan node of the hash join */
func (p *IndexJoinPlanNode) GetRightPlan() Plan {
	common.SH_Assert(len(p.GetChildren()) == 2, "Hash joins should have exactly two children plans.")
	return p.GetChildAt(1)
}

/** @return the left key at the given index */
func (p *IndexJoinPlanNode) GetLeftKeyAt(idx uint32) expression.Expression {
	return p.left_hash_keys[idx]
}

/** @return the left keys */
func (p *IndexJoinPlanNode) GetLeftKeys() []expression.Expression { return p.left_hash_keys }

/** @return the right key at the given index */
func (p *IndexJoinPlanNode) GetRightKeyAt(idx uint32) expression.Expression {
	return p.right_hash_keys[idx]
}

/** @return the right keys */
func (p *IndexJoinPlanNode) GetRightKeys() []expression.Expression { return p.right_hash_keys }

// can not be used
func (p *IndexJoinPlanNode) GetTableOID() uint32 {
	return math.MaxUint32
}

func (p *IndexJoinPlanNode) AccessRowCount() uint64 {
	// TODO: (SDB) not implemented yet
	return 0
}
