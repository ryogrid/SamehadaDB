package plans

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
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
	stats_          *catalog.TableStatistics
}

func GenHashJoinStats(leftPlan Plan, rightPlan Plan) *catalog.TableStatistics {
	leftStats := leftPlan.GetStatistics().GetDeepCopy()
	leftStats.Concat(rightPlan.GetStatistics().GetDeepCopy())
	return leftStats
}

func NewHashJoinPlanNode(output_schema *schema.Schema, children []Plan,
	onPredicate expression.Expression, left_hash_keys []expression.Expression,
	right_hash_keys []expression.Expression) *HashJoinPlanNode {
	return &HashJoinPlanNode{&AbstractPlanNode{output_schema, children}, onPredicate, left_hash_keys, right_hash_keys, GenHashJoinStats(children[0], children[1])}
}

func NewHashJoinPlanNodeWithChilds(left_child Plan, left_hash_keys []expression.Expression, right_child Plan, right_hash_keys []expression.Expression) *HashJoinPlanNode {
	if left_hash_keys == nil || right_hash_keys == nil {
		panic("NewHashJoinPlanNodeWithChilds needs keys info.")
	}
	if len(left_hash_keys) != 1 || len(right_hash_keys) != 1 {
		fmt.Println("NewHashJoinPlanNodeWithChilds supports only one key for left and right now.")
	}
	// TODO: (SDB) one key pair only used on join even if multiple key pairs are passed
	onPredicate := constructOnExpressionFromKeysInfo(left_hash_keys[0:1], right_hash_keys[0:1])
	output_schema := makeMergedOutputSchema(left_child.OutputSchema(), right_child.OutputSchema())

	return &HashJoinPlanNode{&AbstractPlanNode{output_schema, []Plan{left_child, right_child}}, onPredicate, left_hash_keys, right_hash_keys, GenHashJoinStats(left_child, right_child)}
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

func (p *HashJoinPlanNode) AccessRowCount(c *catalog.Catalog) uint64 {
	return p.GetLeftPlan().AccessRowCount(c) + p.GetRightPlan().AccessRowCount(c)

}

func (p *HashJoinPlanNode) GetDebugStr() string {
	//leftColIdx := p.onPredicate.GetChildAt(0).(*expression.ColumnValue).GetColIndex()
	//leftColName := p.GetChildAt(0).OutputSchema().GetColumn(leftColIdx).GetColumnName()
	//rightColIdx := p.onPredicate.GetChildAt(1).(*expression.ColumnValue).GetColIndex()
	//rightColName := p.GetChildAt(1).OutputSchema().GetColumn(rightColIdx).GetColumnName()
	//return "HashJoinPlanNode [" + leftColName + " = " + rightColName + "]"
	return "HashJoinPlanNode"
}

func (p *HashJoinPlanNode) GetStatistics() *catalog.TableStatistics {
	return p.stats_
}

func (p *HashJoinPlanNode) EmitRowCount(c *catalog.Catalog) uint64 {
	return uint64(math.Min(float64(p.GetLeftPlan().EmitRowCount(c)), float64(p.GetRightPlan().EmitRowCount(c))))
}
