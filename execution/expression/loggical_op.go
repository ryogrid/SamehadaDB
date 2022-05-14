package expression

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

type LogicalOpType int

/** LogicalOpType represents the type of comparison that we want to perform. */
const (
	AND LogicalOpType = iota
	OR
	NOT
)

/**
 * LogicalOp represents two expressions being evaluated with logical operator.
 */
type LogicalOp struct {
	*AbstractExpression
	logicalOpType  LogicalOpType
	children_left  Expression
	children_right Expression
}

// if logicalOpType is "NOT", right value must be nil
func NewLogicalOp(left Expression, right Expression, logicalOpType LogicalOpType, colType types.TypeID) Expression {
	ret := &LogicalOp{&AbstractExpression{[2]Expression{}, colType}, logicalOpType, left, right}
	ret.SetChildAt(0, left)
	ret.SetChildAt(1, right)
	return ret
}

func (c *LogicalOp) Evaluate(tuple *tuple.Tuple, schema *schema.Schema) types.Value {
	lhs := c.children[0].Evaluate(tuple, schema)
	rhs := c.children[1].Evaluate(tuple, schema)
	return types.NewBoolean(c.performLogicalOp(lhs, rhs))
}

func (c *LogicalOp) performLogicalOp(lhs types.Value, rhs types.Value) bool {
	switch c.logicalOpType {
	case AND:
		return lhs.ToBoolean() && rhs.ToBoolean()
	case OR:
		return lhs.ToBoolean() || rhs.ToBoolean()
	case NOT:
		return !lhs.ToBoolean()
	default:
		fmt.Println(c.logicalOpType)
		panic("unknown logicalOpType is passed!")
	}
}

func (c *LogicalOp) GetLogicalOpType() LogicalOpType {
	return c.logicalOpType
}

func (c *LogicalOp) EvaluateJoin(left_tuple *tuple.Tuple, left_schema *schema.Schema, right_tuple *tuple.Tuple, right_schema *schema.Schema) types.Value {
	if c.logicalOpType == NOT {
		lhs := c.GetChildAt(0).EvaluateJoin(left_tuple, left_schema, right_tuple, left_schema)
		return types.NewBoolean(!lhs.ToBoolean())
	} else {
		lhs := c.GetChildAt(0).EvaluateJoin(left_tuple, left_schema, right_tuple, left_schema)
		rhs := c.GetChildAt(1).EvaluateJoin(left_tuple, left_schema, right_tuple, right_schema)
		return types.NewBoolean(c.performLogicalOp(lhs, rhs))
	}
}

func (c *LogicalOp) EvaluateAggregate(group_bys []*types.Value, aggregates []*types.Value) types.Value {
	if c.logicalOpType == NOT {
		lhs := c.GetChildAt(0).EvaluateAggregate(group_bys, aggregates)
		return types.NewBoolean(!lhs.ToBoolean())
	} else {
		lhs := c.GetChildAt(0).EvaluateAggregate(group_bys, aggregates)
		rhs := c.GetChildAt(1).EvaluateAggregate(group_bys, aggregates)
		return types.NewBoolean(c.performLogicalOp(lhs, rhs))
	}
}

func (c *LogicalOp) GetChildAt(child_idx uint32) Expression {
	return c.children[child_idx]
}

func (c *LogicalOp) SetChildAt(child_idx uint32, child Expression) {
	c.children[child_idx] = child
}
