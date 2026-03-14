package expression

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

type LogicalOpType int

/** LogicalOpType represents the type of comparison that we want to perform. */
const (
	AND LogicalOpType = iota
	OR
	NOT
)

/**
 * LogicalOp represents two expressions or one expression being evaluated with logical operator.
 */
type LogicalOp struct {
	*AbstractExpression
	logicalOpType  LogicalOpType
	childrenLeft  Expression // referenced as children[0] after struct init...
	childrenRight Expression // referenced as children[1] after struct init...
}

// if logicalOpType is "NOT", right value must be nil
func NewLogicalOp(left Expression, right Expression, logicalOpType LogicalOpType, colType types.TypeID) Expression {
	ret := &LogicalOp{&AbstractExpression{[2]Expression{left, right}, colType}, logicalOpType, left, right}
	ret.SetChildAt(0, left)
	ret.SetChildAt(1, right)
	return ret
}

func (c *LogicalOp) Evaluate(tuple *tuple.Tuple, schema *schema.Schema) types.Value {
	if c.logicalOpType == NOT {
		lhs := c.children[0].Evaluate(tuple, schema)
		return types.NewBoolean(!lhs.ToBoolean())
	} else {
		lhs := c.children[0].Evaluate(tuple, schema)
		rhs := c.children[1].Evaluate(tuple, schema)
		return types.NewBoolean(c.performLogicalOp(lhs, rhs))
	}
}

func (c *LogicalOp) performLogicalOp(lhs types.Value, rhs types.Value) bool {
	switch c.logicalOpType {
	case AND:
		return lhs.ToBoolean() && rhs.ToBoolean()
	case OR:
		return lhs.ToBoolean() || rhs.ToBoolean()
	case NOT:
		fmt.Println(c.logicalOpType)
		panic("NOT op is not valid!")
	default:
		fmt.Println(c.logicalOpType)
		panic("unknown logicalOpType is passed!")
	}
}

func (c *LogicalOp) GetLogicalOpType() LogicalOpType {
	return c.logicalOpType
}

func (c *LogicalOp) EvaluateJoin(leftTuple *tuple.Tuple, leftSchema *schema.Schema, rightTuple *tuple.Tuple, rightSchema *schema.Schema) types.Value {
	if c.logicalOpType == NOT {
		lhs := c.GetChildAt(0).EvaluateJoin(leftTuple, leftSchema, rightTuple, leftSchema)
		return types.NewBoolean(!lhs.ToBoolean())
	} else {
		lhs := c.GetChildAt(0).EvaluateJoin(leftTuple, leftSchema, rightTuple, leftSchema)
		rhs := c.GetChildAt(1).EvaluateJoin(leftTuple, leftSchema, rightTuple, rightSchema)
		return types.NewBoolean(c.performLogicalOp(lhs, rhs))
	}
}

func (c *LogicalOp) EvaluateAggregate(groupBys []*types.Value, aggregates []*types.Value) types.Value {
	if c.logicalOpType == NOT {
		lhs := c.GetChildAt(0).EvaluateAggregate(groupBys, aggregates)
		return types.NewBoolean(!lhs.ToBoolean())
	} else {
		lhs := c.GetChildAt(0).EvaluateAggregate(groupBys, aggregates)
		rhs := c.GetChildAt(1).EvaluateAggregate(groupBys, aggregates)
		return types.NewBoolean(c.performLogicalOp(lhs, rhs))
	}
}

func (c *LogicalOp) GetChildAt(childIdx uint32) Expression {
	if int(childIdx) >= len(c.children) {
		return nil
	}
	return c.children[childIdx]
}

func (c *LogicalOp) SetChildAt(childIdx uint32, child Expression) {
	c.children[childIdx] = child
}

func AppendLogicalCondition(baseConds Expression, opType LogicalOpType, addCond Expression) Expression {
	samehada_util.SHAssert(opType == AND, "only AND is supported")
	return NewLogicalOp(baseConds, addCond, opType, types.Boolean)
}

func (c *LogicalOp) GetType() ExpressionType {
	return ExpressionTypeLogicalOp
}
