package parser

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

type BinaryOpVisitor struct {
	QueryInfo          *QueryInfo
	BinaryOpExpression *BinaryOpExpression
}

func (v *BinaryOpVisitor) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *ast.BinaryOperationExpr:
		lVisitor := &BinaryOpVisitor{v.QueryInfo, new(BinaryOpExpression)}
		node.L.Accept(lVisitor)
		rVisitor := &BinaryOpVisitor{v.QueryInfo, new(BinaryOpExpression)}
		node.R.Accept(rVisitor)

		logicType, compType := GetTypesForBOperationExpr(node.Op)
		v.BinaryOpExpression.LogicalOperationType = logicType
		v.BinaryOpExpression.ComparisonOperationType = compType

		if logicType == -1 && compType != -1 {
			// when expression is comparison case
			if lVisitor.BinaryOpExpression.ComparisonOperationType == -1 &&
				lVisitor.BinaryOpExpression.LogicalOperationType == -1 {
				v.BinaryOpExpression.Left = lVisitor.BinaryOpExpression.Left
			} else {
				v.BinaryOpExpression.Left = lVisitor.BinaryOpExpression
			}

			if rVisitor.BinaryOpExpression.ComparisonOperationType == -1 &&
				rVisitor.BinaryOpExpression.LogicalOperationType == -1 {
				v.BinaryOpExpression.Right = rVisitor.BinaryOpExpression.Left
			} else {
				v.BinaryOpExpression.Right = lVisitor.BinaryOpExpression
			}
		} else {
			v.BinaryOpExpression.Left = lVisitor.BinaryOpExpression
			v.BinaryOpExpression.Right = rVisitor.BinaryOpExpression
		}
		return in, true
	case *ast.IsNullExpr:
		cdv := &ChildDataVisitor{make([]interface{}, 0)}
		node.Accept(cdv)

		v.BinaryOpExpression.LogicalOperationType = -1
		if node.Not {
			v.BinaryOpExpression.ComparisonOperationType = expression.NotEqual
		} else {
			v.BinaryOpExpression.ComparisonOperationType = expression.Equal
		}

		nullVal := types.NewNull()
		v.BinaryOpExpression.Left = cdv.ChildDatas[0]
		v.BinaryOpExpression.Right = &nullVal
		return in, true
	case *ast.ColumnNameExpr:
		v.BinaryOpExpression.LogicalOperationType = -1
		v.BinaryOpExpression.ComparisonOperationType = -1
		leftVal := node.Name.String()
		v.BinaryOpExpression.Left = &leftVal
		return in, true
	case *driver.ValueExpr:
		v.BinaryOpExpression.LogicalOperationType = -1
		v.BinaryOpExpression.ComparisonOperationType = -1
		v.BinaryOpExpression.Left = ValueExprToValue(node)
		return in, true
	default:
	}

	return in, false
}

func (v *BinaryOpVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func GetTypesForBOperationExpr(op opcode.Op) (expression.LogicalOpType, expression.ComparisonType) {
	switch op {
	case opcode.EQ:
		return -1, expression.Equal
	case opcode.GT:
		return -1, expression.GreaterThan
	case opcode.GE:
		return -1, expression.GreaterThanOrEqual
	case opcode.LT:
		return -1, expression.LessThan
	case opcode.LE:
		return -1, expression.LessThanOrEqual
	case opcode.NE:
		return -1, expression.NotEqual
	case opcode.LogicAnd:
		return expression.AND, -1
	case opcode.LogicOr:
		return expression.OR, -1
	default:
		panic("unknown opcode")
	}
}
