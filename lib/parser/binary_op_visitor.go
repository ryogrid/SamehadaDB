package parser

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/opcode"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

type BinaryOpVisitor struct {
	QueryInfo_          *QueryInfo
	BinaryOpExpression_ *BinaryOpExpression
}

func (v *BinaryOpVisitor) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *ast.BinaryOperationExpr:
		l_visitor := &BinaryOpVisitor{v.QueryInfo_, new(BinaryOpExpression)}
		node.L.Accept(l_visitor)
		r_visitor := &BinaryOpVisitor{v.QueryInfo_, new(BinaryOpExpression)}
		node.R.Accept(r_visitor)

		logicType, compType := GetTypesForBOperationExpr(node.Op)
		v.BinaryOpExpression_.LogicalOperationType_ = logicType
		v.BinaryOpExpression_.ComparisonOperationType_ = compType

		if logicType == -1 && compType != -1 {
			// when expression is comparison case
			if l_visitor.BinaryOpExpression_.ComparisonOperationType_ == -1 &&
				l_visitor.BinaryOpExpression_.LogicalOperationType_ == -1 {
				v.BinaryOpExpression_.Left_ = l_visitor.BinaryOpExpression_.Left_
			} else {
				v.BinaryOpExpression_.Left_ = l_visitor.BinaryOpExpression_
			}

			if r_visitor.BinaryOpExpression_.ComparisonOperationType_ == -1 &&
				r_visitor.BinaryOpExpression_.LogicalOperationType_ == -1 {
				v.BinaryOpExpression_.Right_ = r_visitor.BinaryOpExpression_.Left_
			} else {
				v.BinaryOpExpression_.Right_ = l_visitor.BinaryOpExpression_
			}
		} else {
			v.BinaryOpExpression_.Left_ = l_visitor.BinaryOpExpression_
			v.BinaryOpExpression_.Right_ = r_visitor.BinaryOpExpression_
		}
		return in, true
	case *ast.IsNullExpr:
		cdv := &ChildDataVisitor{make([]interface{}, 0)}
		node.Accept(cdv)

		v.BinaryOpExpression_.LogicalOperationType_ = -1
		if node.Not {
			v.BinaryOpExpression_.ComparisonOperationType_ = expression.NotEqual
		} else {
			v.BinaryOpExpression_.ComparisonOperationType_ = expression.Equal
		}

		null_val := types.NewNull()
		v.BinaryOpExpression_.Left_ = cdv.ChildDatas_[0]
		v.BinaryOpExpression_.Right_ = &null_val
		return in, true
	case *ast.ColumnNameExpr:
		v.BinaryOpExpression_.LogicalOperationType_ = -1
		v.BinaryOpExpression_.ComparisonOperationType_ = -1
		left_val := node.Name.String()
		v.BinaryOpExpression_.Left_ = &left_val
		return in, true
	case *driver.ValueExpr:
		v.BinaryOpExpression_.LogicalOperationType_ = -1
		v.BinaryOpExpression_.ComparisonOperationType_ = -1
		v.BinaryOpExpression_.Left_ = ValueExprToValue(node)
		return in, true
	default:
	}

	return in, false
}

func (v *BinaryOpVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func GetTypesForBOperationExpr(opcode_ opcode.Op) (expression.LogicalOpType, expression.ComparisonType) {
	switch opcode_ {
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
