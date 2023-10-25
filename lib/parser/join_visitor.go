package parser

import (
	"github.com/pingcap/parser/ast"
)

type JoinVisitor struct {
	QueryInfo_ *QueryInfo
}

func (v *JoinVisitor) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *ast.TableName:
		tblname := node.Name.String()
		v.QueryInfo_.JoinTables_ = append(v.QueryInfo_.JoinTables_, &tblname)
		return in, true
	case *ast.BinaryOperationExpr:
		bv := &BinaryOpVisitor{v.QueryInfo_, new(BinaryOpExpression)}
		node.Accept(bv)
		v.QueryInfo_.OnExpressions_ = bv.BinaryOpExpression_
		return in, true
	default:
	}
	return in, false
}

func (v *JoinVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
