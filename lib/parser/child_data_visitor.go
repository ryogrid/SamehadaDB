package parser

import (
	"github.com/pingcap/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
)

type ChildDataVisitor struct {
	ChildDatas []interface{}
}

func (v *ChildDataVisitor) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *ast.ColumnName:
		colname := node.Name.String()
		v.ChildDatas = append(v.ChildDatas, &colname)
		return in, true
	case *driver.ValueExpr:
		val := ValueExprToValue(node)
		v.ChildDatas = append(v.ChildDatas, val)
		return in, true
	default:
	}
	return in, false
}

func (v *ChildDataVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
