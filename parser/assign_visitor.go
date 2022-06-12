package parser

import (
	"github.com/pingcap/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/ryogrid/SamehadaDB/types"
)

type AssignVisitor struct {
	Colname_ *string
	Value_   *types.Value
}

func (v *AssignVisitor) Enter(in ast.Node) (ast.Node, bool) {
	//refVal := reflect.ValueOf(in)
	//fmt.Println(refVal.Type())

	switch node := in.(type) {
	case *ast.ColumnName:
		//colname := node.Name.String()
		colname := node.String()
		v.Colname_ = &colname
		return in, true
	case *driver.ValueExpr:
		v.Value_ = ValueExprToValue(node)
		return in, true
	default:
	}

	return in, false
}

func (v *AssignVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
