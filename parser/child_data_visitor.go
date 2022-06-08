package parser

import (
	"fmt"
	"github.com/pingcap/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"reflect"
)

type ChildDataVisitor struct {
	ChildData_ interface{}
}

func (v *ChildDataVisitor) Enter(in ast.Node) (ast.Node, bool) {
	refVal := reflect.ValueOf(in)
	fmt.Println(refVal.Type())
	switch node := in.(type) {
	case *ast.ColumnName:
		colname := node.Name.String()
		v.ChildData_ = &colname
		return in, true
	case *driver.ValueExpr:
		val := ValueExprToValue(node)
		v.ChildData_ = val
		return in, true
	default:
	}
	return in, false
}

func (v *ChildDataVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
