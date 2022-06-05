package parser

import (
	"fmt"
	"github.com/pingcap/parser/ast"
	"reflect"
)

type SelectFieldsVisitor struct {
	QueryInfo_ *QueryInfo
}

func (v *SelectFieldsVisitor) Enter(in ast.Node) (ast.Node, bool) {
	refVal := reflect.ValueOf(in)
	fmt.Println(refVal.Type())
	switch node := in.(type) {
	case *ast.ColumnName:
		colname := node.Name.String()
		v.QueryInfo_.SelectFields_ = append(v.QueryInfo_.SelectFields_, &colname)
		return in, true
	case *ast.SelectField:
		// when specifed wildcard
		if node.WildCard != nil {
			colname := "*"
			v.QueryInfo_.SelectFields_ = append(v.QueryInfo_.SelectFields_, &colname)
			return in, true
		}
	default:
	}
	return in, false
}

func (v *SelectFieldsVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
