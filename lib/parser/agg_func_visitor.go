package parser

import (
	"strings"

	"github.com/pingcap/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

type AggFuncVisitor struct {
	ColumnName_ *string
	TableName_  *string
}

func (v *AggFuncVisitor) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *ast.ColumnName:
		tabname := node.String()
		if strings.Contains(tabname, ".") {
			tabname = strings.Split(tabname, ".")[0]
			v.TableName_ = &tabname
		} else {
			v.TableName_ = nil
		}
		colname := node.Name.String()
		v.ColumnName_ = &colname
		return in, true
	case *driver.ValueExpr:
		val := ValueExprToValue(node)
		if val.ValueType() == types.Integer {
			// val is 1 (Integer) means wildcard maybe...
			colname := "*"
			v.ColumnName_ = &colname
			return in, true
		}
	default:
	}
	return in, false
}

func (v *AggFuncVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
