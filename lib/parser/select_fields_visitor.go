package parser

import (
	"github.com/pingcap/parser/ast"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"strings"
)

type SelectFieldsVisitor struct {
	QueryInfo_ *QueryInfo
}

func (v *SelectFieldsVisitor) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *ast.ColumnName:
		sfield := new(SelectFieldExpression)
		tabname := node.String()
		if strings.Contains(tabname, ".") {
			tabname = strings.Split(tabname, ".")[0]
			sfield.TableName_ = &tabname
		} else {
			sfield.TableName_ = nil
		}
		colname := node.Name.String()
		sfield.ColName_ = &colname
		v.QueryInfo_.SelectFields_ = append(v.QueryInfo_.SelectFields_, sfield)
		return in, true
	case *ast.SelectField:
		// when specifed wildcard
		if node.WildCard != nil {
			sfield := new(SelectFieldExpression)
			colname := "*"
			sfield.ColName_ = &colname
			v.QueryInfo_.SelectFields_ = append(v.QueryInfo_.SelectFields_, sfield)
			return in, true
		}
	case *ast.AggregateFuncExpr:
		av := new(AggFuncVisitor)
		node.Accept(av)
		var sfield *SelectFieldExpression = nil
		aggTypeStr := node.F
		switch aggTypeStr {
		case "count":
			sfield = &SelectFieldExpression{true, plans.COUNT_AGGREGATE, av.TableName_, av.ColumnName_}
		case "max":
			sfield = &SelectFieldExpression{true, plans.MAX_AGGREGATE, av.TableName_, av.ColumnName_}
		case "min":
			sfield = &SelectFieldExpression{true, plans.MIN_AGGREGATE, av.TableName_, av.ColumnName_}
		case "sum":
			sfield = &SelectFieldExpression{true, plans.SUM_AGGREGATE, av.TableName_, av.ColumnName_}
		}

		v.QueryInfo_.SelectFields_ = append(v.QueryInfo_.SelectFields_, sfield)
		return in, true
	default:
	}
	return in, false
}

func (v *SelectFieldsVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
