package parser

import (
	"fmt"
	"github.com/pingcap/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/ryogrid/SamehadaDB/types"
	"reflect"
)

type RootSQLVisitor struct {
	QueryInfo_ *QueryInfo
}

func NewRootSQLVisitor() *RootSQLVisitor {
	ret := new(RootSQLVisitor)
	qinfo := new(QueryInfo)
	qinfo.QueryType_ = new(QueryType)
	qinfo.SelectFields_ = make([]*SelectFieldExpression, 0)
	qinfo.SetExpressions_ = make([]*SetExpression, 0)
	qinfo.TargetCols_ = make([]*string, 0)
	qinfo.ColDefExpressions_ = make([]*ColDefExpression, 0)
	qinfo.OnExpressions_ = new(BinaryOpExpression)
	qinfo.JoinTables_ = make([]*string, 0)
	qinfo.WhereExpression_ = new(BinaryOpExpression)
	qinfo.LimitNum_ = -1
	ret.QueryInfo_ = qinfo

	return ret
}

func (v *RootSQLVisitor) Enter(in ast.Node) (ast.Node, bool) {
	refVal := reflect.ValueOf(in)
	fmt.Println(refVal.Type())
	//return in, false

	switch node := in.(type) {
	case *ast.SelectStmt:
		*v.QueryInfo_.QueryType_ = SELECT
	case *ast.CreateTableStmt:
		*v.QueryInfo_.QueryType_ = CREATE_TABLE
	case *ast.InsertStmt:
		*v.QueryInfo_.QueryType_ = INSERT
	case *ast.DeleteStmt:
		*v.QueryInfo_.QueryType_ = DELETE
	case *ast.UpdateStmt:
		*v.QueryInfo_.QueryType_ = UPDATE
	case *ast.FieldList:
	case *ast.SelectField:
		sv := &SelectFieldsVisitor{v.QueryInfo_}
		node.Accept(sv)
		return in, true
	case *ast.TableRefsClause:
	case *ast.Assignment:
		// when UPDATE
		av := new(AssignVisitor)
		node.Accept(av)
		setExp := new(SetExpression)
		setExp.ColName_ = av.Colname_
		setExp.UpdateValue_ = av.Value_
		v.QueryInfo_.SetExpressions_ = append(v.QueryInfo_.SetExpressions_, setExp)
		return in, true
	case *ast.Join:
		jv := new(JoinVisitor)
		jv.QueryInfo_ = v.QueryInfo_
		node.Accept(jv)
		return in, true
	case *ast.OnCondition:
	case *ast.TableSource:
	case *ast.TableNameExpr:
	case *ast.TableName:
		switch *v.QueryInfo_.QueryType_ {
		//case SELECT:
		//	tbname := node.Name.String()
		//	v.QueryInfo_.FromTable_ = &tbname
		//case UPDATE:
		//	tbname := node.Name.String()
		//	v.QueryInfo_.TargetTable_ = &tbname
		//case INSERT:
		//	tbname := node.Name.String()
		//	v.QueryInfo_.TargetTable_ = &tbname
		//case DELETE:
		//	tbname := node.Name.String()
		//	v.QueryInfo_.FromTable_ = &tbname
		case CREATE_TABLE:
			tbname := node.Name.String()
			v.QueryInfo_.NewTable_ = &tbname
		}
	case *ast.ColumnDef:
		if *v.QueryInfo_.QueryType_ == CREATE_TABLE {
			cdef := new(ColDefExpression)
			cname := node.Name.String()
			cdef.ColName_ = &cname
			col_type := node.Tp.Tp
			switch col_type {
			case 1, 3:
				ctype := types.Integer
				cdef.ColType_ = &ctype
			case 4, 8:
				ctype := types.Float
				cdef.ColType_ = &ctype
			default:
				ctype := types.Varchar
				cdef.ColType_ = &ctype
			}
			v.QueryInfo_.ColDefExpressions_ = append(v.QueryInfo_.ColDefExpressions_, cdef)
			return in, true
		}
	case *ast.ColumnNameExpr:
	case *ast.ColumnName:
		if *v.QueryInfo_.QueryType_ == INSERT {
			cname := node.String()
			v.QueryInfo_.TargetCols_ = append(v.QueryInfo_.TargetCols_, &cname)
			return in, true
		}
	case *ast.BinaryOperationExpr:
		// for WHERE clause. other clauses handles BinaryOperationExpr with self visitor
		new_visitor := &BinaryOpVisitor{v.QueryInfo_, new(BinaryOpExpression)}
		node.Accept(new_visitor)
		v.QueryInfo_.WhereExpression_ = new_visitor.BinaryOpExpression_

		logicType, compType := GetTypesForBOperationExpr(node.Op)
		v.QueryInfo_.WhereExpression_.LogicalOperationType_ = logicType
		v.QueryInfo_.WhereExpression_.ComparisonOperationType_ = compType

		return in, true
	case *driver.ValueExpr:
		// when INSERT
		v.QueryInfo_.Values_ = append(v.QueryInfo_.Values_, ValueExprToValue(node))
		return in, true
	case *ast.Limit:
		cdv := new(ChildDataVisitor)
		node.Accept(cdv)
		val := cdv.ChildData_.(*types.Value)
		v.QueryInfo_.LimitNum_ = val.ToInteger()
		return in, true
	default:
		panic("unknown node for visitor")
	}
	return in, false
}

func (v *RootSQLVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
