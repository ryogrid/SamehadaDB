package parser

import (
	"github.com/pingcap/parser/ast"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/ryogrid/SamehadaDB/types"
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
	qinfo.ColDefExpressions_ = make([]*ColDefExpression, 0)
	qinfo.IndexDefExpressions_ = make([]*IndexDefExpression, 0)
	qinfo.TargetCols_ = make([]*string, 0)
	qinfo.Values_ = make([]*types.Value, 0)
	qinfo.OnExpressions_ = new(BinaryOpExpression)
	qinfo.JoinTables_ = make([]*string, 0)
	qinfo.WhereExpression_ = new(BinaryOpExpression)
	qinfo.LimitNum_ = -1
	qinfo.OffsetNum_ = -1
	qinfo.OrderByExpressions_ = make([]*OrderByExpression, 0)
	ret.QueryInfo_ = qinfo

	return ret
}

func (v *RootSQLVisitor) Enter(in ast.Node) (ast.Node, bool) {
	//refVal := reflect.ValueOf(in)
	//fmt.Println(refVal.Type())
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
	case *ast.Constraint:
		// Index definition at CREATE TABLE
		if *v.QueryInfo_.QueryType_ == CREATE_TABLE {
			// get all specified column
			cdv := &ChildDataVisitor{make([]interface{}, 0)}
			node.Accept(cdv)
			idf := new(IndexDefExpression)
			idf.IndexName_ = &node.Name
			for _, colname := range cdv.ChildDatas_ {
				idf.Colnames_ = append(idf.Colnames_, colname.(*string))
			}
			v.QueryInfo_.IndexDefExpressions_ = append(v.QueryInfo_.IndexDefExpressions_, idf)
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
		cdv := &ChildDataVisitor{make([]interface{}, 0)}
		node.Accept(cdv)
		if len(cdv.ChildDatas_) == 1 {
			v.QueryInfo_.LimitNum_ = cdv.ChildDatas_[0].(*types.Value).ToInteger()
		} else { // 2
			v.QueryInfo_.LimitNum_ = cdv.ChildDatas_[0].(*types.Value).ToInteger()
			v.QueryInfo_.OffsetNum_ = cdv.ChildDatas_[1].(*types.Value).ToInteger()
		}

		return in, true
	case *ast.OrderByClause:
	case *ast.ByItem:
		cdv := &ChildDataVisitor{make([]interface{}, 0)}
		node.Accept(cdv)
		obe := new(OrderByExpression)
		obe.IsDesc_ = node.Desc
		obe.ColName_ = cdv.ChildDatas_[0].(*string)
		v.QueryInfo_.OrderByExpressions_ = append(v.QueryInfo_.OrderByExpressions_, obe)
		return in, true
	default:
		panic("unknown node for visitor")
	}
	return in, false
}

func (v *RootSQLVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
