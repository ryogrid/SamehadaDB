package parser

import (
	"github.com/pingcap/parser/ast"
	"github.com/pingcap/parser/mysql"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

type RootSQLVisitor struct {
	QueryInfo *QueryInfo
}

func NewRootSQLVisitor() *RootSQLVisitor {
	ret := new(RootSQLVisitor)
	qinfo := new(QueryInfo)
	qinfo.QueryType = new(QueryType)
	qinfo.SelectFields = make([]*SelectFieldExpression, 0)
	qinfo.SetExpressions = make([]*SetExpression, 0)
	qinfo.ColDefExpressions = make([]*ColDefExpression, 0)
	qinfo.IndexDefExpressions = make([]*IndexDefExpression, 0)
	qinfo.TargetCols = make([]*string, 0)
	qinfo.Values = make([]*types.Value, 0)
	qinfo.OnExpressions = new(BinaryOpExpression)
	qinfo.JoinTables_ = make([]*string, 0)
	qinfo.WhereExpression = new(BinaryOpExpression)
	qinfo.LimitNum = -1
	qinfo.OffsetNum = -1
	qinfo.OrderByExpressions = make([]*OrderByExpression, 0)
	ret.QueryInfo = qinfo

	return ret
}

func (v *RootSQLVisitor) Enter(in ast.Node) (ast.Node, bool) {
	switch node := in.(type) {
	case *ast.SelectStmt:
		*v.QueryInfo.QueryType = SELECT
	case *ast.CreateTableStmt:
		*v.QueryInfo.QueryType = CreateTable
	case *ast.InsertStmt:
		*v.QueryInfo.QueryType = INSERT
	case *ast.DeleteStmt:
		*v.QueryInfo.QueryType = DELETE
	case *ast.UpdateStmt:
		*v.QueryInfo.QueryType = UPDATE
	case *ast.FieldList:
	case *ast.SelectField:
		sv := &SelectFieldsVisitor{v.QueryInfo}
		node.Accept(sv)
		return in, true
	case *ast.TableRefsClause:
	case *ast.Assignment:
		// when UPDATE
		av := new(AssignVisitor)
		node.Accept(av)
		setExp := new(SetExpression)
		setExp.ColName = av.Colname
		setExp.UpdateValue = av.Value
		v.QueryInfo.SetExpressions = append(v.QueryInfo.SetExpressions, setExp)
		return in, true
	case *ast.Join:
		jv := new(JoinVisitor)
		jv.QueryInfo = v.QueryInfo
		node.Accept(jv)
		return in, true
	case *ast.OnCondition:
	case *ast.TableSource:
	case *ast.TableNameExpr:
	case *ast.TableName:
		switch *v.QueryInfo.QueryType {
		case CreateTable:
			tbname := node.Name.String()
			v.QueryInfo.NewTable = &tbname
		}
	case *ast.ColumnDef:
		if *v.QueryInfo.QueryType == CreateTable {
			cdef := new(ColDefExpression)
			cname := node.Name.String()
			cdef.ColName = &cname
			colType := node.Tp.Tp
			switch colType {
			case mysql.TypeTiny, mysql.TypeLong:
				ctype := types.Integer
				cdef.ColType = &ctype
			case mysql.TypeFloat, mysql.TypeLonglong:
				ctype := types.Float
				cdef.ColType = &ctype
			default:
				ctype := types.Varchar
				cdef.ColType = &ctype
			}
			v.QueryInfo.ColDefExpressions = append(v.QueryInfo.ColDefExpressions, cdef)
			return in, true
		}
	case *ast.Constraint:
		// Index definition at CREATE TABLE
		if *v.QueryInfo.QueryType == CreateTable {
			// get all specified column
			cdv := &ChildDataVisitor{make([]interface{}, 0)}
			node.Accept(cdv)
			idf := new(IndexDefExpression)
			idf.IndexName = &node.Name
			for _, colname := range cdv.ChildDatas {
				idf.Colnames = append(idf.Colnames, colname.(*string))
			}
			v.QueryInfo.IndexDefExpressions = append(v.QueryInfo.IndexDefExpressions, idf)
			return in, true
		}
	case *ast.ColumnNameExpr:
	case *ast.ColumnName:
		if *v.QueryInfo.QueryType == INSERT {
			cname := node.String()
			v.QueryInfo.TargetCols = append(v.QueryInfo.TargetCols, &cname)
			return in, true
		}
	case *ast.BinaryOperationExpr:
		// for WHERE clause. other clauses handles BinaryOperationExpr with self visitor
		newVisitor := &BinaryOpVisitor{v.QueryInfo, new(BinaryOpExpression)}
		node.Accept(newVisitor)
		v.QueryInfo.WhereExpression = newVisitor.BinaryOpExpression

		logicType, compType := GetTypesForBOperationExpr(node.Op)
		v.QueryInfo.WhereExpression.LogicalOperationType = logicType
		v.QueryInfo.WhereExpression.ComparisonOperationType = compType

		return in, true
	case *driver.ValueExpr:
		// when INSERT
		v.QueryInfo.Values = append(v.QueryInfo.Values, ValueExprToValue(node))
		return in, true
	case *ast.Limit:
		cdv := &ChildDataVisitor{make([]interface{}, 0)}
		node.Accept(cdv)
		if len(cdv.ChildDatas) == 1 {
			v.QueryInfo.LimitNum = cdv.ChildDatas[0].(*types.Value).ToInteger()
		} else { // 2
			v.QueryInfo.LimitNum = cdv.ChildDatas[0].(*types.Value).ToInteger()
			v.QueryInfo.OffsetNum = cdv.ChildDatas[1].(*types.Value).ToInteger()
		}

		return in, true
	case *ast.OrderByClause:
	case *ast.ByItem:
		cdv := &ChildDataVisitor{make([]interface{}, 0)}
		node.Accept(cdv)
		obe := new(OrderByExpression)
		obe.IsDesc = node.Desc
		obe.ColName = cdv.ChildDatas[0].(*string)
		v.QueryInfo.OrderByExpressions = append(v.QueryInfo.OrderByExpressions, obe)
		return in, true
	default:
		panic("unknown node for visitor")
	}
	return in, false
}

func (v *RootSQLVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}
