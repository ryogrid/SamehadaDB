package parser

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/types"
	"reflect"
)

type QueryType int32

const (
	SELECT QueryType = iota
	CREATE_TABLE
	INSERT
	DELETE
	UPDATE
)

type PredicateExpression struct {
	BinaryOperationType_ *expression.ComparisonType
	LeftVal              *string
	RightVal             *string
}

type SetExpression struct {
	ColName_     *string
	UpdateValue_ *types.Value
}

type ColDefExpression struct {
	ColName_ *string
	ColType_ *types.TypeID
}

type Visitor interface {
	Enter(n ast.Node) (node ast.Node, skipChildren bool)
	Leave(n ast.Node) (node ast.Node, ok bool)
}

type QueryInfo struct {
	QueryType_         *QueryType
	SelectFields_      []*string
	SetExpression_     *SetExpression
	NewTable_          *string
	TargetTable_       *string
	ColDefExpressions_ []*ColDefExpression
	OnExpressions_     []*PredicateExpression
	FromTable_         *string
	JoinTable_         *string
	WhereExpressions_  []*PredicateExpression
}

type SelectFieldsVisitor struct {
	QueryInfo_ *QueryInfo
}

func (v *SelectFieldsVisitor) Enter(in ast.Node) (ast.Node, bool) {
	//if name, ok := in.(*ast.ColumnName); ok {
	//	v.colNames = append(v.colNames, name.Name.O)
	//}
	refVal := reflect.ValueOf(in) // ValueOfでreflect.Value型のオブジェクトを取得
	fmt.Println(refVal.Type())
	switch node := in.(type) {
	case *ast.ColumnName:
		colname := node.Name.String()
		v.QueryInfo_.SelectFields_ = append(v.QueryInfo_.SelectFields_, &colname)
		return in, true
	default:
	}
	return in, false
}

func (v *SelectFieldsVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

type SimpleSQLVisitor struct {
	QueryInfo_ *QueryInfo
	// member of example code
	colNames []string
}

func NewSimpleSQLVisitor() *SimpleSQLVisitor {
	ret := new(SimpleSQLVisitor)
	qinfo := new(QueryInfo)
	qinfo.QueryType_ = new(QueryType)
	qinfo.SelectFields_ = make([]*string, 0)
	qinfo.SetExpression_ = new(SetExpression)
	qinfo.ColDefExpressions_ = make([]*ColDefExpression, 0)
	qinfo.OnExpressions_ = make([]*PredicateExpression, 0)
	qinfo.WhereExpressions_ = make([]*PredicateExpression, 0)
	ret.QueryInfo_ = qinfo

	return ret
}

func (v *SimpleSQLVisitor) Enter(in ast.Node) (ast.Node, bool) {
	//if name, ok := in.(*ast.ColumnName); ok {
	//	v.colNames = append(v.colNames, name.Name.O)
	//}
	refVal := reflect.ValueOf(in) // ValueOfでreflect.Value型のオブジェクトを取得
	fmt.Println(refVal.Type())

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
	case *ast.Join:
	case *ast.OnCondition:
	case *ast.TableSource:
	case *ast.TableNameExpr:
	case *ast.TableName:
		switch *v.QueryInfo_.QueryType_ {
		case SELECT:
			if v.QueryInfo_.FromTable_ == nil {
				tbname := node.Name.String()
				v.QueryInfo_.FromTable_ = &tbname
			} else {
				tbname := node.Name.String()
				v.QueryInfo_.JoinTable_ = &tbname
			}
		case UPDATE:
			tbname := node.Name.String()
			v.QueryInfo_.TargetTable_ = &tbname
		case INSERT:
			tbname := node.Name.String()
			v.QueryInfo_.TargetTable_ = &tbname
		case DELETE:
			tbname := node.Name.String()
			v.QueryInfo_.FromTable_ = &tbname
		case CREATE_TABLE:
			tbname := node.Name.String()
			v.QueryInfo_.NewTable_ = &tbname
		}
	case *ast.ColumnDef:
	case *ast.ColumnNameExpr:
	case *ast.ColumnName:
	case *ast.BinaryOperationExpr:
	case *driver.ValueExpr:
	default:
		panic("unknown node for visitor")
	}
	return in, false
}

func (v *SimpleSQLVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func extract(rootNode *ast.StmtNode) []string {
	//v := &SimpleSQLVisitor{}
	v := NewSimpleSQLVisitor()
	(*rootNode).Accept(v)
	return v.colNames
}

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	return &stmtNodes[0], nil
}

func TestParsing() {
	//astNode, err := parse("SELECT a, b FROM t WHERE a = daylight")
	//if err != nil {
	//	fmt.Printf("parse error: %v\n", err.Error())
	//	return
	//}
	//fmt.Printf("%v\n", *astNode)

	//if len(os.Args) != 2 {
	//	fmt.Println("usage: colx 'SQL statement'")
	//	return
	//}
	//sql := os.Args[1]
	sql := "SELECT a, b FROM t WHERE a = daylight"
	//sql := "UPDATE employees SET title = 'Mr.' WHERE gender = 'M'"
	//sql := "INSERT INTO syain(id,name,romaji) VALUES (1,'鈴木','suzuki');"
	//sql := "DELETE FROM users WHERE id = 10;"
	//sql := "SELECT staff.a, staff.b, staff.c, friend.d FROM staff INNER JOIN friend ON staff.c = friend.c WHERE friend.d = 10;"
	//sql := "CREATE TABLE name_age_list(id INT, name VARCHAR(256), age FLOAT);"
	astNode, err := parse(sql)
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return
	}
	fmt.Printf("%v\n", extract(astNode))
}
