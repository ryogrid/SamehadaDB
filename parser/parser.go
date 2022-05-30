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

type SimpleSQLVisitor struct {
	QueryType_         *QueryType
	SelectFields_      []string
	SetExpression_     *SetExpression
	NewTableName_      *string
	ColDefExpressions_ []*ColDefExpression
	OnExpressions_     []*PredicateExpression
	FromTable_         *string
	JoinTable_         *string
	WhereExpressions_  []*PredicateExpression
	// member of example code
	colNames []string
}

func (v *SimpleSQLVisitor) Enter(in ast.Node) (ast.Node, bool) {
	//if name, ok := in.(*ast.ColumnName); ok {
	//	v.colNames = append(v.colNames, name.Name.O)
	//}
	refVal := reflect.ValueOf(in) // ValueOfでreflect.Value型のオブジェクトを取得
	fmt.Println(refVal.Type())

	switch ntype := in.(type) {
	case *ast.SelectStmt:
		ntype.Text()
	case *ast.CreateTableStmt:
	case *ast.InsertStmt:
	case *ast.DeleteStmt:
	case *ast.UpdateStmt:
	case *ast.FieldList:
	case *ast.SelectField:
	case *ast.TableRefsClause:
	case *ast.Assignment:
	case *ast.Join:
	case *ast.OnCondition:
	case *ast.TableSource:
	case *ast.TableNameExpr:
	case *ast.TableName:
	case *ast.ColumnDef:
	case *ast.ColumnNameExpr:
	case *ast.ColumnName:
	case *ast.BinaryOperationExpr:
	case *driver.ValueExpr:
	}
	return in, false
}

func (v *SimpleSQLVisitor) Leave(in ast.Node) (ast.Node, bool) {
	return in, true
}

func extract(rootNode *ast.StmtNode) []string {
	v := &SimpleSQLVisitor{}
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
	//sql := "SELECT a, b FROM t WHERE a = daylight"
	//sql := "UPDATE employees SET title = 'Mr.' WHERE gender = 'M'"
	//sql := "INSERT INTO syain(id,name,romaji) VALUES (1,'鈴木','suzuki');"
	//sql := "DELETE FROM users WHERE id = 10;"
	//sql := "SELECT staff.a, staff.b, staff.c, friend.d FROM staff INNER JOIN friend ON staff.c = friend.c WHERE friend.d = 10;"
	sql := "CREATE TABLE name_age_list(id INT, name VARCHAR(256), age FLOAT);"
	astNode, err := parse(sql)
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return
	}
	fmt.Printf("%v\n", extract(astNode))
}
