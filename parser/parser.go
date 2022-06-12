package parser

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/ryogrid/SamehadaDB/types"
)

type QueryInfo struct {
	QueryType_           *QueryType
	SelectFields_        []*SelectFieldExpression // SELECT
	SetExpressions_      []*SetExpression         // UPDATE
	NewTable_            *string                  // CREATE TABLE
	ColDefExpressions_   []*ColDefExpression      // CREATE TABLE
	IndexDefExpressions_ []*IndexDefExpression    // CREATE TABLE
	TargetCols_          []*string                // INSERT
	Values_              []*types.Value           // INSERT
	OnExpressions_       *BinaryOpExpression      // SELECT (with JOIN)
	JoinTables_          []*string                // SELECT
	WhereExpression_     *BinaryOpExpression      // SELECT, UPDATE, DELETE
	LimitNum_            int32                    // SELECT
	OffsetNum_           int32                    // SELECT
	OrderByExpressions_  []*OrderByExpression     // SELECT
}

func extractInfoFromAST(rootNode *ast.StmtNode) *QueryInfo {
	v := NewRootSQLVisitor()
	(*rootNode).Accept(v)
	return v.QueryInfo_
}

func parse(sql string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(sql, "", "")
	if err != nil {
		return nil, err
	}

	return &stmtNodes[0], nil
}

func ProcessSQLStr() *QueryInfo {
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
	//sql := "SELECT a, b FROM t WHERE a = 'daylight'"
	//sql := "SELECT a, b FROM t WHERE a = 10"
	//sql := "SELECT * FROM t WHERE a = 10"
	//sql := "SELECT a, b FROM t WHERE a = TRUE"
	//sql := "SELECT a, b FROM t WHERE a = 10 AND b = 20 AND c != 'daylight';"
	//sql := "SELECT a, b FROM t WHERE a = 10 AND b = 20 AND c != 'daylight' OR d = 50;"
	//sql := "UPDATE employees SET title = 'Mr.' WHERE gender = 'M'"
	//sql := "UPDATE employees SET title = 'Mr.', gflag = 7 WHERE gender = 'M';"
	//sql := "INSERT INTO syain(id,name,romaji) VALUES (1,'鈴木','suzuki');"
	//sql := "DELETE FROM users WHERE id = 10;"
	//sql := "SELECT staff.a, staff.b, staff.c, friend.d FROM staff INNER JOIN friend ON staff.c = friend.c WHERE friend.d = 10;"
	//sql := "CREATE TABLE name_age_list(id INT, name VARCHAR(256), age FLOAT);"
	sql := "CREATE TABLE name_age_list(id INT, name VARCHAR(256), age FLOAT, index id_idx (id), index name_age_idx (name, age));"
	//sql := "SELECT count(*),max(b),min(c),sum(d), b FROM t WHERE a = 10"
	//sql := "SELECT a, b FROM t WHERE a = 10 limit 100 offset 200;"
	//sql := "SELECT a, b FROM t WHERE a = 10 ORDER BY a desc, b;"
	//sql := "SELECT a, b FROM t WHERE a IS NOT NULL and b > 10;"
	//sql := "SELECT a, b FROM t WHERE a IS NULL and b > 10;"
	astNode, err := parse(sql)
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return nil
	}

	return extractInfoFromAST(astNode)
}
