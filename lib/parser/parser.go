package parser

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/ryogrid/SamehadaDB/lib/types"
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

func parse(sqlStr *string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(*sqlStr, "", "")
	if err != nil {
		return nil, err
	}

	return &stmtNodes[0], nil
}

func ProcessSQLStr(sqlStr *string) *QueryInfo {
	astNode, err := parse(sqlStr)
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return nil
	}

	return extractInfoFromAST(astNode)
}

// for utity func on develop phase
func printTraversedNodes(rootNode *ast.StmtNode) {
	v := NewPrintNodesVisitor()
	(*rootNode).Accept(v)
}

// for utity func on develop phase
func PrintParsedNodes(sqlStr *string) {
	astNode, err := parse(sqlStr)
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return
	}

	printTraversedNodes(astNode)
}
