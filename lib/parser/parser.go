package parser

import (
	"fmt"
	"github.com/pingcap/parser"
	"github.com/pingcap/parser/ast"
	_ "github.com/pingcap/tidb/types/parser_driver"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

type QueryInfo struct {
	QueryType           *QueryType
	SelectFields        []*SelectFieldExpression // SELECT
	SetExpressions      []*SetExpression         // UPDATE
	NewTable            *string                  // CREATE TABLE
	ColDefExpressions   []*ColDefExpression      // CREATE TABLE
	IndexDefExpressions []*IndexDefExpression    // CREATE TABLE
	TargetCols          []*string                // INSERT
	Values              []*types.Value           // INSERT
	OnExpressions       *BinaryOpExpression      // SELECT (with JOIN)
	JoinTables_          []*string                // SELECT
	WhereExpression     *BinaryOpExpression      // SELECT, UPDATE, DELETE
	LimitNum            int32                    // SELECT
	OffsetNum           int32                    // SELECT
	OrderByExpressions  []*OrderByExpression     // SELECT
}

func extractInfoFromAST(rootNode *ast.StmtNode) *QueryInfo {
	v := NewRootSQLVisitor()
	(*rootNode).Accept(v)
	return v.QueryInfo
}

func parse(sqlStr *string) (*ast.StmtNode, error) {
	p := parser.New()

	stmtNodes, _, err := p.Parse(*sqlStr, "", "")
	if err != nil {
		return nil, err
	}

	return &stmtNodes[0], nil
}

func ProcessSQLStr(sqlStr *string) (*QueryInfo, error) {
	astNode, err := parse(sqlStr)
	if err != nil {
		fmt.Printf("parse error: %v\n", err.Error())
		return nil, err
	}

	return extractInfoFromAST(astNode), nil
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
