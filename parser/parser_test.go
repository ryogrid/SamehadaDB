package parser

import (
	"github.com/ryogrid/SamehadaDB/execution/expression"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
	"testing"
)

func TestSinglePredicateSelectQuery(t *testing.T) {
	sqlStr := "SELECT a FROM t WHERE a = 'daylight';"
	queryInfo := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "a")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*BinaryOpExpression).Left_.(*types.Value).ToVarchar() == "daylight")

	sqlStr = "SELECT a, b FROM t WHERE a = 10;"
	queryInfo = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[1].ColName_ == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*BinaryOpExpression).Left_.(*types.Value).ToInteger() == 10)

	//sqlStr = "SELECT a, b FROM t WHERE a > 10.5;"
	//queryInfo = ProcessSQLStr(&sqlStr)
}
