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

	sqlStr = "SELECT a, b FROM t WHERE a > 10.5;"
	queryInfo = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[1].ColName_ == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == expression.GreaterThan)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*BinaryOpExpression).Left_.(*types.Value).ToFloat() == 10.5)
}

func TestMultiPredicateSelectQuery(t *testing.T) {
	// ((a = 10 AND b = 20) AND c != 'daylight') OR (d = 50)

	// OR -- AND -- AND -- EQ -- BExp -- 'a' (*string)
	// |      |      |     |---- BExp -- 10 (*types.Value)
	// |      |      |
	// |      |      |---- EQ -- BExp -- 'b' (*string)
	// |      |            |---- BExp -- 20 (*types.Value)
	// |      |
	// |      |---- NE -- BExp -- 'c' (*string)
	// |            |---- BExp -- 'varchar' (*types.Value)
	// |
	// |--- EQ -- BExp -- 'd' (*string)
	//      |---- BExp -- 50 (*types.Value)

	sqlStr := "SELECT a, b FROM t WHERE a = 10 AND b = 20 AND c != 'daylight' OR d = 50;"
	queryInfo := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "a")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")

	// ((a = 10 AND b = 20) AND c != 'daylight') *OR* (d = 50)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == expression.OR)

	// ((a = 10 AND b = 20) *AND* c != 'daylight')
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).ComparisonOperationType_ == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).LogicalOperationType_ == expression.AND)

	// (a = 10 *AND* b = 20)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression).ComparisonOperationType_ == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression).LogicalOperationType_ == expression.AND)

	// a = 10
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression).ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression).LogicalOperationType_ == -1)
	aEq10 := queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, *aEq10.Left_.(*BinaryOpExpression).Left_.(*string) == "a")
	testingpkg.SimpleAssert(t, aEq10.Right_.(*BinaryOpExpression).Left_.(*types.Value).ToInteger() == 10)

	// b = 20
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression).ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression).LogicalOperationType_ == -1)
	bEq20 := queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression).Right_.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, *bEq20.Left_.(*BinaryOpExpression).Left_.(*string) == "b")
	testingpkg.SimpleAssert(t, bEq20.Right_.(*BinaryOpExpression).Left_.(*types.Value).ToInteger() == 20)

	// c != 'daylight'
	cNEDlight := queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Right_.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, cNEDlight.ComparisonOperationType_ == expression.NotEqual)
	testingpkg.SimpleAssert(t, cNEDlight.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *cNEDlight.Left_.(*BinaryOpExpression).Left_.(*string) == "c")
	testingpkg.SimpleAssert(t, cNEDlight.Right_.(*BinaryOpExpression).Left_.(*types.Value).ToVarchar() == "daylight")

	// (d = 50)
	dEq50 := queryInfo.WhereExpression_.Right_.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, dEq50.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, cNEDlight.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *dEq50.Left_.(*BinaryOpExpression).Left_.(*string) == "d")
	testingpkg.SimpleAssert(t, dEq50.Right_.(*BinaryOpExpression).Left_.(*types.Value).ToInteger() == 50)

	// ((a = 10 AND b = 20) AND (c != 'daylight' OR d = 50))
	sqlStr = "SELECT a, b FROM t WHERE a = 10 AND b = 20 AND (c != 'daylight' OR d = 50);"
	queryInfo = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[1].ColName_ == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")

	//testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == expression.Equal)
	//testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == -1)
	//testingpkg.SimpleAssert(t, *queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*string) == "a")
	//testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*BinaryOpExpression).Left_.(*types.Value).ToInteger() == 10)
}
