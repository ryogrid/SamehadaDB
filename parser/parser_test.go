package parser

import (
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
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
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression_.Left_.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*types.Value).ToVarchar() == "daylight")

	sqlStr = "SELECT a, b FROM t WHERE a = 10;"
	queryInfo = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[1].ColName_ == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression_.Left_.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*types.Value).ToInteger() == 10)

	sqlStr = "SELECT a, b FROM t WHERE a > 10.5;"
	queryInfo = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[1].ColName_ == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == expression.GreaterThan)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression_.Left_.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*types.Value).ToFloat() == 10.5)
}

func TestMultiPredicateSelectQuery(t *testing.T) {
	// ((a = 10 AND b = 20) AND c != 'daylight') OR (d = 50)

	// OR -- (L) AND -- AND -- EQ -- 'a' (*string)
	// |         |      |      |---- 10 (*types.Value)
	// |         |      |
	// |         |      |---- EQ -- 'b' (*string)
	// |         |            |---- 20 (*types.Value)
	// |         |
	// |         |---- NE -- 'c' (*string)
	// |               |---- 'daylight' (*types.Value)
	// |
	// |--- (R) EQ -- 'd' (*string)
	//          |---- 50 (*types.Value)

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
	aEq10 := queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, aEq10.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, aEq10.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *aEq10.Left_.(*string) == "a")
	testingpkg.SimpleAssert(t, aEq10.Right_.(*types.Value).ToInteger() == 10)

	// b = 20
	bEq20 := queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression).Right_.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, bEq20.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, bEq20.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *bEq20.Left_.(*string) == "b")
	testingpkg.SimpleAssert(t, bEq20.Right_.(*types.Value).ToInteger() == 20)

	// c != 'daylight'
	cNEDlight := queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Right_.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, cNEDlight.ComparisonOperationType_ == expression.NotEqual)
	testingpkg.SimpleAssert(t, cNEDlight.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *cNEDlight.Left_.(*string) == "c")
	testingpkg.SimpleAssert(t, cNEDlight.Right_.(*types.Value).ToVarchar() == "daylight")

	// (d = 50)
	dEq50 := queryInfo.WhereExpression_.Right_.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, dEq50.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, dEq50.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *dEq50.Left_.(*string) == "d")
	testingpkg.SimpleAssert(t, dEq50.Right_.(*types.Value).ToInteger() == 50)

	// (a = 10 AND b = 20) AND (c != 'daylight' OR d = 50)

	// AND -- (L) AND -- EQ -- 'a' (*string)      L
	//  |          |     |---- 10 (*types.Value)
	//  |          |
	//  |          |---- EQ -- 'b' (*string)
	//  |                |---- 20 (*types.Value)
	//  |
	//  ----- (R) OR --- NE -- 'c' (*string)
	//            |      |---- 'daylight' (*types.Value)
	//            |
	//            |----- EQ -- 'd' (*string)
	//                   |---- 50 (*types.Value)

	sqlStr = "SELECT a, b FROM t WHERE a = 10 AND b = 20 AND (c != 'daylight' OR d = 50);"
	queryInfo = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[1].ColName_ == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")

	// (a = 10 AND b = 20) *AND* (c != 'daylight' OR d = 50)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == expression.AND)

	// (a = 10 *AND* b = 20)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).ComparisonOperationType_ == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).LogicalOperationType_ == expression.AND)

	// a = 10
	aEq10 = queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Left_.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, aEq10.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, aEq10.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *aEq10.Left_.(*string) == "a")
	testingpkg.SimpleAssert(t, aEq10.Right_.(*types.Value).ToInteger() == 10)

	// b = 20
	bEq20 = queryInfo.WhereExpression_.Left_.(*BinaryOpExpression).Right_.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, bEq20.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, bEq20.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *bEq20.Left_.(*string) == "b")
	testingpkg.SimpleAssert(t, bEq20.Right_.(*types.Value).ToInteger() == 20)

	// (c != 'daylight' *OR* d = 50)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*BinaryOpExpression).ComparisonOperationType_ == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*BinaryOpExpression).LogicalOperationType_ == expression.OR)

	// c != 'daylight'
	cNEDlight = queryInfo.WhereExpression_.Right_.(*BinaryOpExpression).Left_.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, cNEDlight.ComparisonOperationType_ == expression.NotEqual)
	testingpkg.SimpleAssert(t, cNEDlight.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *cNEDlight.Left_.(*string) == "c")
	testingpkg.SimpleAssert(t, cNEDlight.Right_.(*types.Value).ToVarchar() == "daylight")

	// d = 50
	dEq50 = queryInfo.WhereExpression_.Right_.(*BinaryOpExpression).Right_.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, dEq50.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, dEq50.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *dEq50.Left_.(*string) == "d")
	testingpkg.SimpleAssert(t, dEq50.Right_.(*types.Value).ToInteger() == 50)
}

func TestWildcardSelectQuery(t *testing.T) {
	sqlStr := "SELECT * FROM t WHERE a = 10;"
	queryInfo := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "*")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression_.Left_.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*types.Value).ToInteger() == 10)
}

func TestAggFuncSelectQuery(t *testing.T) {
	sqlStr := "SELECT count(*), max(b), min(c), sum(d), b FROM t WHERE a = 10;"
	queryInfo := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)

	testingpkg.SimpleAssert(t, queryInfo.SelectFields_[0].IsAgg_ == true)
	testingpkg.SimpleAssert(t, queryInfo.SelectFields_[0].AggType_ == plans.COUNT_AGGREGATE)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "*")
	testingpkg.SimpleAssert(t, queryInfo.SelectFields_[1].IsAgg_ == true)
	testingpkg.SimpleAssert(t, queryInfo.SelectFields_[1].AggType_ == plans.MAX_AGGREGATE)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[1].ColName_ == "b")
	testingpkg.SimpleAssert(t, queryInfo.SelectFields_[2].IsAgg_ == true)
	testingpkg.SimpleAssert(t, queryInfo.SelectFields_[2].AggType_ == plans.MIN_AGGREGATE)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[2].ColName_ == "c")
	testingpkg.SimpleAssert(t, queryInfo.SelectFields_[3].IsAgg_ == true)
	testingpkg.SimpleAssert(t, queryInfo.SelectFields_[3].AggType_ == plans.SUM_AGGREGATE)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[3].ColName_ == "d")
	testingpkg.SimpleAssert(t, queryInfo.SelectFields_[4].IsAgg_ == false)
	testingpkg.SimpleAssert(t, queryInfo.SelectFields_[4].AggType_ == 0)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[4].ColName_ == "b")

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression_.Left_.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*types.Value).ToInteger() == 10)
}

func TestLimitOffsetSelectQuery(t *testing.T) {
	sqlStr := "SELECT a, b FROM t WHERE a = 10 LIMIT 100 OFFSET 200;"
	queryInfo := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[1].ColName_ == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")

	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression_.Left_.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*types.Value).ToInteger() == 10)

	testingpkg.SimpleAssert(t, queryInfo.LimitNum_ == 100)
	testingpkg.SimpleAssert(t, queryInfo.OffsetNum_ == 200)

	sqlStr = "SELECT a, b FROM t WHERE a = 10;"
	queryInfo = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, queryInfo.LimitNum_ == -1)
	testingpkg.SimpleAssert(t, queryInfo.OffsetNum_ == -1)
}

func TestIsNullIsNotNullSelectQuery(t *testing.T) {
	// (a IS NULL) AND (b > 10)
	sqlStr := "SELECT a, b FROM t WHERE a IS NULL AND b > 10;"
	queryInfo := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[1].ColName_ == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")

	// (a IS NULL) *AND* (b > 10)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == expression.AND)

	// (a IS NULL)
	aIsNull := queryInfo.WhereExpression_.Left_.(*BinaryOpExpression)
	// (a *IS* NULL)
	testingpkg.SimpleAssert(t, aIsNull.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, aIsNull.LogicalOperationType_ == -1)

	testingpkg.SimpleAssert(t, *aIsNull.Left_.(*string) == "a")
	testingpkg.SimpleAssert(t, aIsNull.Right_.(*types.Value).IsNull() == true)

	// (b > 10)
	bGT10 := queryInfo.WhereExpression_.Right_.(*BinaryOpExpression)
	// (b *>* 10)
	testingpkg.SimpleAssert(t, bGT10.ComparisonOperationType_ == expression.GreaterThan)
	testingpkg.SimpleAssert(t, bGT10.LogicalOperationType_ == -1)

	testingpkg.SimpleAssert(t, *bGT10.Left_.(*string) == "b")
	testingpkg.SimpleAssert(t, bGT10.Right_.(*types.Value).ToInteger() == 10)

	// (a IS NOT NULL) AND (b > 10)
	sqlStr = "SELECT a, b FROM t WHERE a IS NOT NULL AND b > 10;"
	queryInfo = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[1].ColName_ == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")

	// (a IS NOT NULL) *AND* (b > 10)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == expression.AND)

	// (a IS NOT NULL)
	aIsNotNull := queryInfo.WhereExpression_.Left_.(*BinaryOpExpression)

	// (a *IS NOT* NULL)
	testingpkg.SimpleAssert(t, aIsNotNull.ComparisonOperationType_ == expression.NotEqual)
	testingpkg.SimpleAssert(t, aIsNotNull.LogicalOperationType_ == -1)

	testingpkg.SimpleAssert(t, *aIsNotNull.Left_.(*string) == "a")
	testingpkg.SimpleAssert(t, aIsNotNull.Right_.(*types.Value).IsNull() == true)

	// (b > 10)
	bGT10 = queryInfo.WhereExpression_.Right_.(*BinaryOpExpression)
	// (b *>* 10)
	testingpkg.SimpleAssert(t, bGT10.ComparisonOperationType_ == expression.GreaterThan)
	testingpkg.SimpleAssert(t, bGT10.LogicalOperationType_ == -1)

	testingpkg.SimpleAssert(t, *bGT10.Left_.(*string) == "b")
	testingpkg.SimpleAssert(t, bGT10.Right_.(*types.Value).ToInteger() == 10)
}

func TestSimpleJoinSelectQuery(t *testing.T) {
	sqlStr := "SELECT staff.a, staff.b, staff.c, friend.d FROM staff INNER JOIN friend ON staff.c = friend.c WHERE friend.d = 10;"
	queryInfo := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)

	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].TableName_ == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[1].TableName_ == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[1].ColName_ == "b")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[2].TableName_ == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[2].ColName_ == "c")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[3].TableName_ == "friend")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[3].ColName_ == "d")

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[1] == "friend")

	testingpkg.SimpleAssert(t, queryInfo.OnExpressions_.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.OnExpressions_.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *queryInfo.OnExpressions_.Left_.(*string) == "staff.c")
	testingpkg.SimpleAssert(t, *queryInfo.OnExpressions_.Right_.(*string) == "friend.c")

	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression_.Left_.(*string) == "friend.d")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*types.Value).ToInteger() == 10)

	sqlStr = "SELECT staff.a, staff.b, staff.c, friend.d, e FROM staff INNER JOIN friend ON staff.c = friend.c WHERE friend.d = 10;"
	queryInfo = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == SELECT)

	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].TableName_ == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[0].ColName_ == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[1].TableName_ == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[1].ColName_ == "b")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[2].TableName_ == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[2].ColName_ == "c")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[3].TableName_ == "friend")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[3].ColName_ == "d")
	testingpkg.SimpleAssert(t, queryInfo.SelectFields_[4].TableName_ == nil)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields_[4].ColName_ == "e")

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[1] == "friend")

	testingpkg.SimpleAssert(t, queryInfo.OnExpressions_.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.OnExpressions_.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *queryInfo.OnExpressions_.Left_.(*string) == "staff.c")
	testingpkg.SimpleAssert(t, *queryInfo.OnExpressions_.Right_.(*string) == "friend.c")

	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression_.Left_.(*string) == "friend.d")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*types.Value).ToInteger() == 10)
}

func TestSimpleCreateTableQuery(t *testing.T) {
	sqlStr := "CREATE TABLE name_age_list(name VARCHAR(256), age INT);"
	queryInfo := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == CREATE_TABLE)
	testingpkg.SimpleAssert(t, *queryInfo.NewTable_ == "name_age_list")
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions_[0].ColName_ == "name")
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions_[0].ColType_ == types.Varchar)
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions_[1].ColName_ == "age")
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions_[1].ColType_ == types.Integer)
}

func TestCreateTableWithIndexDefQuery(t *testing.T) {
	sqlStr := "CREATE TABLE name_age_list(id INT, name VARCHAR(256), age FLOAT, index id_idx (id), index name_age_idx (name, age));"
	queryInfo := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == CREATE_TABLE)

	testingpkg.SimpleAssert(t, *queryInfo.NewTable_ == "name_age_list")

	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions_[0].ColName_ == "id")
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions_[0].ColType_ == types.Integer)
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions_[1].ColName_ == "name")
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions_[1].ColType_ == types.Varchar)
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions_[2].ColName_ == "age")
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions_[2].ColType_ == types.Float)

	testingpkg.SimpleAssert(t, *queryInfo.IndexDefExpressions_[0].IndexName_ == "id_idx")
	testingpkg.SimpleAssert(t, *queryInfo.IndexDefExpressions_[0].Colnames_[0] == "id")
	testingpkg.SimpleAssert(t, *queryInfo.IndexDefExpressions_[1].IndexName_ == "name_age_idx")
	testingpkg.SimpleAssert(t, *queryInfo.IndexDefExpressions_[1].Colnames_[0] == "name")
	testingpkg.SimpleAssert(t, *queryInfo.IndexDefExpressions_[1].Colnames_[1] == "age")
}

func TestInsertQuery(t *testing.T) {
	sqlStr := "INSERT INTO syain(name) VALUES ('鈴木');"
	queryInfo := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == INSERT)

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "syain")

	testingpkg.SimpleAssert(t, *queryInfo.TargetCols_[0] == "name")
	testingpkg.SimpleAssert(t, queryInfo.Values_[0].ToVarchar() == "鈴木")

	sqlStr = "INSERT INTO syain(id,name,romaji) VALUES (1,'鈴木','suzuki');"
	queryInfo = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == INSERT)

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "syain")

	testingpkg.SimpleAssert(t, *queryInfo.TargetCols_[0] == "id")
	testingpkg.SimpleAssert(t, queryInfo.Values_[0].ToInteger() == 1)
	testingpkg.SimpleAssert(t, *queryInfo.TargetCols_[1] == "name")
	testingpkg.SimpleAssert(t, queryInfo.Values_[1].ToVarchar() == "鈴木")
	testingpkg.SimpleAssert(t, *queryInfo.TargetCols_[2] == "romaji")
	testingpkg.SimpleAssert(t, queryInfo.Values_[2].ToVarchar() == "suzuki")
}

func TestSimpleDeleteQuery(t *testing.T) {
	sqlStr := "DELETE FROM users WHERE id = 10;"
	queryInfo := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType_ == DELETE)

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "users")

	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.ComparisonOperationType_ == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.LogicalOperationType_ == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression_.Left_.(*string) == "id")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression_.Right_.(*types.Value).ToInteger() == 10)
}
