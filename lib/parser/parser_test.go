package parser

import (
	"testing"

	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	testingpkg "github.com/ryogrid/SamehadaDB/lib/testing/testing_assert"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

func TestSinglePredicateSelectQuery(t *testing.T) {
	sqlStr := "SELECT a FROM t WHERE a = 'daylight';"
	queryInfo, _ := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[0].ColName == "a")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression.Left.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Right.(*types.Value).ToVarchar() == "daylight")

	sqlStr = "SELECT a, b FROM t WHERE a = 10;"
	queryInfo, _ = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[0].ColName == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[1].ColName == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression.Left.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Right.(*types.Value).ToInteger() == 10)

	sqlStr = "SELECT a, b FROM t WHERE a > 10.5;"
	queryInfo, _ = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[0].ColName == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[1].ColName == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == expression.GreaterThan)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression.Left.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Right.(*types.Value).ToFloat() == 10.5)
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
	queryInfo, _ := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[0].ColName == "a")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")

	// ((a = 10 AND b = 20) AND c != 'daylight') *OR* (d = 50)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == expression.OR)

	// ((a = 10 AND b = 20) *AND* c != 'daylight')
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Left.(*BinaryOpExpression).ComparisonOperationType == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Left.(*BinaryOpExpression).LogicalOperationType == expression.AND)

	// (a = 10 *AND* b = 20)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Left.(*BinaryOpExpression).Left.(*BinaryOpExpression).ComparisonOperationType == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Left.(*BinaryOpExpression).Left.(*BinaryOpExpression).LogicalOperationType == expression.AND)

	// a = 10
	aEq10 := queryInfo.WhereExpression.Left.(*BinaryOpExpression).Left.(*BinaryOpExpression).Left.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, aEq10.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, aEq10.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *aEq10.Left.(*string) == "a")
	testingpkg.SimpleAssert(t, aEq10.Right.(*types.Value).ToInteger() == 10)

	// b = 20
	bEq20 := queryInfo.WhereExpression.Left.(*BinaryOpExpression).Left.(*BinaryOpExpression).Right.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, bEq20.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, bEq20.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *bEq20.Left.(*string) == "b")
	testingpkg.SimpleAssert(t, bEq20.Right.(*types.Value).ToInteger() == 20)

	// c != 'daylight'
	cNEDlight := queryInfo.WhereExpression.Left.(*BinaryOpExpression).Right.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, cNEDlight.ComparisonOperationType == expression.NotEqual)
	testingpkg.SimpleAssert(t, cNEDlight.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *cNEDlight.Left.(*string) == "c")
	testingpkg.SimpleAssert(t, cNEDlight.Right.(*types.Value).ToVarchar() == "daylight")

	// (d = 50)
	dEq50 := queryInfo.WhereExpression.Right.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, dEq50.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, dEq50.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *dEq50.Left.(*string) == "d")
	testingpkg.SimpleAssert(t, dEq50.Right.(*types.Value).ToInteger() == 50)

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
	queryInfo, _ = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[0].ColName == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[1].ColName == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")

	// (a = 10 AND b = 20) *AND* (c != 'daylight' OR d = 50)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == expression.AND)

	// (a = 10 *AND* b = 20)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Left.(*BinaryOpExpression).ComparisonOperationType == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Left.(*BinaryOpExpression).LogicalOperationType == expression.AND)

	// a = 10
	aEq10 = queryInfo.WhereExpression.Left.(*BinaryOpExpression).Left.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, aEq10.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, aEq10.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *aEq10.Left.(*string) == "a")
	testingpkg.SimpleAssert(t, aEq10.Right.(*types.Value).ToInteger() == 10)

	// b = 20
	bEq20 = queryInfo.WhereExpression.Left.(*BinaryOpExpression).Right.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, bEq20.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, bEq20.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *bEq20.Left.(*string) == "b")
	testingpkg.SimpleAssert(t, bEq20.Right.(*types.Value).ToInteger() == 20)

	// (c != 'daylight' *OR* d = 50)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Right.(*BinaryOpExpression).ComparisonOperationType == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Right.(*BinaryOpExpression).LogicalOperationType == expression.OR)

	// c != 'daylight'
	cNEDlight = queryInfo.WhereExpression.Right.(*BinaryOpExpression).Left.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, cNEDlight.ComparisonOperationType == expression.NotEqual)
	testingpkg.SimpleAssert(t, cNEDlight.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *cNEDlight.Left.(*string) == "c")
	testingpkg.SimpleAssert(t, cNEDlight.Right.(*types.Value).ToVarchar() == "daylight")

	// d = 50
	dEq50 = queryInfo.WhereExpression.Right.(*BinaryOpExpression).Right.(*BinaryOpExpression)
	testingpkg.SimpleAssert(t, dEq50.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, dEq50.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *dEq50.Left.(*string) == "d")
	testingpkg.SimpleAssert(t, dEq50.Right.(*types.Value).ToInteger() == 50)
}

func TestWildcardSelectQuery(t *testing.T) {
	sqlStr := "SELECT * FROM t WHERE a = 10;"
	queryInfo, _ := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[0].ColName == "*")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression.Left.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Right.(*types.Value).ToInteger() == 10)
}

func TestAggFuncSelectQuery(t *testing.T) {
	sqlStr := "SELECT count(*), max(b), min(c), sum(d), b FROM t WHERE a = 10;"
	queryInfo, _ := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == SELECT)

	testingpkg.SimpleAssert(t, queryInfo.SelectFields[0].IsAgg == true)
	testingpkg.SimpleAssert(t, queryInfo.SelectFields[0].AggType == plans.COUNT_AGGREGATE)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[0].ColName == "*")
	testingpkg.SimpleAssert(t, queryInfo.SelectFields[1].IsAgg == true)
	testingpkg.SimpleAssert(t, queryInfo.SelectFields[1].AggType == plans.MAX_AGGREGATE)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[1].ColName == "b")
	testingpkg.SimpleAssert(t, queryInfo.SelectFields[2].IsAgg == true)
	testingpkg.SimpleAssert(t, queryInfo.SelectFields[2].AggType == plans.MIN_AGGREGATE)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[2].ColName == "c")
	testingpkg.SimpleAssert(t, queryInfo.SelectFields[3].IsAgg == true)
	testingpkg.SimpleAssert(t, queryInfo.SelectFields[3].AggType == plans.SUM_AGGREGATE)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[3].ColName == "d")
	testingpkg.SimpleAssert(t, queryInfo.SelectFields[4].IsAgg == false)
	testingpkg.SimpleAssert(t, queryInfo.SelectFields[4].AggType == 0)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[4].ColName == "b")

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression.Left.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Right.(*types.Value).ToInteger() == 10)
}

func TestLimitOffsetSelectQuery(t *testing.T) {
	sqlStr := "SELECT a, b FROM t WHERE a = 10 LIMIT 100 OFFSET 200;"
	queryInfo, _ := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[0].ColName == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[1].ColName == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")

	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression.Left.(*string) == "a")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Right.(*types.Value).ToInteger() == 10)

	testingpkg.SimpleAssert(t, queryInfo.LimitNum == 100)
	testingpkg.SimpleAssert(t, queryInfo.OffsetNum == 200)

	sqlStr = "SELECT a, b FROM t WHERE a = 10;"
	queryInfo, _ = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, queryInfo.LimitNum == -1)
	testingpkg.SimpleAssert(t, queryInfo.OffsetNum == -1)
}

func TestIsNullIsNotNullSelectQuery(t *testing.T) {
	// (a IS NULL) AND (b > 10)
	sqlStr := "SELECT a, b FROM t WHERE a IS NULL AND b > 10;"
	queryInfo, _ := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[0].ColName == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[1].ColName == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")

	// (a IS NULL) *AND* (b > 10)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == expression.AND)

	// (a IS NULL)
	aIsNull := queryInfo.WhereExpression.Left.(*BinaryOpExpression)
	// (a *IS* NULL)
	testingpkg.SimpleAssert(t, aIsNull.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, aIsNull.LogicalOperationType == -1)

	testingpkg.SimpleAssert(t, *aIsNull.Left.(*string) == "a")
	testingpkg.SimpleAssert(t, aIsNull.Right.(*types.Value).IsNull() == true)

	// (b > 10)
	bGT10 := queryInfo.WhereExpression.Right.(*BinaryOpExpression)
	// (b *>* 10)
	testingpkg.SimpleAssert(t, bGT10.ComparisonOperationType == expression.GreaterThan)
	testingpkg.SimpleAssert(t, bGT10.LogicalOperationType == -1)

	testingpkg.SimpleAssert(t, *bGT10.Left.(*string) == "b")
	testingpkg.SimpleAssert(t, bGT10.Right.(*types.Value).ToInteger() == 10)

	// (a IS NOT NULL) AND (b > 10)
	sqlStr = "SELECT a, b FROM t WHERE a IS NOT NULL AND b > 10;"
	queryInfo, _ = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == SELECT)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[0].ColName == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[1].ColName == "b")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "t")

	// (a IS NOT NULL) *AND* (b > 10)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == -1)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == expression.AND)

	// (a IS NOT NULL)
	aIsNotNull := queryInfo.WhereExpression.Left.(*BinaryOpExpression)

	// (a *IS NOT* NULL)
	testingpkg.SimpleAssert(t, aIsNotNull.ComparisonOperationType == expression.NotEqual)
	testingpkg.SimpleAssert(t, aIsNotNull.LogicalOperationType == -1)

	testingpkg.SimpleAssert(t, *aIsNotNull.Left.(*string) == "a")
	testingpkg.SimpleAssert(t, aIsNotNull.Right.(*types.Value).IsNull() == true)

	// (b > 10)
	bGT10 = queryInfo.WhereExpression.Right.(*BinaryOpExpression)
	// (b *>* 10)
	testingpkg.SimpleAssert(t, bGT10.ComparisonOperationType == expression.GreaterThan)
	testingpkg.SimpleAssert(t, bGT10.LogicalOperationType == -1)

	testingpkg.SimpleAssert(t, *bGT10.Left.(*string) == "b")
	testingpkg.SimpleAssert(t, bGT10.Right.(*types.Value).ToInteger() == 10)
}

func TestSimpleJoinSelectQuery(t *testing.T) {
	sqlStr := "SELECT staff.a, staff.b, staff.c, friend.d FROM staff INNER JOIN friend ON staff.c = friend.c WHERE friend.d = 10;"
	queryInfo, _ := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == SELECT)

	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[0].TableName == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[0].ColName == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[1].TableName == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[1].ColName == "b")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[2].TableName == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[2].ColName == "c")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[3].TableName == "friend")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[3].ColName == "d")

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[1] == "friend")

	testingpkg.SimpleAssert(t, queryInfo.OnExpressions.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.OnExpressions.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *queryInfo.OnExpressions.Left.(*string) == "staff.c")
	testingpkg.SimpleAssert(t, *queryInfo.OnExpressions.Right.(*string) == "friend.c")

	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression.Left.(*string) == "friend.d")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Right.(*types.Value).ToInteger() == 10)

	sqlStr = "SELECT staff.a, staff.b, staff.c, friend.d, e FROM staff INNER JOIN friend ON staff.c = friend.c WHERE friend.d = 10;"
	queryInfo, _ = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == SELECT)

	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[0].TableName == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[0].ColName == "a")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[1].TableName == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[1].ColName == "b")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[2].TableName == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[2].ColName == "c")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[3].TableName == "friend")
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[3].ColName == "d")
	testingpkg.SimpleAssert(t, queryInfo.SelectFields[4].TableName == nil)
	testingpkg.SimpleAssert(t, *queryInfo.SelectFields[4].ColName == "e")

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "staff")
	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[1] == "friend")

	testingpkg.SimpleAssert(t, queryInfo.OnExpressions.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.OnExpressions.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *queryInfo.OnExpressions.Left.(*string) == "staff.c")
	testingpkg.SimpleAssert(t, *queryInfo.OnExpressions.Right.(*string) == "friend.c")

	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression.Left.(*string) == "friend.d")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Right.(*types.Value).ToInteger() == 10)
}

func TestSimpleCreateTableQuery(t *testing.T) {
	sqlStr := "CREATE TABLE name_age_list(name VARCHAR(256), age INT);"
	queryInfo, _ := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == CreateTable)
	testingpkg.SimpleAssert(t, *queryInfo.NewTable == "name_age_list")
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions[0].ColName == "name")
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions[0].ColType == types.Varchar)
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions[1].ColName == "age")
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions[1].ColType == types.Integer)
}

func TestCreateTableWithIndexDefQuery(t *testing.T) {
	sqlStr := "CREATE TABLE name_age_list(id INT, name VARCHAR(256), age FLOAT, index id_idx (id), index name_age_idx (name, age));"
	queryInfo, _ := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == CreateTable)

	testingpkg.SimpleAssert(t, *queryInfo.NewTable == "name_age_list")

	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions[0].ColName == "id")
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions[0].ColType == types.Integer)
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions[1].ColName == "name")
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions[1].ColType == types.Varchar)
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions[2].ColName == "age")
	testingpkg.SimpleAssert(t, *queryInfo.ColDefExpressions[2].ColType == types.Float)

	testingpkg.SimpleAssert(t, *queryInfo.IndexDefExpressions[0].IndexName == "id_idx")
	testingpkg.SimpleAssert(t, *queryInfo.IndexDefExpressions[0].Colnames[0] == "id")
	testingpkg.SimpleAssert(t, *queryInfo.IndexDefExpressions[1].IndexName == "name_age_idx")
	testingpkg.SimpleAssert(t, *queryInfo.IndexDefExpressions[1].Colnames[0] == "name")
	testingpkg.SimpleAssert(t, *queryInfo.IndexDefExpressions[1].Colnames[1] == "age")
}

func TestInsertQuery(t *testing.T) {
	sqlStr := "INSERT INTO syain(name) VALUES ('鈴木');"
	queryInfo, _ := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == INSERT)

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "syain")

	testingpkg.SimpleAssert(t, *queryInfo.TargetCols[0] == "name")
	testingpkg.SimpleAssert(t, queryInfo.Values[0].ToVarchar() == "鈴木")

	sqlStr = "INSERT INTO syain(id,name,romaji) VALUES (1,'鈴木','suzuki');"
	queryInfo, _ = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == INSERT)

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "syain")

	testingpkg.SimpleAssert(t, *queryInfo.TargetCols[0] == "id")
	testingpkg.SimpleAssert(t, queryInfo.Values[0].ToInteger() == 1)
	testingpkg.SimpleAssert(t, *queryInfo.TargetCols[1] == "name")
	testingpkg.SimpleAssert(t, queryInfo.Values[1].ToVarchar() == "鈴木")
	testingpkg.SimpleAssert(t, *queryInfo.TargetCols[2] == "romaji")
	testingpkg.SimpleAssert(t, queryInfo.Values[2].ToVarchar() == "suzuki")
}

func TestInsertQueryIncludesSpaceOnString(t *testing.T) {
	sqlStr := "INSERT INTO syain(name) VALUES ('鈴 木');"
	queryInfo, _ := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == INSERT)

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "syain")

	testingpkg.SimpleAssert(t, *queryInfo.TargetCols[0] == "name")
	testingpkg.SimpleAssert(t, queryInfo.Values[0].ToVarchar() == "鈴 木")
}

func TestSimpleDeleteQuery(t *testing.T) {
	sqlStr := "DELETE FROM users WHERE id = 10;"
	queryInfo, _ := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == DELETE)

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "users")

	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression.Left.(*string) == "id")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Right.(*types.Value).ToInteger() == 10)
}

func TestSimpleUpdateQuery(t *testing.T) {
	sqlStr := "UPDATE employees SET title = 'Mr.' WHERE gender = 'M';"
	queryInfo, _ := ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == UPDATE)

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "employees")
	testingpkg.SimpleAssert(t, *queryInfo.SetExpressions[0].ColName == "title")
	testingpkg.SimpleAssert(t, queryInfo.SetExpressions[0].UpdateValue.ToVarchar() == "Mr.")

	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression.Left.(*string) == "gender")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Right.(*types.Value).ToVarchar() == "M")

	sqlStr = "UPDATE employees SET title = 'Mr.', gflag = 7 WHERE gender = 'M';"
	queryInfo, _ = ProcessSQLStr(&sqlStr)
	testingpkg.SimpleAssert(t, *queryInfo.QueryType == UPDATE)

	testingpkg.SimpleAssert(t, *queryInfo.JoinTables_[0] == "employees")
	testingpkg.SimpleAssert(t, *queryInfo.SetExpressions[0].ColName == "title")
	testingpkg.SimpleAssert(t, queryInfo.SetExpressions[0].UpdateValue.ToVarchar() == "Mr.")
	testingpkg.SimpleAssert(t, *queryInfo.SetExpressions[1].ColName == "gflag")
	testingpkg.SimpleAssert(t, queryInfo.SetExpressions[1].UpdateValue.ToInteger() == 7)

	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.ComparisonOperationType == expression.Equal)
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.LogicalOperationType == -1)
	testingpkg.SimpleAssert(t, *queryInfo.WhereExpression.Left.(*string) == "gender")
	testingpkg.SimpleAssert(t, queryInfo.WhereExpression.Right.(*types.Value).ToVarchar() == "M")
}
