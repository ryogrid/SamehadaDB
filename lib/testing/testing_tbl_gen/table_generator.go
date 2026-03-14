package testing_tbl_gen

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/execution/executors"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"math"
	"math/rand"
	"time"

	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/storage/tuple"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

type ColumnInsertMeta struct {
	/**
	 * Name of the column
	 */
	Name string
	/**
	 * Type of the column
	 */
	Type types.TypeID
	/**
	 * Whether the column is nullable
	 */
	Nullable bool
	/**
	 * Distribution of values
	 */
	Dist int32
	/**
	 * min value of the column
	 */
	Min int32
	/**
	 * max value of the column
	 */
	Max int32
	/**
	 * Counter to generate serial data
	 */
	SerialCounter int32
}

type TableInsertMeta struct {
	/**
	 * Name of the table
	 */
	Name string
	/**
	 * Number of rows
	 */
	NumRows uint32
	/**
	 * Row
	 */
	ColMeta []*ColumnInsertMeta
}

type MakeSchemaMeta struct {
	ColName string
	Expr    expression.ColumnValue
}

type MakeSchemaMetaAgg struct {
	ColName string
	Expr    expression.AggregateValueExpression
}

const DistSerial int32 = 0
const DistUniform int32 = 1

const Test1Size uint32 = 1000
const Test2Size uint32 = 100
const TestVarlenSize uint32 = 10

func GenNumericValues(colMeta *ColumnInsertMeta, count uint32) []types.Value {
	var values []types.Value
	if colMeta.Dist == DistSerial {
		for i := 0; i < int(count); i++ {
			values = append(values, types.NewInteger(colMeta.SerialCounter))
			colMeta.SerialCounter += 1
		}
		return values
	}

	seed := time.Now().UnixNano()
	rand.Seed(seed)
	for i := 0; i < int(count); i++ {
		values = append(values, types.NewInteger(rand.Int31n(colMeta.Max)))
	}
	return values
}

func GenNumericValuesFloat(colMeta *ColumnInsertMeta, count uint32) []types.Value {
	var values []types.Value
	if colMeta.Dist == DistSerial {
		for i := 0; i < int(count); i++ {
			values = append(values, types.NewInteger(colMeta.SerialCounter))
			colMeta.SerialCounter += 1
		}
		return values
	}

	seed := time.Now().UnixNano()
	rand.Seed(seed)
	for i := 0; i < int(count); i++ {
		values = append(values, types.NewFloat(float32(rand.Int31n(colMeta.Max))/float32(seed)))
	}
	return values
}

func MakeValues(colMeta *ColumnInsertMeta, count uint32) []types.Value {
	switch colMeta.Type {
	case types.Integer:
		return GenNumericValues(colMeta, count)
	case types.Float:
		return GenNumericValuesFloat(colMeta, count)
	default:
		panic("Not yet implemented")
	}
}

func FillTable(info *catalog.TableMetadata, tableMeta *TableInsertMeta, txn *access.Transaction) {
	var numInserted uint32 = 0
	var batchSize uint32 = 128
	for numInserted < tableMeta.NumRows {
		var values [][]types.Value
		var numValues = uint32(math.Min(float64(batchSize), float64(tableMeta.NumRows-numInserted)))
		for _, colMeta := range tableMeta.ColMeta {
			values = append(values, MakeValues(colMeta, numValues))
		}

		for i := 0; i < int(numValues); i++ {
			var entry []types.Value
			for idx := range tableMeta.ColMeta {
				entry = append(entry, values[idx][i])
			}
			tpl := tuple.NewTupleFromSchema(entry, info.Schema())
			rid, err := info.Table().InsertTuple(tpl, txn, info.OID(), false)
			if rid == nil || err != nil {
				fmt.Printf("InsertTuple failed on FillTable rid = %v, err = %v", rid, err)
				panic("InsertTuple failed on FillTable!")
			}
			// insert entry to index
			for idx := range tableMeta.ColMeta {
				index := info.GetIndex(idx)
				if index != nil {
					index.InsertEntry(tpl, *rid, txn)
				}
			}
			numInserted++
		}
	}
}

func MakeComparisonExpression(lhs expression.Expression, rhs expression.Expression,
	compType expression.ComparisonType) expression.Expression {

	retExp := expression.NewComparison(lhs, rhs, compType, types.Boolean)
	return retExp
}

func MakeAggregateValueExpression(isGroupByTerm bool, colIndex uint32) expression.Expression {
	return expression.NewAggregateValueExpression(isGroupByTerm, colIndex, types.Integer)
}

func MakeConstantValueExpression(val *types.Value) expression.Expression {
	return expression.NewConstantValue(*val, val.ValueType())
}

func MakeOutputSchema(exprs []MakeSchemaMeta) *schema.Schema {
	var cols = make([]*column.Column, 0)
	for _, input := range exprs {
		cols = append(cols, column.NewColumn(input.ColName, input.Expr.GetReturnType(), false, index_constants.IndexKindInvalid, types.PageID(-1), input.Expr))
	}
	return schema.NewSchema(cols)
}

func MakeOutputSchemaAgg(exprs []MakeSchemaMetaAgg) *schema.Schema {
	var cols = make([]*column.Column, 0)
	for _, input := range exprs {
		cols = append(cols, column.NewColumn(input.ColName, input.Expr.GetReturnType(), false, index_constants.IndexKindInvalid, types.PageID(-1), input.Expr))
	}
	return schema.NewSchema(cols)
}

func GenerateTestTabls(c *catalog.Catalog, execCtx *executors.ExecutorContext,
	txn *access.Transaction) (*catalog.TableMetadata, *catalog.TableMetadata) {
	columnA := column.NewColumn("colA", types.Integer, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	columnB := column.NewColumn("colB", types.Integer, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	columnC := column.NewColumn("colC", types.Integer, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	columnD := column.NewColumn("colD", types.Integer, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	sc := schema.NewSchema([]*column.Column{columnA, columnB, columnC, columnD})
	tableMetadata1 := c.CreateTable("test_1", sc, txn)

	column1 := column.NewColumn("col1", types.Integer, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	column2 := column.NewColumn("col2", types.Integer, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	column3 := column.NewColumn("col3", types.Integer, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	column4 := column.NewColumn("col3", types.Integer, false, index_constants.IndexKindInvalid, types.PageID(-1), nil)
	sc = schema.NewSchema([]*column.Column{column1, column2, column3, column4})
	tableMetadata2 := c.CreateTable("test_2", sc, txn)

	tableMeta1 := &TableInsertMeta{"test_1",
		Test1Size,
		[]*ColumnInsertMeta{
			{"colA", types.Integer, false, DistSerial, 0, 0, 0},
			{"colB", types.Integer, false, DistUniform, 0, 9, 0},
			{"colC", types.Integer, false, DistUniform, 0, 9999, 0},
			{"colD", types.Integer, false, DistUniform, 0, 99999, 0},
		}}
	tableMeta2 := &TableInsertMeta{"test_2",
		Test2Size,
		[]*ColumnInsertMeta{
			{"col1", types.Integer, false, DistSerial, 0, 0, 0},
			{"col2", types.Integer, false, DistUniform, 0, 9, 0},
			{"col3", types.Integer, false, DistUniform, 0, 1024, 0},
			{"col4", types.Integer, false, DistUniform, 0, 2048, 0},
		}}
	FillTable(tableMetadata1, tableMeta1, txn)
	FillTable(tableMetadata2, tableMeta2, txn)

	return tableMetadata1, tableMetadata2
}
