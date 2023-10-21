package testing_tbl_gen

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/execution/executors"
	"github.com/ryogrid/SamehadaDB/storage/index/index_constants"
	"math"
	"math/rand"
	"time"

	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

type ColumnInsertMeta struct {
	/**
	 * Name of the column
	 */
	Name_ string
	/**
	 * Type of the column
	 */
	Type_ types.TypeID
	/**
	 * Whether the column is nullable
	 */
	Nullable_ bool
	/**
	 * Distribution of values
	 */
	Dist_ int32
	/**
	 * min value of the column
	 */
	Min_ int32
	/**
	 * max value of the column
	 */
	Max_ int32
	/**
	 * Counter to generate serial data
	 */
	Serial_counter_ int32
}

type TableInsertMeta struct {
	/**
	 * Name of the table
	 */
	Name_ string
	/**
	 * Number of rows
	 */
	Num_rows_ uint32
	/**
	 * Columns
	 */
	Col_meta_ []*ColumnInsertMeta
}

type MakeSchemaMeta struct {
	Col_name_ string
	Expr_     expression.ColumnValue
}

type MakeSchemaMetaAgg struct {
	Col_name_ string
	Expr_     expression.AggregateValueExpression
}

const DistSerial int32 = 0
const DistUniform int32 = 1

const TEST1_SIZE uint32 = 1000
const TEST2_SIZE uint32 = 100
const TEST_VARLEN_SIZE uint32 = 10

func GenNumericValues(col_meta *ColumnInsertMeta, count uint32) []types.Value {
	var values []types.Value
	if col_meta.Dist_ == DistSerial {
		for i := 0; i < int(count); i++ {
			values = append(values, types.NewInteger(col_meta.Serial_counter_))
			col_meta.Serial_counter_ += 1
		}
		return values
	}

	seed := time.Now().UnixNano()
	rand.Seed(seed)
	for i := 0; i < int(count); i++ {
		values = append(values, types.NewInteger(rand.Int31n(col_meta.Max_)))
	}
	return values
}

func GenNumericValuesFloat(col_meta *ColumnInsertMeta, count uint32) []types.Value {
	var values []types.Value
	if col_meta.Dist_ == DistSerial {
		for i := 0; i < int(count); i++ {
			values = append(values, types.NewInteger(col_meta.Serial_counter_))
			col_meta.Serial_counter_ += 1
		}
		return values
	}

	seed := time.Now().UnixNano()
	rand.Seed(seed)
	for i := 0; i < int(count); i++ {
		values = append(values, types.NewFloat(float32(rand.Int31n(col_meta.Max_))/float32(seed)))
	}
	return values
}

func MakeValues(col_meta *ColumnInsertMeta, count uint32) []types.Value {
	switch col_meta.Type_ {
	case types.Integer:
		return GenNumericValues(col_meta, count)
	case types.Float:
		return GenNumericValuesFloat(col_meta, count)
	default:
		panic("Not yet implemented")
	}
}

func FillTable(info *catalog.TableMetadata, table_meta *TableInsertMeta, txn *access.Transaction) {
	var num_inserted uint32 = 0
	var batch_size uint32 = 128
	for num_inserted < table_meta.Num_rows_ {
		var values [][]types.Value
		var num_values uint32 = uint32(math.Min(float64(batch_size), float64(table_meta.Num_rows_-num_inserted)))
		for _, col_meta := range table_meta.Col_meta_ {
			values = append(values, MakeValues(col_meta, num_values))
		}

		for i := 0; i < int(num_values); i++ {
			var entry []types.Value
			for idx := range table_meta.Col_meta_ {
				entry = append(entry, values[idx][i])
			}
			tuple_ := tuple.NewTupleFromSchema(entry, info.Schema())
			rid, err := info.Table().InsertTuple(tuple_, false, txn, info.OID())
			if rid == nil || err != nil {
				fmt.Printf("InsertTuple failed on FillTable rid = %v, err = %v", rid, err)
				panic("InsertTuple failed on FillTable!")
			}
			// insert entry to index
			for idx := range table_meta.Col_meta_ {
				index_ := info.GetIndex(idx)
				if index_ != nil {
					index_.InsertEntry(tuple_, *rid, txn)
				}
			}
			num_inserted++
		}
	}
}

func MakeComparisonExpression(lhs expression.Expression, rhs expression.Expression,
	comp_type expression.ComparisonType) expression.Expression {

	ret_exp := expression.NewComparison(lhs, rhs, comp_type, types.Boolean)
	return ret_exp
}

func MakeAggregateValueExpression(is_group_by_term bool, col_index uint32) expression.Expression {
	return expression.NewAggregateValueExpression(is_group_by_term, col_index, types.Integer)
}

func MakeConstantValueExpression(val *types.Value) expression.Expression {
	return expression.NewConstantValue(*val, val.ValueType())
}

func MakeOutputSchema(exprs []MakeSchemaMeta) *schema.Schema {
	var cols []*column.Column = make([]*column.Column, 0)
	for _, input := range exprs {
		cols = append(cols, column.NewColumn(input.Col_name_, input.Expr_.GetReturnType(), false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), input.Expr_))
	}
	return schema.NewSchema(cols)
}

func MakeOutputSchemaAgg(exprs []MakeSchemaMetaAgg) *schema.Schema {
	var cols []*column.Column = make([]*column.Column, 0)
	for _, input := range exprs {
		cols = append(cols, column.NewColumn(input.Col_name_, input.Expr_.GetReturnType(), false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), input.Expr_))
	}
	return schema.NewSchema(cols)
}

func GenerateTestTabls(c *catalog.Catalog, exec_ctx *executors.ExecutorContext,
	txn *access.Transaction) (*catalog.TableMetadata, *catalog.TableMetadata) {
	columnA := column.NewColumn("colA", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnB := column.NewColumn("colB", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnC := column.NewColumn("colC", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	columnD := column.NewColumn("colD", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB, columnC, columnD})
	tableMetadata1 := c.CreateTable("test_1", schema_, txn)

	column1 := column.NewColumn("col1", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	column2 := column.NewColumn("col2", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	column3 := column.NewColumn("col3", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	column4 := column.NewColumn("col3", types.Integer, false, index_constants.INDEX_KIND_INVALID, types.PageID(-1), nil)
	schema_ = schema.NewSchema([]*column.Column{column1, column2, column3, column4})
	tableMetadata2 := c.CreateTable("test_2", schema_, txn)

	tableMeta1 := &TableInsertMeta{"test_1",
		TEST1_SIZE,
		[]*ColumnInsertMeta{
			{"colA", types.Integer, false, DistSerial, 0, 0, 0},
			{"colB", types.Integer, false, DistUniform, 0, 9, 0},
			{"colC", types.Integer, false, DistUniform, 0, 9999, 0},
			{"colD", types.Integer, false, DistUniform, 0, 99999, 0},
		}}
	tableMeta2 := &TableInsertMeta{"test_2",
		TEST2_SIZE,
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
