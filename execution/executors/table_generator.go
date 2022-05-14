package executors

import (
	"fmt"
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
	name_ string
	/**
	 * Type of the column
	 */
	type_ types.TypeID
	/**
	 * Whether the column is nullable
	 */
	nullable_ bool
	/**
	 * Distribution of values
	 */
	dist_ int32
	/**
	 * Min value of the column
	 */
	min_ int32
	/**
	 * Max value of the column
	 */
	max_ int32
	/**
	 * Counter to generate serial data
	 */
	serial_counter_ int32
}

type TableInsertMeta struct {
	/**
	 * Name of the table
	 */
	name_ string
	/**
	 * Number of rows
	 */
	num_rows_ uint32
	/**
	 * Columns
	 */
	col_meta_ []*ColumnInsertMeta
}

type MakeSchemaMeta struct {
	col_name_ string
	expr_     expression.ColumnValue
}

type MakeSchemaMetaAgg struct {
	col_name_ string
	expr_     expression.AggregateValueExpression
}

const DistSerial int32 = 0
const DistUniform int32 = 1

// TODO: (SDB) when TEST1_SIZE is 1000, TestTestTableGenerator testcase and TestSimpleAggregation testcase fails.
//             maybe, TestTableGeneration func or tuple insertion logic of TableHeap has some bugs...
const TEST1_SIZE uint32 = 1000 //100 //20 //1000
const TEST2_SIZE uint32 = 100
const TEST_VARLEN_SIZE uint32 = 10

func GenNumericValues(col_meta *ColumnInsertMeta, count uint32) []types.Value {
	var values []types.Value
	if col_meta.dist_ == DistSerial {
		for i := 0; i < int(count); i++ {
			values = append(values, types.NewInteger(col_meta.serial_counter_))
			col_meta.serial_counter_ += 1
		}
		return values
	}

	seed := time.Now().UnixNano()
	rand.Seed(seed)
	for i := 0; i < int(count); i++ {
		values = append(values, types.NewInteger(rand.Int31n(col_meta.max_)))
	}
	return values
}

func GenNumericValuesFloat(col_meta *ColumnInsertMeta, count uint32) []types.Value {
	var values []types.Value
	if col_meta.dist_ == DistSerial {
		for i := 0; i < int(count); i++ {
			values = append(values, types.NewInteger(col_meta.serial_counter_))
			col_meta.serial_counter_ += 1
		}
		return values
	}

	seed := time.Now().UnixNano()
	rand.Seed(seed)
	for i := 0; i < int(count); i++ {
		values = append(values, types.NewFloat(float32(rand.Int31n(col_meta.max_))/float32(seed)))
	}
	return values
}

func MakeValues(col_meta *ColumnInsertMeta, count uint32) []types.Value {
	//var values []types.Value
	switch col_meta.type_ {
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
	for num_inserted < table_meta.num_rows_ {
		var values [][]types.Value
		var num_values uint32 = uint32(math.Min(float64(batch_size), float64(table_meta.num_rows_-num_inserted)))
		for _, col_meta := range table_meta.col_meta_ {
			values = append(values, MakeValues(col_meta, num_values))
		}

		for i := 0; i < int(num_values); i++ {
			var entry []types.Value
			for idx := range table_meta.col_meta_ {
				entry = append(entry, values[idx][i])
			}
			rid, err := info.Table().InsertTuple(tuple.NewTupleFromSchema(entry, info.Schema()), txn)
			if rid == nil || err != nil {
				fmt.Printf("InsertTuple failed on FillTable rid = %v, err = %v", rid, err)
				panic("InsertTuple failed on FillTable!")
			}
			num_inserted++
		}
	}
	//fmt.Printf("num_inserted %d\n", num_inserted)
}

func MakeColumnValueExpression(schema_ *schema.Schema, tuple_idx uint32,
	col_name string) expression.Expression {
	col_idx := schema_.GetColIndex(col_name)
	col_type := schema_.GetColumn(col_idx).GetType()
	col_val := expression.NewColumnValue(tuple_idx, col_idx, col_type)
	return col_val
}

func MakeComparisonExpression(lhs expression.Expression, rhs expression.Expression,
	comp_type expression.ComparisonType) expression.Expression {
	//casted_lhs := lhs.(*expression.ColumnValue)
	//ret_exp := expression.NewComparison(casted_lhs, rhs, comp_type, types.Boolean)

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
		cols = append(cols, column.NewColumn(input.col_name_, input.expr_.GetReturnType(), false, input.expr_))
		// if input.expr_.GetReturnType() != types.Varchar {
		// 	cols = append(cols, *column.NewColumn(input.col_name_, input.expr_.GetReturnType(), false, input.expr_))
		// } else {
		// 	cols = append(cols, column.NewColumn(input[0], input[1].GetReturnType(), MAX_VARCHAR_SIZE, input[1]))
		// }
	}
	return schema.NewSchema(cols)
}

func MakeOutputSchemaAgg(exprs []MakeSchemaMetaAgg) *schema.Schema {
	var cols []*column.Column = make([]*column.Column, 0)
	for _, input := range exprs {
		cols = append(cols, column.NewColumn(input.col_name_, input.expr_.GetReturnType(), false, input.expr_))
	}
	return schema.NewSchema(cols)
}

func GenerateTestTabls(c *catalog.Catalog, exec_ctx *ExecutorContext,
	txn *access.Transaction) (*catalog.TableMetadata, *catalog.TableMetadata) {
	columnA := column.NewColumn("colA", types.Integer, false, nil)
	columnB := column.NewColumn("colB", types.Integer, false, nil)
	columnC := column.NewColumn("colC", types.Integer, false, nil)
	columnD := column.NewColumn("colD", types.Integer, false, nil)
	schema_ := schema.NewSchema([]*column.Column{columnA, columnB, columnC, columnD})
	//columnA.SetExpr(MakeColumnValueExpression(schema_, 0, "colA").(*expression.ColumnValue))
	tableMetadata1 := c.CreateTable("test_1", schema_, txn)

	column1 := column.NewColumn("col1", types.Integer, false, nil)
	column2 := column.NewColumn("col2", types.Integer, false, nil)
	column3 := column.NewColumn("col3", types.Integer, false, nil)
	column4 := column.NewColumn("col3", types.Integer, false, nil)
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
