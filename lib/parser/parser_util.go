package parser

import (
	ptypes "github.com/pingcap/tidb/types"
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"strconv"
	"strings"
)

type QueryType int32

const (
	SELECT QueryType = iota
	CREATE_TABLE
	INSERT
	DELETE
	UPDATE
)

func ValueExprToValue(expr *driver.ValueExpr) *types.Value {
	switch expr.Datum.Kind() {
	case ptypes.KindInt64, ptypes.KindUint64:
		val_str := expr.String()
		istr := strings.Split(val_str, " ")[1]
		ival, _ := strconv.Atoi(istr)
		ret := types.NewInteger(int32(ival))
		return &ret
	case ptypes.KindMysqlDecimal:
		val_str := expr.String()
		fstr := strings.Split(val_str, " ")[1]
		fval, _ := strconv.ParseFloat(fstr, 32)
		ret := types.NewFloat(float32(fval))
		return &ret
	default:
		val_str := expr.String()
		target_str := strings.Split(val_str, " ")[1]
		ret := types.NewVarchar(target_str)
		return &ret
	}
}

func GetPredicateExprFromStr(schema_ *schema.Schema, pred *string) expression.Expression {
	sqlStr := "SELECT * FROM dummy WHERE " + *pred + ";"
	qi := ProcessSQLStr(&sqlStr)
	return ConvParsedBinaryOpExprToExpIFOne(schema_, qi.WhereExpression_)
}
