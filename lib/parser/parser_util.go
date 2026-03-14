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
	CreateTable
	INSERT
	DELETE
	UPDATE
)

func ValueExprToValue(expr *driver.ValueExpr) *types.Value {
	switch expr.Datum.Kind() {
	case ptypes.KindInt64, ptypes.KindUint64:
		valStr := expr.String()
		istr := strings.Split(valStr, " ")[1]
		ival, _ := strconv.Atoi(istr)
		ret := types.NewInteger(int32(ival))
		return &ret
	case ptypes.KindMysqlDecimal:
		valStr := expr.String()
		fstr := strings.Split(valStr, " ")[1]
		fval, _ := strconv.ParseFloat(fstr, 32)
		ret := types.NewFloat(float32(fval))
		return &ret
	default: // varchar
		valStr := expr.String()
		splited := strings.Split(valStr, " ")
		targetStr := strings.Join(splited[1:], " ")
		ret := types.NewVarchar(targetStr)
		return &ret
	}
}

func GetPredicateExprFromStr(sc *schema.Schema, pred *string) expression.Expression {
	sqlStr := "SELECT * FROM dummy WHERE " + *pred + ";"
	qi, _ := ProcessSQLStr(&sqlStr)
	return ConvParsedBinaryOpExprToExpIFOne(sc, qi.WhereExpression)
}
