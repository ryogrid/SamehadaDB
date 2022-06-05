package parser

import (
	driver "github.com/pingcap/tidb/types/parser_driver"
	"github.com/ryogrid/SamehadaDB/types"
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

type QueryInfo struct {
	QueryType_         *QueryType
	SelectFields_      []*string
	SetExpressions_    []*SetExpression
	NewTable_          *string   // for CREATE TABLE
	TargetTable_       *string   // for INSERT, UPDATE
	TargetCols_        []*string // for INSERT
	ColDefExpressions_ []*ColDefExpression
	Values_            []*types.Value // for INSERT
	OnExpressions_     *BinaryOpExpression
	FromTable_         *string // for SELECT, DELETE
	JoinTable_         *string
	//WhereExpressions_  []*ComparisonExpression
	WhereExpression_ *BinaryOpExpression
}

func ValueExprToValue(expr *driver.ValueExpr) *types.Value {
	switch expr.Datum.Kind() {
	case 1:
		val_str := expr.String()
		istr := strings.Split(val_str, " ")[1]
		ival, _ := strconv.Atoi(istr)
		ret := types.NewInteger(int32(ival))
		return &ret
	case 8:
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
