package parser

import (
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
)

type BinaryOpExpType int

const (
	Compare BinaryOpExpType = iota
	Logical
	IsNull
	ColumnName
	Constant
)

type BinaryOpExpression struct {
	LogicalOperationType_    expression.LogicalOpType
	ComparisonOperationType_ expression.ComparisonType
	Left_                    interface{}
	Right_                   interface{}
}

func (expr *BinaryOpExpression) GetType() BinaryOpExpType {
	isValue := func(v interface{}) bool {
		switch v.(type) {
		case *types.Value:
			return true
		default:
			return false
		}
	}

	if expr.ComparisonOperationType_ != -1 {
		if expr.Right_ != nil && isValue(expr.Right_) && expr.Right_.(types.Value).IsNull() {
			return IsNull
		} else {
			return Compare
		}
	} else if expr.LogicalOperationType_ != -1 {
		return Logical
	} else {
		panic("BinaryOpExpression tree is broken")
		//return ColumnNameOrConstant
	}
}

func (expr *BinaryOpExpression) TouchedColumns() mapset.Set[string] {
	// TODO: (SDB) [OPT] not implemented yet (BinaryOpExpression::TouchedColumns)
	ret := mapset.NewSet[string]()
	switch expr.GetType() {
	case Compare:
		if samehada_util.IsColumnName(expr.Left_) {
			ret.Add(*expr.Left_.(*string))
		}
		if samehada_util.IsColumnName(expr.Right_) {
			ret.Add(*expr.Right_.(*string))
		}
	case Logical:
		ret = ret.Union(expr.Left_.(*BinaryOpExpression).TouchedColumns())
		ret = ret.Union(expr.Right_.(*BinaryOpExpression).TouchedColumns())
	case IsNull:
		if samehada_util.IsColumnName(expr.Left_) {
			ret.Add(*expr.Left_.(*string))
		}
	default:
		panic("BinaryOpExpression tree is broken")
	}
	return ret
}

type SetExpression struct {
	ColName_     *string
	UpdateValue_ *types.Value
}

type ColDefExpression struct {
	ColName_ *string
	ColType_ *types.TypeID
}

type IndexDefExpression struct {
	IndexName_ *string
	Colnames_  []*string
}

type SelectFieldExpression struct {
	IsAgg_     bool
	AggType_   plans.AggregationType
	TableName_ *string // if specified
	ColName_   *string
}

func (sf *SelectFieldExpression) TouchedColumns() mapset.Set[string] {
	// TODO: (SDB) need to support aggregation function
	ret := mapset.NewSet[string]()
	colName := *sf.ColName_
	if sf.TableName_ != nil {
		colName = *sf.TableName_ + "." + *sf.ColName_
	}
	ret.Add(colName)
	return ret
}

type OrderByExpression struct {
	IsDesc_  bool
	ColName_ *string
}

func ConvParsedBinaryOpExprToExpIFOne(convSrc *BinaryOpExpression) expression.Expression {
	// TODO: (SDB) [OPT] not implemented yet (ConvParsedBinaryOpExprToExpIFOne)
	return nil
}

func ConvParsedSelectionExprToSchema(convSrc []*SelectFieldExpression) *schema.Schema {
	// TODO: (SDB) [OPT] not implemented yet (ConvParsedSelectionExprToSchema)
	return nil
}

func ConvColumnStrsToExpIfOnes(convSrc []*string) []expression.Expression {
	// TODO: (SDB) [OPT] not implemented yet (ConvColumnStrsToExpIfOnes)
	return nil
}
