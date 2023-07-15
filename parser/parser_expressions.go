package parser

import (
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
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
		return ColumnName
	}
}

func (expr *BinaryOpExpression) TouchedColumns() mapset.Set[*column.Column] {
	// TODO: (SDB) not implemented yet (BinaryOpExpression::TouchedColumns)
	return nil
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

func (sf *SelectFieldExpression) TouchedColumns() mapset.Set[*column.Column] {
	// TODO: (SDB) not implemented yet (SeelectFieldExpression::SelectFieldExpression)
	return nil
}

type OrderByExpression struct {
	IsDesc_  bool
	ColName_ *string
}

func ConvParsedSelectFieldExpToOutputSchema(selection []*SelectFieldExpression) *schema.Schema {
	// TODO: (SDB) not implemented yet
	return nil
}

func ConvParsedBinaryOpExprToExpIFOne(convSrc *BinaryOpExpression) expression.Expression {
	// TODO: (SDB) not implemented yet (ConvParsedBinaryOpExprToExpIFOne)
	return nil
}

func ConvParsedSelectionExprToExpIFOne(convSrc []*SelectFieldExpression) expression.Expression {
	// TODO: (SDB) not implemented yet (ConvParsedSelectionExprToExpIFOne)
	return nil
}
