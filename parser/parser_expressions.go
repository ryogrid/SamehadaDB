package parser

import (
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
	"strings"
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
		if expr.Right_ != nil && isValue(expr.Right_) && expr.Right_.(*types.Value).IsNull() {
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
	// note: alphabets on column and table name is stored in lowercase
	ret := mapset.NewSet[string]()
	switch expr.GetType() {
	case Compare:
		if samehada_util.IsColumnName(expr.Left_) {
			ret.Add(strings.ToLower(*expr.Left_.(*string)))
		}
		if samehada_util.IsColumnName(expr.Right_) {
			ret.Add(strings.ToLower(*expr.Right_.(*string)))
		}
	case Logical:
		ret = ret.Union(expr.Left_.(*BinaryOpExpression).TouchedColumns())
		ret = ret.Union(expr.Right_.(*BinaryOpExpression).TouchedColumns())
	case IsNull:
		if samehada_util.IsColumnName(expr.Left_) {
			ret.Add(strings.ToLower(*expr.Left_.(*string)))
		}
	default:
		panic("BinaryOpExpression tree is broken")
	}
	return ret
}

func (expr *BinaryOpExpression) GetDeepCopy() *BinaryOpExpression {
	ret := &BinaryOpExpression{}
	ret.LogicalOperationType_ = expr.LogicalOperationType_
	ret.ComparisonOperationType_ = expr.ComparisonOperationType_
	if expr.Left_ == nil {
		ret.Left_ = nil
	} else {
		switch expr.Left_.(type) {
		case *string:
			tmpStr := *expr.Left_.(*string)
			ret.Left_ = &tmpStr
		case *types.Value:
			ret.Left_ = expr.Left_.(*types.Value).GetDeepCopy()
		case *BinaryOpExpression:
			ret.Left_ = expr.Left_.(*BinaryOpExpression).GetDeepCopy()
		default:
			panic("BinaryOpExpression tree is broken")
		}
	}
	if expr.Right_ == nil {
		ret.Right_ = nil
	} else {
		switch expr.Right_.(type) {
		case *string:
			tmpStr := *expr.Right_.(*string)
			ret.Right_ = &tmpStr
		case *types.Value:
			ret.Right_ = expr.Right_.(*types.Value).GetDeepCopy()
		case *BinaryOpExpression:
			ret.Right_ = expr.Right_.(*BinaryOpExpression).GetDeepCopy()
		default:
			panic("BinaryOpExpression tree is broken")
		}
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
	// note: alphabets on table and column name is stored in lowercase

	// TODO: (SDB) need to support aggregation function
	ret := mapset.NewSet[string]()
	colName := *sf.ColName_
	if sf.TableName_ != nil {
		colName = *sf.TableName_ + "." + *sf.ColName_
	}
	ret.Add(strings.ToLower(colName))
	return ret
}

type OrderByExpression struct {
	IsDesc_  bool
	ColName_ *string
}

// attiontion: this func can be used only for predicate of SelectionPlanNode
func ConvBinaryOpExpReafToExpIFOne(sc *schema.Schema, convSrc interface{}) expression.Expression {
	switch convSrc.(type) {
	case *string:
		return expression.NewColumnValue(0, sc.GetColIndex(*convSrc.(*string)), sc.GetColumn(sc.GetColIndex(*convSrc.(*string))).GetType())
	case *types.Value:
		return expression.NewConstantValue(*convSrc.(*types.Value), convSrc.(*types.Value).ValueType())
	default:
		panic("BinaryOpExpression tree is broken")
	}
}

// attiontion: this func can be used only for predicate of SelectionPlanNode
func ConvParsedBinaryOpExprToExpIFOne(sc *schema.Schema, convSrc *BinaryOpExpression) expression.Expression {
	switch convSrc.GetType() {
	case Logical: // node of logical operation
		left_side_pred := ConvParsedBinaryOpExprToExpIFOne(sc, convSrc.Left_.(*BinaryOpExpression))
		right_side_pred := ConvParsedBinaryOpExprToExpIFOne(sc, convSrc.Right_.(*BinaryOpExpression))
		return expression.NewLogicalOp(left_side_pred, right_side_pred, convSrc.LogicalOperationType_, types.Boolean)
	case Compare: // node of compare operation
		leftExp := ConvBinaryOpExpReafToExpIFOne(sc, convSrc.Left_)
		rightExp := ConvBinaryOpExpReafToExpIFOne(sc, convSrc.Right_)

		return expression.NewComparison(leftExp, rightExp, convSrc.ComparisonOperationType_, types.Boolean)
	case IsNull: // node of is null operation
		tmpColIdx := sc.GetColIndex(*convSrc.Left_.(*string))
		return expression.NewComparison(
			expression.NewColumnValue(0, tmpColIdx, sc.GetColumn(tmpColIdx).GetType()),
			expression.NewConstantValue(*convSrc.Right_.(*types.Value).GetDeepCopy(), convSrc.Right_.(*types.Value).ValueType()),
			convSrc.ComparisonOperationType_,
			types.Boolean)
	default:
		panic("BinaryOpExpression tree is " +
			"broken")
	}
}

// TODO: (SDB) need to support aggregation function on select field
func ConvParsedSelectionExprToSchema(c *catalog.Catalog, convSrc []*SelectFieldExpression) *schema.Schema {
	outColDefs := make([]*column.Column, 0)
	for _, sfield := range convSrc {
		tableName := sfield.TableName_
		colName := sfield.ColName_
		sc := c.GetTableByName(*tableName).Schema()
		colIdx := sc.GetColIndex(*colName)
		colType := sc.GetColumn(colIdx).GetType()
		hasIndex := sc.GetColumn(colIdx).HasIndex()
		indexKind := index_constants.INDEX_KIND_INVALID
		indexHeaderPageID := types.PageID(-1)
		if hasIndex {
			indexKind = sc.GetColumn(colIdx).IndexKind()
			indexHeaderPageID = sc.GetColumn(colIdx).IndexHeaderPageId()
		}

		outColDefs = append(outColDefs, column.NewColumn(*colName, colType, hasIndex, indexKind, indexHeaderPageID, nil))
	}
	return schema.NewSchema(outColDefs)
}

func ConvColumnStrsToExpIfOnes(c *catalog.Catalog, convSrc []*string, isLeftOnJoin bool) []expression.Expression {
	ret := make([]expression.Expression, 0)
	for _, colStr := range convSrc {
		samehada_util.SHAssert(strings.Contains(*colStr, "."), "column name must includes table name as prefix!")
		splited := strings.Split(*colStr, ".")
		tableName := splited[0]
		colName := splited[1]
		sc := c.GetTableByName(tableName).Schema()
		colIdx := sc.GetColIndex(colName)
		colType := sc.GetColumn(colIdx).GetType()

		if isLeftOnJoin {
			ret = append(ret, expression.NewColumnValue(0, colIdx, colType))
		} else {
			ret = append(ret, expression.NewColumnValue(1, colIdx, colType))
		}
	}

	return ret
}
