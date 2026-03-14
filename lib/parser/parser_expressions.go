package parser

import (
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/types"
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
	LogicalOperationType    expression.LogicalOpType
	ComparisonOperationType expression.ComparisonType
	Left                    interface{}
	Right                   interface{}
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

	if expr.ComparisonOperationType != -1 {
		if expr.Right != nil && isValue(expr.Right) && expr.Right.(*types.Value).IsNull() {
			return IsNull
		} else {
			return Compare
		}
	} else if expr.LogicalOperationType != -1 {
		return Logical
	} else {
		panic("BinaryOpExpression tree is broken")
	}
}

func (expr *BinaryOpExpression) TouchedColumns() mapset.Set[string] {
	// note: alphabets on column and table name is stored in lowercase
	ret := mapset.NewSet[string]()
	switch expr.GetType() {
	case Compare:
		if samehada_util.IsColumnName(expr.Left) {
			ret.Add(strings.ToLower(*expr.Left.(*string)))
		}
		if samehada_util.IsColumnName(expr.Right) {
			ret.Add(strings.ToLower(*expr.Right.(*string)))
		}
	case Logical:
		ret = ret.Union(expr.Left.(*BinaryOpExpression).TouchedColumns())
		ret = ret.Union(expr.Right.(*BinaryOpExpression).TouchedColumns())
	case IsNull:
		if samehada_util.IsColumnName(expr.Left) {
			ret.Add(strings.ToLower(*expr.Left.(*string)))
		}
	default:
		panic("BinaryOpExpression tree is broken")
	}
	return ret
}

func (expr *BinaryOpExpression) GetDeepCopy() *BinaryOpExpression {
	ret := &BinaryOpExpression{}
	ret.LogicalOperationType = expr.LogicalOperationType
	ret.ComparisonOperationType = expr.ComparisonOperationType
	if expr.Left == nil {
		ret.Left = nil
	} else {
		switch expr.Left.(type) {
		case *string:
			tmpStr := *expr.Left.(*string)
			ret.Left = &tmpStr
		case *types.Value:
			ret.Left = expr.Left.(*types.Value).GetDeepCopy()
		case *BinaryOpExpression:
			ret.Left = expr.Left.(*BinaryOpExpression).GetDeepCopy()
		default:
			panic("BinaryOpExpression tree is broken")
		}
	}
	if expr.Right == nil {
		ret.Right = nil
	} else {
		switch expr.Right.(type) {
		case *string:
			tmpStr := *expr.Right.(*string)
			ret.Right = &tmpStr
		case *types.Value:
			ret.Right = expr.Right.(*types.Value).GetDeepCopy()
		case *BinaryOpExpression:
			ret.Right = expr.Right.(*BinaryOpExpression).GetDeepCopy()
		default:
			panic("BinaryOpExpression tree is broken")
		}
	}
	return ret
}

func (expr *BinaryOpExpression) AppendBinaryOpExpWithAnd(expr2 *BinaryOpExpression) *BinaryOpExpression {
	return &BinaryOpExpression{expression.AND, -1, expr, expr2}
}

type SetExpression struct {
	ColName     *string
	UpdateValue *types.Value
}

type ColDefExpression struct {
	ColName *string
	ColType *types.TypeID
}

type IndexDefExpression struct {
	IndexName *string
	Colnames  []*string
}

type SelectFieldExpression struct {
	IsAgg     bool
	AggType   plans.AggregationType
	TableName *string // if specified
	ColName   *string
}

func (sf *SelectFieldExpression) TouchedColumns() mapset.Set[string] {
	// note: alphabets on table and column name is stored in lowercase

	// TODO: (SDB) need to support aggregation function
	ret := mapset.NewSet[string]()
	colName := *sf.ColName
	if sf.TableName != nil {
		colName = *sf.TableName + "." + *sf.ColName
	}
	ret.Add(strings.ToLower(colName))
	return ret
}

type OrderByExpression struct {
	IsDesc  bool
	ColName *string
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
		leftSidePred := ConvParsedBinaryOpExprToExpIFOne(sc, convSrc.Left.(*BinaryOpExpression))
		rightSidePred := ConvParsedBinaryOpExprToExpIFOne(sc, convSrc.Right.(*BinaryOpExpression))
		return expression.NewLogicalOp(leftSidePred, rightSidePred, convSrc.LogicalOperationType, types.Boolean)
	case Compare: // node of compare operation
		leftExp := ConvBinaryOpExpReafToExpIFOne(sc, convSrc.Left)
		rightExp := ConvBinaryOpExpReafToExpIFOne(sc, convSrc.Right)

		return expression.NewComparison(leftExp, rightExp, convSrc.ComparisonOperationType, types.Boolean)
	case IsNull: // node of is null operation
		tmpColIdx := sc.GetColIndex(*convSrc.Left.(*string))
		return expression.NewComparison(
			expression.NewColumnValue(0, tmpColIdx, sc.GetColumn(tmpColIdx).GetType()),
			expression.NewConstantValue(*convSrc.Right.(*types.Value).GetDeepCopy(), convSrc.Right.(*types.Value).ValueType()),
			convSrc.ComparisonOperationType,
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
		tableName := sfield.TableName
		colName := sfield.ColName
		sc := c.GetTableByName(*tableName).Schema()
		colIdx := sc.GetColIndex(*colName)
		colType := sc.GetColumn(colIdx).GetType()
		hasIndex := sc.GetColumn(colIdx).HasIndex()
		indexKind := index_constants.IndexKindInvalid
		indexHeaderPageID := types.PageID(-1)
		if hasIndex {
			indexKind = sc.GetColumn(colIdx).IndexKind()
			indexHeaderPageID = sc.GetColumn(colIdx).IndexHeaderPageID()
		}

		outColDefs = append(outColDefs, column.NewColumn(*tableName+"."+*colName, colType, hasIndex, indexKind, indexHeaderPageID, nil))
	}
	return schema.NewSchema(outColDefs)
}

// childPlan can be nil
func ConvColumnStrsToExpIfOnes(c *catalog.Catalog, childPlan plans.Plan, convSrc []*string, isLeftOnJoin bool) []expression.Expression {
	ret := make([]expression.Expression, 0)
	for _, colStr := range convSrc {
		samehada_util.SHAssert(strings.Contains(*colStr, "."), "column name must includes table name as prefix!")
		splited := strings.Split(*colStr, ".")
		tableName := splited[0]
		var sc *schema.Schema
		if childPlan != nil {
			sc = childPlan.OutputSchema()
		} else {
			sc = c.GetTableByName(tableName).Schema()
		}
		colIdx := sc.GetColIndex(*colStr)
		colType := sc.GetColumn(colIdx).GetType()

		if isLeftOnJoin {
			ret = append(ret, expression.NewColumnValue(0, colIdx, colType))
		} else {
			ret = append(ret, expression.NewColumnValue(1, colIdx, colType))
		}
	}

	return ret
}
