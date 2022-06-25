package planner

import (
	"errors"
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/executors"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"strings"
)

type SimplePlanner struct {
	qi       *parser.QueryInfo
	catalog_ *catalog.Catalog
	bpm      *buffer.BufferPoolManager
	txn      *access.Transaction
}

func NewSimplePlanner(c *catalog.Catalog, bpm *buffer.BufferPoolManager) *SimplePlanner {
	return &SimplePlanner{nil, c, bpm, nil}
}

func (pner *SimplePlanner) MakePlan(qi *parser.QueryInfo, txn *access.Transaction) (error, plans.Plan) {
	pner.qi = qi
	pner.txn = txn

	switch *pner.qi.QueryType_ {
	case parser.SELECT:
		return pner.MakeSelectPlan()
	case parser.CREATE_TABLE:
		return pner.MakeCreateTablePlan()
	case parser.INSERT:
		return pner.MakeInsertPlan()
	case parser.DELETE:
		return pner.MakeDeletePlan()
	case parser.UPDATE:
		return pner.MakeUpdatePlan()
	default:
		panic("unknown query type")
	}
}

func (pner *SimplePlanner) MakeSelectPlanWithoutJoin() (error, plans.Plan) {
	//columns := []*column.Column{}
	//for _, c := range testCase.Columns {
	//	columns = append(columns, column.NewColumn(c.Name, c.Kind, false, nil))
	//}
	//outSchema := schema.NewSchema(columns)
	//
	//tmpColVal_ := expression.NewColumnValue(0, testCase.TableMetadata.Schema().GetColIndex(testCase.OnPredicate.LeftColumn), GetValueType(testCase.OnPredicate.RightColumn))
	//tmpColVal := tmpColVal_.(*expression.ColumnValue)
	//expression := expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(testCase.OnPredicate.RightColumn), GetValueType(testCase.OnPredicate.RightColumn)), testCase.OnPredicate.Operator, types.Boolean)
	//seqPlan := plans.NewSeqScanPlanNode(outSchema, expression, testCase.TableMetadata.OID())

	tblName := *pner.qi.JoinTables_[0]
	tableMetadata := pner.catalog_.GetTableByName(tblName)

	tgtTblSchema := tableMetadata.Schema()
	tgtTblColumns := tgtTblSchema.GetColumns()

	outColDefs := make([]*column.Column, 0)
	var outSchema *schema.Schema = nil
	if !(len(pner.qi.SelectFields_) == 1 && *pner.qi.SelectFields_[0].ColName_ == "*") {
		// column existance check
		for _, sfield := range pner.qi.SelectFields_ {
			colName := sfield.ColName_
			isOk := false
			for _, existCol := range tgtTblColumns {
				if existCol.GetColumnName() == *colName {
					isOk = true
					outColDefs = append(outColDefs, column.NewColumn(*colName, existCol.GetType(), false, existCol.GetExpr()))
					break
				}
			}
			if !isOk {
				return PrintAndCreateError("column " + *colName + " does not exist.")
			}
		}
		// Attention: this method call modifies passed Column objects
		outSchema = schema.NewSchema(outColDefs)
	} else {
		outSchema = tgtTblSchema
	}

	expression := pner.ConstructPredicate([]*schema.Schema{tgtTblSchema})

	return nil, plans.NewSeqScanPlanNode(outSchema, expression, tableMetadata.OID())
}

func (pner *SimplePlanner) MakeSelectPlanWithJoin() (error, plans.Plan) {
	//var join_plan *plans.HashJoinPlanNode
	//var out_final *schema.Schema
	//{
	//	// colA and colB have a tuple index of 0 because they are the left side of the join
	//	//var allocated_exprs []*expression.ColumnValue
	//	colA := executors.MakeColumnValueExpression(out_schema1, 0, "colA")
	//	colA_c := column.NewColumn("colA", types.Integer, false, nil)
	//	colA_c.SetIsLeft(true)
	//	colB_c := column.NewColumn("colB", types.Integer, false, nil)
	//	colB_c.SetIsLeft(true)
	//	// col1 and col2 have a tuple index of 1 because they are the right side of the join
	//	col1 := executors.MakeColumnValueExpression(out_schema2, 1, "col1")
	//	col1_c := column.NewColumn("col1", types.Integer, false, nil)
	//	col1_c.SetIsLeft(false)
	//	col2_c := column.NewColumn("col2", types.Integer, false, nil)
	//	col2_c.SetIsLeft(false)
	//	var left_keys []expression.Expression
	//	left_keys = append(left_keys, colA)
	//	var right_keys []expression.Expression
	//	right_keys = append(right_keys, col1)
	//	predicate := executors.MakeComparisonExpression(colA, col1, expression.Equal)
	//	out_final = schema.NewSchema([]*column.Column{colA_c, colB_c, col1_c, col2_c})
	//	plans_ := []plans.Plan{scan_plan1, scan_plan2}
	//	join_plan = plans.NewHashJoinPlanNode(out_final, plans_, predicate,
	//		left_keys, right_keys)

	tblNameL := *pner.qi.JoinTables_[0]
	tableMetadataL := pner.catalog_.GetTableByName(tblNameL)
	tgtTblSchemaL := tableMetadataL.Schema()
	tgtTblColumnsL := tgtTblSchemaL.GetColumns()

	tblNameR := *pner.qi.JoinTables_[1]
	tableMetadataR := pner.catalog_.GetTableByName(tblNameR)
	tgtTblSchemaR := tableMetadataR.Schema()
	tgtTblColumnsR := tgtTblSchemaR.GetColumns()

	var outSchemaL *schema.Schema
	var scanPlanL plans.Plan
	{
		var columns []*column.Column = make([]*column.Column, 0)
		for _, col := range tgtTblColumnsL {
			columns = append(columns, column.NewColumn(col.GetColumnName(), col.GetType(), false, col.GetExpr()))
		}
		outSchemaL = schema.NewSchema(columns)
		scanPlanL = plans.NewSeqScanPlanNode(outSchemaL, nil, tableMetadataL.OID())
	}

	var outSchemaR *schema.Schema
	var scanPlanR plans.Plan
	{
		var columns []*column.Column = make([]*column.Column, 0)
		for _, col := range tgtTblColumnsR {
			columns = append(columns, column.NewColumn(col.GetColumnName(), col.GetType(), false, col.GetExpr()))
		}
		outSchemaR = schema.NewSchema(columns)
		scanPlanR = plans.NewSeqScanPlanNode(outSchemaR, nil, tableMetadataR.OID())
	}

	var joinPlan *plans.HashJoinPlanNode
	//var outFinal *schema.Schema
	{
		finalOutCols := make([]*column.Column, 0)

		// new columns have tuple index of 0 because they are the left side of the join
		colValL := executors.MakeColumnValueExpression(outSchemaL, 0, strings.Split(*pner.qi.OnExpressions_.Left_.(*string), ".")[1])
		// new columns have tuple index of 1 because they are the right side of the join
		colValR := executors.MakeColumnValueExpression(outSchemaR, 1, strings.Split(*pner.qi.OnExpressions_.Right_.(*string), ".")[1])

		if !(len(pner.qi.SelectFields_) == 1 && *pner.qi.SelectFields_[0].ColName_ == "*") {
			// column existance check
			for _, sfield := range pner.qi.SelectFields_ {
				colName := sfield.ColName_
				tblName := sfield.TableName_

				if *tblName != tblNameL && *tblName != tblNameR {
					return PrintAndCreateError("specified selection " + *tblName + "." + *colName + " is invalid.")
				}

				if pner.catalog_.GetTableByName(*tblName) == nil {
					return PrintAndCreateError("table " + *tblName + " does not exist.")
				}

				tmpSchema := pner.catalog_.GetTableByName(*tblName).Schema()
				colIdx := tmpSchema.GetColIndex(*colName)
				if colIdx == math.MaxUint32 {
					return PrintAndCreateError("column " + *colName + " does not exist on " + *tblName + ".")
				}

				colDef := tmpSchema.GetColumn(colIdx)
				col := column.NewColumn(*tblName+"."+*colName, colDef.GetType(), false, colDef.GetExpr())
				if *tblName == tblNameL {
					col.SetIsLeft(true)
				} else { // Right
					col.SetIsLeft(false)
				}
				// Attention: this method call modifies passed Column objects
				finalOutCols = append(finalOutCols, col)
			}
		} else {
			for _, colDef := range tgtTblColumnsL {
				col := column.NewColumn(tblNameL+"."+colDef.GetColumnName(), colDef.GetType(), false, colDef.GetExpr())
				col.SetIsLeft(true)
				finalOutCols = append(finalOutCols, col)
			}
			for _, colDef := range tgtTblColumnsR {
				col := column.NewColumn(tblNameR+"."+colDef.GetColumnName(), colDef.GetType(), false, colDef.GetExpr())
				col.SetIsLeft(false)
				finalOutCols = append(finalOutCols, col)
			}
		}

		outFinal := schema.NewSchema(finalOutCols)

		onPredicate := executors.MakeComparisonExpression(colValL, colValR, expression.Equal)

		var left_keys []expression.Expression
		left_keys = append(left_keys, colValL)
		var right_keys []expression.Expression
		right_keys = append(right_keys, colValR)

		scanPlans_ := []plans.Plan{scanPlanL, scanPlanR}
		joinPlan = plans.NewHashJoinPlanNode(outFinal, scanPlans_, onPredicate,
			left_keys, right_keys)
	}

	if pner.qi.WhereExpression_.Left_ != nil && pner.qi.WhereExpression_.Left_ != nil {
		whereExp := pner.ConstructPredicate([]*schema.Schema{tgtTblSchemaL, tgtTblSchemaR})
		// filter joined recoreds with predicate which is specified on WHERE clause if needed
		filterPlan := plans.NewFilterPlanNode(joinPlan, whereExp)
		return nil, filterPlan
	} else {
		return nil, joinPlan
	}
}

func (pner *SimplePlanner) MakeSelectPlan() (error, plans.Plan) {
	if len(pner.qi.JoinTables_) == 1 {
		return pner.MakeSelectPlanWithoutJoin()
	} else {
		return pner.MakeSelectPlanWithJoin()
	}
}

func processPredicateTreeNode(node *parser.BinaryOpExpression, tgtTblSchemas []*schema.Schema) expression.Expression {
	if node.LogicalOperationType_ != -1 { // node of logical operation
		left_side_pred := processPredicateTreeNode(node.Left_.(*parser.BinaryOpExpression), tgtTblSchemas)
		right_side_pred := processPredicateTreeNode(node.Right_.(*parser.BinaryOpExpression), tgtTblSchemas)
		return expression.NewLogicalOp(left_side_pred, right_side_pred, node.LogicalOperationType_, types.Boolean)
	} else { // node of conpare operation
		// TODO: (SDB) need to validate specified table name prefix, column name and literal (processPredicateTreeNode of SimplePlanner)
		colName := *node.Left_.(*string)
		tmpColIdx := tgtTblSchemas[0].GetColIndex(colName)
		tmpColVal := expression.NewColumnValue(0, tmpColIdx, node.Right_.(*types.Value).ValueType())
		constVal := expression.NewConstantValue(*node.Right_.(*types.Value), node.Right_.(*types.Value).ValueType())

		return expression.NewComparison(tmpColVal, constVal, node.ComparisonOperationType_, types.Boolean)
	}
}

func (pner *SimplePlanner) ConstructPredicate(tgtTblSchemas []*schema.Schema) expression.Expression {
	return processPredicateTreeNode(pner.qi.WhereExpression_, tgtTblSchemas)
}

func (pner *SimplePlanner) MakeCreateTablePlan() (error, plans.Plan) {
	if pner.catalog_.GetTableByName(*pner.qi.NewTable_) != nil {
		return PrintAndCreateError("already " + *pner.qi.NewTable_ + " exists.")
	}

	columns := make([]*column.Column, 0)
	for _, cdefExp := range pner.qi.ColDefExpressions_ {
		columns = append(columns, column.NewColumn(*cdefExp.ColName_, *cdefExp.ColType_, false, nil))
	}
	schema_ := schema.NewSchema(columns)

	pner.catalog_.CreateTable(*pner.qi.NewTable_, schema_, pner.txn)

	return nil, nil
}

func PrintAndCreateError(msg string) (error, plans.Plan) {
	fmt.Println(msg)
	return errors.New(msg), nil
}

func (pner *SimplePlanner) MakeInsertPlan() (error, plans.Plan) {
	tableMetadata := pner.catalog_.GetTableByName(*pner.qi.JoinTables_[0])
	if tableMetadata == nil {
		return PrintAndCreateError("table " + *pner.qi.JoinTables_[0] + " not found.")
	}

	schema_ := tableMetadata.Schema()
	tgtColNum := len(pner.qi.TargetCols_)
	insRows := make([][]types.Value, 0)
	insertRowCnt := 0
	valCnt := 0
	row := make([]types.Value, 0)
	for idx, colName := range pner.qi.TargetCols_ {
		val := pner.qi.Values_[idx-(tgtColNum*insertRowCnt)]
		if schema_.GetColIndex(*colName) == math.MaxUint32 {
			return PrintAndCreateError("data type of " + *colName + " is wrong.")
		}
		valType := schema_.GetColumn(schema_.GetColIndex(*colName)).GetType()
		if val.ValueType() != valType {
			return PrintAndCreateError("data type of " + *colName + " is wrong.")
		}
		row = append(row, *val)
		valCnt++
		if valCnt == tgtColNum {
			// to next record
			insRows = append(insRows, row)
			valCnt = 0
			insertRowCnt++
			row = make([]types.Value, 0)
		}
	}

	return nil, plans.NewInsertPlanNode(insRows, tableMetadata.OID())
}

// TODO: (SDB) need to implement MakeDeletePlan method
func (pner *SimplePlanner) MakeDeletePlan() (error, plans.Plan) {
	return nil, nil
}

func (pner *SimplePlanner) MakeUpdatePlan() (error, plans.Plan) {
	return nil, nil
}
