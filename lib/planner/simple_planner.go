package planner

import (
	"errors"
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/expression"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/parser"
	"github.com/ryogrid/SamehadaDB/lib/planner/optimizer"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/testing/testing_tbl_gen"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"math"
	"strings"
)

type SimplePlanner struct {
	qi       *parser.QueryInfo
	cat *catalog.Catalog
	bpm      *buffer.BufferPoolManager
	txn      *access.Transaction
}

func NewSimplePlanner(c *catalog.Catalog, bpm *buffer.BufferPoolManager) *SimplePlanner {
	return &SimplePlanner{nil, c, bpm, nil}
}

func (pner *SimplePlanner) MakePlan(qi *parser.QueryInfo, txn *access.Transaction) (error, plans.Plan) {
	pner.qi = qi
	pner.txn = txn

	switch *pner.qi.QueryType {
	case parser.SELECT:
		return pner.MakeSelectPlan()
	case parser.CreateTable:
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
	tblName := *pner.qi.JoinTables_[0]
	tableMetadata := pner.cat.GetTableByName(tblName)

	tgtTblSchema := tableMetadata.Schema()
	tgtTblColumns := tgtTblSchema.GetColumns()
	hasWhere := pner.qi.WhereExpression.Left != nil && pner.qi.WhereExpression.Right != nil

	outColDefs := make([]*column.Column, 0)
	var outSchema *schema.Schema = nil
	if !(len(pner.qi.SelectFields) == 1 && *pner.qi.SelectFields[0].ColName == "*") {
		// column existance check
		for _, sfield := range pner.qi.SelectFields {
			colName := sfield.ColName
			isOk := false
			for _, existCol := range tgtTblColumns {
				if strings.Split(existCol.GetColumnName(), ".")[1] == *colName {
					isOk = true
					outColDefs = append(outColDefs, column.NewColumn(*colName, existCol.GetType(), false, index_constants.IndexKindInvalid, types.PageID(-1), existCol.GetExpr()))
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

	var predicate expression.Expression = nil
	if hasWhere {
		predicate = pner.ConstructPredicate([]*schema.Schema{tgtTblSchema})
	}

	return nil, plans.NewSeqScanPlanNode(pner.cat, outSchema, predicate, tableMetadata.OID())
}

func (pner *SimplePlanner) MakeOptimizedSelectPlanWithJoin() (error, plans.Plan) {
	optPlan, err := optimizer.NewSelingerOptimizer(pner.qi, pner.cat).Optimize()
	return err, optPlan
}

func (pner *SimplePlanner) MakeSelectPlanWithJoin() (error, plans.Plan) {
	tblNameL := *pner.qi.JoinTables_[0]
	tableMetadataL := pner.cat.GetTableByName(tblNameL)
	tgtTblSchemaL := tableMetadataL.Schema()
	tgtTblColumnsL := tgtTblSchemaL.GetColumns()

	tblNameR := *pner.qi.JoinTables_[1]
	tableMetadataR := pner.cat.GetTableByName(tblNameR)
	tgtTblSchemaR := tableMetadataR.Schema()
	tgtTblColumnsR := tgtTblSchemaR.GetColumns()

	hasWhere := pner.qi.WhereExpression.Left != nil && pner.qi.WhereExpression.Right != nil

	var outSchemaL *schema.Schema
	var scanPlanL plans.Plan
	{
		var columns = make([]*column.Column, 0)
		for _, col := range tgtTblColumnsL {
			columns = append(columns, column.NewColumn(col.GetColumnName(), col.GetType(), false, index_constants.IndexKindInvalid, types.PageID(-1), col.GetExpr()))
		}
		outSchemaL = schema.NewSchema(columns)
		scanPlanL = plans.NewSeqScanPlanNode(pner.cat, outSchemaL, nil, tableMetadataL.OID())
	}

	var outSchemaR *schema.Schema
	var scanPlanR plans.Plan
	{
		var columns = make([]*column.Column, 0)
		for _, col := range tgtTblColumnsR {
			columns = append(columns, column.NewColumn(col.GetColumnName(), col.GetType(), false, index_constants.IndexKindInvalid, types.PageID(-1), col.GetExpr()))
		}
		outSchemaR = schema.NewSchema(columns)
		scanPlanR = plans.NewSeqScanPlanNode(pner.cat, outSchemaR, nil, tableMetadataR.OID())
	}

	var joinPlan *plans.HashJoinPlanNode
	var outFinal *schema.Schema
	var filterOut *schema.Schema
	{
		finalOutCols := make([]*column.Column, 0)

		// new columns have tuple index of 0 because they are the left side of the join
		colValL := expression.MakeColumnValueExpression(outSchemaL, 0, *pner.qi.OnExpressions.Left.(*string))
		// new columns have tuple index of 1 because they are the right side of the join
		colValR := expression.MakeColumnValueExpression(outSchemaR, 1, *pner.qi.OnExpressions.Right.(*string))

		for _, colDef := range tgtTblColumnsL {
			col := column.NewColumn(colDef.GetColumnName(), colDef.GetType(), false, index_constants.IndexKindInvalid, types.PageID(-1), colDef.GetExpr())
			col.SetIsLeft(true)
			finalOutCols = append(finalOutCols, col)
		}
		for _, colDef := range tgtTblColumnsR {
			col := column.NewColumn(colDef.GetColumnName(), colDef.GetType(), false, index_constants.IndexKindInvalid, types.PageID(-1), colDef.GetExpr())
			col.SetIsLeft(false)
			finalOutCols = append(finalOutCols, col)
		}
		// output schema of HashJoinExecutor
		outFinal = schema.NewSchema(finalOutCols)

		if hasWhere {
			// query has WHERE clause
			if len(pner.qi.SelectFields) == 1 && *pner.qi.SelectFields[0].ColName == "*" {
				// both schema includes all columns
				filterOut = outFinal
			} else {
				filterOutCols := make([]*column.Column, 0)
				// column existance check
				for _, sfield := range pner.qi.SelectFields {
					colName := sfield.ColName
					tblName := sfield.TableName

					if *tblName != tblNameL && *tblName != tblNameR {
						return PrintAndCreateError("specified selection " + *tblName + "." + *colName + " is invalid.")
					}

					if pner.cat.GetTableByName(*tblName) == nil {
						return PrintAndCreateError("table " + *tblName + " does not exist.")
					}

					tmpSchema := pner.cat.GetTableByName(*tblName).Schema()
					colIdx := tmpSchema.GetColIndex(*colName)
					if colIdx == math.MaxUint32 {
						return PrintAndCreateError("column " + *colName + " does not exist on " + *tblName + ".")
					}

					colDef := tmpSchema.GetColumn(colIdx)
					col := column.NewColumn(*tblName+"."+*colName, colDef.GetType(), false, index_constants.IndexKindInvalid, types.PageID(-1), colDef.GetExpr())
					if *tblName == tblNameL {
						col.SetIsLeft(true)
					} else { // Right
						col.SetIsLeft(false)
					}
					// Attention: this method call modifies passed Column objects
					filterOutCols = append(filterOutCols, col)
				}
				filterOut = schema.NewSchema(finalOutCols)
			}
		}

		onPredicate := testing_tbl_gen.MakeComparisonExpression(colValL, colValR, expression.Equal)

		var leftKeys []expression.Expression
		leftKeys = append(leftKeys, colValL)
		var rightKeys []expression.Expression
		rightKeys = append(rightKeys, colValR)

		scanPlans := []plans.Plan{scanPlanL, scanPlanR}
		joinPlan = plans.NewHashJoinPlanNode(outFinal, scanPlans, onPredicate,
			leftKeys, rightKeys)
	}

	if hasWhere {
		whereExp := pner.ConstructPredicate([]*schema.Schema{outFinal})
		// filter joined recoreds with predicate which is specified on WHERE clause if needed

		filterPlan := plans.NewSelectionPlanNode(joinPlan, whereExp)
		finalPlan := plans.NewProjectionPlanNode(filterPlan, filterOut)
		return nil, finalPlan
	} else {
		// has no WHERE clause
		return nil, joinPlan
	}
}

func (pner *SimplePlanner) MakeSelectPlan() (error, plans.Plan) {
	if optimizer.CheckIncludesORInPredicate(pner.qi.WhereExpression) {
		// optimizer does not support OR, so use planning logic without optimization...
		if len(pner.qi.JoinTables_) == 1 {
			return pner.MakeSelectPlanWithoutJoin()
		} else {
			return pner.MakeSelectPlanWithJoin()
		}
	} else {
		return pner.MakeOptimizedSelectPlanWithJoin()
	}
}

// TODO: (SDB) duplicated functionality with expression.ConvParsedBinaryOpExprToExpIFOne func???
func processPredicateTreeNode(node *parser.BinaryOpExpression, tgtTblSchemas []*schema.Schema) expression.Expression {
	if node.LogicalOperationType != -1 { // node of logical operation
		leftSidePred := processPredicateTreeNode(node.Left.(*parser.BinaryOpExpression), tgtTblSchemas)
		rightSidePred := processPredicateTreeNode(node.Right.(*parser.BinaryOpExpression), tgtTblSchemas)
		return expression.NewLogicalOp(leftSidePred, rightSidePred, node.LogicalOperationType, types.Boolean)
	} else { // node of compare operation
		colName := *node.Left.(*string)
		specfiedVal := node.Right.(*types.Value)

		// TODO: (SDB) need to validate specified table name prefix, column name and literal (processPredicateTreeNode)
		//             without use of panic function

		tmpColIdx := tgtTblSchemas[0].GetColIndex(colName)

		tmpColVal := expression.NewColumnValue(0, tmpColIdx, specfiedVal.ValueType())
		constVal := expression.NewConstantValue(*specfiedVal, specfiedVal.ValueType())

		return expression.NewComparison(tmpColVal, constVal, node.ComparisonOperationType, types.Boolean)
	}
}

func (pner *SimplePlanner) ConstructPredicate(tgtTblSchemas []*schema.Schema) expression.Expression {
	return processPredicateTreeNode(pner.qi.WhereExpression, tgtTblSchemas)
}

func (pner *SimplePlanner) MakeCreateTablePlan() (error, plans.Plan) {
	if pner.cat.GetTableByName(*pner.qi.NewTable) != nil {
		return PrintAndCreateError("already " + *pner.qi.NewTable + " exists.")
	}

	columns := make([]*column.Column, 0)
	for _, cdefExp := range pner.qi.ColDefExpressions {
		columns = append(columns, column.NewColumn(*cdefExp.ColName, *cdefExp.ColType, true, index_constants.IndexKindSkipList, types.PageID(-1), nil))
		//columns = append(columns, column.NewColumn(*cdefExp.ColName, *cdefExp.ColType, true, index_constants.IndexKindBtree, types.PageID(-1), nil))
	}
	sc := schema.NewSchema(columns)

	pner.cat.CreateTable(*pner.qi.NewTable, sc, pner.txn)

	return nil, nil
}

func PrintAndCreateError(msg string) (error, plans.Plan) {
	fmt.Println(msg)
	return errors.New(msg), nil
}

func (pner *SimplePlanner) MakeInsertPlan() (error, plans.Plan) {
	tableMetadata := pner.cat.GetTableByName(*pner.qi.JoinTables_[0])
	if tableMetadata == nil {
		return PrintAndCreateError("table " + *pner.qi.JoinTables_[0] + " not found.")
	}

	sc := tableMetadata.Schema()
	tgtColNum := len(pner.qi.TargetCols)
	insRows := make([][]types.Value, 0)
	insertRowCnt := 0
	valCnt := 0
	row := make([]types.Value, 0)
	for idx, colName := range pner.qi.TargetCols {
		val := pner.qi.Values[idx-(tgtColNum*insertRowCnt)]
		if sc.GetColIndex(*colName) == math.MaxUint32 {
			return PrintAndCreateError("specified column name " + *colName + " does not exist on table " + *pner.qi.JoinTables_[0] + ".")
		}
		valType := sc.GetColumn(sc.GetColIndex(*colName)).GetType()
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

func (pner *SimplePlanner) MakeDeletePlan() (error, plans.Plan) {
	if optimizer.CheckIncludesORInPredicate(pner.qi.WhereExpression) {
		// optimizer does not support OR, so use planning logic without optimization...
		tableMetadata := pner.cat.GetTableByName(*pner.qi.JoinTables_[0])
		if tableMetadata == nil {
			return PrintAndCreateError("table " + *pner.qi.JoinTables_[0] + " not found.")
		}

		tgtTblSchema := tableMetadata.Schema()

		expr := pner.ConstructPredicate([]*schema.Schema{tgtTblSchema})
		seqScanPlanP := plans.NewSeqScanPlanNode(pner.cat, tgtTblSchema, expr, tableMetadata.OID())
		deletePlan := plans.NewDeletePlanNode(seqScanPlanP)

		return nil, deletePlan
	} else {
		_, selectPlan := pner.MakeOptimizedSelectPlanWithJoin()
		deletePlan := plans.NewDeletePlanNode(selectPlan)
		return nil, deletePlan
	}

}

func (pner *SimplePlanner) MakeUpdatePlan() (error, plans.Plan) {

	// optimizer does not support OR, so use planning logic without optimization...
	tableMetadata := pner.cat.GetTableByName(*pner.qi.JoinTables_[0])
	if tableMetadata == nil {
		return PrintAndCreateError("table " + *pner.qi.JoinTables_[0] + " not found.")
	}
	tgtTblSchema := tableMetadata.Schema()
	hasWhere := pner.qi.WhereExpression.Left != nil && pner.qi.WhereExpression.Right != nil

	updateColIdxs := make([]int, 0)

	// first, create update column idx list
	for _, setExp := range pner.qi.SetExpressions {
		colIdx := tgtTblSchema.GetColIndex(*setExp.ColName)
		if colIdx == math.MaxUint32 {
			return PrintAndCreateError("column " + *setExp.ColName + " does not exist on table " + *pner.qi.JoinTables_[0] + ".")
		}
		updateColIdxs = append(updateColIdxs, int(colIdx))
	}

	// second, construc values list includes dummy value (not update target)
	updateVals := make([]types.Value, tgtTblSchema.GetColumnCount())
	// fill all elems with dummy
	for idx := range tgtTblSchema.GetColumns() {
		updateVals[idx] = types.NewNull()
	}
	// overwrite elem which is update target
	for idx, colIdx := range updateColIdxs {
		updateVals[colIdx] = *pner.qi.SetExpressions[idx].UpdateValue
	}

	var predicate expression.Expression = nil
	if hasWhere {
		predicate = pner.ConstructPredicate([]*schema.Schema{tgtTblSchema})
	}

	var scanPlan plans.Plan
	if optimizer.CheckIncludesORInPredicate(pner.qi.WhereExpression) {
		scanPlan = plans.NewSeqScanPlanNode(pner.cat, tgtTblSchema, predicate, tableMetadata.OID())
	} else {
		_, scanPlan = pner.MakeOptimizedSelectPlanWithJoin()
	}

	return nil, plans.NewUpdatePlanNode(updateVals, updateColIdxs, scanPlan)
}
