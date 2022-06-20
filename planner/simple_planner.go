package planner

import (
	"errors"
	"fmt"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
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

// TODO: (SDB) need to implement MakeSelectPlan method
func (pner *SimplePlanner) MakeSelectPlan() (error, plans.Plan) {
	//columns := []*column.Column{}
	//for _, c := range testCase.Columns {
	//	columns = append(columns, column.NewColumn(c.Name, c.Kind, false, nil))
	//}
	//outSchema := schema.NewSchema(columns)
	//
	//tmpColVal_ := expression.NewColumnValue(0, testCase.TableMetadata.Schema().GetColIndex(testCase.Predicate.LeftColumn), GetValueType(testCase.Predicate.RightColumn))
	//tmpColVal := tmpColVal_.(*expression.ColumnValue)
	//expression := expression.NewComparison(tmpColVal, expression.NewConstantValue(GetValue(testCase.Predicate.RightColumn), GetValueType(testCase.Predicate.RightColumn)), testCase.Predicate.Operator, types.Boolean)
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
					outColDefs = append(outColDefs, existCol)
					break
				}
			}
			if !isOk {
				msg := "column " + *colName + " does not exist."
				fmt.Println(msg)
				return errors.New(msg), nil
			}
		}
		outSchema = schema.NewSchema(outColDefs)
	} else {
		outSchema = tgtTblSchema
	}

	tmpColIdx := tgtTblSchema.GetColIndex(*pner.qi.WhereExpression_.Left_.(*string))
	tmpColVal := expression.NewColumnValue(0, tmpColIdx, pner.qi.WhereExpression_.Right_.(*types.Value).ValueType())
	expression := expression.NewComparison(tmpColVal, expression.NewConstantValue(*pner.qi.WhereExpression_.Right_.(*types.Value), pner.qi.WhereExpression_.Right_.(*types.Value).ValueType()), pner.qi.WhereExpression_.ComparisonOperationType_, types.Boolean)

	return nil, plans.NewSeqScanPlanNode(outSchema, expression, tableMetadata.OID())
}

func (pner *SimplePlanner) MakeCreateTablePlan() (error, plans.Plan) {
	if pner.catalog_.GetTableByName(*pner.qi.NewTable_) != nil {
		msg := "already " + *pner.qi.NewTable_ + " exists."
		fmt.Println(msg)
		return errors.New(msg), nil
	}

	columns := make([]*column.Column, 0)
	for _, cdefExp := range pner.qi.ColDefExpressions_ {
		columns = append(columns, column.NewColumn(*cdefExp.ColName_, *cdefExp.ColType_, false, nil))
	}
	schema_ := schema.NewSchema(columns)

	pner.catalog_.CreateTable(*pner.qi.NewTable_, schema_, pner.txn)

	return nil, nil
}

func (pner *SimplePlanner) MakeInsertPlan() (error, plans.Plan) {
	tableMetadata := pner.catalog_.GetTableByName(*pner.qi.JoinTables_[0])
	if tableMetadata == nil {
		msg := "table " + *pner.qi.JoinTables_[0] + " not found."
		fmt.Println(msg)
		return errors.New(msg), nil
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
			msg := "data type of " + *colName + " is wrong."
			fmt.Println(msg)
			return errors.New(msg), nil
		}
		valType := schema_.GetColumn(schema_.GetColIndex(*colName)).GetType()
		if val.ValueType() != valType {
			msg := "data type of " + *colName + " is wrong."
			fmt.Println(msg)
			return errors.New(msg), nil
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
