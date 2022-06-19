package planner

import (
	"github.com/pkg/errors"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
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
	return nil, nil
}

func (pner *SimplePlanner) MakeCreateTablePlan() (error, plans.Plan) {
	if pner.catalog_.GetTableByName(*pner.qi.NewTable_) != nil {
		return errors.New("already " + *pner.qi.NewTable_ + " exists."), nil
	}

	columns := make([]*column.Column, 0)
	for _, cdefExp := range pner.qi.ColDefExpressions_ {
		columns = append(columns, column.NewColumn(*cdefExp.ColName_, *cdefExp.ColType_, false, nil))
	}
	schema_ := schema.NewSchema(columns)

	pner.catalog_.CreateTable(*pner.qi.NewTable_, schema_, pner.txn)

	return nil, nil
}

// TODO: (SDB) need to implement MakeInsertPlan method
func (pner *SimplePlanner) MakeInsertPlan() (error, plans.Plan) {
	return nil, nil
}

// TODO: (SDB) need to implement MakeDeletePlan method
func (pner *SimplePlanner) MakeDeletePlan() (error, plans.Plan) {
	return nil, nil
}

func (pner *SimplePlanner) MakeUpdatePlan() (error, plans.Plan) {
	return nil, nil
}
