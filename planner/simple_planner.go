package planner

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/test_util"
)

type SimplePlanner struct {
	qi  *parser.QueryInfo
	c   *catalog.Catalog
	shi test_util.SamehadaInstance
	txn *access.Transaction
}

func NewSimplePlanner(c *catalog.Catalog, shi test_util.SamehadaInstance) *SimplePlanner {
	return &SimplePlanner{nil, c, shi, nil}
}

// TODO: (SDB) need implement MakePlan of Simple Planner
func (pner *SimplePlanner) MakePlan(qi *parser.QueryInfo, txn *access.Transaction) *plans.Plan {
	pner.qi = qi
	pner.txn = txn

	return nil
}
