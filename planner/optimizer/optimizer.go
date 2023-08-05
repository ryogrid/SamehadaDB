package optimizer

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/executors"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/storage/access"
)

type Optimizer interface {
	Optimize(*parser.QueryInfo, *executors.ExecutorContext, *catalog.Catalog, *access.Transaction) (plans.Plan, error)
}
