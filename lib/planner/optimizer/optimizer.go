package optimizer

import (
	"github.com/ryogrid/SamehadaDB/lib/catalog"
	"github.com/ryogrid/SamehadaDB/lib/execution/executors"
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/parser"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
)

type Optimizer interface {
	Optimize(*parser.QueryInfo, *executors.ExecutorContext, *catalog.Catalog, *access.Transaction) (plans.Plan, error)
}
