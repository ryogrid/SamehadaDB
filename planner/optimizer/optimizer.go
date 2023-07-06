package optimizer

import (
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
)

type Optimizer interface {
	// TODO: (SDB) need adding appropriate arguments and return values
	bestScan(*parser.SelectFieldExpression, *parser.BinaryOpExpression, *schema.Schema, *catalog.Catalog, *catalog.TableStatistics) (plans.Plan, error)
	bestJoin(*parser.BinaryOpExpression, plans.Plan, plans.Plan) (plans.Plan, error)
	Optimize() (plans.Plan, error)
}
