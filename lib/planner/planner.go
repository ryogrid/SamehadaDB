package planner

import (
	"github.com/ryogrid/SamehadaDB/lib/execution/plans"
	"github.com/ryogrid/SamehadaDB/lib/parser"
	"github.com/ryogrid/SamehadaDB/lib/storage/access"
)

type Planner interface {
	MakePlan(*parser.QueryInfo, *access.Transaction) (error, plans.Plan)
}
