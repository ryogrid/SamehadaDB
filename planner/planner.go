package planner

import (
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/storage/access"
)

type Planner interface {
	MakePlan(*parser.QueryInfo, *access.Transaction) *plans.Plan
}
