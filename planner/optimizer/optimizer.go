package optimizer

import (
	"github.com/ryogrid/SamehadaDB/execution/plans"
)

type Optimizer interface {
	// TODO: (SDB) need adding appropriate arguments and return values
	bestScan() (error, plans.Plan)
	bestJoin() (error, plans.Plan)
	Optimize() (error, plans.Plan)
}
