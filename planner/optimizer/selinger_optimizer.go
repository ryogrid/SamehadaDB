package optimizer

import (
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
)

type CostAndPlan struct {
	cost int64
	plan plans.Plan
}

type Range struct {
	// TODO: (SDB) not implemented yet
}

type SelingerOptimizer struct {
	// TODO: (SDB) not implemented yet
}

func NewSelingerOptimizer() Optimizer {
	// TODO: (SDB) not implemented yet
	return nil
}

func (so *SelingerOptimizer) bestScan() (error, plans.Plan) {
	// TODO: (SDB) not implemented yet
	return nil, nil
}

func (so *SelingerOptimizer) bestJoin(where *parser.BinaryOpExpression, baseTableCP plans.Plan, JoinTableCP plans.Plan) (error, plans.Plan) {
	// TODO: (SDB) not implemented yet
	return nil, nil
}

func (so *SelingerOptimizer) Optimize() (error, plans.Plan) {
	// TODO: (SDB) not implemented yet
	return nil, nil
}
