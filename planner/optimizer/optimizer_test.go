package optimizer

import (
	"fmt"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/parser"
	//"strings"
	"testing"
)

func TestSimplePlanOptimization(t *testing.T) {
	// TODO: (SDB) not implemented yet
}

/*func arrToStr(arr []string) string {
	str := ""
	for _, s := range arr {
		str += s + ","
	}
	return str
}

func strToArr(str string) []string {
	return strings.Split(str, ",")
}*/

func containsAny(map1 mapset.Set[string], map2 mapset.Set[string]) bool {
	interSet := map1.Intersect(map2)
	if interSet.Cardinality() > 0 {
		return true
	} else {
		return false
	}
}

func TestBestJoin(t *testing.T) {
	// TODO: (SDB) not implemented yet
	query := new(parser.QueryInfo)
	optimalPlans := make(map[mapset.Set[string]]CostAndPlan, 100)
	for ii := 0; ii < len(query.JoinTables_); ii += 1 {
		for baseTableFrom, baseTableCP := range optimalPlans {
			for joinTableFrom, joinTableCP := range optimalPlans {
				if containsAny(baseTableFrom, joinTableFrom) {
					continue
				}
				_, bestJoinPlan := new(SelingerOptimizer).bestJoin(query.WhereExpression_, baseTableCP.plan, joinTableCP.plan)
				fmt.Println(bestJoinPlan)

				joinedTables := baseTableFrom.Union(joinTableFrom)
				common.SH_Assert(1 < joinedTables.Cardinality(), "joinedTables.Cardinality() is illegal!")
				cost := bestJoinPlan.AccessRowCount()
				/*
					auto iter = optimal_plans.find(joined_tables);
					if (iter == optimal_plans.end()) {
						optimal_plans.emplace(joined_tables, CostAndPlan{cost, best_join});
					} else if (cost < iter->second.cost) {
						iter->second = CostAndPlan{cost, best_join};
					}
				*/
			}
		}
	}
}
