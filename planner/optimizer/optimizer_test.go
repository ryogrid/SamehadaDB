package optimizer

import (
	"fmt"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/parser"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"

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

func makeSet[T comparable](from []*T) mapset.Set[T] {
	joined := mapset.NewSet[T]()
	for _, f := range from {
		joined.Add(*f)
	}
	return joined
}

func TestBestJoin(t *testing.T) {
	// TODO: (SDB) setup of query is needed at TestBestJoin
	query := new(parser.QueryInfo)
	optimalPlans := make(map[mapset.Set[string]]CostAndPlan)
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

				if existedPlan, ok := optimalPlans[joinedTables]; ok {
					optimalPlans[joinedTables] = CostAndPlan{cost, bestJoinPlan}
				} else if cost < existedPlan.cost {
					optimalPlans[joinedTables] = CostAndPlan{cost, bestJoinPlan}
				}
			}
		}
	}
	_, ok := optimalPlans[makeSet(query.JoinTables_)]
	testingpkg.Assert(t, ok, "plan which includes all tables is not found")

}
