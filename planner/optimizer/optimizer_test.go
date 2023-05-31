package optimizer

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/parser"
	"strings"
	"testing"
)

func TestSimplePlanOptimization(t *testing.T) {
	// TODO: (SDB) not implemented yet
}

func arrToStr(arr []string) string {
	str := ""
	for _, s := range arr {
		str += s + ","
	}
	return str
}

func strToArr(str string) []string {
	return strings.Split(str, ",")
}

func containsAny(arr1 []string, arr2 []string) bool {
	for _, s1 := range arr1 {
		for _, s2 := range arr2 {
			if s1 == s2 {
				return true
			}
		}
	}
	return false
}

func TestBestJoin(t *testing.T) {
	// TODO: (SDB) not implemented yet
	query := new(parser.QueryInfo)
	optimalPlans := make(map[string]CostAndPlan, 100)
	for ii := 0; ii < len(query.JoinTables_); ii += 1 {
		for baseTableFrom, baseTableCP := range optimalPlans {
			for jonTableFrom, joinTableCP := range optimalPlans {
				if containsAny(strToArr(baseTableFrom), strToArr(jonTableFrom)) {
					continue
				}
				_, bestJoinPlan := new(SelingerOptimizer).bestJoin(query.WhereExpression_, baseTableCP.plan, joinTableCP.plan)
				fmt.Println(bestJoinPlan)
				/*
					std::unordered_set<std::string> joined_tables =
							Union(base_table.first, join_table.first);
						assert(1 < joined_tables.size());
						size_t cost = best_join->AccessRowCount();
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
