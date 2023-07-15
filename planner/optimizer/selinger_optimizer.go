package optimizer

import (
	"fmt"
	mapset "github.com/deckarep/golang-set/v2"
	stack "github.com/golang-collections/collections/stack"
	pair "github.com/notEpsilon/go-pair"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/executors"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"sort"
)

type CostAndPlan struct {
	cost uint64
	plan plans.Plan
}

type Direction bool

const (
	DIR_RIGHT Direction = false
	DIR_LEFT  Direction = true
)

type Range struct {
	min           *types.Value
	max           *types.Value
	min_inclusive bool
	max_inclusive bool
}

func NewRange(valType types.TypeID) *Range {
	retRange := new(Range)
	switch valType {
	case types.Integer:
		retRange.min = samehada_util.GetPonterOfValue(types.NewInteger(math.MinInt32))
		retRange.max = samehada_util.GetPonterOfValue(types.NewInteger(math.MaxInt32))
	case types.Float:
		retRange.min = samehada_util.GetPonterOfValue(types.NewFloat(math.SmallestNonzeroFloat32))
		retRange.max = samehada_util.GetPonterOfValue(types.NewFloat(math.MaxFloat32))
	case types.Varchar:
		retRange.min = samehada_util.GetPonterOfValue(types.NewVarchar("")).SetInfMin()
		retRange.max = samehada_util.GetPonterOfValue(types.NewVarchar("")).SetInfMax()
	default:
		panic("invalid type")
	}
	retRange.min_inclusive = false
	retRange.max_inclusive = false
	return retRange
}

func (r *Range) Empty() bool {
	// TODO: (SDB) not implemented yet
	return false
}

func (r *Range) Update(op expression.ComparisonType, rhs *types.Value, dir Direction) {
	// TODO: (SDB) not implemented yet
}

type SelingerOptimizer struct {
	c *catalog.Catalog
	// TODO: (SDB) not implemented yet
}

func NewSelingerOptimizer() *SelingerOptimizer {
	// TODO: (SDB) not implemented yet
	return nil
}

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

func isColumnName(v interface{}) bool {
	switch v.(type) {
	case *string:
		return true
	default:
		return false
	}
}

func isConstantValue(v interface{}) bool {
	switch v.(type) {
	case *types.Value:
		return true
	default:
		return false
	}
}

func touchOnly(where expression.Expression, colName string) bool {
	// TODO: (SDB) not implemented yet
	return false
}

// TODO: (SDB) caller should pass *where* args which is deep copied
func (so *SelingerOptimizer) bestScan(selection []*parser.SelectFieldExpression, where *parser.BinaryOpExpression, from *catalog.TableMetadata, c *catalog.Catalog, stats *catalog.TableStatistics) (plans.Plan, error) {
	// TODO: (SDB) not implemented yet

	AvailableKeyIndex := func() map[int]int {
		retMap := make(map[int]int, 0)
		idxArr := from.Indexes()
		for idx, idxObj := range idxArr {
			if idxObj != nil {
				retMap[idx] = idx
			}
		}
		return retMap
	}

	// const Schema& sc = from.GetSchema();
	sc := from.Schema()
	var minimamCost uint64 = math.MaxUint64
	var bestScan plans.Plan
	// Get { index key column offset => index offset } map
	//std::unordered_map<slot_t, size_t> candidates = from.AvailableKeyIndex();
	candidates := AvailableKeyIndex()
	// Prepare all range of candidates
	ranges := make(map[int]*Range, 0)
	for key, _ := range candidates {
		ranges[key] = NewRange(sc.GetColumn(uint32(key)).GetType())
	}

	//std::vector<Expression> stack
	stack_ := stack.New[*parser.BinaryOpExpression]()
	stack_.Push(where)
	//std::vector<Expression> related_ops
	relatedOps := make([]*parser.BinaryOpExpression, 0)
	for stack_.Len() > 0 {
		//Expression exp = stack.back()
		//stack.pop_back()
		exp := stack_.Pop().(*parser.BinaryOpExpression)
		//if (exp->Type() == TypeTag::kBinaryExp) {
		if exp.GetType() == parser.Logical {
			//const BinaryExpression& be = exp->AsBinaryExpression();
			if exp.LogicalOperationType_ == expression.AND {
				stack_.Push(exp.Left_)
				stack_.Push(exp.Right_)
				continue
			} else {
				panic("OR on predicate is not supported now!")
			}
		} else if exp.GetType() == parser.Compare {
			if isColumnName(exp.Left_) && isConstantValue(exp.Right_) {
				//const ColumnValue& cv = be.Left()->AsColumnValue();
				//const int offset = sc.Offset(cv.GetColumnName());
				//if (0 <= offset) {
				colIdx := sc.GetColIndex(*exp.Left_.(*string))
				if colIdx != math.MaxUint32 {
					relatedOps = append(relatedOps, exp)
					//auto iter = ranges.find(offset);
					//if (iter != ranges.end()) {
					//	iter->second.Update(be.Op(),
					//		be.Right()->AsConstantValue().GetValue(),
					//		Range::Dir::kRight);
					//}
					if rng, ok := ranges[int(colIdx)]; ok {
						rng.Update(exp.ComparisonOperationType_, exp.Right_.(*types.Value), DIR_RIGHT)
					}
				}
			} else if isColumnName(exp.Right_) && isConstantValue(exp.Left_) {
				//const ColumnValue& cv = be.Right()->AsColumnValue();
				//const int offset = sc.Offset(cv.GetColumnName());
				//if (0 <= offset) {
				colIdx := sc.GetColIndex(*exp.Right_.(*string))
				if colIdx != math.MaxUint32 {
					relatedOps = append(relatedOps, exp)
					//auto iter = ranges.find(offset);
					//if (iter != ranges.end()) {
					//	iter->second.Update(be.Op(),
					//		be.Left()->AsConstantValue().GetValue(),
					//		Range::Dir::kLeft);
					//}
					if rng, ok := ranges[int(colIdx)]; ok {
						rng.Update(exp.ComparisonOperationType_, exp.Left_.(*types.Value), DIR_LEFT)
					}
				}
			}
		}
	}

	// Expression scan_exp;
	//var scanExp *parser.BinaryOpExpression
	var scanExp expression.Expression = nil
	if len(relatedOps) > 0 {
		scanExp = samehada_util.ConvParsedBinaryOpExprToExpIFOne(relatedOps[0])
		for ii := 1; ii < len(relatedOps); ii++ {
			// append addiitional condition with AND operator

			//scan_exp = BinaryExpressionExp(scan_exp, BinaryOperation::kAnd, related_ops[i]);
			scanExp = expression.AppendLogicalCondition(scanExp, expression.AND, samehada_util.ConvParsedBinaryOpExprToExpIFOne(relatedOps[ii]))
		}
	}

	// Build all IndexScan.
	//for (const auto& range : ranges) {
	//slot_t key = range.first;
	//const Range& span = range.second;
	for key, span := range ranges {
		if span.Empty() {
			continue
		}
		//targetIndex := from.GetIndex(candidates[key])
		// Plan new_plan = IndexScanSelect(from, target_idx, stat, *span.min,*span.max, scan_exp, select);
		var newPlan plans.Plan = plans.NewRangeScanWithIndexPlanNode(schema.ConvSelectFieldExpToOutputSchema(selection), from.OID(), int32(key), nil, span.min, span.max)
		// if (!TouchOnly(scan_exp, from.GetSchema().GetColumn(key).Name())) {
		if !touchOnly(scanExp, sc.GetColumn(uint32(key)).GetColumnName()) {
			//new_plan = std::make_shared<SelectionPlan>(new_plan, scan_exp, stat);
			newPlan = plans.NewSelectionPlanNode(newPlan, newPlan.OutputSchema(), scanExp)
		}
		if len(selection) != int(newPlan.OutputSchema().GetColumnCount()) {
			//new_plan = std::make_shared<ProjectionPlan>(new_plan, select);
			newPlan = plans.NewProjectionPlanNode(newPlan, samehada_util.ConvParsedSelectionExprToExpIFOne(selection))
		}
		if newPlan.AccessRowCount() < minimamCost {
			bestScan = newPlan
			minimamCost = newPlan.AccessRowCount()
		}
	}

	// Plan full_scan_plan(new FullScanPlan(from, stat));
	fullScanPlan := plans.NewSeqScanPlanNode(sc, nil, from.OID())
	if scanExp != nil {
		// full_scan_plan = std::make_shared<SelectionPlan>(full_scan_plan, scan_exp, stat);
		fullScanPlan = plans.NewSeqScanPlanNode(sc, scanExp, from.OID())
	}

	if len(selection) != int(sc.GetColumnCount()) {
		// full_scan_plan = std::make_shared<ProjectionPlan>(full_scan_plan, select);
		fullScanPlan = plans.NewProjectionPlanNode(fullScanPlan, samehada_util.ConvParsedSelectionExprToExpIFOne(selection))
	}
	if fullScanPlan.AccessRowCount() < minimamCost {
		bestScan = fullScanPlan
		minimamCost = fullScanPlan.AccessRowCount()
	}

	return bestScan, nil
}

func findBestScans(query *parser.QueryInfo, exec_ctx *executors.ExecutorContext, c *catalog.Catalog, txn *access.Transaction) map[mapset.Set[string]]CostAndPlan {
	// TODO: (SDB) not implemented yet (findBestScans)

	optimalPlans := make(map[mapset.Set[string]]CostAndPlan)

	// 1. Initialize every single tables to start.
	touchedColumns := query.WhereExpression_.TouchedColumns()
	for _, item := range query.SelectFields_ {
		touchedColumns = touchedColumns.Union(item.TouchedColumns())
	}
	for _, from := range query.JoinTables_ {
		tbl := c.GetTableByName(*from)
		stats := c.GetTableByName(*from).GetStatistics()
		projectTarget := make([]*column.Column, 0)
		// Push down all selection & projection.
		for ii := 0; ii < int(tbl.GetColumnNum()); ii++ {
			for _, touchedCol := range touchedColumns.ToSlice() {
				tableCol := tbl.Schema().GetColumn(uint32(ii))
				// TODO: (SDB) GetColumnName() value should contain table name. if not,  rewrite of this comparison code is needed
				if tableCol.GetColumnName() == touchedCol.GetColumnName() {
					projectTarget = append(projectTarget, tableCol)
				}
			}
		}
		scan, _ := NewSelingerOptimizer().bestScan(query.SelectFields_, query.WhereExpression_, tbl, c, stats)
		optimalPlans[makeSet([]*string{from})] = CostAndPlan{scan.AccessRowCount(), scan}
	}

	return optimalPlans
}

// TODO: (SDB) caller should pass *where* args which is deep copied
func (so *SelingerOptimizer) bestJoin(where *parser.BinaryOpExpression, left plans.Plan, right plans.Plan) (plans.Plan, error) {
	// TODO: (SDB) not implemented yet

	// pair<ColumnName, ColumnName>
	var equals []pair.Pair[*string, *string] = make([]pair.Pair[*string, *string], 0)
	//stack<Expression> exp
	exp := stack.New[*parser.BinaryOpExpression]()
	exp.Push(where)
	// vector<Expression> relatedExp
	var relatedExp = make([]*parser.BinaryOpExpression, 0)
	for exp.Len() > 0 {
		here := exp.Pop().(*parser.BinaryOpExpression)
		if here.GetType() == parser.Compare {
			if here.ComparisonOperationType_ == expression.Equal && isColumnName(here.Left_) && isColumnName(here.Right_) {
				cvL := here.Left_.(*string)
				cvR := here.Right_.(*string)
				if left.OutputSchema().IsHaveColumn(cvL) && right.OutputSchema().IsHaveColumn(cvR) {
					equals = append(equals, pair.Pair[*string, *string]{cvL, cvR})
					relatedExp = append(relatedExp, here)
				} else if right.OutputSchema().IsHaveColumn(cvL) && left.OutputSchema().IsHaveColumn(cvR) {
					equals = append(equals, pair.Pair[*string, *string]{cvR, cvL})
					relatedExp = append(relatedExp, here)
				}
			}
			// note:
			// ignore case that *here* variable represents a constant value
		} else if here.GetType() == parser.Logical {
			if here.LogicalOperationType_ == expression.AND {
				exp.Push(here.Left_)
				exp.Push(here.Right_)
			}
		}
	}

	candidates := make([]plans.Plan, 0)
	if len(equals) > 0 {
		left_cols := make([]*string, len(equals))
		right_cols := make([]*string, len(equals))
		for _, cn := range equals {
			left_cols = append(left_cols, cn.First)
			right_cols = append(right_cols, cn.Second)
		}

		// TODO: (SDB) need finalize of setup Plan Nodes later

		// HashJoin (note: appended plans are temporal (not completely setuped))
		// candidates.push_back(std::make_shared<ProductPlan>(left, left_cols, right, right_cols));
		var tmpPlan plans.Plan = plans.NewHashJoinPlanNode(nil, []plans.Plan{left, right}, nil, nil, nil)
		candidates = append(candidates, tmpPlan)
		// candidates.push_back(std::make_shared<ProductPlan>(right, right_cols, left, left_cols));
		// TODO: (SDB) finalize of setup Plan Nodes is needed later (HashJoinPlanNode)
		tmpPlan = plans.NewHashJoinPlanNode(nil, []plans.Plan{right, left}, nil, nil, nil)
		candidates = append(candidates, tmpPlan)

		// IndexJoin

		// if (const Table* right_tbl = right->ScanSource()) {
		// first item on condition below checks whether right Plan deal only one table
		if right.GetTableOID() != math.MaxUint32 && len(so.c.GetTableByOID(right.GetTableOID()).Indexes()) > 0 {
			// for (size_t i = 0; i < right_tbl->IndexCount(); ++i) {
			// const Index& right_idx = right_tbl->GetIndex(i);
			for _, right_idx := range so.c.GetTableByOID(right.GetTableOID()).Indexes() {
				//ASSIGN_OR_CRASH(std::shared_ptr<TableStatistics>, stat,
				//	ctx.GetStats(right_tbl->GetSchema().Name()));
				for _, rcol := range right_cols {
					if right_idx.GetTupleSchema().IsHaveColumn(rcol) {
						// note: appended plans are temporal (not completely setuped)
						// candidates.push_back(std::make_shared<ProductPlan>(left, left_cols, *right_tbl, right_idx, right_cols, *stat));
						// TODO: (SDB) finalize of setup Plan Nodes is needed later (IndexJoinPlanNode)
						candidates = append(candidates, plans.NewIndexJoinPlanNode(nil, []plans.Plan{left, right}, nil, nil, nil))
					}
				}
			}
		}
	}

	// when *where* hash no condition which matches records of *left* and *light*
	if len(candidates) == 0 {
		if 0 < len(relatedExp) {
			// when *where* hash conditions related to columns of *left* and *light*

			finalSelection := relatedExp[len(relatedExp)-1]
			relatedExp = relatedExp[:len(relatedExp)-1]

			// construct SelectionPlan which has a NestedLoopJoinPlan as child with usable predicate

			for _, exp := range relatedExp {
				finalSelection = &parser.BinaryOpExpression{expression.AND, -1, finalSelection, exp}
			}

			// TODO: (SDB) finalize of setup Plan Nodes is needed later (NestedLoopJoinPlanNode)
			// Plan ans = std::make_shared<ProductPlan>(left, right);
			ans := plans.NewNestedLoopJoinPlanNode(nil, []plans.Plan{left, right}, nil, nil, nil)
			// candidates.push_back(std::make_shared<SelectionPlan>(ans, final_selection, ans->GetStats()));
			candidates = append(candidates, plans.NewSelectionPlanNode(ans, ans.OutputSchema(), samehada_util.ConvParsedBinaryOpExprToExpIFOne(finalSelection)))
		} else {
			// unfortunatelly, construction of NestedLoopJoinPlan with no optimization is needed

			// TODO: (SDB) finalize of setup Plan Nodes is needed later (NestedLoopJoinPlanNode)
			candidates = append(candidates, plans.NewNestedLoopJoinPlanNode(nil, []plans.Plan{left, right}, nil, nil, nil))
		}
	}

	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].AccessRowCount() < candidates[j].AccessRowCount()
	})
	return candidates[0], nil
}

func findBestJoin(optimalPlans map[mapset.Set[string]]CostAndPlan, query *parser.QueryInfo, exec_ctx *executors.ExecutorContext, c *catalog.Catalog, txn *access.Transaction) plans.Plan {
	// TODO: (SDB) need to setup of optimalPlans which is needed at BestJoin
	// TODO: (SDB) need to setup of statistics data of tables related to query

	for ii := 0; ii < len(query.JoinTables_); ii += 1 {
		for baseTableFrom, baseTableCP := range optimalPlans {
			for joinTableFrom, joinTableCP := range optimalPlans {
				if containsAny(baseTableFrom, joinTableFrom) {
					continue
				}
				// TODO: (SDB) (len(baseTable) + len(joinTable) == ii + 1) should be checked?
				//             and (len(baseTable) == 1 or len(joinTable) == 1) should be checked?
				bestJoinPlan, _ := NewSelingerOptimizer().bestJoin(query.WhereExpression_, baseTableCP.plan, joinTableCP.plan)
				fmt.Println(bestJoinPlan)

				joinedTables := baseTableFrom.Union(joinTableFrom)
				common.SH_Assert(1 < joinedTables.Cardinality(), "joinedTables.Cardinality() is illegal!")
				cost := bestJoinPlan.AccessRowCount()

				// TODO: (SDB) update target should be changed to tempolal table?
				//             (its scope is same ii value loop and it is merged to optimalPlans at end of the ii loop)
				if existedPlan, ok := optimalPlans[joinedTables]; ok {
					optimalPlans[joinedTables] = CostAndPlan{cost, bestJoinPlan}
				} else if cost < existedPlan.cost {
					optimalPlans[joinedTables] = CostAndPlan{cost, bestJoinPlan}
				}
			}
		}
	}
	optimalPlan, ok := optimalPlans[makeSet(query.JoinTables_)]
	samehada_util.SHAssert(ok, "plan which includes all tables is not found")

	// Attach final projection and emit the result
	solution := optimalPlan.plan
	solution = plans.NewProjectionPlanNode(solution, samehada_util.ConvParsedSelectionExprToExpIFOne(query.SelectFields_))

	return solution
}

// TODO: (SDB) caller should check predicate whether it is optimizable and
//	           if not, caller can't call this function
//             (predicate including bracket or OR operation case is not supported now)

// TODO: (SDB) adding support of ON clause (Optimize, bestJoin, bestScan)
func (so *SelingerOptimizer) Optimize(query *parser.QueryInfo, exec_ctx *executors.ExecutorContext, c *catalog.Catalog, txn *access.Transaction) (plans.Plan, error) {
	optimalPlans := findBestScans(query, exec_ctx, c, txn)
	return findBestJoin(optimalPlans, query, exec_ctx, c, txn), nil
}
