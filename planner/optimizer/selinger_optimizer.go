package optimizer

import (
	"fmt"
	mapset "github.com/deckarep/golang-set/v2"
	stack "github.com/golang-collections/collections/stack"
	pair "github.com/notEpsilon/go-pair"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
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
	Min          *types.Value
	Max          *types.Value
	MinInclusive bool
	MaxInclusive bool
}

func NewRange(valType types.TypeID) *Range {
	retRange := new(Range)
	switch valType {
	case types.Integer:
		retRange.Min = samehada_util.GetPonterOfValue(types.NewInteger(math.MinInt32)).SetInfMin()
		retRange.Max = samehada_util.GetPonterOfValue(types.NewInteger(math.MaxInt32)).SetInfMax()
	case types.Float:
		retRange.Min = samehada_util.GetPonterOfValue(types.NewFloat(math.SmallestNonzeroFloat32)).SetInfMin()
		retRange.Max = samehada_util.GetPonterOfValue(types.NewFloat(math.MaxFloat32)).SetInfMax()
	case types.Varchar:
		retRange.Min = samehada_util.GetPonterOfValue(types.NewVarchar("")).SetInfMin()
		retRange.Max = samehada_util.GetPonterOfValue(types.NewVarchar("")).SetInfMax()
	case types.Boolean:
		retRange.Min = samehada_util.GetPonterOfValue(types.NewBoolean(false)).SetInfMin()
		retRange.Max = samehada_util.GetPonterOfValue(types.NewBoolean(true)).SetInfMax()
	default:
		panic("invalid type")
	}
	retRange.MinInclusive = false
	retRange.MaxInclusive = false
	return retRange
}

func (r *Range) Empty() bool {
	if r.Max.ValueType() == types.Boolean {
		// can't check whether empty or not on current internal design of Value and Range type
		return false
	}

	// check whether field is changed from initial value
	return r.Min.IsInfMin() && r.Max.IsInfMax()
}

func (r *Range) Update(op expression.ComparisonType, rhs *types.Value, dir Direction) {
	switch op {
	case expression.Equal:
		// e.g. x == 10
		r.Max = rhs.GetDeepCopy()
		r.Min = rhs.GetDeepCopy()
		r.MaxInclusive = true
		r.MinInclusive = true
	case expression.NotEqual:
		// e.g. x != 10
		// We have nothing to do here.
	case expression.LessThan, expression.GreaterThan:
		if (dir == DIR_RIGHT && op == expression.LessThan) ||
			(dir == DIR_LEFT && op == expression.GreaterThan) {
			// e.g. x < 10
			// e.g. 10 > x
			if r.Max.IsInfMax() || ((!r.Max.IsInfMax()) && rhs.CompareLessThan(*r.Max)) {
				r.Max = rhs.GetDeepCopy()
				r.MaxInclusive = false
			}
		} else {
			// e.g. 10 < x
			// e.g. x > 10
			if r.Min.IsInfMin() || ((!r.Min.IsInfMin()) && r.Min.CompareLessThan(*rhs)) {
				r.Min = rhs.GetDeepCopy()
				r.MinInclusive = false
			}
		}
	case expression.LessThanOrEqual, expression.GreaterThanOrEqual:
		if (dir == DIR_RIGHT && op == expression.LessThanOrEqual) ||
			(dir == DIR_LEFT && op == expression.GreaterThanOrEqual) {
			// e.g. x <= 10
			// e.g. 10 >= x
			if r.Max.IsInfMax() || ((!r.Max.IsInfMax()) && rhs.CompareLessThanOrEqual(*r.Max)) {
				r.Max = rhs.GetDeepCopy()
				r.MaxInclusive = true
			}
		} else {
			// e.g. 10 <= x
			// e.g. x >= 10
			r.Min = rhs.GetDeepCopy()
			r.MinInclusive = true
		}
	default:
		panic("invalid operator to update")
	}
}

type SelingerOptimizer struct {
	qi *parser.QueryInfo
	c  *catalog.Catalog
}

func NewSelingerOptimizer(qi *parser.QueryInfo, c *catalog.Catalog) *SelingerOptimizer {
	ret := new(SelingerOptimizer)
	ret.qi = qi
	ret.c = c
	return ret
}

func containsAny(map1 mapset.Set[string], map2 mapset.Set[string]) bool {
	interSet := map1.Intersect(map2)
	if interSet.Cardinality() > 0 {
		return true
	} else {
		return false
	}
}

func touchOnly(from *catalog.TableMetadata, where expression.Expression, colName string) bool {
	if where.GetType() == expression.EXPRESSION_TYPE_COLUMN_VALUE {
		// 		   const ColumnValue& cv = where->AsColumnValue();
		//		   return cv.GetColumnName() == col_name;
		cv := where.(*expression.ColumnValue)
		return from.Schema().GetColumn(cv.GetColIndex()).GetColumnName() == colName
	} else if where.GetType() == expression.EXPRESSION_TYPE_LOGICAL_OP || where.GetType() == expression.EXPRESSION_TYPE_COMPARISON {
		//		   const BinaryExpression& be = where->AsBinaryExpression();
		//		   return TouchOnly(be.Left(), col_name) && TouchOnly(be.Right(), col_name);
		return touchOnly(from, where.GetChildAt(0), colName) && touchOnly(from, where.GetChildAt(1), colName)
	}
	//samehada_util.SHAssert(where.GetType() == expression.EXPRESSION_TYPE_CONSTANT_VALUE, "invalid expression type")
	return true
}

// attention: caller should pass *where* args which is deep copied
func (so *SelingerOptimizer) findBestScan(outNeededCols []*column.Column, where *parser.BinaryOpExpression, from *catalog.TableMetadata, c *catalog.Catalog, stats *catalog.TableStatistics) (plans.Plan, error) {
	availableKeyIndex := func() map[int]int {
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
	//std::unordered_map<slot_t, size_t> candidates = from.availableKeyIndex();
	candidates := availableKeyIndex()
	// Prepare all range of candidates
	ranges := make(map[int]*Range, 0)
	for key, _ := range candidates {
		ranges[key] = NewRange(sc.GetColumn(uint32(key)).GetType())
	}

	//std::vector<Expression> stack
	stack_ := stack.New()
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
			if samehada_util.IsColumnName(exp.Left_) && samehada_util.IsConstantValue(exp.Right_) {
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
			} else if samehada_util.IsColumnName(exp.Right_) && samehada_util.IsConstantValue(exp.Left_) {
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
		scanExp = parser.ConvParsedBinaryOpExprToExpIFOne(sc, relatedOps[0])
		for ii := 1; ii < len(relatedOps); ii++ {
			// append addiitional condition with AND operator

			//scan_exp = BinaryExpressionExp(scan_exp, BinaryOperation::kAnd, related_ops[i]);
			scanExp = expression.AppendLogicalCondition(scanExp, expression.AND, parser.ConvParsedBinaryOpExprToExpIFOne(sc, relatedOps[ii]))
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

		isPredicateCheckNeeded := false
		newPlan := plans.NewRangeScanWithIndexPlanNode(c, sc, from.OID(), int32(key), nil, span.Min, span.Max)
		if !span.MinInclusive || !span.MaxInclusive {
			// Range scan is not inclusive, so we need to check predicate
			isPredicateCheckNeeded = true
		}
		if !touchOnly(from, scanExp, sc.GetColumn(uint32(key)).GetColumnName()) || isPredicateCheckNeeded {
			// when scanExp includes item which is not related to current index key, add selection about these
			// e.g. index key is a and scanExp is (1 <= a AND a <= 10 AND c = 2), then add selection (1 <= a AND a <= 10 AND c = 2) to newPlan
			//      currently, though selection related column a is needless, but it is included...
			newPlan = plans.NewSelectionPlanNode(newPlan, scanExp)
		}
		if len(outNeededCols) != int(newPlan.OutputSchema().GetColumnCount()) {
			//new_plan = std::make_shared<ProjectionPlan>(new_plan, select);
			newPlan = plans.NewProjectionPlanNode(newPlan, schema.NewSchema(outNeededCols))
		}
		if newPlan.AccessRowCount(c) < minimamCost {
			bestScan = newPlan
			minimamCost = newPlan.AccessRowCount(c)
		}
	}

	// Plan full_scan_plan(new FullScanPlan(from, stat));
	fullScanPlan := plans.NewSeqScanPlanNode(c, sc, nil, from.OID())
	if scanExp != nil {
		// full_scan_plan = std::make_shared<SelectionPlan>(full_scan_plan, scan_exp, stat);
		fullScanPlan = plans.NewSelectionPlanNode(fullScanPlan, scanExp)
	}
	if len(outNeededCols) != int(sc.GetColumnCount()) {
		// full_scan_plan = std::make_shared<ProjectionPlan>(full_scan_plan, select);
		fullScanPlan = plans.NewProjectionPlanNode(fullScanPlan, schema.NewSchema(outNeededCols))
	}
	if fullScanPlan.AccessRowCount(c) < minimamCost {
		bestScan = fullScanPlan
		minimamCost = fullScanPlan.AccessRowCount(c)
	}

	return bestScan, nil
}

func (so *SelingerOptimizer) findBestScans() map[string]CostAndPlan {
	optimalPlans := make(map[string]CostAndPlan)

	// 1. Initialize every single tables to start.
	touchedColumns := so.qi.WhereExpression_.TouchedColumns()
	for _, item := range so.qi.SelectFields_ {
		touchedColumns = touchedColumns.Union(item.TouchedColumns())
	}
	for _, from := range so.qi.JoinTables_ {
		tbl := so.c.GetTableByName(*from)
		stats := so.c.GetTableByName(*from).GetStatistics()
		projectTarget := make([]*column.Column, 0)
		// Push down all selection & projection.
		for ii := 0; ii < int(tbl.GetColumnNum()); ii++ {
			for _, touchedCol := range touchedColumns.ToSlice() {
				tableCol := tbl.Schema().GetColumn(uint32(ii))
				if tableCol.GetColumnName() == touchedCol {
					projectTarget = append(projectTarget, tableCol)
				}
			}
		}
		//scan, _ := NewSelingerOptimizer().findBestScan(qi.SelectFields_, qi.WhereExpression_, tbl, c, stats)
		scan, _ := so.findBestScan(projectTarget, so.qi.WhereExpression_.GetDeepCopy(), tbl, so.c, stats)
		optimalPlans[samehada_util.StrSetToString(samehada_util.MakeSet([]*string{from}))] = CostAndPlan{scan.AccessRowCount(so.c), scan}
	}

	return optimalPlans
}

// attention: caller should pass *where* args which is deep copied
func (so *SelingerOptimizer) findBestJoinInner(where *parser.BinaryOpExpression, left plans.Plan, right plans.Plan, c *catalog.Catalog) (plans.Plan, error) {
	// pair<ColumnName, ColumnName>
	var equals []pair.Pair[*string, *string] = make([]pair.Pair[*string, *string], 0)
	//stack<Expression> exp
	exp := stack.New()
	exp.Push(where)
	// vector<Expression> relatedExp
	var relatedExp = make([]*parser.BinaryOpExpression, 0)
	for exp.Len() > 0 {
		here := exp.Pop().(*parser.BinaryOpExpression)
		if here.GetType() == parser.Compare {
			if here.ComparisonOperationType_ == expression.Equal && samehada_util.IsColumnName(here.Left_) && samehada_util.IsColumnName(here.Right_) {
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
		left_cols := make([]*string, 0)
		right_cols := make([]*string, 0)
		for _, cn := range equals {
			left_cols = append(left_cols, cn.First)
			right_cols = append(right_cols, cn.Second)
		}

		// HashJoin
		// candidates.push_back(std::make_shared<ProductPlan>(left, left_cols, right, right_cols));
		var tmpPlan plans.Plan = plans.NewHashJoinPlanNodeWithChilds(left, parser.ConvColumnStrsToExpIfOnes(so.c, left_cols, true), right, parser.ConvColumnStrsToExpIfOnes(so.c, right_cols, false))
		candidates = append(candidates, tmpPlan)
		// candidates.push_back(std::make_shared<ProductPlan>(right, right_cols, left, left_cols));
		tmpPlan = plans.NewHashJoinPlanNodeWithChilds(right, parser.ConvColumnStrsToExpIfOnes(so.c, right_cols, true), left, parser.ConvColumnStrsToExpIfOnes(so.c, left_cols, false))
		candidates = append(candidates, tmpPlan)

		// IndexJoin

		// if (const Table* right_tbl = right->ScanSource()) {
		// checks whether right Plan deal only one table (join or aggregation on tree is NG)
		if right.GetTableOID() != math.MaxUint32 || right.GetType() == plans.Projection {
			rightSchema := right.OutputSchema()
			rightOID := right.GetTableOID()
			if right.GetType() == plans.Projection {
				// when right plan is ProjectionPlan, using child plan's schema and OID is needed to get target table info
				rightSchema = right.GetChildAt(0).OutputSchema()
				rightOID = right.GetChildAt(0).GetTableOID()
			}
			if len(so.c.GetTableByOID(rightOID).Indexes()) > 0 {
				// for (size_t i = 0; i < right_tbl->IndexCount(); ++i) {
				// const Index& right_idx = right_tbl->GetIndex(i);
				for _, right_idx := range so.c.GetTableByOID(rightOID).Indexes() {
					//ASSIGN_OR_CRASH(std::shared_ptr<TableStatistics>, stat,
					//	ctx.GetStats(right_tbl->GetSchema().Name()));
					for _, rcol := range right_cols {
						if right_idx.GetTupleSchema().IsHaveColumn(rcol) {
							// candidates.push_back(std::make_shared<ProductPlan>(left, left_cols, *right_tbl, right_idx, right_cols, *stat));
							// right scan plan is not used because IndexJoinExecutor does point scans internally
							candidates = append(candidates, plans.NewIndexJoinPlanNode(so.c, left, parser.ConvColumnStrsToExpIfOnes(so.c, left_cols, true), rightSchema, rightOID, parser.ConvColumnStrsToExpIfOnes(so.c, right_cols, false)))
						}
					}
				}
			}
		}
	}

	// when *where* hash no condition which matches records of *left* and *light*
	if len(candidates) == 0 {
		// append NestedLoopJoinPlan without table concatinating predicate
		// for avoiding no condidate situation
		candidates = append(candidates, plans.NewNestedLoopJoinPlanNode([]plans.Plan{left, right}))
	}

	// attach more selection if we can
	if len(relatedExp) > 0 {
		finalSelection := relatedExp[len(relatedExp)-1]
		relatedExp = relatedExp[:len(relatedExp)-1]

		for _, exp_ := range relatedExp {
			finalSelection = &parser.BinaryOpExpression{expression.AND, -1, finalSelection, exp_}
		}

		orgLen := len(candidates)
		for ii := 0; ii < orgLen; ii++ {
			attachExp := parser.ConvParsedBinaryOpExprToExpIFOne(candidates[ii].OutputSchema(), finalSelection)
			// Note:
			// when candidates[ii] is HashJoinPlanNode or IndexJoinPlanNode, predicate items which
			// are already applied on join process are attached. but ignore the duplication here...
			candidates = append(candidates, plans.NewSelectionPlanNode(candidates[ii], attachExp))
		}
	}

	// TODO: (SDB) [OPT] need to review that cost of join is estimated collectly (SelingerOptimizer::findBestJoin)
	//                   ex: (A(BCD)) =>  join order is (((AB)C)D)
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].AccessRowCount(c) < candidates[j].AccessRowCount(c)
	})
	return candidates[0], nil
}

func (so *SelingerOptimizer) findBestJoin(optimalPlans map[string]CostAndPlan) plans.Plan {
	for ii := 1; ii < len(so.qi.JoinTables_); ii += 1 {
		for baseTableFromOrg, baseTableCP := range optimalPlans {
			baseTableFrom := samehada_util.StringToMapset(baseTableFromOrg)
			for joinTableFromOrg, joinTableCP := range optimalPlans {
				joinTableFrom := samehada_util.StringToMapset(joinTableFromOrg)
				// Note: checking num of tables joined table includes avoids occurring problem
				//       related to adding element into optimalPlans on this range loop
				if containsAny(baseTableFrom, joinTableFrom) || (baseTableFrom.Cardinality()+joinTableFrom.Cardinality() != ii+1) {
					continue
				}
				// Note: for making left-deep Selinger, checking joinTableFrom.Cardinality() == 1 is needed here
				//       current impl can construct bushy plan tree, but it searches more candidates than left-deep Selinger

				bestJoinPlan, _ := so.findBestJoinInner(so.qi.WhereExpression_.GetDeepCopy(), baseTableCP.plan, joinTableCP.plan, so.c)
				fmt.Println(bestJoinPlan)

				joinedTables := baseTableFrom.Union(joinTableFrom)
				common.SH_Assert(1 < joinedTables.Cardinality(), "joinedTables.Cardinality() is illegal!")
				cost := bestJoinPlan.AccessRowCount(so.c)

				if existedPlan, ok := optimalPlans[samehada_util.StrSetToString(joinedTables)]; ok {
					optimalPlans[samehada_util.StrSetToString(joinedTables)] = CostAndPlan{cost, bestJoinPlan}
				} else if cost < existedPlan.cost {
					optimalPlans[samehada_util.StrSetToString(joinedTables)] = CostAndPlan{cost, bestJoinPlan}
				}
			}
		}
	}
	optimalPlan, ok := optimalPlans[samehada_util.StrSetToString(samehada_util.MakeSet(so.qi.JoinTables_))]
	samehada_util.SHAssert(ok, "plan which includes all tables is not found")

	// Attach final projection and emit the result
	solution := optimalPlan.plan
	solution = plans.NewProjectionPlanNode(solution, parser.ConvParsedSelectionExprToSchema(so.c, so.qi.SelectFields_))

	return solution
}

// TODO: (SDB) [OPT] caller should check predicate whether it is optimizable and if not, caller can't call this function (SelingerOptimizer::Optimize)
//                   cases below are not supported now.
//                   - predicate including bracket or OR operation or column name without table name prefix
//                   - projection including asterisk or aggregate operation

// TODO: (SDB) [OPT] adding support of ON clause (Optimize, findBestJoin, findBestJoinInner, findBestScans, findBestScan)
func (so *SelingerOptimizer) Optimize() (plans.Plan, error) {
	optimalPlans := so.findBestScans()
	return so.findBestJoin(optimalPlans), nil
}
