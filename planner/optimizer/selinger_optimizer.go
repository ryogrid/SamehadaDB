package optimizer

import (
	stack "github.com/golang-collections/collections/stack"
	pair "github.com/notEpsilon/go-pair"
	"github.com/ryogrid/SamehadaDB/catalog"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
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
	DIR_LIGHT Direction = false
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
	minimamCost := math.MaxUint64
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
	stack := stack.New[*parser.BinaryOpExpression]()
	stack.Push(where)
	//std::vector<Expression> related_ops
	relatedOps := make([]*parser.BinaryOpExpression, 0)
	for stack.Len() > 0 {
		//Expression exp = stack.back()
		//stack.pop_back()
		exp := stack.Pop().(*parser.BinaryOpExpression)
		//if (exp->Type() == TypeTag::kBinaryExp) {
		if exp.GetType() == parser.Logical {
			/*
					const BinaryExpression& be = exp->AsBinaryExpression();
					if (be.Op() == BinaryOperation::kAnd) {
						stack.push_back(be.Left());
						stack.push_back(be.Right());
						continue;
					}
					if (IsComparison(be.Op())) {
						if (be.Left()->Type() == TypeTag::kColumnValue &&
							be.Right()->Type() == TypeTag::kConstantValue) {
							const ColumnValue& cv = be.Left()->AsColumnValue();
							const int offset = sc.Offset(cv.GetColumnName());
							if (0 <= offset) {
								related_ops.push_back(exp);
								auto iter = ranges.find(offset);
								if (iter != ranges.end()) {
									iter->second.Update(be.Op(),
										be.Right()->AsConstantValue().GetValue(),
										Range::Dir::kRight);
								}
							}
						} else if (be.Left()->Type() == TypeTag::kColumnValue &&
							be.Right()->Type() == TypeTag::kConstantValue) {
							const ColumnValue& cv = be.Right()->AsColumnValue();
							const int offset = sc.Offset(cv.GetColumnName());
							if (0 <= offset) {
								related_ops.push_back(exp);
								auto iter = ranges.find(offset);
								if (iter != ranges.end()) {
									iter->second.Update(be.Op(),
										be.Left()->AsConstantValue().GetValue(),
										Range::Dir::kLeft);
								}
							}
						}
					}
					if (be.Op() == BinaryOperation::kOr) {
						assert(!"Not supported!");
					}
				}
			*/
		}
	}

	// Expression scan_exp;
	// TODO: (SDB) doing BinaryOpExpression to Expression subtype conversion is better here?
	var scanExp *parser.BinaryOpExpression
	if len(relatedOps) > 0 {
		scanExp = relatedOps[0]
		for ii := 1; ii < len(relatedOps); ii++ {
			/*
			   scan_exp =
			       BinaryExpressionExp(scan_exp, BinaryOperation::kAnd, related_ops[i]);
			*/
		}
	}

	// Build all IndexScan.
	//for (const auto& range : ranges) {
	//slot_t key = range.first;
	//const Range& span = range.second;
	for key, span := range ranges {
		/*
		   if (span.Empty()) {
		     continue;
		   }
		   const Index& target_idx = from.GetIndex(candidates[key]);
		   Plan new_plan = IndexScanSelect(from, target_idx, stat, *span.min,
		                                   *span.max, scan_exp, select);
		   // (ryo_grid) ?????
		   if (!TouchOnly(scan_exp, from.GetSchema().GetColumn(key).Name())) {
		     new_plan = std::make_shared<SelectionPlan>(new_plan, scan_exp, stat);
		   }
		   if (select.size() != new_plan->GetSchema().ColumnCount()) {
		     new_plan = std::make_shared<ProjectionPlan>(new_plan, select);
		   }
		   if (new_plan->AccessRowCount() < minimum_cost) {
		     best_scan = new_plan;
		     minimum_cost = new_plan->AccessRowCount();
		   }
		*/
	}

	/*
	  Plan full_scan_plan(new FullScanPlan(from, stat));
	  if (scan_exp) {
	    full_scan_plan =
	        std::make_shared<SelectionPlan>(full_scan_plan, scan_exp, stat);
	  }
	  if (select.size() != full_scan_plan->GetSchema().ColumnCount()) {
	    full_scan_plan = std::make_shared<ProjectionPlan>(full_scan_plan, select);
	  }
	  if (full_scan_plan->AccessRowCount() < minimum_cost) {
	    best_scan = full_scan_plan;
	    minimum_cost = full_scan_plan->AccessRowCount();
	  }
	  return best_scan;
	*/

	return nil, nil
}

func (so *SelingerOptimizer) bestJoin(where *parser.BinaryOpExpression, left plans.Plan, right plans.Plan) (plans.Plan, error) {
	// TODO: (SDB) not implemented yet

	isColumnName := func(v interface{}) bool {
		switch v.(type) {
		case *string:
			return true
		default:
			return false
		}
	}

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

// TODO: (SDB) adding support of ON clause (Optimize, bestJoin, bestScan)
func (so *SelingerOptimizer) Optimize() (plans.Plan, error) {
	// TODO: (SDB) not implemented yet
	return nil, nil
}
