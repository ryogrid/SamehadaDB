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

func NewSelingerOptimizer() Optimizer {
	// TODO: (SDB) not implemented yet
	return nil
}

func (so *SelingerOptimizer) bestScan() (error, plans.Plan) {
	// TODO: (SDB) not implemented yet
	return nil, nil
}

func (so *SelingerOptimizer) bestJoin(where *parser.BinaryOpExpression, left plans.Plan, right plans.Plan) plans.Plan {
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
	exp := stack.New()
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
	return candidates[0]
}

func (so *SelingerOptimizer) Optimize() (error, plans.Plan) {
	// TODO: (SDB) not implemented yet
	return nil, nil
}
