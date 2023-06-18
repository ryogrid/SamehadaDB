package optimizer

import (
	stack "github.com/golang-collections/collections/stack"
	pair "github.com/notEpsilon/go-pair"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/parser"
	"github.com/ryogrid/SamehadaDB/types"
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

func (so *SelingerOptimizer) bestJoin(where *parser.BinaryOpExpression, left plans.Plan, right plans.Plan) (error, plans.Plan) {
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
	var relatedExp = make([]interface{}, 0)
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

	/*
	  std::vector<Plan> candidates;
	  if (!equals.empty()) {
	    std::vector<ColumnName> left_cols;
	    std::vector<ColumnName> right_cols;
	    left_cols.reserve(equals.size());
	    right_cols.reserve(equals.size());
	    for (auto&& cn : equals) {
	      left_cols.emplace_back(std::move(cn.first));
	      right_cols.emplace_back(std::move(cn.second));
	    }
	    // HashJoin.
	    candidates.push_back(
	        std::make_shared<ProductPlan>(left, left_cols, right, right_cols));
	    candidates.push_back(
	        std::make_shared<ProductPlan>(right, right_cols, left, left_cols));

	    // IndexJoin.
	    // 通常のSelingerと異なりベースとするテーブルは既にjoinされている前提で処理している
	    // また、最適コストを算出済みの部分組み合わせは一方向にjoinしていく前提ではない？？？
	    // (右側にしか新たに加えるテーブルを置かないルールで、左から右にjoinしていくことになる？？？
	    //  HashJoinの '新 ++ ベース' のケースがあるのでそうはならない？？？)
	    if (const Table* right_tbl = right->ScanSource()) {
	      for (size_t i = 0; i < right_tbl->IndexCount(); ++i) {
	        const Index& right_idx = right_tbl->GetIndex(i);
	        const Schema& right_schema = right_tbl->GetSchema();
	        ASSIGN_OR_CRASH(std::shared_ptr<TableStatistics>, stat,
	                        ctx.GetStats(right_tbl->GetSchema().Name()));
	        for (const auto& rcol : right_cols) {
	          if (rcol == right_schema.GetColumn(right_idx.sc_.key_[0]).Name()) {
	            candidates.push_back(std::make_shared<ProductPlan>(
	                left, left_cols, *right_tbl, right_idx, right_cols, *stat));
	          }
	        }
	      }
	    }
	  }
	  if (candidates.empty()) {
	    if (0 < related_exp.size()) {
	      Expression final_selection = related_exp.back();
	      related_exp.pop_back();
	      // T1.C1 = T2.C2 のようなカラム値が同値かチェックする item が大本のwhere句にあったが
	      // 上のwhileループ内でcandidatesに加えられるような組み合わせが存在しなかった場合candidatesが
	      // 空なのでここにきて、ここでは使われなかった条件を活かして出力カラム数を減らす用selectionを構築する？
	      for (const auto& e : related_exp) {
	        final_selection =
	            BinaryExpressionExp(e, BinaryOperation::kAnd, final_selection);
	      }
	      Plan ans = std::make_shared<ProductPlan>(left, right);
	      candidates.push_back(std::make_shared<SelectionPlan>(ans, final_selection,
	                                                           ans->GetStats()));
	    } else {
	      // T1.C1 = T2.C2 のようなカラム値が同値かチェックする item が大本のwhere句に無かった場合
	      // 上のwhileループがスルーされ、candidatesが空なのでここにくる。
	      // 活かすことのできる同値の条件が存在しないためselectionの付加は行わない？
	      candidates.push_back(std::make_shared<ProductPlan>(left, right));
	    }
	  }
	  std::sort(candidates.begin(), candidates.end(),
	            [](const Plan& a, const Plan& b) {
	              return a->AccessRowCount() < b->AccessRowCount();
	            });
	  return candidates[0];
	*/

	return nil, nil
}

func (so *SelingerOptimizer) Optimize() (error, plans.Plan) {
	// TODO: (SDB) not implemented yet
	return nil, nil
}
