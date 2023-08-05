package catalog

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/expression"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/access"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
)

type distinctCounter struct {
	max      *types.Value
	min      *types.Value
	count    int64
	distinct int64
	colType  types.TypeID
	counter  map[interface{}]bool
}

func NewDistinctCounter(colType types.TypeID) *distinctCounter {
	switch colType {
	case types.Integer:
		return &distinctCounter{samehada_util.GetPonterOfValue(types.NewInteger(math.MaxInt32)), samehada_util.GetPonterOfValue(types.NewInteger(math.MinInt32)), 0, 0, colType, make(map[interface{}]bool, 0)}
	case types.Float:
		return &distinctCounter{samehada_util.GetPonterOfValue(types.NewFloat(math.MaxFloat32)), samehada_util.GetPonterOfValue(types.NewFloat(math.SmallestNonzeroFloat32)), 0, 0, colType, make(map[interface{}]bool, 0)}
	case types.Varchar:
		return &distinctCounter{samehada_util.GetPonterOfValue(types.NewVarchar("")).SetInfMax(), samehada_util.GetPonterOfValue(types.NewVarchar("")).SetInfMin(), 0, 0, colType, make(map[interface{}]bool, 0)}
	default:
		panic("unkown type")
	}
}

// ret shallow copied addr
func retValAccordingToCompareResult(compReslt bool, trueVal *types.Value, falseVal *types.Value) *types.Value {
	if compReslt {
		return trueVal
	} else {
		return falseVal
	}
}

func (dc *distinctCounter) Add(value *types.Value) {
	//max = max < v ? v : max;
	//min = v < min ? v : min;
	dc.max = retValAccordingToCompareResult(dc.max.CompareLessThan(*value), value, dc.max)
	dc.min = retValAccordingToCompareResult(dc.min.CompareGreaterThan(*value), value, dc.min)
	// counter_.insert(v);
	dc.counter[value.ToIFValue()] = true
	dc.count++
}

func (dc *distinctCounter) Output(o *columnStats) {
	// TODO: (SDB) [OPT] not implemented yet (distinctCounter::Output)

	/*
	   	o.max = max;
	    o.min = min;
	    o.count = count;
	    o.distinct = counter_.size();
	*/
}

/*
func (dc *distinctCounter[T]) ToColumnStats() *ColumnStats[T] {
	return nil
}
*/

type columnStats struct {
	max      *types.Value
	min      *types.Value
	count    int64
	distinct int64
	colType  types.TypeID
	latch    common.ReaderWriterLatch
}

func NewColumnStats(colType types.TypeID) *columnStats {
	switch colType {
	case types.Integer:
		return &columnStats{samehada_util.GetPonterOfValue(types.NewInteger(math.MaxInt32)), samehada_util.GetPonterOfValue(types.NewInteger(math.MinInt32)), 0, 0, colType, common.NewRWLatch()}
	case types.Float:
		return &columnStats{samehada_util.GetPonterOfValue(types.NewFloat(math.MaxFloat32)), samehada_util.GetPonterOfValue(types.NewFloat(math.SmallestNonzeroFloat32)), 0, 0, colType, common.NewRWLatch()}
	case types.Varchar:
		return &columnStats{samehada_util.GetPonterOfValue(types.NewVarchar("")).SetInfMax(), samehada_util.GetPonterOfValue(types.NewVarchar("")).SetInfMin(), 0, 0, colType, common.NewRWLatch()}
	default:
		panic("unkown type")
	}
}

func (cs *columnStats) Count() int32 {
	// TODO: (SDB) [OPT] not implemented yet (ColumnStats::Count)
	/*
	   	    switch (type) {
	            case ValueType::kNull:
	              assert(!"never reach here");
	            case ValueType::kInt64:
	              return stat.int_stats.count;
	            case ValueType::kVarChar:
	              return stat.varchar_stats.count;
	            case ValueType::kDouble:
	              return stat.double_stats.count;
	          }
	*/
	panic("not implemented yet (ColumnStats::Count)")
}

func (cs *columnStats) Distinct() int32 {
	// TODO: (SDB) [OPT] not implemented yet (ColumnStats::Distinct)
	/*
	   	    switch (type) {
	            case ValueType::kNull:
	              assert(!"never reach here");
	            case ValueType::kInt64:
	              return stat.int_stats.distinct;
	            case ValueType::kVarChar:
	              return stat.varchar_stats.distinct;
	            case ValueType::kDouble:
	              return stat.double_stats.distinct;
	          }
	          abort();
	          return 0;
	*/
	panic("not implemented yet (ColumnStats::Distinct)")
}

func (cs *columnStats) Check(sample *types.Value) {
	// TODO: (SDB) [OPT] not implemented yet (ColumnStats::Check)

	/*
	   max = std::max(max, sample.value.int_value);
	   min = std::min(min, sample.value.int_value);
	   ++count;
	*/
}

/*
func (cs *ColumnStats[T]) UpdateStatistics() {
}

func (cs *ColumnStats[T]) ReductionFactor(sc schema.Schema, planTree plans.Plan) float64 {
	return -1.0
}
*/

func (cs *columnStats) EstimateCount() float64 {
	// TODO: (SDB) [OPT] not implemented yet (ColumnStats::EstimateCount)

	/*
	   switch (type) {
	     case ValueType::kNull:
	       assert(!"never reach here");
	     case ValueType::kInt64:
	       return stat.int_stats.EstimateCount(from, to);
	     case ValueType::kVarChar:
	       assert(!"never reach here");
	     case ValueType::kDouble:
	       assert(!"never reach here");
	   }
	   abort();
	   return 0.0;
	*/
	return -1.0
}

type TableStatistics struct {
	colStats []*columnStats
}

func NewTableStatistics(schema_ *schema.Schema) *TableStatistics {
	colStats := make([]*columnStats, 0)
	for ii := 0; ii < int(schema_.GetColumnCount()); ii++ {
		colStats = append(colStats, NewColumnStats(schema_.GetColumn(uint32(ii)).GetType()))
	}
	return &TableStatistics{colStats}
}

func (ts *TableStatistics) Update(target *TableMetadata, txn *access.Transaction) error {
	rows := 0
	schema_ := target.Schema()
	//Iterator it = target.BeginFullScan(txn);
	it := target.Table().Iterator(txn)

	distCounters := make([]*distinctCounter, 0)
	for ii := 0; ii < int(schema_.GetColumnCount()); ii++ {
		distCounters = append(distCounters, NewDistinctCounter(schema_.GetColumn(uint32(ii)).GetType()))
	}

	for !it.End() {
		//const Row& row = *it;
		tuple_ := it.Next()
		for ii := 0; ii < len(ts.colStats); ii++ {
			distCounters[ii].Add(samehada_util.GetPonterOfValue(tuple_.GetValue(schema_, uint32(ii))))
		}
		rows++
	}
	for ii := 0; ii < len(ts.colStats); ii++ {
		cs := ts.colStats[ii]
		switch cs.colType {
		case types.Null:
			panic("never reach here")
		case types.Integer:
			distCounters[ii].Output(cs)
		case types.Float:
			distCounters[ii].Output(cs)
		case types.Varchar:
			distCounters[ii].Output(cs)
		default:
			panic("unkown or not supported type")
		}
	}

	return nil
}

func isBinaryExp(exp expression.Expression) bool {
	return exp.GetType() == expression.EXPRESSION_TYPE_COMPARISON || exp.GetType() == expression.EXPRESSION_TYPE_LOGICAL_OP
}

// Returns estimated inverted selection ratio if the `sc` is selected by
// `predicate`. If the predicate selects rows to 1 / x, returns x.
// Returning 1 means no selection (pass through).
func (ts *TableStatistics) ReductionFactor(sc schema.Schema, predicate expression.Expression) float64 {
	samehada_util.SHAssert(sc.GetColumnCount() > 0, "no column in schema")
	if isBinaryExp(predicate) {
		boCmp, okCmp := predicate.(*expression.Comparison)
		if okCmp && boCmp.GetComparisonType() == expression.Equal {
			if boCmp.GetChildAt(0).GetType() == expression.EXPRESSION_TYPE_COLUMN_VALUE &&
				boCmp.GetChildAt(1).GetType() == expression.EXPRESSION_TYPE_COLUMN_VALUE {
				// 			     const auto* lcv =
				//			         reinterpret_cast<const ColumnValue*>(bo->Left().get());
				//			     const auto* rcv =
				//			         reinterpret_cast<const ColumnValue*>(bo->Right().get());
				//			     if (columns.find(lcv->GetColumnName()) != columns.end() &&
				//			         columns.find(rcv->GetColumnName()) != columns.end()) {
				//			       int offset_left = sc.Offset(lcv->GetColumnName());
				//			       assert(0 <= offset_left && offset_left < (int)stats_.size());
				//			       int offset_right = sc.Offset(rcv->GetColumnName());
				//			       assert(0 <= offset_right && offset_right < (int)stats_.size());
				lcv := boCmp.GetChildAt(0).(*expression.ColumnValue)
				rcv := boCmp.GetChildAt(1).(*expression.ColumnValue)
				colIndexLeft := lcv.GetColIndex()
				samehada_util.SHAssert(colIndexLeft >= 0 && int(colIndexLeft) < len(ts.colStats), "invalid column index (Left)")
				colIndexRight := rcv.GetColIndex()
				samehada_util.SHAssert(colIndexRight >= 0 && int(colIndexRight) < len(ts.colStats), "invalid column index (Right)")
				// return std::min(static_cast<double>(stats_[offset_left].distinct()),static_cast<double>(stats_[offset_right].distinct()));
				return math.Min(float64(ts.colStats[colIndexLeft].Distinct()), float64(ts.colStats[colIndexRight].Distinct()))
			}
			if boCmp.GetChildAt(0).GetType() == expression.EXPRESSION_TYPE_COLUMN_VALUE {
				// 				   const auto* lcv =
				//				       reinterpret_cast<const ColumnValue*>(bo->Left().get());
				//				   LOG(WARN) << lcv->GetColumnName() << " in " << sc;
				//				   int offset_left = sc.Offset(lcv->GetColumnName());
				//				   assert(0 <= offset_left && offset_left < (int)stats_.size());
				lcv := boCmp.GetChildAt(0).(*expression.ColumnValue)
				colIndexLeft := lcv.GetColIndex()
				samehada_util.SHAssert(colIndexLeft >= 0 && int(colIndexLeft) < len(ts.colStats), "invalid column index (Left)")
				// return static_cast<double>(stats_[offset_left].distinct());
				return float64(ts.colStats[colIndexLeft].Distinct())
			}
			if boCmp.GetChildAt(1).GetType() == expression.EXPRESSION_TYPE_COLUMN_VALUE {
				// 				   const auto* rcv =
				//				       reinterpret_cast<const ColumnValue*>(bo->Left().get());
				//				   int offset_right = sc.Offset(rcv->GetColumnName());
				rcv := boCmp.GetChildAt(1).(*expression.ColumnValue)
				colIndexRight := rcv.GetColIndex()
				samehada_util.SHAssert(colIndexRight >= 0 && int(colIndexRight) < len(ts.colStats), "invalid column index (Right)")
				// return static_cast<double>(stats_[offset_right].distinct());
				return float64(ts.colStats[colIndexRight].Distinct())
			}
			if boCmp.GetChildAt(0).GetType() == expression.EXPRESSION_TYPE_CONSTANT_VALUE &&
				boCmp.GetChildAt(1).GetType() == expression.EXPRESSION_TYPE_CONSTANT_VALUE {
				left := boCmp.GetChildAt(0).(*expression.ConstantValue).GetValue()
				right := boCmp.GetChildAt(1).(*expression.ConstantValue).GetValue()
				if left.CompareEquals(*right) {
					return 1
				}
				// return std::numeric_limits<double>::max();
				return math.MaxFloat64
			}
		}
		// TODO: (SDB) [OPT] GreaterThan, GreaterEqual, LessThan, LessEqual, NotEqual

		boLogi, okLogi := predicate.(*expression.LogicalOp)
		if okLogi {
			if boLogi.GetLogicalOpType() == expression.AND {
				// return ReductionFactor(sc, bo->Left()) * ReductionFactor(sc, bo->Right());
				return ts.ReductionFactor(sc, boLogi.GetChildAt(0)) * ts.ReductionFactor(sc, boLogi.GetChildAt(1))
			}
			if boLogi.GetLogicalOpType() == expression.OR {
				// TODO: what should be returned?
				// return ReductionFactor(sc, bo->Left()) + ReductionFactor(sc, bo->Right());
				return ts.ReductionFactor(sc, boLogi.GetChildAt(0)) * ts.ReductionFactor(sc, boLogi.GetChildAt(1))
			}
		}
	}

	return 1
}

func (ts *TableStatistics) ColumnNum() int32 {
	return int32(len(ts.colStats))
}

func (ts *TableStatistics) EstimateCount(col_idx int32, from *types.Value, to *types.Value) float64 {
	// TODO: (SDB) [OPT] not implemented yet (TableStatistics::EstimateCount)

	/*
	  if (to <= from) {
	    std::swap(from, to);
	  }
	  assert(from <= to);
	  from = std::max(min, from);
	  to = std::min(max, to);
	  return (from - to) * static_cast<double>(count) / distinct;
	*/
	return -1.0
}

func (ts *TableStatistics) TransformBy(col_idx int32, from *types.Value, to *types.Value) float64 {
	// TODO: (SDB) [OPT] not implemented yet (TableStatistics::TransformBy)

	/*
	   TableStatistics ret(*this);
	   double multiplier = EstimateCount(col_idx, from, to);
	   for (auto& st : ret.stats_) {
	     st *= multiplier / st.count();
	   }
	   return ret;
	*/
	return -1.0
}

func (ts *TableStatistics) Concat(rhs *TableStatistics) {
	// TODO: (SDB) [OPT] not implemented yet (TableStatistics::Concat)

	/*
	   stats_.reserve(stats_.size() + rhs.stats_.size());
	   for (const auto& s : rhs.stats_) {
	     stats_.emplace_back(s);
	   }
	*/
}
