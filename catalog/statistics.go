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
		return &distinctCounter{samehada_util.GetPonterOfValue(types.NewInteger(math.MinInt32)), samehada_util.GetPonterOfValue(types.NewInteger(math.MaxInt32)), 0, 0, colType, make(map[interface{}]bool, 0)}
	case types.Float:
		return &distinctCounter{samehada_util.GetPonterOfValue(types.NewFloat(math.SmallestNonzeroFloat32)), samehada_util.GetPonterOfValue(types.NewFloat(math.MaxFloat32)), 0, 0, colType, make(map[interface{}]bool, 0)}
	case types.Varchar:
		return &distinctCounter{samehada_util.GetPonterOfValue(types.NewVarchar("")).SetInfMin(), samehada_util.GetPonterOfValue(types.NewVarchar("")).SetInfMax(), 0, 0, colType, make(map[interface{}]bool, 0)}
	case types.Boolean:
		return &distinctCounter{samehada_util.GetPonterOfValue(types.NewBoolean(false)).SetInfMin(), samehada_util.GetPonterOfValue(types.NewBoolean(true)).SetInfMax(), 0, 0, colType, make(map[interface{}]bool, 0)}
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
	dc.max = retValAccordingToCompareResult(dc.max.CompareLessThan(*value), value, dc.max)
	dc.min = retValAccordingToCompareResult(dc.min.CompareGreaterThan(*value), value, dc.min)
	dc.counter[value.ToIFValue()] = true
	dc.count++
}

func (dc *distinctCounter) Output(o *columnStats) {
	o.max = dc.max
	o.min = dc.min
	o.count = dc.count
	// o.distinct = counter_.size();
	o.distinct = int64(len(dc.counter))
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
		return &columnStats{samehada_util.GetPonterOfValue(types.NewInteger(math.MinInt32)), samehada_util.GetPonterOfValue(types.NewInteger(math.MaxInt32)), 0, 0, colType, common.NewRWLatch()}
	case types.Float:
		return &columnStats{samehada_util.GetPonterOfValue(types.NewFloat(math.SmallestNonzeroFloat32)), samehada_util.GetPonterOfValue(types.NewFloat(math.MaxFloat32)), 0, 0, colType, common.NewRWLatch()}
	case types.Varchar:
		return &columnStats{samehada_util.GetPonterOfValue(types.NewVarchar("")).SetInfMin(), samehada_util.GetPonterOfValue(types.NewVarchar("")).SetInfMax(), 0, 0, colType, common.NewRWLatch()}
	case types.Boolean:
		return &columnStats{samehada_util.GetPonterOfValue(types.NewBoolean(true)).SetInfMin(), samehada_util.GetPonterOfValue(types.NewBoolean(false)).SetInfMax(), 0, 0, colType, common.NewRWLatch()}
	default:
		panic("unkown type")
	}
}

func (cs *columnStats) Count() int64 {
	cs.latch.RLock()
	defer cs.latch.RUnlock()

	switch cs.colType {
	case types.Integer:
		return cs.count
	case types.Float:
		return cs.count
	case types.Varchar:
		return cs.count
	case types.Boolean:
		return cs.count
	default:
		panic("unkown or not supported type")
	}
}

func (cs *columnStats) Distinct() int64 {
	cs.latch.RLock()
	defer cs.latch.RUnlock()

	switch cs.colType {
	case types.Integer:
		return cs.distinct
	case types.Float:
		return cs.distinct
	case types.Varchar:
		return cs.distinct
	case types.Boolean:
		return cs.distinct
	default:
		panic("unkown or not supported type")
	}
}

func (cs *columnStats) Check(sample *types.Value) {
	cs.latch.WLock()
	defer cs.latch.WUnlock()

	cs.max = retValAccordingToCompareResult(sample.CompareGreaterThan(*cs.max), sample, cs.max)
	cs.min = retValAccordingToCompareResult(sample.CompareLessThan(*cs.min), sample, cs.min)
	cs.count++
}

/*
func (cs *ColumnStats[T]) UpdateStatistics() {
}

func (cs *ColumnStats[T]) ReductionFactor(sc schema.Schema, planTree plans.Plan) float64 {
	return -1.0
}
*/

func (cs *columnStats) EstimateCount(from *types.Value, to *types.Value) float64 {
	cs.latch.WLock()
	defer cs.latch.WUnlock()

	if cs.colType == types.Integer || cs.colType == types.Float {
		if to.CompareLessThanOrEqual(*from) {
			to.Swap(from)
		}
		samehada_util.SHAssert(from.CompareLessThanOrEqual(*to), "from must be less than or equal to to")
		from = retValAccordingToCompareResult(from.CompareLessThan(*cs.min), cs.min, from)
		to = retValAccordingToCompareResult(to.CompareLessThan(*cs.max), to, cs.max)
		tmpVal := to.Sub(from)
		if cs.colType == types.Integer {
			return float64(tmpVal.ToInteger()) * float64(cs.count) / float64(cs.distinct)
		} else { // Float
			return float64(tmpVal.ToFloat()) * float64(cs.count) / float64(cs.distinct)
		}
	} else if cs.colType == types.Varchar {
		if to.CompareLessThanOrEqual(*from) {
			to.Swap(from)
		}
		if to.CompareLessThan(*cs.min) || cs.max.CompareLessThanOrEqual(*from) {
			return 1
		}
		return 2 // FIXME: there must be a better estimation!
	} else if cs.colType == types.Boolean {
		return 1 // FIXME: there must be a better estimation!
	} else {
		panic("unkown or not supported type")
	}
}

func (cs *columnStats) Multiply(multiplier float64) *columnStats {
	cs.count = int64(math.Floor(float64(cs.count) * multiplier))
	cs.distinct = int64(math.Floor(float64(cs.distinct) * multiplier))
	return cs
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
	it := target.Table().Iterator(txn)

	distCounters := make([]*distinctCounter, 0)
	for ii := 0; ii < int(schema_.GetColumnCount()); ii++ {
		distCounters = append(distCounters, NewDistinctCounter(schema_.GetColumn(uint32(ii)).GetType()))
	}

	for t := it.Current(); !it.End(); t = it.Next() {
		for ii := 0; ii < len(ts.colStats); ii++ {
			distCounters[ii].Add(samehada_util.GetPonterOfValue(t.GetValue(schema_, uint32(ii))))
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
		case types.Boolean:
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
func (ts *TableStatistics) ReductionFactor(sc *schema.Schema, predicate expression.Expression) float64 {
	samehada_util.SHAssert(sc.GetColumnCount() > 0, "no column in schema")
	if isBinaryExp(predicate) {
		boCmp, okCmp := predicate.(*expression.Comparison)
		if okCmp && boCmp.GetComparisonType() == expression.Equal {
			if boCmp.GetChildAt(0).GetType() == expression.EXPRESSION_TYPE_COLUMN_VALUE &&
				boCmp.GetChildAt(1).GetType() == expression.EXPRESSION_TYPE_COLUMN_VALUE {
				lcv := boCmp.GetChildAt(0).(*expression.ColumnValue)
				rcv := boCmp.GetChildAt(1).(*expression.ColumnValue)
				colIndexLeft := lcv.GetColIndex()
				samehada_util.SHAssert(colIndexLeft >= 0 && int(colIndexLeft) < len(ts.colStats), "invalid column index (Left)")
				colIndexRight := rcv.GetColIndex()
				samehada_util.SHAssert(colIndexRight >= 0 && int(colIndexRight) < len(ts.colStats), "invalid column index (Right)")
				return math.Min(float64(ts.colStats[colIndexLeft].Distinct()), float64(ts.colStats[colIndexRight].Distinct()))
			}
			if boCmp.GetChildAt(0).GetType() == expression.EXPRESSION_TYPE_COLUMN_VALUE {
				lcv := boCmp.GetChildAt(0).(*expression.ColumnValue)
				colIndexLeft := lcv.GetColIndex()
				samehada_util.SHAssert(colIndexLeft >= 0 && int(colIndexLeft) < len(ts.colStats), "invalid column index (Left)")
				// return static_cast<double>(stats_[offset_left].distinct());
				return float64(ts.colStats[colIndexLeft].Distinct())
			}
			if boCmp.GetChildAt(1).GetType() == expression.EXPRESSION_TYPE_COLUMN_VALUE {
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
				return math.MaxFloat64
			}
		}
		// TODO: (SDB) GreaterThan, GreaterEqual, LessThan, LessEqual, NotEqual (TableStatistics::ReductionFactor)

		boLogi, okLogi := predicate.(*expression.LogicalOp)
		if okLogi {
			if boLogi.GetLogicalOpType() == expression.AND {
				return ts.ReductionFactor(sc, boLogi.GetChildAt(0)) * ts.ReductionFactor(sc, boLogi.GetChildAt(1))
			}
			if boLogi.GetLogicalOpType() == expression.OR {
				// TODO: what should be returned?
				return ts.ReductionFactor(sc, boLogi.GetChildAt(0)) * ts.ReductionFactor(sc, boLogi.GetChildAt(1))
			}
		}
	}

	return 1
}

func (ts *TableStatistics) Rows() uint64 {
	ans := uint64(0)
	for _, st := range ts.colStats {
		ans = uint64(math.Max(float64(ans), float64(st.Count())))
	}
	return ans
}

func (ts *TableStatistics) ColumnNum() int32 {
	return int32(len(ts.colStats))
}

func (ts *TableStatistics) EstimateCount(col_idx int32, from *types.Value, to *types.Value) float64 {
	return ts.colStats[col_idx].EstimateCount(from, to)
}

func (ts *TableStatistics) TransformBy(col_idx int32, from *types.Value, to *types.Value) *TableStatistics {
	multiplier := ts.EstimateCount(col_idx, from, to)
	for _, st := range ts.colStats {
		st.Multiply(multiplier / float64(st.Count()))
	}

	return ts
}

func (ts *TableStatistics) Concat(rhs *TableStatistics) {
	for _, s := range rhs.colStats {
		ts.colStats = append(ts.colStats, s)
	}
}

func (ts *TableStatistics) Multiply(multiplier float64) {
	for _, st := range ts.colStats {
		st.Multiply(multiplier)
	}
}

func (ts *TableStatistics) GetDeepCopy() *TableStatistics {
	// TODO: (SDB) [OPT] need to implement TableStatistics::GetDeepCopy method and its testing
	// TODO: (SDB) [OPT] need to changed calls of SamehadaUtil::DeepCopy for TableStatistics object to TableStatistics::GetDeepCopy method
	panic("not implemented yet")
}
