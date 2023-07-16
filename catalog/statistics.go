package catalog

import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/execution/plans"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
)

type distinctCounter[T int32 | float32 | string] struct {
	max      T
	min      T
	count    int64
	distinct int64
	colType  types.TypeID
	counter  map[T]bool
}

func NewDistinctCounter[T int32 | float32 | string](colType types.TypeID) *distinctCounter[T] {
	// TODO: (SDB) [OPT] not implemented yet (NewDistinctCounter)

	return nil
}

func (dc *distinctCounter[T]) Add(value T) {
	// TODO: (SDB) [OPT] not implemented yet (distinctCounter::Add)
}

func (dc *distinctCounter[T]) ToColumnStats() *ColumnStats[T] {
	// TODO: (SDB) [OPT] not implemented yet (distinctCounter::ToColumnStats)
	return nil
}

type ColumnStats[T int32 | float32 | string] struct {
	Max      T
	Min      T
	Count    int64
	Distinct int64
	ColType  types.TypeID
	Latch    common.ReaderWriterLatch
}

func NewColumnStats[T int32 | float32 | string](colType types.TypeID) *ColumnStats[T] {
	// TODO: (SDB) [OPT] not implemented yet (NewColumnStats)
	return nil
}

func (cs *ColumnStats[T]) UpdateStatistics() {
	// TODO: (SDB) [OPT] not implemented yet (ColumnStats::UpdateStatistics)
}

func (cs *ColumnStats[T]) ReductionFactor(sc schema.Schema, planTree plans.Plan) float64 {
	// TODO: (SDB) [OPT] not implemented yet (ColumnStats::ReductionFactor)
	return -1.0
}

func (cs *ColumnStats[T]) EstimateCount() float64 {
	// TODO: (SDB) [OPT] not implemented yet (ColumnStats::EstimateCount)
	return -1.0
}

type TableStatistics struct {
	// any => *ColumnStats[T]
	colStats []any
}

func NewTableStatistics(schema_ *schema.Schema) *TableStatistics {
	// TODO: (SDB) [OPT] not implemented yet (NewTableStatistics)
	return nil
}

func (ts *TableStatistics) ColumnNum() int32 {
	return int32(len(ts.colStats))
}

func (ts *TableStatistics) EstimateCount(col_idx int32, from *types.Value, to *types.Value) float64 {
	// TODO: (SDB) [OPT] not implemented yet (TableStatistics::EstimateCount)
	return -1.0
}

func (ts *TableStatistics) TransformBy(col_idx int32, from *types.Value, to *types.Value) float64 {
	// TODO: (SDB) [OPT] not implemented yet (TableStatistics::TransformBy)
	return -1.0
}

func (ts *TableStatistics) Concat(rhs *TableStatistics) {
	// TODO: (SDB) [OPT] not implemented yet (TableStatistics::Concat)
}
