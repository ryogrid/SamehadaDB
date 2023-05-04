package catalog

import (
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
	// TODO: (SDB) not implemented yet
	return nil
}

func (dc *distinctCounter[T]) Add(value T) {
	// TODO: (SDB) not implemented yet
}

func (dc *distinctCounter[T]) ToColumnStats() *ColumnStats[T] {
	// TODO: (SDB) not implemented yet
	return nil
}

type ColumnStats[T int32 | float32 | string] struct {
	Nax      T
	Min      T
	Count    int64
	Distinct int64
	ColType  types.TypeID
}

func NewColumnStats[T int32 | float32 | string](colType types.TypeID) *ColumnStats[T] {
	// TODO: (SDB) not implemented yet
	return nil
}

func (cs *ColumnStats[T]) UpdateStatistics() {
	// TODO: (SDB) not implemented yet
}

func (cs *ColumnStats[T]) ReductionFactor(sc schema.Schema, planTree plans.Plan) float64 {
	// TODO: (SDB) not implemented yet
	return -1.0
}

func (cs *ColumnStats[T]) EstimateCount() float64 {
	// TODO: (SDB) not implemented yet
	return -1.0
}

type TableStatistics struct {
	// any => *ColumnStats[T]
	stats []any
}

func NewTableStatistics() *TableStatistics {
	// TODO: (SDB) not implemented yet
	return nil
}

func (ts *TableStatistics) Columns() int32 {
	return int32(len(ts.stats))
}

func (ts *TableStatistics) EstimateCount(col_idx int32, from *types.Value, to *types.Value) float64 {
	// TODO: (SDB) not implemented yet
	return -1.0
}

func (ts *TableStatistics) TransformBy(col_idx int32, from *types.Value, to *types.Value) float64 {
	// TODO: (SDB) not implemented yet
	return -1.0
}

func (ts *TableStatistics) Concat(rhs *TableStatistics) {
	// TODO: (SDB) not implemented yet
}
