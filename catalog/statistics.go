package catalog

type TableStatistics[T int32 | float32 | string] struct {
	Nax      T
	Min      T
	Count    int64
	Distinct int64
}

func NewTableStatistics[T int32 | float32 | string]() *TableStatistics[T] {
	// TODO: (SDB) not implemented yet
	return nil
}

func (ts *TableStatistics[T]) UpdateStatistics() {
	// TODO: (SDB) not implemented yet
}
