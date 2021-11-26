package executors

import "github.com/ryogrid/SaitomDB/storage/table"

type Done bool

// Executor represents a relational algebra operator in the ite
//
// Init initializes this executor.
// This function must be called before Next() is called!
//
// Next produces the next tuple
type Executor interface {
	Init()
	Next() (*table.Tuple, Done, error)
}
