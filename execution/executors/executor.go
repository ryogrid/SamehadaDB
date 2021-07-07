package executors

import (
	"github.com/brunocalza/go-bustub/storage/table"
)

// Executor executes a plan
//
// Init initializes this executor.
// This function must be called before Next() is called!
//
// Next produces the next tuple from this executor
type Executor interface {
	Init()
	Next() (*table.Tuple, bool, error)
}
