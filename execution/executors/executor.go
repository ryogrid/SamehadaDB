// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package executors

import (
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
)

type Done bool

// Executor represents a relational algebra operator in the ite
//
// Init initializes this executor.
// This function must be called before Next() is called!
//
// Next produces the next tuple
type Executor interface {
	Init()
	Next() (*tuple.Tuple, Done, error)
	GetOutputSchema() *schema.Schema
}
