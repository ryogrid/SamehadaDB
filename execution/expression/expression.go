// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package expression

import (
	"github.com/ryogrid/SamehadaDB/storage/table"
	"github.com/ryogrid/SamehadaDB/types"
)

type Expression interface {
	Evaluate(*table.Tuple, *table.Schema) types.Value
}
