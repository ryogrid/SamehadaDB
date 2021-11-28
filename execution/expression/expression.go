// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in license/go-bustub dir

package expression

import (
	"github.com/ryogrid/SaitomDB/storage/table"
	"github.com/ryogrid/SaitomDB/types"
)

type Expression interface {
	Evaluate(*table.Tuple, *table.Schema) types.Value
}
