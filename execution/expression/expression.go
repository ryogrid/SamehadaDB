package expression

import (
	"github.com/ryogrid/SaitomDB/storage/table"
	"github.com/ryogrid/SaitomDB/types"
)

type Expression interface {
	Evaluate(*table.Tuple, *table.Schema) types.Value
}
