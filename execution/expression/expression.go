package expression

import (
	"github.com/brunocalza/go-bustub/storage/table"
	"github.com/brunocalza/go-bustub/types"
)

type Expression interface {
	Evaluate(*table.Tuple, *table.Schema) types.Value
}
