package index

import (
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
)

type IndexRangeScanIterator interface {
	Next() (bool, error, *types.Value, *page.RID)
}
