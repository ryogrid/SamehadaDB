package index

import (
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

type IndexRangeScanIterator interface {
	Next() (bool, error, *types.Value, *page.RID)
}
