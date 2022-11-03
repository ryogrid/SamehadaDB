package catalog_interface

import (
	"github.com/ryogrid/SamehadaDB/storage/index"
	"github.com/ryogrid/SamehadaDB/storage/tuple"
	"github.com/ryogrid/SamehadaDB/types"
)

type CatalogInterface interface {
	GetRollbackNeededIndexes(indexMap map[uint32][]index.Index, oid uint32) []index.Index
	GetColValFromTupleForRollback(tuple_ *tuple.Tuple, colIdx uint32, oid uint32) *types.Value
}
