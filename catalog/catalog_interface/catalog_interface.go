package catalog_interface

import "github.com/ryogrid/SamehadaDB/storage/index"

type CatalogInterface interface {
	GetRollbackNeededIndexes(map[uint32][]index.Index, uint32) []index.Index
}
