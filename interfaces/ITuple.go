package interfaces

import (
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
)

type ITuple interface {
	// NewTupleFromSchema creates a new tuple based on input value
	GetValue(schema *ISchema, colIndex uint32) types.Value
	Size() uint32
	Data() []byte
	GetRID() *page.RID
	Copy(offset uint32, data []byte)
}
