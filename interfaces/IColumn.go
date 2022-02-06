package interfaces

import "github.com/ryogrid/SamehadaDB/types"

type IColumn interface {
	IsInlined() bool
	GetType() types.TypeID
	GetOffset() uint32
	FixedLength() uint32
	VariableLength() uint32
	GetColumnName() string
}
