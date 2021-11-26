package table

import (
	"github.com/ryogrid/SaitomDB/types"
)

type Column struct {
	columnName     string
	columnType     types.TypeID
	fixedLength    uint32 // For a non-inlined column, this is the size of a pointer. Otherwise, the size of the fixed length column
	variableLength uint32 // For an inlined column, 0. Otherwise, the length of the variable length column
	columnOffset   uint32 // Column offset in the tuple
}

func NewColumn(name string, columnType types.TypeID) *Column {
	if columnType != types.Varchar {
		return &Column{name, columnType, columnType.Size(), 0, 0}
	}

	return &Column{name, types.Varchar, 4, 255, 0}
}

func (c *Column) IsInlined() bool {
	return c.columnType != types.Varchar
}

func (c *Column) GetType() types.TypeID {
	return c.columnType
}

func (c *Column) GetOffset() uint32 {
	return c.columnOffset
}

func (c *Column) FixedLength() uint32 {
	return c.fixedLength
}

func (c *Column) VariableLength() uint32 {
	return c.variableLength
}

func (c *Column) GetColumnName() string {
	return c.columnName
}
