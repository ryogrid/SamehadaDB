// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package column

import (
	"github.com/ryogrid/SamehadaDB/types"
)

type Column struct {
	columnName     string
	columnType     types.TypeID
	fixedLength    uint32 // For a non-inlined column, this is the size of a pointer. Otherwise, the size of the fixed length column
	variableLength uint32 // For an inlined column, 0. Otherwise, the length of the variable length column
	columnOffset   uint32 // Column offset in the tuple
	hasIndex       bool   // whether the column has index data
	isLeft         bool   // when temporal schema, this is used for join
}

func NewColumn(name string, columnType types.TypeID, hasIndex bool) *Column {
	if columnType != types.Varchar {
		return &Column{name, columnType, columnType.Size(), 0, 0, hasIndex, true}
	}

	return &Column{name, types.Varchar, 4, 255, 0, hasIndex, true}
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

func (c *Column) SetOffset(offset uint32) {
	c.columnOffset = offset
}

func (c *Column) FixedLength() uint32 {
	return c.fixedLength
}

func (c *Column) SetFixedLength(fixedLength uint32) {
	c.fixedLength = fixedLength
}

func (c *Column) VariableLength() uint32 {
	return c.variableLength
}

func (c *Column) SetVariableLength(variableLength uint32) {
	c.variableLength = variableLength
}

func (c *Column) GetColumnName() string {
	return c.columnName
}

func (c *Column) HasIndex() bool {
	return c.hasIndex
}

func (c *Column) SetHasIndex(hasIndex bool) {
	c.hasIndex = hasIndex
}

// // TODO: (SDB) dummy method for pass compile
// func (c *Column) GetExpr() interface{} {
// 	panic("GetExpr is dummy")
// }

func (c *Column) IsLeft() bool {
	return c.isLeft
}

func (c *Column) SetIsLeft(isLeft bool) {
	c.isLeft = isLeft
}
