// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package column

import (
	"github.com/ryogrid/SamehadaDB/types"
)

type Column struct {
	columnName        string
	columnType        types.TypeID
	fixedLength       uint32 // For a non-inlined column, this is the size of a pointer. Otherwise, the size of the fixed length column
	variableLength    uint32 // For an inlined column, 0. Otherwise, the length of the variable length column
	columnOffset      uint32 // Column offset in the tuple
	hasIndex          bool   // whether the column has index data
	indexHeaderPageId types.PageID
	isLeft            bool // when temporal schema, this is used for join
	// should be pointer of subtype of expression.Expression
	// this member is used and needed at temporarily created table (schema) on query execution
	expr_ interface{}
}

// TODO: (SDB) need to add argument to set header page of index data or define new method for that
// expr argument should be pointer of subtype of expression.Expression
func NewColumn(name string, columnType types.TypeID, hasIndex bool, indexHeaderPageID types.PageID, expr interface{}) *Column {
	if columnType != types.Varchar {
		return &Column{name, columnType, columnType.Size(), 0, 0, hasIndex, indexHeaderPageID, true, expr}
	}

	return &Column{name, types.Varchar, 4, 255, 0, hasIndex, indexHeaderPageID, true, expr}
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

func (c *Column) IndexHeaderPageId() types.PageID {
	return c.indexHeaderPageId
}

func (c *Column) SetIndexHeaderPageId(pageId types.PageID) {
	c.indexHeaderPageId = pageId
}

func (c *Column) IsLeft() bool {
	return c.isLeft
}

func (c *Column) SetIsLeft(isLeft bool) {
	c.isLeft = isLeft
}

// returned value should be used with type validation at expression.Expression
func (c *Column) GetExpr() interface{} {
	return c.expr_
}

func (c *Column) SetExpr(expr interface{}) {
	c.expr_ = expr
}
