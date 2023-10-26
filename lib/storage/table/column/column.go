// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package column

import (
	"github.com/ryogrid/SamehadaDB/lib/storage/index/index_constants"
	"github.com/ryogrid/SamehadaDB/lib/types"
	"strings"
)

type Column struct {
	// note: columnName field includes table name. e.g. "table1.column1"
	//       and GetColumnName() returns it as it is.
	columnName        string
	columnType        types.TypeID
	fixedLength       uint32 // For a non-inlined column, this is the size of a pointer. Otherwise, the size of the fixed length column
	variableLength    uint32 // For an inlined column, 0. Otherwise, the length of the variable length column
	columnOffset      uint32 // Column offset in the tuple
	hasIndex          bool   // whether the column has index data
	indexKind         index_constants.IndexKind
	indexHeaderPageId types.PageID
	isLeft            bool // when temporal schema, this is used for join
	// should be pointer of subtype of expression.Expression
	// this member is used and needed at temporarily created table (schema) on query execution
	expr_ interface{}
}

// indexHeaderPageID should be types.PageID(-1) if there is no special reason
// the ID is set when CreateTable is called
// expr argument should be pointer of subtype of expression.Expression
func NewColumn(name string, columnType types.TypeID, hasIndex bool, indexKind index_constants.IndexKind, indexHeaderPageID types.PageID, expr interface{}) *Column {
	// note: alphabets on column name is stored in lowercase

	if columnType != types.Varchar {
		return &Column{strings.ToLower(name), columnType, columnType.Size(), 0, 0, hasIndex, indexKind, indexHeaderPageID, true, expr}
	}

	return &Column{strings.ToLower(name), types.Varchar, 4, 255, 0, hasIndex, indexKind, indexHeaderPageID, true, expr}
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

func (c *Column) SetColumnName(colName string) {
	c.columnName = colName
}

func (c *Column) HasIndex() bool {
	return c.hasIndex
}

func (c *Column) SetHasIndex(hasIndex bool) {
	c.hasIndex = hasIndex
}

func (c *Column) IndexKind() index_constants.IndexKind {
	return c.indexKind
}

func (c *Column) SetIndexKind(kind index_constants.IndexKind) {
	c.indexKind = kind
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
