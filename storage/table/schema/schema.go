// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package schema

import (
	"github.com/ryogrid/SamehadaDB/storage/table/column"
	"math"
)

type Schema struct {
	length           uint32           // Fixed-length column size, i.e. the number of bytes used by one tuple
	columns          []*column.Column // All the columns in the schema, inlined and uninlined.
	tupleIsInlined   bool             // True if all the columns are inlined, false otherwise
	uninlinedColumns []uint32         // Indices of all uninlined columns
}

func NewSchema(columns []*column.Column) *Schema {
	schema := &Schema{}
	schema.tupleIsInlined = true

	var currentOffset uint32
	currentOffset = 0
	for i := uint32(0); i < uint32(len(columns)); i++ {
		column := columns[i]

		if !column.IsInlined() {
			schema.tupleIsInlined = false
			schema.uninlinedColumns = append(schema.uninlinedColumns, i)
		}

		column.SetOffset(currentOffset)
		currentOffset += column.FixedLength()

		schema.columns = append(schema.columns, column)
	}
	schema.length = currentOffset
	return schema
}

func (s *Schema) GetColumn(colIndex uint32) *column.Column {
	return s.columns[colIndex]
}

func (s *Schema) GetUnlinedColumns() []uint32 {
	return s.uninlinedColumns
}

func (s *Schema) GetColumnCount() uint32 {
	return uint32(len(s.columns))
}

func (s *Schema) Length() uint32 {
	return s.length
}

func (s *Schema) GetColIndex(columnName string) uint32 {
	for i := uint32(0); i < s.GetColumnCount(); i++ {
		if s.columns[i].GetColumnName() == columnName {
			return i
		}
	}

	return math.MaxUint32
}

func (s *Schema) GetColumns() []*column.Column {
	return s.columns
}

func (s *Schema) IsHaveColumn(columnName *string) bool {
	for _, col := range s.columns {
		if col.GetColumnName() == *columnName {
			return true
		}
	}
	return false
}

func CopySchema(from *Schema, attrs []uint32) *Schema {
	var cols_obj []column.Column
	var cols_p []*column.Column
	for _, col := range from.columns {
		cols_obj = append(cols_obj, *col)
	}
	for _, col := range cols_obj {
		cols_p = append(cols_p, &col)
	}

	ret := new(Schema)
	ret.columns = cols_p
	return ret
}
