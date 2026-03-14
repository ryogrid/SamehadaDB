// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package schema

import (
	"github.com/ryogrid/SamehadaDB/lib/storage/table/column"
	"math"
	"strings"
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
		column := *columns[i]

		if !column.IsInlined() {
			schema.tupleIsInlined = false
			schema.uninlinedColumns = append(schema.uninlinedColumns, i)
		}

		column.SetOffset(currentOffset)
		currentOffset += column.FixedLength()

		schema.columns = append(schema.columns, &column)
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
	// note: alphabets on column name is stored in lowercase
	colName := strings.ToLower(columnName)

	for i := uint32(0); i < s.GetColumnCount(); i++ {
		if strings.Contains(colName, ".") && s.columns[i].GetColumnName() == colName {
			return i
		} else if !strings.Contains(colName, ".") {
			if s.columns[i].GetColumnName() == colName {
				return i
			} else if strings.Contains(s.columns[i].GetColumnName(), ".") && strings.Split(s.columns[i].GetColumnName(), ".")[1] == colName {
				return i
			}
		}
	}

	return math.MaxUint32
}

func (s *Schema) GetColumns() []*column.Column {
	return s.columns
}

func (s *Schema) IsHaveColumn(columnName *string) bool {
	for _, col := range s.columns {
		if strings.Contains(*columnName, ".") && col.GetColumnName() == *columnName {
			return true
		}
		//} else if !strings.Contains(*columnName, ".") && strings.Split(col.GetColumnName(), ".")[1] == *columnName {
		//	return true
		//}
	}
	return false
}

func CopySchema(from *Schema, attrs []uint32) *Schema {
	var colsObj []column.Column
	var colsP []*column.Column
	for _, col := range from.columns {
		colsObj = append(colsObj, *col)
	}
	for _, col := range colsObj {
		colsP = append(colsP, &col)
	}

	ret := new(Schema)
	ret.columns = colsP
	return ret
}
