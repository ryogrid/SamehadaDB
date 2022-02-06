package interfaces

type ISchema interface {
	GetColumn(colIndex uint32) IColumn
	GetUnlinedColumns() []uint32
	GetColumnCount() uint32
	Length() uint32
	GetColIndex(columnName string) uint32
	GetColumns() []IColumn
}
