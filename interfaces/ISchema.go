package interfaces

type ISchema interface {
	NewSchema(columns []*IColumn) *ISchema
	GetUnlinedColumns() []uint32
	GetColumnCount() uint32
	Length() uint32
	GetColIndex(columnName string) uint32
	GetColumns() []*IColumn
}
