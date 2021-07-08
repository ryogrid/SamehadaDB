package table

import (
	"unsafe"

	"github.com/brunocalza/go-bustub/storage/page"
	"github.com/brunocalza/go-bustub/types"
)

type Tuple struct {
	rid  *page.RID
	size uint32
	data *[]byte
}

// NewTupleFromSchema creates a new tuple based on input value
func NewTupleFromSchema(values []types.Value, schema *Schema) *Tuple {
	// calculate tuple size
	tupleSize := schema.Length()
	tuple := &Tuple{}
	tuple.size = tupleSize

	// allocate memory
	data := make([]byte, tupleSize)
	tuple.data = &data

	// serialize each attribute base on the input value
	columnCount := schema.GetColumnCount()

	for i := uint32(0); i < columnCount; i++ {
		column := schema.GetColumn(i)
		address := uintptr(unsafe.Pointer(&(*tuple.data)[0])) + uintptr(column.GetOffset())
		values[i].SerializeTo(address)
	}

	return tuple
}

func (t *Tuple) GetValue(schema *Schema, colIndex uint32) types.Value {
	column := schema.GetColumn(colIndex)
	address := uintptr(unsafe.Pointer(&(*t.data)[0])) + uintptr(column.GetOffset())
	value := types.DeserializeFrom(address, column.GetType())
	if value == nil {
		panic(value)
	}
	return *value
}

func (t *Tuple) Size() uint32 {
	return t.size
}

func (t *Tuple) Data() *[]byte {
	return t.data
}

func (t *Tuple) GetRID() *page.RID {
	return t.rid
}
