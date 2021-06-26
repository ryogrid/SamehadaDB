package table

import (
	"unsafe"

	"github.com/brunocalza/go-bustub/storage/page"
	"github.com/brunocalza/go-bustub/types"
)

type Tuple struct {
	isAllocated bool
	rid         *page.RID
	size        uint32
	data        *[]byte
}

func NewTupleFromSchema(values []types.Value, schema *Schema) *Tuple {
	// calculate tuple size
	tupleSize := schema.Length()
	// TODO increase size of unlinedcolumns
	tuple := &Tuple{}
	tuple.size = tupleSize

	// allocate memory
	data := make([]byte, tupleSize)
	tuple.data = &data
	tuple.isAllocated = true

	// serialize each attribute base on the input value
	columnCount := schema.GetColumnCount()
	//offset := schema.Length()

	var i uint32
	for i = 0; i < columnCount; i++ {
		column := schema.GetColumn(i)
		value := values[i].(types.IntegerType)
		// TODO; handle varchar

		// Serialize
		size := int(unsafe.Sizeof(value))
		for i := 0; i < size; i++ {
			position := (*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&(*tuple.data)[0])) + uintptr(column.columnOffset) + uintptr(i)))
			byt := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(&value)) + uintptr(i)))
			*position = byt
		}
	}

	return tuple
}

func (t *Tuple) GetValue(schema *Schema, colIndex uint32) types.Value {
	column := schema.GetColumn(colIndex)
	//columnData := t.GetDataPtr(schema, colIndex)

	switch column.GetType() {
	case types.Integer:
		value := (*int32)(unsafe.Pointer(uintptr(unsafe.Pointer(&(*t.data)[0])) + uintptr(column.GetOffset())))
		return types.NewIntegerType(*value)
	default:
		return nil
	}
}

func (t *Tuple) GetDataPtr(schema *Schema, colIndex uint32) unsafe.Pointer {
	column := schema.GetColumn(colIndex)
	if (*column).IsInlined() {
		data := (*t.data)[0:1]
		return unsafe.Pointer(&data)
	}

	//TODO: handle varchar
	data := (*t.data)[0:1]
	return unsafe.Pointer(&data)
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
