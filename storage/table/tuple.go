package table

import (
	"github.com/brunocalza/go-bustub/storage/page"
	"github.com/brunocalza/go-bustub/types"
)

type Tuple struct {
	rid  *page.RID
	size uint32
	data []byte
}

// NewTupleFromSchema creates a new tuple based on input value
func NewTupleFromSchema(values []types.Value, schema *Schema) *Tuple {
	// calculate tuple size
	tupleSize := schema.Length()
	tuple := &Tuple{}
	tuple.size = tupleSize

	// allocate memory
	tuple.data = make([]byte, tupleSize)

	// serialize each attribute base on the input value
	for i := uint32(0); i < schema.GetColumnCount(); i++ {
		tuple.Copy(schema.GetColumn(i).GetOffset(), values[i].Serialize())
	}

	return tuple
}

func (t *Tuple) GetValue(schema *Schema, colIndex uint32) types.Value {
	column := schema.GetColumn(colIndex)
	value := types.NewValueFromBytes(t.data[column.GetOffset():], column.GetType())
	if value == nil {
		panic(value)
	}
	return *value
}

func (t *Tuple) Size() uint32 {
	return t.size
}

func (t *Tuple) Data() []byte {
	return t.data
}

func (t *Tuple) GetRID() *page.RID {
	return t.rid
}

func (t *Tuple) Copy(offset uint32, data []byte) {
	copy(t.data[offset:], data)
}
