// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

//package table
package tuple

import (
	"bytes"
	"encoding/binary"

	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/types"
)

var TupleSizeOffsetInLogrecord = 4 // payload size info in Bytes

/**
 * Tuple format:
 * ---------------------------------------------------------------------
 * | FIXED-SIZE or VARIED-SIZED OFFSET | PAYLOAD OF VARIED-SIZED FIELD |
 * ---------------------------------------------------------------------
 */
type Tuple struct {
	rid  *page.RID
	size uint32
	data []byte
}

func NewTuple(rid *page.RID, size uint32, data []byte) *Tuple {
	return &Tuple{rid, size, data}
}

// NewTupleFromSchema creates a new tuple based on input value
func NewTupleFromSchema(values []types.Value, schema *schema.Schema) *Tuple {
	// calculate tuple size considering varchar columns
	tupleSize := schema.Length()
	for _, colIndex := range schema.GetUnlinedColumns() {
		tupleSize += values[colIndex].Size()
	}
	tuple := &Tuple{}
	tuple.size = tupleSize

	// allocate memory
	tuple.data = make([]byte, tupleSize)

	// serialize each attribute base on the input value
	tupleEndOffset := schema.Length()
	for i := uint32(0); i < schema.GetColumnCount(); i++ {
		if schema.GetColumn(i).IsInlined() {
			tuple.Copy((*(schema.GetColumn(i))).GetOffset(), values[i].Serialize())
		} else {
			tuple.Copy((*(schema.GetColumn(i))).GetOffset(), types.UInt32(tupleEndOffset).Serialize())
			tuple.Copy(tupleEndOffset, values[i].Serialize())
			tupleEndOffset += values[i].Size()
		}
	}
	return tuple
}

func (t *Tuple) GetValue(schema *schema.Schema, colIndex uint32) types.Value {
	column := *(schema.GetColumn(colIndex))
	//fmt.Printf("column at GetValue: %+v \n", column)
	//column := (*((*interfaces.ISchema)(unsafe.Pointer(&(schema.(interfaces.ISchema)))))).GetColumn(colIndex)
	//column := (schema.(interfaces.ISchema)).GetColumn(colIndex)
	offset := column.GetOffset()
	//castedColumn := (*Column)(unsafe.Pointer(&column))
	if !column.IsInlined() {
		offset = uint32(types.NewUInt32FromBytes(t.data[offset : offset+column.FixedLength()]))
	}

	value := types.NewValueFromBytes(t.data[offset:], column.GetType())
	if value == nil {
		panic(value)
	}
	return *value
}

func (t *Tuple) GetValueInBytes(schema *schema.Schema, colIndex uint32) []byte {
	column := *(schema.GetColumn(colIndex))
	offset := column.GetOffset()
	if !column.IsInlined() {
		offset = uint32(types.NewUInt32FromBytes(t.data[offset : offset+column.FixedLength()]))
	}

	switch column.GetType() {
	case types.Integer:
		v := new(int32)
		binary.Read(bytes.NewBuffer(t.data[offset:]), binary.LittleEndian, v)
		retBuf := new(bytes.Buffer)
		binary.Write(retBuf, binary.LittleEndian, v)
		return retBuf.Bytes()
	case types.Varchar:
		data := t.data[offset:]
		lengthInBytes := data[0:2]
		length := new(int16)
		binary.Read(bytes.NewBuffer(lengthInBytes), binary.LittleEndian, length)
		return data[2:(*length + 2)]
	default:
		panic("illegal type column found in schema")
	}
}

func (t *Tuple) Size() uint32 {
	return t.size
}

func (t *Tuple) SetSize(size uint32) {
	t.size = size
}

func (t *Tuple) Data() []byte {
	return t.data
}

func (t *Tuple) SetData(data []byte) {
	t.data = data
}

func (t *Tuple) GetRID() *page.RID {
	return t.rid
}

func (t *Tuple) SetRID(rid *page.RID) {
	t.rid = rid
}

func (t *Tuple) Copy(offset uint32, data []byte) {
	copy(t.data[offset:], data)
}

func (tuple_ *Tuple) SerializeTo(storage []byte) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, tuple_.size)
	sizeInBytes := buf.Bytes()
	// memcpy(storage, &tuple_.size, sizeof(int32_t))
	// memcpy(storage+sizeof(int32_t), tuple_.data, tuple_.size)
	copy(storage, sizeInBytes)
	copy(storage[TupleSizeOffsetInLogrecord:TupleSizeOffsetInLogrecord+int(tuple_.size)], tuple_.data)
}

func (tuple_ *Tuple) DeserializeFrom(storage []byte) {
	//size := len(storage) - TupleOffset
	buf := bytes.NewBuffer(storage)
	binary.Read(buf, binary.LittleEndian, &tuple_.size)
	// Construct a tuple.
	//tuple_.size = uint32(size)
	tuple_.data = make([]byte, tuple_.size)
	//memcpy(this.data, storage+sizeof(int32_t), this.size)
	copy(tuple_.data, storage[TupleSizeOffsetInLogrecord:TupleSizeOffsetInLogrecord+int(tuple_.size)])
}
