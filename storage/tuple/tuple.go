// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

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
func NewTupleFromSchema(values []types.Value, schema_ *schema.Schema) *Tuple {
	// calculate tuple size considering varchar columns
	tupleSize := schema_.Length()
	for _, colIndex := range schema_.GetUnlinedColumns() {
		tupleSize += values[colIndex].Size()
	}
	tuple_ := &Tuple{}
	tuple_.size = tupleSize

	// allocate memory
	tuple_.data = make([]byte, tupleSize)

	// serialize each attribute base on the input value
	tupleEndOffset := schema_.Length()
	for i := uint32(0); i < schema_.GetColumnCount(); i++ {
		if schema_.GetColumn(i).IsInlined() {
			tuple_.Copy((*(schema_.GetColumn(i))).GetOffset(), values[i].Serialize())
		} else {
			tuple_.Copy((*(schema_.GetColumn(i))).GetOffset(), types.UInt32(tupleEndOffset).Serialize())
			tuple_.Copy(tupleEndOffset, values[i].Serialize())
			tupleEndOffset += values[i].Size()
		}
	}
	return tuple_
}

// generate tuple obj for hash index search
// generated tuple filled only specifed column only due to use methods
// defined on Index interface
func GenTupleForIndexSearch(schema_ *schema.Schema, colIndex uint32, keyVal *types.Value) *Tuple {
	if keyVal == nil {
		return nil
	}
	colmuns := schema_.GetColumns()
	values := make([]types.Value, 0)
	for idx, columnObj := range colmuns {
		switch columnObj.GetType() {
		case types.Integer:
			if idx == int(colIndex) {
				values = append(values, *keyVal)
			} else {
				values = append(values, types.NewInteger(0))
			}
		case types.Float:
			if idx == int(colIndex) {
				values = append(values, *keyVal)
			} else {
				values = append(values, types.NewFloat(0.0))
			}
		case types.Varchar:
			if idx == int(colIndex) {
				values = append(values, *keyVal)
			} else {
				values = append(values, types.NewVarchar(""))
			}
		}
	}
	return NewTupleFromSchema(values, schema_)
}

func (t *Tuple) GetValue(schema *schema.Schema, colIndex uint32) types.Value {
	column := *(schema.GetColumn(colIndex))
	offset := column.GetOffset()
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
		buf := bytes.NewBuffer(t.data[offset:])
		isNull := new(bool)
		binary.Read(buf, binary.LittleEndian, isNull)
		v := new(int32)
		binary.Read(buf, binary.LittleEndian, v)
		retBuf := new(bytes.Buffer)
		binary.Write(retBuf, binary.LittleEndian, *isNull)
		binary.Write(retBuf, binary.LittleEndian, *v)
		return retBuf.Bytes()
	case types.Float:
		buf := bytes.NewBuffer(t.data[offset:])
		isNull := new(bool)
		binary.Read(buf, binary.LittleEndian, isNull)
		v := new(float32)
		binary.Read(buf, binary.LittleEndian, v)
		retBuf := new(bytes.Buffer)
		binary.Write(retBuf, binary.LittleEndian, *isNull)
		binary.Write(retBuf, binary.LittleEndian, *v)
		return retBuf.Bytes()
	case types.Boolean:
		buf := bytes.NewBuffer(t.data[offset:])
		isNull := new(bool)
		binary.Read(buf, binary.LittleEndian, isNull)
		v := new(bool)
		binary.Read(buf, binary.LittleEndian, v)
		retBuf := new(bytes.Buffer)
		binary.Write(retBuf, binary.LittleEndian, *isNull)
		binary.Write(retBuf, binary.LittleEndian, *v)
		return retBuf.Bytes()
	case types.Varchar:
		buf := bytes.NewBuffer(t.data[offset:])
		isNull := new(bool)
		binary.Read(buf, binary.LittleEndian, isNull)
		length := new(int16)
		binary.Read(buf, binary.LittleEndian, length)
		retBuf := new(bytes.Buffer)
		binary.Write(retBuf, binary.LittleEndian, *isNull)
		binary.Write(retBuf, binary.LittleEndian, *length)
		retArr := make([]byte, 0)
		retArr = append(retArr, retBuf.Bytes()...)
		retArr = append(retArr, t.data[offset+(1+2):offset+(uint32(*length)+(1+2))]...)
		return retArr
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
	copy(storage, sizeInBytes)
	copy(storage[TupleSizeOffsetInLogrecord:TupleSizeOffsetInLogrecord+int(tuple_.size)], tuple_.data)
}

func (tuple_ *Tuple) DeserializeFrom(storage []byte) {
	buf := bytes.NewBuffer(storage)
	binary.Read(buf, binary.LittleEndian, &tuple_.size)
	// Construct a tuple.
	tuple_.data = make([]byte, tuple_.size)
	copy(tuple_.data, storage[TupleSizeOffsetInLogrecord:TupleSizeOffsetInLogrecord+int(tuple_.size)])
}

func (tuple_ *Tuple) GetDeepCopy() *Tuple {
	ret := new(Tuple)
	ret.data = make([]byte, 0)
	tuple_.Copy(0, ret.data)
	ret.SetSize(tuple_.size)
	copied_rid := new(page.RID)
	copied_rid.Set(tuple_.rid.GetPageId(), tuple_.rid.GetSlotNum())
	ret.rid = copied_rid
	return ret
}
