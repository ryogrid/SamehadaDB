// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package tuple

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/storage/page"
	"github.com/ryogrid/SamehadaDB/lib/storage/table/schema"
	"github.com/ryogrid/SamehadaDB/lib/types"
)

const TupleSizeOffsetInLogrecord = 4 // payload size info in Bytes

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
func NewTupleFromSchema(values []types.Value, sc *schema.Schema) *Tuple {
	// calculate tuple size considering varchar columns
	tupleSize := sc.Length()
	for _, colIndex := range sc.GetUnlinedColumns() {
		tupleSize += values[colIndex].Size()
	}
	tpl := &Tuple{}
	tpl.size = tupleSize

	// allocate memory
	tpl.data = make([]byte, tupleSize)

	// serialize each attribute base on the input value
	tupleEndOffset := sc.Length()
	for i := uint32(0); i < sc.GetColumnCount(); i++ {
		if sc.GetColumn(i).IsInlined() {
			tpl.Copy((*(sc.GetColumn(i))).GetOffset(), values[i].Serialize())
		} else {
			tpl.Copy((*(sc.GetColumn(i))).GetOffset(), types.UInt32(tupleEndOffset).Serialize())
			tpl.Copy(tupleEndOffset, values[i].Serialize())
			tupleEndOffset += values[i].Size()
		}
	}
	return tpl
}

// generate tuple obj for hash index search
// generated tuple filled only specifed column only due to use methods
// defined on Index interface
func GenTupleForIndexSearch(sc *schema.Schema, colIndex uint32, keyVal *types.Value) *Tuple {
	if keyVal == nil {
		return nil
	}
	columns := sc.GetColumns()
	values := make([]types.Value, 0)
	for idx, columnObj := range columns {
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
	return NewTupleFromSchema(values, sc)
}

func (t *Tuple) GetValue(schema *schema.Schema, colIndex uint32) types.Value {
	column := *(schema.GetColumn(colIndex))
	offset := column.GetOffset()
	if !column.IsInlined() {
		offset = uint32(types.NewUInt32FromBytes(t.data[offset : offset+column.FixedLength()]))
	}

	if int(offset) >= len(t.data) {
		fmt.Println("Tuple::GetValue: offset is out of range", offset, len(t.data))
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

func (t *Tuple) SerializeTo(storage []byte) {
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, t.size)
	sizeInBytes := buf.Bytes()
	copy(storage, sizeInBytes)
	copy(storage[TupleSizeOffsetInLogrecord:TupleSizeOffsetInLogrecord+int(t.size)], t.data)
}

func (t *Tuple) DeserializeFrom(storage []byte) {
	buf := bytes.NewBuffer(storage)
	binary.Read(buf, binary.LittleEndian, &t.size)
	// Construct a tuple.
	t.data = make([]byte, t.size)
	copy(t.data, storage[TupleSizeOffsetInLogrecord:TupleSizeOffsetInLogrecord+int(t.size)])
}

func (t *Tuple) GetDeepCopy() *Tuple {
	ret := new(Tuple)
	ret.data = make([]byte, 0)
	t.Copy(0, ret.data)
	ret.SetSize(t.size)
	copiedRID := new(page.RID)
	copiedRID.Set(t.rid.GetPageID(), t.rid.GetSlotNum())
	ret.rid = copiedRID
	return ret
}
