package types

import (
	"bytes"
	"encoding/binary"
)

type Value struct {
	integer   *int32
	boolean   *bool
	valueType TypeID
}

func NewInteger(value int32) Value {
	return Value{&value, nil, Integer}
}

func NewBoolean(value bool) Value {
	return Value{nil, &value, Boolean}
}

func (v Value) CompareEquals(right Value) bool {
	switch v.valueType {
	case Integer:
		return *v.integer == *right.integer
	}
	return false
}

func (v Value) CompareNotEquals(right Value) bool {
	switch v.valueType {
	case Integer:
		return *v.integer != *right.integer
	}
	return false
}

func (v Value) Serialize() []byte {
	switch v.valueType {
	case Integer:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, v.ToInteger())
		return buf.Bytes()
	}
	return []byte{}
}

func NewValueFromBytes(data []byte, valueType TypeID) (ret *Value) {
	switch valueType {
	case Integer:
		v := new(int32)
		binary.Read(bytes.NewBuffer(data), binary.LittleEndian, v)
		vInteger := NewInteger(*v)
		ret = &vInteger
	}
	return ret
}

func (v Value) ToBoolean() bool {
	return *v.boolean
}

func (v Value) ToInteger() int32 {
	return *v.integer
}
