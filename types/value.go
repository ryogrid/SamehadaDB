package types

import (
	"unsafe"
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

func (v Value) SerializeTo(address uintptr) {
	var size uint32
	var valueAddress uintptr
	switch v.valueType {
	case Integer:
		size = uint32(unsafe.Sizeof(*v.integer))
		valueAddress = uintptr(unsafe.Pointer(v.integer))
	}

	for j := uint32(0); j < size; j++ {
		position := (*byte)(unsafe.Pointer(address + uintptr(j)))
		byt := *(*byte)(unsafe.Pointer(valueAddress + uintptr(j)))
		*position = byt
	}
}

func DeserializeFrom(address uintptr, valueType TypeID) *Value {
	switch valueType {
	case Integer:
		return &Value{(*int32)(unsafe.Pointer(address)), nil, Integer}
	}
	return nil
}
