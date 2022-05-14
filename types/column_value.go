// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package types

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

// A value is an class that represents a view over SQL data stored in
// some materialized state. All values have a type and comparison functions,
// and implement other type-specific functionality.
type Value struct {
	valueType TypeID
	integer   *int32
	boolean   *bool
	varchar   *string
	float     *float32
}

func NewInteger(value int32) Value {
	return Value{Integer, &value, nil, nil, nil}
}

func NewFloat(value float32) Value {
	return Value{Float, nil, nil, nil, &value}
}

func NewBoolean(value bool) Value {
	return Value{Boolean, nil, &value, nil, nil}
}

func NewVarchar(value string) Value {
	return Value{Varchar, nil, nil, &value, nil}
}

// NewValueFromBytes is used for deserialization
func NewValueFromBytes(data []byte, valueType TypeID) (ret *Value) {
	switch valueType {
	case Integer:
		v := new(int32)
		binary.Read(bytes.NewBuffer(data), binary.LittleEndian, v)
		vInteger := NewInteger(*v)
		ret = &vInteger
	case Float:
		v := new(float32)
		binary.Read(bytes.NewBuffer(data), binary.LittleEndian, v)
		vFloat := NewFloat(*v)
		ret = &vFloat
	case Varchar:
		lengthInBytes := data[0:2]
		length := new(int16)
		binary.Read(bytes.NewBuffer(lengthInBytes), binary.LittleEndian, length)
		varchar := NewVarchar(string(data[2:(*length + 2)]))
		ret = &varchar
	case Boolean:
		v := new(bool)
		binary.Read(bytes.NewBuffer(data), binary.LittleEndian, v)
		vBoolean := NewBoolean(*v)
		ret = &vBoolean
	}
	return ret
}

func (v Value) CompareEquals(right Value) bool {
	switch v.valueType {
	case Integer:
		return *v.integer == *right.integer
	case Float:
		return *v.float == *right.float
	case Varchar:
		return *v.varchar == *right.varchar
	case Boolean:
		return *v.boolean == *right.boolean
	}
	return false
}

func (v Value) CompareNotEquals(right Value) bool {
	switch v.valueType {
	case Integer:
		return *v.integer != *right.integer
	case Float:
		return *v.float != *right.float
	case Varchar:
		return *v.varchar != *right.varchar
	case Boolean:
		return *v.boolean != *right.boolean
	}
	return false
}

func (v Value) CompareGreaterThan(right Value) bool {
	switch v.valueType {
	case Integer:
		return *v.integer > *right.integer
	case Float:
		return *v.float > *right.float
	case Varchar:
		return *v.varchar > *right.varchar
	case Boolean:
		return false
	}
	return false
}

func (v Value) CompareGreaterThanOrEqual(right Value) bool {
	switch v.valueType {
	case Integer:
		return *v.integer >= *right.integer
	case Float:
		return *v.float >= *right.float
	case Varchar:
		return *v.varchar >= *right.varchar
	case Boolean:
		return *v.boolean == *right.boolean
	}
	return false
}

func (v Value) CompareLessThan(right Value) bool {
	switch v.valueType {
	case Integer:
		return *v.integer < *right.integer
	case Float:
		return *v.float < *right.float
	case Varchar:
		return *v.varchar < *right.varchar
	case Boolean:
		return false
	}
	return false
}

func (v Value) CompareLessThanOrEqual(right Value) bool {
	switch v.valueType {
	case Integer:
		return *v.integer <= *right.integer
	case Float:
		return *v.float <= *right.float
	case Varchar:
		return *v.varchar <= *right.varchar
	case Boolean:
		return *v.boolean == *right.boolean
	default:
		panic("illegal valueType is passed!")
	}

}

func (v Value) Serialize() []byte {
	switch v.valueType {
	case Integer:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, v.ToInteger())
		return buf.Bytes()
	case Float:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, v.ToFloat())
		return buf.Bytes()
	case Varchar:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, uint16(len(v.ToVarchar())))
		lengthInBytes := buf.Bytes()
		return append(lengthInBytes, []byte(v.ToVarchar())...)
	case Boolean:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, v.ToBoolean())
		return buf.Bytes()
	}
	return []byte{}
}

// Size returns the size in bytes that the type will occupy inside the tuple
func (v Value) Size() uint32 {
	switch v.valueType {
	case Integer:
		return 4
	case Float:
		return 4
	case Varchar:
		return uint32(len(*v.varchar)) + 2 // varchar occupies the size of the string + 2 bytes for length storage
	case Boolean:
		return uint32(unsafe.Sizeof(true))
	}
	panic("not implemented")
}

func (v Value) ToBoolean() bool {
	return *v.boolean
}

func (v Value) ToInteger() int32 {
	return *v.integer
}

func (v Value) ToFloat() float32 {
	return *v.float
}

func (v Value) ToVarchar() string {
	return *v.varchar
}

func (v Value) ValueType() TypeID {
	return v.valueType
}

func (v Value) SetNull() {
	v.valueType = Null
}

func (v Value) IsNull() bool {
	return v.valueType == Null
}

func (v Value) Add(other *Value) *Value {
	switch v.valueType {
	case Integer:
		ret := NewInteger(*v.integer + *other.integer)
		return &ret
	case Float:
		ret := NewFloat(*v.float + *other.float)
		return &ret
	default:
		panic("Add is implemented to Integer and Float only.")
	}
}

func (v Value) Max(other *Value) *Value {
	switch v.valueType {
	case Integer:
		if *v.integer >= *other.integer {
			ret := NewInteger(*v.integer)
			return &ret
		} else {
			ret := NewInteger(other.ToInteger())
			return &ret
		}
	case Float:
		if *v.float >= *other.float {
			ret := NewFloat(*v.float)
			return &ret
		} else {
			ret := NewFloat(other.ToFloat())
			return &ret
		}
	default:
		panic("Max is implemented to Integer and Float only.")
	}
}

func (v Value) Min(other *Value) *Value {
	switch v.valueType {
	case Integer:
		if *v.integer <= *other.integer {
			ret := NewInteger(*v.integer)
			return &ret
		} else {
			ret := NewInteger(other.ToInteger())
			return &ret
		}
	case Float:
		if *v.float <= *other.float {
			ret := NewFloat(*v.float)
			return &ret
		} else {
			ret := NewFloat(other.ToFloat())
			return &ret
		}
	default:
		panic("Max is implemented to Integer and Float only.")
	}
}
