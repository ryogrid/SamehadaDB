// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

// A value is an class that represents a view over SQL data stored in
// some materialized state. All values have a type and comparison functions,
// and implement other type-specific functionality.
type Value struct {
	valueType TypeID
	isNull    bool
	integer   *int32
	boolean   *bool
	varchar   *string
	float     *float32
}

func NewInteger(value int32) Value {
	return Value{Integer, false, &value, nil, nil, nil}
}

func NewFloat(value float32) Value {
	return Value{Float, false, nil, nil, nil, &value}
}

func NewBoolean(value bool) Value {
	return Value{Boolean, false, nil, &value, nil, nil}
}

func NewVarchar(value string) Value {
	return Value{Varchar, false, nil, nil, &value, nil}
}

// NewValueFromBytes is used for deserialization
func NewValueFromBytes(data []byte, valueType TypeID) (ret *Value) {
	switch valueType {
	case Integer:
		buf := bytes.NewBuffer(data)
		isNull := new(bool)
		binary.Read(buf, binary.LittleEndian, isNull)
		v := new(int32)
		binary.Read(buf, binary.LittleEndian, v)
		vInteger := NewInteger(*v)
		if *isNull {
			vInteger.SetNull()
		}
		ret = &vInteger
	case Float:
		buf := bytes.NewBuffer(data)
		isNull := new(bool)
		binary.Read(buf, binary.LittleEndian, isNull)
		v := new(float32)
		binary.Read(buf, binary.LittleEndian, v)
		vFloat := NewFloat(*v)
		if *isNull {
			vFloat.SetNull()
		}
		ret = &vFloat
	case Varchar:
		buf := bytes.NewBuffer(data)
		isNull := new(bool)
		binary.Read(buf, binary.LittleEndian, isNull)
		//lengthInBytes := data[0:2]
		length := new(int16)
		binary.Read(buf, binary.LittleEndian, length)
		//binary.Read(bytes.NewBuffer(lengthInBytes), binary.LittleEndian, length)
		varchar := NewVarchar(string(data[1+2 : (*length + (1 + 2))]))
		if *isNull {
			varchar.SetNull()
		}
		ret = &varchar
	case Boolean:
		buf := bytes.NewBuffer(data)
		isNull := new(bool)
		binary.Read(buf, binary.LittleEndian, isNull)
		v := new(bool)
		binary.Read(bytes.NewBuffer(data), binary.LittleEndian, v)
		vBoolean := NewBoolean(*v)
		if *isNull {
			vBoolean.SetNull()
		}
		ret = &vBoolean
	default:
		fmt.Printf("%v is illegal\n", valueType)
		panic("")
	}
	return ret
}

func (v Value) CompareEquals(right Value) bool {
	if v.IsNull() && right.IsNull() {
		return true
	} else if v.IsNull() || right.IsNull() {
		return false
	}

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
	if v.IsNull() && right.IsNull() {
		return false
	} else if v.IsNull() || right.IsNull() {
		return true
	}

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
	if v.IsNull() {
		return false
	}

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
	if v.IsNull() && right.IsNull() {
		return true
	} else if v.IsNull() || right.IsNull() {
		return false
	}

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
	if v.IsNull() {
		return false
	}

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
	if v.IsNull() && right.IsNull() {
		return true
	} else if v.IsNull() || right.IsNull() {
		return false
	}

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
		binary.Write(buf, binary.LittleEndian, v.isNull)
		binary.Write(buf, binary.LittleEndian, v.ToInteger())
		return buf.Bytes()
	case Float:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, v.isNull)
		binary.Write(buf, binary.LittleEndian, v.ToFloat())
		return buf.Bytes()
	case Varchar:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, v.isNull)
		binary.Write(buf, binary.LittleEndian, uint16(len(v.ToVarchar())))
		isNullAndLength := buf.Bytes()
		return append(isNullAndLength, []byte(v.ToVarchar())...)
	case Boolean:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, v.isNull)
		binary.Write(buf, binary.LittleEndian, v.ToBoolean())
		return buf.Bytes()
	}
	return []byte{}
}

// Size returns the size in bytes that the type will occupy inside the tuple
func (v Value) Size() uint32 {
	// all type occupies the whether NULL or not + 1 byte for the info storage
	switch v.valueType {
	case Integer:
		return v.valueType.Size()
	case Float:
		return v.valueType.Size()
	case Varchar:
		return uint32(len(*v.varchar)) + 1 + 2 // varchar occupies the size of the string + 2 bytes for length storage
	case Boolean:
		return v.valueType.Size()
	}
	panic("not implemented")
}

// if you use this to get column value
// NULL value check is needed in general
func (v Value) ToBoolean() bool {
	return *v.boolean
}

// if you use this to get column value
// NULL value check is needed in general
func (v Value) ToInteger() int32 {
	return *v.integer
}

// if you use this to get column value
// NULL value check is needed in general
func (v Value) ToFloat() float32 {
	return *v.float
}

// if you use this to get column value
// NULL value check is needed in general
func (v Value) ToVarchar() string {
	return *v.varchar
}

func (v Value) ValueType() TypeID {
	return v.valueType
}

// note: (need to be) only way to get Value object which has NULL value
//       a value filed correspoding to value type is initialized to default value
func (v Value) SetNull() {
	v.isNull = true
	switch v.valueType {
	case Integer:
		v.integer = new(int32)
		return
	case Float:
		v.float = new(float32)
		return
	case Varchar:
		v.varchar = new(string)
		return
	case Boolean:
		v.boolean = new(bool)
		return
	}
	panic("not implemented")
}

func (v Value) IsNull() bool {
	return v.isNull
}

func (v Value) Add(other *Value) *Value {
	if other.IsNull() {
		return &v
	}

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
	if other.IsNull() {
		return &v
	}

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
	if other.IsNull() {
		return &v
	}

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
