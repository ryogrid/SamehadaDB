// this code is from https://github.com/brunocalza/go-bustub
// there is license and copyright notice in licenses/go-bustub dir

package types

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
)

// A value is an class that represents a view over SQL data stored in
// some materialized state. All values have a type and comparison functions,
// and implement other type-specific functionality.
type Value struct {
	valueType TypeID
	isNull    *bool
	integer   *int32
	boolean   *bool
	varchar   *string
	float     *float32
}

func NewInteger(value int32) Value {
	tmpBool := false
	return Value{Integer, &tmpBool, &value, nil, nil, nil}
}

func NewFloat(value float32) Value {
	tmpBool := false
	return Value{Float, &tmpBool, nil, nil, nil, &value}
}

func NewBoolean(value bool) Value {
	tmpBool := false
	return Value{Boolean, &tmpBool, nil, &value, nil, nil}
}

func NewVarchar(value string) Value {
	tmpBool := false
	return Value{Varchar, &tmpBool, nil, nil, &value, nil}
}

func NewValue(value interface{}) Value {
	tmpBool := false
	switch value.(type) {
	case int32:
		val := value.(int32)
		return Value{Integer, &tmpBool, &val, nil, nil, nil}
	case float32:
		val := value.(float32)
		return Value{Float, &tmpBool, nil, nil, nil, &val}
	case bool:
		val := value.(bool)
		return Value{Boolean, &tmpBool, nil, &val, nil, nil}
	case string:
		val := value.(string)
		return Value{Varchar, &tmpBool, nil, nil, &val, nil}
	default:
		panic("not supported type passed")
	}
}

// it can be used only when you can not know value type to be compared or something
func NewNull() Value {
	tmpTrue := true
	tmpVal := int32(0)
	return Value{Integer, &tmpTrue, &tmpVal, nil, nil, nil}
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
		//length := new(int16)
		length := new(uint16)
		binary.Read(buf, binary.LittleEndian, length)
		//varchar := NewVarchar(string(data[1+2 : (*length + (1 + 2))]))
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
		binary.Read(buf, binary.LittleEndian, v)
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
	if v.IsInfMax() == true && right.IsInfMax() == true {
		return true
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
	if v.IsInfMax() && right.IsInfMax() {
		return false
	} else if v.IsInfMax() || right.IsInfMax() {
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
	if v.IsInfMax() && right.IsInfMax() {
		return false
	} else if v.IsInfMax() {
		return true
	} else if right.IsInfMax() {
		return false
	}
	if v.IsInfMin() && right.IsInfMin() {
		return false
	} else if v.IsInfMin() {
		return false
	} else if right.IsInfMin() {
		return true
	}

	switch v.valueType {
	case Integer:
		return *v.integer > *right.integer
	case Float:
		return *v.float > *right.float
	case Varchar:
		return *v.varchar > *right.varchar
	case Boolean:
		return *v.boolean == true && *right.boolean == false
	}
	return false
}

func (v Value) CompareGreaterThanOrEqual(right Value) bool {
	if v.IsNull() && right.IsNull() {
		return true
	} else if v.IsNull() || right.IsNull() {
		return false
	}
	if v.IsInfMax() && right.IsInfMax() {
		return true
	} else if v.IsInfMax() {
		return true
	} else if right.IsInfMax() {
		return false
	}
	if v.IsInfMin() && right.IsInfMin() {
		return true
	} else if v.IsInfMin() {
		return false
	} else if right.IsInfMin() {
		return true
	}

	switch v.valueType {
	case Integer:
		return *v.integer >= *right.integer
	case Float:
		return *v.float >= *right.float
	case Varchar:
		return *v.varchar >= *right.varchar
	case Boolean:
		return *v.boolean == *right.boolean || (*v.boolean == true && *right.boolean == false)
	}
	return false
}

func (v Value) CompareLessThan(right Value) bool {
	if v.IsNull() {
		return false
	}
	if v.IsInfMax() && right.IsInfMax() {
		return false
	} else if v.IsInfMax() {
		return false
	} else if right.IsInfMax() {
		return true
	}
	if v.IsInfMin() && right.IsInfMin() {
		return false
	} else if v.IsInfMin() {
		return true
	} else if right.IsInfMin() {
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
		return *v.boolean == false && *right.boolean == true
	}
	return false
}

func (v Value) CompareLessThanOrEqual(right Value) bool {
	if v.IsNull() && right.IsNull() {
		return true
	} else if v.IsNull() || right.IsNull() {
		return false
	}
	if v.IsInfMax() && right.IsInfMax() {
		return true
	} else if v.IsInfMax() {
		return false
	} else if right.IsInfMax() {
		return true
	}
	if v.IsInfMin() && right.IsInfMin() {
		return true
	} else if v.IsInfMin() {
		return true
	} else if right.IsInfMin() {
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
		return *v.boolean == *right.boolean || (*v.boolean == false && *right.boolean == true)
	default:
		panic("illegal valueType is passed!")
	}
}

func (v Value) Serialize() []byte {
	switch v.valueType {
	case Integer:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, *v.isNull)
		binary.Write(buf, binary.LittleEndian, v.ToInteger())
		return buf.Bytes()
	case Float:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, *v.isNull)
		binary.Write(buf, binary.LittleEndian, v.ToFloat())
		return buf.Bytes()
	case Varchar:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, *v.isNull)
		binary.Write(buf, binary.LittleEndian, uint16(len(v.ToVarchar())))
		isNullAndLength := buf.Bytes()
		return append(isNullAndLength, []byte(v.ToVarchar())...)
	case Boolean:
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, *v.isNull)
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

func (v Value) ToString() string {
	// all type occupies the whether NULL or not + 1 byte for the info storage
	switch v.valueType {
	case Integer:
		return strconv.Itoa(int(*v.integer))
	case Float:
		return strconv.FormatFloat(float64(*v.float), 'f', -1, 64)
	case Varchar:
		return *v.varchar
	case Boolean:
		if *v.boolean {
			return "true"
		} else {
			return "false"
		}
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
	if v.valueType != Integer {
		// TODO: (SDB) temporal modification for Varchar (AddWLatchRecord, RemoveWLatchRecord...)
		return math.MaxInt32
	}
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

func (v Value) ToIFValue() interface{} {
	switch v.valueType {
	case Integer:
		return *v.integer
	case Boolean:
		return *v.boolean
	case Varchar:
		return *v.varchar
	case Float:
		return *v.float
	default:
		panic("not supported type!")
	}
}

func (v Value) ValueType() TypeID {
	return v.valueType
}

// note: (need to be) only way to get Value object which has NULL value
//
//	a value filed correspoding to value type is initialized to default value
func (v Value) SetNull() *Value {
	*v.isNull = true
	switch v.valueType {
	case Integer:
		*v.integer = 0
		return &v
	case Float:
		*v.float = 0
		return &v
	case Varchar:
		*v.varchar = ""
		return &v
	case Boolean:
		*v.boolean = false
		return &v
	}
	panic("not implemented")
}

func (v Value) IsNull() bool {
	return *v.isNull
}

func (v Value) SetInfMax() *Value {
	switch v.valueType {
	case Integer:
		*v.integer = math.MaxInt32
		return &v
	case Float:
		*v.float = math.MaxFloat32
		return &v
	case Varchar:
		*v.varchar = "SamehadaDBInfMaxValue"
		return &v
	case Boolean:
		*v.boolean = true
		return &v
	}
	panic("not implemented")
}

func (v Value) SetInfMin() *Value {
	switch v.valueType {
	case Integer:
		*v.integer = math.MinInt32
		return &v
	case Float:
		*v.float = -1.0 * math.MaxFloat32
		return &v
	case Varchar:
		*v.varchar = "SamehadaDBInfMinValue"
		return &v
	case Boolean:
		*v.boolean = false
		return &v
	}
	panic("not implemented")
}

func (v Value) IsInfMax() bool {
	switch v.valueType {
	case Integer:
		return *v.integer == math.MaxInt32
	case Float:
		return *v.float == math.MaxFloat32
	case Varchar:
		return *v.varchar == "SamehadaDBInfMaxValue"
	case Boolean:
		return *v.boolean == true
	}
	panic("not implemented")
}

func (v Value) IsInfMin() bool {
	switch v.valueType {
	case Integer:
		return *v.integer == math.MinInt32
	case Float:
		return *v.float == -1.0*math.MaxFloat32
	case Varchar:
		return *v.varchar == "SamehadaDBInfMinValue"
	case Boolean:
		return *v.boolean == false
	default:
		panic("not implemented")
	}

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

func (v Value) Sub(other *Value) *Value {
	if other.IsNull() {
		return &v
	}

	switch v.valueType {
	case Integer:
		ret := NewInteger(*v.integer - *other.integer)
		return &ret
	case Float:
		ret := NewFloat(*v.float - *other.float)
		return &ret
	default:
		panic("Sub is implemented to Integer and Float only.")
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
	case Varchar:
		if *v.varchar >= *other.varchar {
			ret := NewVarchar(*v.varchar)
			return &ret
		} else {
			ret := NewVarchar(other.ToVarchar())
			return &ret
		}
	default:
		panic("max is implemented to Integer, Float and Varchar only.")
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
	case Varchar:
		if *v.varchar <= *other.varchar {
			ret := NewVarchar(*v.varchar)
			return &ret
		} else {
			ret := NewVarchar(other.ToVarchar())
			return &ret
		}
	default:
		panic("max is implemented to Integer, Float and Varchar only.")
	}
}

func (v Value) Swap(other *Value) {
	if v.isNull != nil || other.isNull != nil {
		panic("not implemented for NULL value")
	}
	//samehada_util.SHAssert(v.valueType == other.valueType, "type mismatch")

	switch v.valueType {
	case Integer:
		*v.integer, *other.integer = *other.integer, *v.integer
	case Float:
		*v.float, *other.float = *other.float, *v.float
	case Varchar:
		*v.varchar, *other.varchar = *other.varchar, *v.varchar
	case Boolean:
		*v.boolean, *other.boolean = *other.boolean, *v.boolean
	default:
		panic("unkown or not supported type")
	}
}

func (v Value) GetDeepCopy() *Value {
	switch v.valueType {
	case Integer:
		return NewValueFromBytes(v.Serialize(), Integer)
	case Float:
		return NewValueFromBytes(v.Serialize(), Float)
	case Varchar:
		return NewValueFromBytes(v.Serialize(), Varchar)
	case Boolean:
		return NewValueFromBytes(v.Serialize(), Boolean)
	default:
		panic("unkown or not supported type")
	}
}
