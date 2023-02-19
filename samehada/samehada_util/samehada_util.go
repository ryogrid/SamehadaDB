package samehada_util

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"math/rand"
	"os"
	"strconv"
	"unsafe"
)

func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func PackRIDtoUint32(value *page.RID) uint32 {
	buf1 := new(bytes.Buffer)
	buf2 := new(bytes.Buffer)
	pack_buf := make([]byte, 4)
	binary.Write(buf1, binary.BigEndian, value.PageId)
	binary.Write(buf2, binary.BigEndian, value.SlotNum)
	pageIdInBytes := buf1.Bytes()
	slotNumInBytes := buf2.Bytes()
	copy(pack_buf[:2], pageIdInBytes[2:])
	copy(pack_buf[2:], slotNumInBytes[2:])
	return binary.BigEndian.Uint32(pack_buf)
}

func UnpackUint32toRID(value uint32) page.RID {
	packed_buf := new(bytes.Buffer)
	binary.Write(packed_buf, binary.BigEndian, value)
	packedDataInBytes := packed_buf.Bytes()
	var PageId types.PageID
	var SlotNum uint32
	buf := make([]byte, 4)
	copy(buf[2:], packedDataInBytes[:2])
	PageId = types.PageID(binary.BigEndian.Uint32(buf))
	copy(buf[2:], packedDataInBytes[2:])
	SlotNum = binary.BigEndian.Uint32(buf)
	ret := new(page.RID)
	ret.PageId = PageId
	ret.SlotNum = SlotNum
	return *ret
}

func PackRIDtoUint64(value *page.RID) uint64 {
	buf1 := new(bytes.Buffer)
	buf2 := new(bytes.Buffer)
	pack_buf := make([]byte, 8)
	binary.Write(buf1, binary.BigEndian, value.PageId)
	binary.Write(buf2, binary.BigEndian, value.SlotNum)
	pageIdInBytes := buf1.Bytes()
	slotNumInBytes := buf2.Bytes()
	copy(pack_buf[4:], slotNumInBytes[:])
	copy(pack_buf[:4], pageIdInBytes[:])
	return binary.BigEndian.Uint64(pack_buf)
}

func UnpackUint64toRID(value uint64) page.RID {
	packed_buf := new(bytes.Buffer)
	binary.Write(packed_buf, binary.BigEndian, value)
	packedDataInBytes := packed_buf.Bytes()
	var PageId types.PageID
	var SlotNum uint32
	buf := make([]byte, 4)
	copy(buf[:4], packedDataInBytes[:4])
	PageId = types.PageID(binary.BigEndian.Uint32(buf))
	copy(buf[:4], packedDataInBytes[4:])
	SlotNum = binary.BigEndian.Uint32(buf)
	ret := new(page.RID)
	ret.PageId = PageId
	ret.SlotNum = SlotNum
	return *ret
}

func GetPonterOfValue(value types.Value) *types.Value {
	val := value
	return &val
}

// min length is 1
func GetRandomStr(maxLength int32) *string {
	alphabets :=
		"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz!#$%&'(),-./:;<=>?@[]^_`{|}~"
	//"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var len_ int
	//for len_ < 50 {
	//	len_ = 1 + (rand.Intn(math.MaxInt32))%(int(maxLength)-1)
	//}
	len_ = 1 + (rand.Intn(math.MaxInt32))%(int(maxLength)-1)

	s := ""
	for j := 0; j < len_; j++ {
		idx := rand.Intn(52)
		s = s + alphabets[idx:idx+1]
	}

	return &s
}

func RemovePrimitiveFromList[T int32 | float32 | string](list []T, elem T) []T {
	list_ := append(make([]T, 0), list...)
	for i, r := range list {
		if r == elem {
			list_ = append(list[:i], list[i+1:]...)
			break
		}
	}
	return list_
}

func IsContainList[T comparable](list interface{}, searchItem interface{}) bool {
	for _, t := range list.([]T) {
		if t == searchItem.(T) {
			return true
		}
	}
	return false
}

//func GetParentFuncName() string {
//	_, name, _, _ := runtime.Caller(1)
//	return name
//}

// maxVal is *int32 when get int32 and float32
func GetRandomPrimitiveVal[T int32 | float32 | string](keyType types.TypeID, maxVal interface{}) T {
	switch keyType {
	case types.Integer:
		var val int32
		specifiedMax, ok := maxVal.(int32)
		if ok {
			val = rand.Int31n(specifiedMax)
		} else {
			val = rand.Int31()
		}
		if val < 0 {
			val = -1 * ((-1 * val) % (math.MaxInt32 >> 10))
		} else {
			val = val % (math.MaxInt32 >> 10)
		}
		var ret interface{} = val
		return ret.(T)
	case types.Float:
		//var ret interface{}
		//specifiedMax, ok := maxVal.(float32)
		//if ok {
		//	ret = specifiedMax * rand.Float32()
		//} else {
		//	ret = rand.Float32()
		//}
		//val := 100.0 + rand.Float32()
		//val := rand.Float32()
		val := rand.Int31n(0xFFFF)
		var ret interface{} = float32(val)
		return ret.(T)
	case types.Varchar:
		//var ret interface{} = *samehada_util.GetRandomStr(1000)
		//var ret interface{} = *GetRandomStr(500)
		var ret interface{} = *GetRandomStr(200)
		return ret.(T)
	default:
		panic("not supported keyType")
	}
}

func ChoiceValFromMap[T int32 | float32 | string, V int32 | float32 | string](m map[T]V) T {
	l := len(m)
	i := 0

	index := rand.Intn(l)

	var ans T
	for k, _ := range m {
		if index == i {
			ans = k
			break
		} else {
			i++
		}
	}
	return ans
}

func GetValueForSkipListEntry(val interface{}) uint64 {
	var ret uint64
	switch val.(type) {
	case int32:
		ret = uint64(val.(int32))
	case float32:
		ret = uint64(val.(float32))
	case string:
		ret = uint64(len(val.(string)))
	default:
		panic("unsupported type!")
	}
	return ret
}

func StrideAdd(base interface{}, k interface{}) interface{} {
	switch base.(type) {
	case int32:
		return base.(int32) + k.(int32)
	case float32:
		return base.(float32) + float32(k.(int32))
		//return base.(float32) * float32(math.Pow(1.05, float64(k.(int32))))
	case string:
		//buf := make([]byte, k.(int32))
		//memset(buf, 'Z')
		return base.(string) + "+" + strconv.Itoa(int(k.(int32)))
	default:
		panic("not supported type")
	}
}

func StrideMul(base interface{}, k interface{}) interface{} {
	switch base.(type) {
	case int32:
		return base.(int32) * k.(int32)
	case float32:
		return base.(float32) * float32(k.(int32))
	case string:
		//return "DEADBEAF" + base.(string)
		//buf := make([]byte, k.(int32))
		//memset(buf, 'A')
		return base.(string) + "*" + strconv.Itoa(int(k.(int32)))
	default:
		panic("not supported type")
	}
}

const SIGN_MASK_BIG uint32 = 0x80000000
const SIGN_MASK_SMALL byte = 0x80

// true = big endian, false = little endian
func getEndian() (ret bool) {
	var i int = 0x1
	bs := (*[4]byte)(unsafe.Pointer(&i))
	if bs[0] == 0 {
		return true
	} else {
		return false
	}
}

// note: this converts with considering endian of program execution environment
func encodeToDicOrderComparableBytes(orgVal interface{}, valType types.TypeID) []byte {
	switch valType {
	case types.Float:
		f := orgVal.(float32)
		u := math.Float32bits(f)
		if f >= 0 {
			u |= SIGN_MASK_BIG
		} else {
			u = ^u
		}
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, u)
		return buf.Bytes()
	case valType:
		i := orgVal.(int32)
		u := uint32(i)
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.BigEndian, u)
		convedArr := buf.Bytes()
		convedArr[0] ^= SIGN_MASK_SMALL
		return convedArr
	default:
		panic("not supported type")
	}
}

// note: this converts with considering endian of program execution environment
func decodeFromDicOrderComparableBytes(convedArr []byte, valType types.TypeID) interface{} {
	switch valType {
	case types.Float:
		buf := bytes.NewBuffer(convedArr)
		var u uint32
		binary.Read(buf, binary.BigEndian, &u)

		if u&SIGN_MASK_BIG > 0 {
			u &= ^SIGN_MASK_BIG
		} else {
			u = ^u
		}
		return math.Float32frombits(u)
	case types.Integer:
		convedArr_ := make([]byte, 4)
		copy(convedArr_, convedArr)
		convedArr_[0] ^= SIGN_MASK_SMALL
		buf := bytes.NewBuffer(convedArr_)
		var u uint32
		binary.Read(buf, binary.BigEndian, &u)
		return int32(u)
	default:
		panic("not suppoted type")
	}

}

func EncodeValueAndRIDToDicOrderComparableBytes(orgVal *types.Value, rid *page.RID) *types.Value {
	arrToFill := make([]byte, 0)
	switch orgVal.ValueType() {
	case types.Integer:
		convedBytes := encodeToDicOrderComparableBytes(orgVal.ToInteger(), types.Integer)
		totalSizeBytes := types.UInt16(len(convedBytes) + 8).Serialize()
		// {false, firstByte of str len, secondeByte}
		arrToFill = append(arrToFill, []byte{0, totalSizeBytes[1], totalSizeBytes[0]}...)
		arrToFill = append(arrToFill, convedBytes...)
		arrToFill = append(arrToFill, types.UInt64(PackRIDtoUint64(rid)).Serialize()...)
		return types.NewValueFromBytes(arrToFill, types.Varchar)
	case types.Float:
		convedBytes := encodeToDicOrderComparableBytes(orgVal.ToFloat(), types.Float)
		totalSizeBytes := types.UInt16(len(convedBytes) + 8).Serialize()
		// {false, firstByte of str len, secondeByte}
		arrToFill = append(arrToFill, []byte{0, totalSizeBytes[1], totalSizeBytes[0]}...)
		arrToFill = append(arrToFill, convedBytes...)
		arrToFill = append(arrToFill, types.UInt64(PackRIDtoUint64(rid)).Serialize()...)
		return types.NewValueFromBytes(arrToFill, types.Varchar)
	case types.Varchar:
		convedBytes := orgVal.Serialize()
		strLen := uint16(len(orgVal.ToString()))
		strLenBytes := types.UInt16(strLen + 8).Serialize()
		// {false, firstByte of str len, secondeByte}
		arrToFill = append(arrToFill, []byte{0, strLenBytes[1], strLenBytes[0]}...)
		arrToFill = append(arrToFill, convedBytes[3:]...)
		arrToFill = append(arrToFill, types.UInt64(PackRIDtoUint64(rid)).Serialize()...)
		fmt.Println(arrToFill)
		return types.NewValueFromBytes(arrToFill, types.Varchar)
	default:
		panic("not supported type")
	}
}

func ExtractOrgKeyFromDicOrderComparableEncodedVarchar(encodedVal *types.Value, valType types.TypeID) *types.Value {
	switch valType {
	case types.Integer:
		encodedStrBytes := encodedVal.Serialize()
		retVal := types.NewValue(decodeFromDicOrderComparableBytes(encodedStrBytes[3:len(encodedStrBytes)-8], valType).(int32))
		return &retVal
	case types.Float:
		encodedStrBytes := encodedVal.Serialize()
		retVal := types.NewValue(decodeFromDicOrderComparableBytes(encodedStrBytes[3:len(encodedStrBytes)-8], valType).(float32))
		return &retVal
	case types.Varchar:
		encodedStr := encodedVal.ToString()
		orgStr := encodedStr[:len(encodedStr)-8]
		return GetPonterOfValue(types.NewVarchar(orgStr))
	default:
		panic("not supported type")
	}
}

func TimeoutPanic() {
	common.RuntimeStack()
	os.Stdout.Sync()
	panic("timeout reached")
}
