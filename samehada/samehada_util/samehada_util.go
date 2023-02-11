package samehada_util

import (
	"bytes"
	"encoding/binary"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/storage/page"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"math/rand"
	"os"
	"strconv"
)

func FileExists(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

func PackRIDtoUint32(value *page.RID) uint32 {
	buf1 := new(bytes.Buffer)
	buf2 := new(bytes.Buffer)
	pack_buf := make([]byte, 4)
	binary.Write(buf1, binary.LittleEndian, value.PageId)
	binary.Write(buf2, binary.LittleEndian, value.SlotNum)
	pageIdInBytes := buf1.Bytes()
	slotNumInBytes := buf2.Bytes()
	copy(pack_buf[:2], pageIdInBytes[:2])
	copy(pack_buf[2:], slotNumInBytes[:2])
	return binary.LittleEndian.Uint32(pack_buf)
}

func UnpackUint32toRID(value uint32) page.RID {
	packed_buf := new(bytes.Buffer)
	binary.Write(packed_buf, binary.LittleEndian, value)
	packedDataInBytes := packed_buf.Bytes()
	var PageId types.PageID
	var SlotNum uint32
	buf := make([]byte, 4)
	copy(buf[:2], packedDataInBytes[:2])
	PageId = types.PageID(binary.LittleEndian.Uint32(buf))
	copy(buf[:2], packedDataInBytes[2:])
	SlotNum = binary.LittleEndian.Uint32(buf)
	ret := new(page.RID)
	ret.PageId = PageId
	ret.SlotNum = SlotNum
	return *ret
}

func PackRIDtoUint64(value *page.RID) uint64 {
	buf1 := new(bytes.Buffer)
	buf2 := new(bytes.Buffer)
	pack_buf := make([]byte, 8)
	binary.Write(buf1, binary.LittleEndian, value.PageId)
	binary.Write(buf2, binary.LittleEndian, value.SlotNum)
	pageIdInBytes := buf1.Bytes()
	slotNumInBytes := buf2.Bytes()
	copy(pack_buf[:4], pageIdInBytes[:])
	copy(pack_buf[4:], slotNumInBytes[:])
	return binary.LittleEndian.Uint64(pack_buf)
}

func UnpackUint64toRID(value uint64) page.RID {
	packed_buf := new(bytes.Buffer)
	binary.Write(packed_buf, binary.LittleEndian, value)
	packedDataInBytes := packed_buf.Bytes()
	var PageId types.PageID
	var SlotNum uint32
	buf := make([]byte, 4)
	copy(buf[:4], packedDataInBytes[:4])
	PageId = types.PageID(binary.LittleEndian.Uint32(buf))
	copy(buf[:4], packedDataInBytes[4:])
	SlotNum = binary.LittleEndian.Uint32(buf)
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

// note: this converts with considering endian of program execution environment
func convToDicOrderComparableBytes[T int32 | float32](orgVal T, valType types.TypeID) []byte {
	// TODO: (SDB) need implement ConvToDicOrderComparableBytes
	if valType == types.Float {
		const signMask uint32 = 0x80000000
		f := orgVal.(float32)
		u := math.Float32bits(f)
		if f >= 0 {
			u |= signMask
		} else {
			u = ^u
		}
		retBuf := new(bytes.Buffer)
		binary.Write(retBuf, binary.BigEndian, u)
		return retBuf.Bytes()
	}

	panic("not implemented yet")
}

func ConvValueAndRIDToDicOrderComparableBytes(orgVal *types.Value, rid *page.RID) []byte {
	// TODO: (SDB) need implement ConvToDicOrderComparableBytes
	panic("not implemented yet")
}

func TimeoutPanic() {
	common.RuntimeStack()
	os.Stdout.Sync()
	panic("timeout reached")
}
