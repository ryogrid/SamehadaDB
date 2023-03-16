package samehada_util

import (
	"github.com/ryogrid/SamehadaDB/storage/page"
	testing2 "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"testing"
)

func testEncDecToComparableByteArr(t *testing.T, val interface{}, valType types.TypeID) *types.Value {
	switch valType {
	case types.Integer:
		encdBytes := encodeToDicOrderComparableBytes(val, types.Integer)
		testing2.SimpleAssert(t, val.(int32) == decodeFromDicOrderComparableBytes(encdBytes, types.Integer).(int32))
		tmpByteArr := make([]byte, 0)
		tmpByteArr = append(tmpByteArr, []byte{0, 4, 0}...)
		tmpByteArr = append(tmpByteArr, encdBytes...)
		return types.NewValueFromBytes(tmpByteArr, types.Varchar)
	case types.Float:
		encdBytes := encodeToDicOrderComparableBytes(val, types.Float)
		testing2.SimpleAssert(t, val.(float32) == decodeFromDicOrderComparableBytes(encdBytes, types.Float).(float32))
		tmpByteArr := make([]byte, 0)
		tmpByteArr = append(tmpByteArr, []byte{0, 4, 0}...)
		tmpByteArr = append(tmpByteArr, encdBytes...)
		return types.NewValueFromBytes(tmpByteArr, types.Varchar)
	}

	t.Fail()
	return nil
}

func testEncDecOrgKeyAndRIDConcated(t *testing.T, val *types.Value, valType types.TypeID, rid page.RID) *types.Value {
	encdKey := EncodeValueAndRIDToDicOrderComparableVarchar(val, &rid)
	decedKey := ExtractOrgKeyFromDicOrderComparableEncodedVarchar(encdKey, valType)
	testing2.SimpleAssert(t, val.CompareEquals(*decedKey))
	return encdKey
}

func TestEncodeAndDecodeComparableByteArr(t *testing.T) {
	intVal1 := testEncDecToComparableByteArr(t, int32(1), types.Integer)
	intVal2 := testEncDecToComparableByteArr(t, int32(-5), types.Integer)
	intVal3 := testEncDecToComparableByteArr(t, int32(math.MaxInt32), types.Integer)
	intVal4 := testEncDecToComparableByteArr(t, int32(math.MinInt32), types.Integer)
	intVal5 := testEncDecToComparableByteArr(t, int32(0), types.Integer)
	intVal6 := testEncDecToComparableByteArr(t, int32(10), types.Integer)
	intVal7 := testEncDecToComparableByteArr(t, int32(-8), types.Integer)

	testing2.SimpleAssert(t, intVal1.CompareGreaterThan(*intVal2))
	testing2.SimpleAssert(t, intVal3.CompareGreaterThan(*intVal4))
	testing2.SimpleAssert(t, intVal1.CompareGreaterThan(*intVal5))
	testing2.SimpleAssert(t, intVal6.CompareGreaterThan(*intVal1))
	testing2.SimpleAssert(t, intVal7.CompareLessThan(*intVal2))
	testing2.SimpleAssert(t, intVal4.CompareLessThan(*intVal7))

	floatVal1 := testEncDecToComparableByteArr(t, float32(2.5), types.Float)
	floatVal2 := testEncDecToComparableByteArr(t, float32(-2.5), types.Float)
	floatVal3 := testEncDecToComparableByteArr(t, float32(math.MaxFloat32), types.Float)
	floatVal4 := testEncDecToComparableByteArr(t, float32(-1.0*math.MaxFloat32), types.Float)
	floatVal5 := testEncDecToComparableByteArr(t, float32(0), types.Float)
	floatVal6 := testEncDecToComparableByteArr(t, float32(10.2), types.Float)

	testing2.SimpleAssert(t, floatVal1.CompareGreaterThan(*floatVal2))
	testing2.SimpleAssert(t, floatVal3.CompareGreaterThan(*floatVal4))
	testing2.SimpleAssert(t, floatVal1.CompareGreaterThan(*floatVal5))
	testing2.SimpleAssert(t, floatVal1.CompareGreaterThan(*floatVal5))
	testing2.SimpleAssert(t, floatVal6.CompareGreaterThan(*floatVal1))
	testing2.SimpleAssert(t, floatVal2.CompareGreaterThan(*floatVal4))
}

func TestEncDecOrgKeyAndRIDConcated(t *testing.T) {
	intVal1 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewInteger(int32(1))), types.Integer, page.RID{-1, 128})
	intVal2 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewInteger(int32(-5))), types.Integer, page.RID{-1, 128})
	intVal3 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewInteger(int32(math.MaxInt32))), types.Integer, page.RID{-1, 128})
	intVal4 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewInteger(int32(math.MinInt32))), types.Integer, page.RID{-1, 128})
	intVal5 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewInteger(int32(0))), types.Integer, page.RID{-1, 128})
	intVal6 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewInteger(int32(10))), types.Integer, page.RID{-1, 128})
	intVal7 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewInteger(int32(-8))), types.Integer, page.RID{-1, 128})

	testing2.SimpleAssert(t, intVal1.CompareGreaterThan(*intVal2))
	testing2.SimpleAssert(t, intVal3.CompareGreaterThan(*intVal4))
	testing2.SimpleAssert(t, intVal1.CompareGreaterThan(*intVal5))
	testing2.SimpleAssert(t, intVal6.CompareGreaterThan(*intVal1))
	testing2.SimpleAssert(t, intVal7.CompareLessThan(*intVal2))
	testing2.SimpleAssert(t, intVal4.CompareLessThan(*intVal7))

	floatVal1 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewFloat(float32(2.5))), types.Float, page.RID{-1, 128})
	floatVal2 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewFloat(float32(-2.5))), types.Float, page.RID{-1, 128})
	floatVal3 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewFloat(float32(math.MaxFloat32))), types.Float, page.RID{-1, 128})
	floatVal4 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewFloat(float32(-1.0*math.MaxFloat32))), types.Float, page.RID{-1, 128})
	floatVal5 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewFloat(float32(0))), types.Float, page.RID{-1, 128})
	floatVal6 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewFloat(float32(10.2))), types.Float, page.RID{-1, 128})

	testing2.SimpleAssert(t, floatVal1.CompareGreaterThan(*floatVal2))
	testing2.SimpleAssert(t, floatVal3.CompareGreaterThan(*floatVal4))
	testing2.SimpleAssert(t, floatVal1.CompareGreaterThan(*floatVal5))
	testing2.SimpleAssert(t, floatVal1.CompareGreaterThan(*floatVal5))
	testing2.SimpleAssert(t, floatVal6.CompareGreaterThan(*floatVal1))
	testing2.SimpleAssert(t, floatVal2.CompareGreaterThan(*floatVal4))

	charVal1 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewVarchar("abcd")), types.Varchar, page.RID{-1, 128})
	charVal2 := testEncDecOrgKeyAndRIDConcated(t, GetPonterOfValue(types.NewVarchar("abcde")), types.Varchar, page.RID{-1, 128})
	testing2.SimpleAssert(t, charVal2.CompareGreaterThan(*charVal1))
}
