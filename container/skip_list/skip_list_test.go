// TODO: (SDB) not implemented yet skip_list_test.go

package skip_list

import (
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"math/rand"
	"testing"

	testingpkg "github.com/ryogrid/SamehadaDB/testing"
)

func GetPonterOfValue(value types.Value) *types.Value {
	val := value
	return &val
}
func TestSkipListOnMem(t *testing.T) {
	val := types.NewInteger(0)
	sl := NewSkipListOnMem(1, &val, math.MaxUint32, true)

	for i := 0; i < 1000; i++ {
		insVal := rand.Int31()
		sl.InsertOnMem(GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
		res := sl.GetValueOnMem(GetPonterOfValue(types.NewInteger(int32(insVal))))
		if res == math.MaxUint32 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.SimpleAssert(t, uint32(insVal) == res)
		}
	}

	// look for a key that does not exist
	res := sl.GetValueOnMem(GetPonterOfValue(types.NewInteger(int32(120))))
	testingpkg.SimpleAssert(t, math.MaxUint32 == res)

	// delete some values
	for i := 0; i < 100; i++ {
		sl.RemoveOnMem(GetPonterOfValue(types.NewInteger(int32(i))), uint32(i))
		res := sl.GetValueOnMem(GetPonterOfValue(types.NewInteger(int32(i))))

		testingpkg.SimpleAssert(t, math.MaxUint32 == res)
	}
}
