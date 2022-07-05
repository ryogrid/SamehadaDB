// TODO: (SDB) not implemented yet skip_list_test.go

package skip_list

import (
	"fmt"
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

	//// look for a key that does not exist
	//res := sl.GetValueOnMem(GetPonterOfValue(types.NewInteger(int32(120))))
	//testingpkg.SimpleAssert(t, math.MaxUint32 == res)

	// delete some values
	for i := 0; i < 100; i++ {
		sl.RemoveOnMem(GetPonterOfValue(types.NewInteger(int32(i))), uint32(i))
		res := sl.GetValueOnMem(GetPonterOfValue(types.NewInteger(int32(i))))

		testingpkg.SimpleAssert(t, math.MaxUint32 == res)
	}
}

func TestSkipListItrOnMem(t *testing.T) {
	val := types.NewInteger(0)
	sl := NewSkipListOnMem(1, &val, math.MaxUint32, true)

	for i := 0; i < 10; i++ {
		insVal := rand.Int31()
		fmt.Println(insVal)
		sl.InsertOnMem(GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
	}

	fmt.Println("--------------")
	sl.CheckElemListOnMem()

	fmt.Println("--------------")
	lastKeyVal := int32(0)
	startVal := int32(77777)
	endVal := int32(math.MaxInt32 / 2)
	startValP := GetPonterOfValue(types.NewInteger(startVal))
	endValP := GetPonterOfValue(types.NewInteger(endVal))

	itr1 := sl.IteratorOnMem(startValP, endValP)
	for done, _, key, _ := itr1.Next(); !done; done, _, key, _ = itr1.Next() {
		testingpkg.SimpleAssert(t, startVal <= key.ToInteger() && key.ToInteger() <= endVal && lastKeyVal < key.ToInteger())
		fmt.Println(key.ToInteger())
		lastKeyVal = key.ToInteger()
	}

	fmt.Println("--------------")
	lastKeyVal = int32(0)
	startValP = nil
	endValP = GetPonterOfValue(types.NewInteger(endVal))
	itr2 := sl.IteratorOnMem(startValP, endValP)
	for done, _, key, _ := itr2.Next(); !done; done, _, key, _ = itr2.Next() {
		testingpkg.SimpleAssert(t, key.ToInteger() <= endVal && lastKeyVal < key.ToInteger())
		fmt.Println(key.ToInteger())
		lastKeyVal = key.ToInteger()
	}

	fmt.Println("--------------")
	lastKeyVal = int32(0)
	startValP = GetPonterOfValue(types.NewInteger(startVal))
	endValP = nil
	itr3 := sl.IteratorOnMem(startValP, endValP)
	for done, _, key, _ := itr3.Next(); !done; done, _, key, _ = itr3.Next() {
		testingpkg.SimpleAssert(t, startVal <= key.ToInteger() && lastKeyVal < key.ToInteger())
		fmt.Println(key.ToInteger())
		lastKeyVal = key.ToInteger()
	}

	fmt.Println("--------------")
	lastKeyVal = int32(0)
	startValP = nil
	endValP = nil
	nodeCnt := 0
	itr4 := sl.IteratorOnMem(startValP, endValP)
	for done, _, key, _ := itr4.Next(); !done; done, _, key, _ = itr4.Next() {
		testingpkg.SimpleAssert(t, lastKeyVal < key.ToInteger())
		fmt.Println(key.ToInteger())
		lastKeyVal = key.ToInteger()
		nodeCnt++
	}
	// if rand func doesn't return duplicated value...
	testingpkg.SimpleAssert(t, nodeCnt == 250)
}
