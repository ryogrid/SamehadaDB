package skip_list_fuzz

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/container/skip_list"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"math/rand"
	"os"
	"testing"
)

func confirmSkipListContent(t *testing.T, sl *skip_list.SkipList, step int32) int32 {
	entryCnt := int32(0)
	lastKeyVal := int32(-1)
	dupCheckMap := make(map[int32]int32)
	itr := sl.Iterator(nil, nil)
	for done, _, key, _ := itr.Next(); !done; done, _, key, _ = itr.Next() {
		curVal := key.ToInteger()
		fmt.Printf("lastKeyVal=%d curVal=%d entryCnt=%d\n", lastKeyVal, curVal, entryCnt)
		_, ok := dupCheckMap[curVal]
		if step != -1 {
			if !(lastKeyVal == -1 || (lastKeyVal <= curVal && (curVal-lastKeyVal == step))) {
				fmt.Println("!!! curVal or lastKeyVal is invalid !!!")
			} else if ok {
				fmt.Println("!!! curVal is duplicated !!!")
			}
		}
		//testingpkg.SimpleAssert(t, lastKeyVal == -1 || (lastKeyVal <= key.ToInteger() && (key.ToInteger()-lastKeyVal == step)))
		//testingpkg.SimpleAssert(t, lastKeyVal != key.ToInteger())
		lastKeyVal = curVal
		dupCheckMap[lastKeyVal] = lastKeyVal
		entryCnt++
	}

	fmt.Printf("entryCnt=%d\n", entryCnt)
	return entryCnt

}

func FuzzSkipLisMixOpPageBackedOnMem(f *testing.F) {
	const MAX_ENTRIES = 700

	f.Add(150, 10)
	f.Fuzz(func(t *testing.T, opTimes uint8, skipRand uint8, initialEntryNum uint8) {
		os.Remove("test.db")
		os.Remove("test.log")

		shi := samehada.NewSamehadaInstanceForTesting()
		sl := skip_list.NewSkipList(shi.GetBufferPoolManager(), types.Integer)

		// override global rand seed (seed has been set on NewSkipList)
		rand.Seed(3)

		tmpSkipRand := skipRand
		// skip random value series
		for tmpSkipRand > 0 {
			rand.Int31()
			tmpSkipRand--
		}

		insVals := make([]int32, 0)

		// initial entries
		useInitialEntryNum := 3 * int(initialEntryNum)
		for ii := 0; ii < useInitialEntryNum; ii++ {
			if len(insVals) < MAX_ENTRIES {
				insVal := rand.Int31()
				insVals = append(insVals, insVal)
				sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
				insVals = append(insVals, insVal)
			}
		}

		removedVals := make([]int32, 0)
		// uint8 range is small...
		useOpTimes := int(opTimes * 2)
		for ii := 0; ii < useOpTimes; ii++ {
			// get 0-2
			opType := rand.Intn(3)
			switch opType {
			case 0: // Insert
				if len(insVals) < MAX_ENTRIES {
					insVal := rand.Int31()
					insVals = append(insVals, insVal)
					sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
					insVals = append(insVals, insVal)
				}
			case 1: // Delete
				// get 0-5 value
				tmpRand := rand.Intn(6)
				if tmpRand == 0 {
					// 20% is Remove to not existing entry
					tmpIdx := int(math.Abs(float64(rand.Intn(len(removedVals)) - 1)))
					tmpVal := removedVals[tmpIdx]
					isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(tmpVal))), uint32(tmpVal))
					testingpkg.SimpleAssert(t, isDeleted == false)
				} else {
					// 80% is Remove to existing entry
					if len(insVals) > 0 {
						tmpIdx := int(math.Abs(float64(rand.Intn(len(insVals)) - 1)))
						isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVals[tmpIdx]))), uint32(insVals[tmpIdx]))
						testingpkg.SimpleAssert(t, isDeleted == true)
						if len(insVals) == 1 {
							// make empty
							insVals = make([]int32, 0)
						} else if len(insVals) == tmpIdx+1 {
							insVals = insVals[:len(insVals)-1]
						} else {
							insVals = append(insVals[:tmpIdx], insVals[tmpIdx+1:]...)
						}
					}
				}
			case 2: // Get
				tmpIdx := int(math.Abs(float64(rand.Intn(len(insVals)) - 1)))
				gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVals[tmpIdx]))))
				testingpkg.SimpleAssert(t, gotVal == uint32(insVals[tmpIdx]))
			}
		}
	})
}
