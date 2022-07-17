package skip_list_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/container/skip_list"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
	"math/rand"
	"os"
	"testing"
)

//func TestBSearchOfSkipLisBlockPageBackedOnMem(t *testing.T) {
//	os.Remove("test.db")
//	os.Remove("test.log")
//	shi := samehada.NewSamehadaInstanceForTesting()
//	bpm := shi.GetBufferPoolManager()
//
//	bpage := skip_list_page.NewSkipListBlockPage(bpm, 1, &skip_list_page.SkipListPair{
//		Key:   types.NewInteger(-1),
//		Value: 0,
//	})
//
//	// ------- when element num is even number -----
//	bpage.Entries = make([]*skip_list_page.SkipListPair, 0)
//	bpage.Entries = append(bpage.Entries, &skip_list_page.SkipListPair{
//		Key:   types.NewInteger(-1),
//		Value: 0,
//	})
//	// set entries
//	for ii := 1; ii < 50; ii++ {
//		bpage.Entries = append(bpage.Entries, &skip_list_page.SkipListPair{types.NewInteger(int32(ii * 10)), uint32(ii * 10)})
//	}
//	bpage.EntryCnt = int32(len(bpage.Entries))
//
//	for ii := 1; ii < 100; ii++ {
//		key := types.NewInteger(int32(ii * 5))
//		found, entry, idx := bpage.FindEntryByKey(&key)
//		//fmt.Println(ii)
//		if ii%2 == 0 {
//			testingpkg.SimpleAssert(t, found == true && entry.Value == uint32(key.ToInteger()))
//		} else {
//			testingpkg.SimpleAssert(t, found == false && uint32(key.ToInteger())-bpage.ValueAt(idx) == 5)
//		}
//	}
//
//	// ------- when element num is odd number -----
//	bpage.Entries = make([]*skip_list_page.SkipListPair, 0)
//	bpage.Entries = append(bpage.Entries, &skip_list_page.SkipListPair{
//		Key:   types.NewInteger(-1),
//		Value: 0,
//	})
//	// set entries
//	for ii := 1; ii < 51; ii++ {
//		bpage.Entries = append(bpage.Entries, &skip_list_page.SkipListPair{types.NewInteger(int32(ii * 10)), uint32(ii * 10)})
//	}
//	bpage.EntryCnt = int32(len(bpage.Entries))
//
//	for ii := 1; ii < 102; ii++ {
//		key := types.NewInteger(int32(ii * 5))
//		found, entry, idx := bpage.FindEntryByKey(&key)
//		//fmt.Println(ii)
//		if ii%2 == 0 {
//			testingpkg.SimpleAssert(t, found == true && entry.Value == uint32(key.ToInteger()))
//		} else {
//			testingpkg.SimpleAssert(t, found == false && uint32(key.ToInteger())-bpage.ValueAt(idx) == 5)
//		}
//	}
//}

func countSkipListContent(sl *skip_list.SkipList) int32 {
	entryCnt := int32(0)
	itr := sl.Iterator(nil, nil)
	for done, _, _, _ := itr.Next(); !done; done, _, _, _ = itr.Next() {
		entryCnt++
	}

	//fmt.Printf("entryCnt=%d\n", entryCnt)
	return entryCnt
}

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

//func TestSkipLisPageBackedOnMem(t *testing.T) {
//	os.Remove("test.db")
//	os.Remove("test.log")
//
//	shi := samehada.NewSamehadaInstanceForTesting()
//	sl := skip_list.NewSkipList(shi.GetBufferPoolManager(), types.Integer)
//
//	// override global rand seed (seed has been set on NewSkipList)
//	rand.Seed(3)
//
//	insVals := make([]int32, 0)
//	for i := 0; i < 250; i++ {
//		insVals = append(insVals, int32(i*11))
//	}
//	// shuffle value list for inserting
//	rand.Shuffle(len(insVals), func(i, j int) { insVals[i], insVals[j] = insVals[j], insVals[i] })
//
//	// Insert entries
//	insCnt := 0
//	for _, insVal := range insVals {
//		//fmt.Printf("insCnt: %d\n", insCnt)
//		insCnt++
//		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//	}
//
//	//confirmSkipListContent(t, sl, 11)
//
//	// Get entries
//	for i := 0; i < 250; i++ {
//		//fmt.Printf("get entry i=%d key=%d\n", i, i*11)
//		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
//		if res == math.MaxUint32 {
//			t.Errorf("result should not be nil")
//		} else {
//			testingpkg.SimpleAssert(t, uint32(i*11) == res)
//		}
//	}
//
//	// delete some values
//	for i := 0; i < 100; i++ {
//		// check existance before delete
//		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
//		//fmt.Printf("check existance before delete : i=%d\n", i)
//		testingpkg.SimpleAssert(t, res == uint32(i*11))
//
//		// check no existance after delete
//		sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(i*11))), uint32(i*11))
//
//		res = sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
//		testingpkg.SimpleAssert(t, math.MaxUint32 == res)
//		//fmt.Println("contents listing after delete")
//		//confirmSkipListContent(t, sl, -1)
//	}
//}
//
//func TestSkipListItrPageBackedOnMem(t *testing.T) {
//	os.Remove("test.db")
//	os.Remove("test.log")
//
//	shi := samehada.NewSamehadaInstanceForTesting()
//	sl := skip_list.NewSkipList(shi.GetBufferPoolManager(), types.Integer)
//
//	insVals := make([]int32, 0)
//	for i := 0; i < 250; i++ {
//		insVals = append(insVals, int32(i*11))
//	}
//	// shuffle value list for inserting
//	rand.Shuffle(len(insVals), func(i, j int) { insVals[i], insVals[j] = insVals[j], insVals[i] })
//
//	for _, insVal := range insVals {
//		//fmt.Println(insVal)
//		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//	}
//
//	//fmt.Println("--------------")
//	//sl.CheckElemListOnMem()
//
//	//fmt.Println("--------------")
//	lastKeyVal := int32(0)
//	startVal := int32(77777)
//	endVal := int32(math.MaxInt32 / 2)
//	startValP := samehada_util.GetPonterOfValue(types.NewInteger(startVal))
//	endValP := samehada_util.GetPonterOfValue(types.NewInteger(endVal))
//
//	itr1 := sl.Iterator(startValP, endValP)
//	for done, _, key, _ := itr1.Next(); !done; done, _, key, _ = itr1.Next() {
//		testingpkg.SimpleAssert(t, startVal <= key.ToInteger() && key.ToInteger() <= endVal && lastKeyVal <= key.ToInteger())
//		//fmt.Println(key.ToInteger())
//		lastKeyVal = key.ToInteger()
//	}
//
//	//fmt.Println("--------------")
//	lastKeyVal = int32(0)
//	startValP = nil
//	endValP = samehada_util.GetPonterOfValue(types.NewInteger(endVal))
//	itr2 := sl.Iterator(startValP, endValP)
//	for done, _, key, _ := itr2.Next(); !done; done, _, key, _ = itr2.Next() {
//		testingpkg.SimpleAssert(t, key.ToInteger() <= endVal && lastKeyVal <= key.ToInteger())
//		//fmt.Println(key.ToInteger())
//		lastKeyVal = key.ToInteger()
//	}
//
//	//fmt.Println("--------------")
//	lastKeyVal = int32(0)
//	startValP = samehada_util.GetPonterOfValue(types.NewInteger(startVal))
//	endValP = nil
//	itr3 := sl.Iterator(startValP, endValP)
//	for done, _, key, _ := itr3.Next(); !done; done, _, key, _ = itr3.Next() {
//		testingpkg.SimpleAssert(t, startVal <= key.ToInteger() && lastKeyVal <= key.ToInteger())
//		//fmt.Println(key.ToInteger())
//		lastKeyVal = key.ToInteger()
//	}
//
//	//fmt.Println("--------------")
//	lastKeyVal = int32(0)
//	startValP = nil
//	endValP = nil
//	nodeCnt := 0
//	itr4 := sl.Iterator(startValP, endValP)
//	for done, _, key, _ := itr4.Next(); !done; done, _, key, _ = itr4.Next() {
//		//fmt.Printf("lastKeyVal=%d curVal=%d nodeCnt=%d\n", lastKeyVal, key.ToInteger(), nodeCnt)
//		testingpkg.SimpleAssert(t, lastKeyVal <= key.ToInteger())
//		lastKeyVal = key.ToInteger()
//		nodeCnt++
//	}
//
//	//fmt.Printf("nodeCnt=%d\n", nodeCnt)
//	testingpkg.SimpleAssert(t, nodeCnt == 250)
//}

//func FuzzSkipLisMixOpPageBackedOnMem(f *testing.F) {
//	const MAX_ENTRIES = 700
//
//	f.Add(uint8(150), uint8(10), uint16(0))
//	f.Add(uint8(150), uint8(10), uint16(300))
//	f.Add(uint8(150), uint8(10), uint16(600))
//	f.Fuzz(func(t *testing.T, opTimes uint8, skipRand uint8, initialEntryNum uint16) {
//		os.Remove("test.db")
//		os.Remove("test.log")
//
//		//fmt.Println("fuzzing test now!")
//
//		shi := samehada.NewSamehadaInstanceForTesting()
//		sl := skip_list.NewSkipList(shi.GetBufferPoolManager(), types.Integer)
//
//		// override global rand seed (seed has been set on NewSkipList)
//		rand.Seed(3)
//
//		tmpSkipRand := skipRand
//		// skip random value series
//		for tmpSkipRand > 0 {
//			rand.Int31()
//			tmpSkipRand--
//		}
//
//		insVals := make([]int32, 0)
//
//		// initial entries
//		// uint8 range is small...
//		useInitialEntryNum := int(initialEntryNum)
//		for ii := 0; ii < useInitialEntryNum; ii++ {
//			if len(insVals) < MAX_ENTRIES {
//				insVal := rand.Int31()
//				insVals = append(insVals, insVal)
//				sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//				insVals = append(insVals, insVal)
//			}
//		}
//
//		removedVals := make([]int32, 0)
//		// uint8 range is small...
//		useOpTimes := int(opTimes * 2)
//		for ii := 0; ii < useOpTimes; ii++ {
//			// get 0-2
//			opType := rand.Intn(3)
//			switch opType {
//			case 0: // Insert
//				if len(insVals) < MAX_ENTRIES {
//					insVal := rand.Int31()
//					insVals = append(insVals, insVal)
//					sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//					insVals = append(insVals, insVal)
//				}
//			case 1: // Delete
//				// get 0-5 value
//				tmpRand := rand.Intn(6)
//				if tmpRand == 0 {
//					// 20% is Remove to not existing entry
//					tmpIdx := int(math.Abs(float64(rand.Intn(len(removedVals)) - 1)))
//					tmpVal := removedVals[tmpIdx]
//					isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(tmpVal))), uint32(tmpVal))
//					testingpkg.SimpleAssert(t, isDeleted == false)
//				} else {
//					// 80% is Remove to existing entry
//					if len(insVals) > 0 {
//						tmpIdx := int(math.Abs(float64(rand.Intn(len(insVals)) - 1)))
//						isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVals[tmpIdx]))), uint32(insVals[tmpIdx]))
//						testingpkg.SimpleAssert(t, isDeleted == true)
//						if len(insVals) == 1 {
//							// make empty
//							insVals = make([]int32, 0)
//						} else if len(insVals) == tmpIdx+1 {
//							insVals = insVals[:len(insVals)-1]
//						} else {
//							insVals = append(insVals[:tmpIdx], insVals[tmpIdx+1:]...)
//						}
//					}
//				}
//			case 2: // Get
//				if len(insVals) > 0 {
//					tmpIdx := int(math.Abs(float64(rand.Intn(len(insVals)) - 1)))
//					gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVals[tmpIdx]))))
//					testingpkg.SimpleAssert(t, gotVal == uint32(insVals[tmpIdx]))
//				}
//			}
//		}
//	})

const MAX_ENTRIES = 700

func insertRandom(sl *skip_list.SkipList, num int32, insVals *[]int32, checkDupMap map[int32]int32) {
	if int32(len(*insVals))+num < MAX_ENTRIES {
		for ii := 0; ii < int(num); ii++ {
			//insVal := rand.Int31()
			insVal := rand.Int31()
			for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
				insVal = rand.Int31()
			}
			checkDupMap[insVal] = insVal

			sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
			tmpInsVal := append(*insVals, insVal)
			insVals = &tmpInsVal
		}
	}
}

func removeRandom(t *testing.T, sl *skip_list.SkipList, num int32, insVals *[]int32, removedVals *[]int32) {
	if int32(len(*insVals))-num > 0 {
		for ii := 0; ii < int(num); ii++ {
			tmpIdx := int(rand.Intn(len(*insVals)))
			insValsPointed := *insVals
			insVal := insValsPointed[tmpIdx]
			isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
			if isDeleted != true {
				fmt.Println("isDeleted should be true!")
				common.RuntimeStack()
			}
			testingpkg.SimpleAssert(t, isDeleted == true)
			if len(*insVals) == 1 {
				// make empty
				insValsP := make([]int32, 0)
				insVals = &insValsP
			} else if len(*insVals) == tmpIdx+1 {
				insValsPointed := *insVals
				insValsPointed = insValsPointed[:len(*insVals)-1]
				insVals = &insValsPointed
			} else {
				insValsPointed := *insVals
				insValsPointed = append(insValsPointed[:tmpIdx], insValsPointed[tmpIdx+1:]...)
				insVals = &insValsPointed
			}
			removedValsPointed := *removedVals
			removedValsPointed = append(removedValsPointed, insVal)
			removedVals = &removedValsPointed
		}
	}
}

func testSkipLisMixOpPageBackedOnMemInner(t *testing.T, bulkSize int32, opTimes uint8, skipRand uint8, initialEntryNum uint16) {
	os.Remove("test.db")
	os.Remove("test.log")

	//fmt.Println("fuzzing test now!")
	checkDupMap := make(map[int32]int32)

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
	useInitialEntryNum := int(initialEntryNum)
	for ii := 0; ii < useInitialEntryNum; ii++ {
		if len(insVals) < MAX_ENTRIES {
			// avoid duplication
			insVal := rand.Int31()
			for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
				insVal = rand.Int31()
			}
			checkDupMap[insVal] = insVal

			sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
			insVals = append(insVals, insVal)
		}
	}

	// check num of stored entries on sl is same with num of initial entries (if differ, there are bug)
	if int32(useInitialEntryNum) != countSkipListContent(sl) {
		fmt.Println("initial entries are invalid!")
		common.RuntimeStack()
	}

	removedVals := make([]int32, 0)
	// uint8 range is small...
	useOpTimes := int(opTimes * 4)
	for ii := 0; ii < useOpTimes; ii++ {
		// get 0-2
		opType := rand.Intn(3)
		switch opType {
		case 0: // Insert
			if int32(len(insVals))+bulkSize < MAX_ENTRIES {
				//insVal := rand.Int31()
				insertRandom(sl, bulkSize, &insVals, checkDupMap)
				//sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
				//insVals = append(insVals, insVal)
			}
		case 1: // Delete
			// get 0-5 value
			tmpRand := rand.Intn(6)
			if tmpRand == 0 {
				// 20% is Remove to not existing entry
				if len(removedVals) != 0 {
					tmpIdx := int(rand.Intn(len(removedVals)))
					tmpVal := removedVals[tmpIdx]
					isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(tmpVal))), uint32(tmpVal))
					testingpkg.SimpleAssert(t, isDeleted == false)
				}
			} else {
				// 80% is Remove to existing entry
				if int32(len(insVals))-bulkSize > 0 {
					removeRandom(t, sl, bulkSize, &insVals, &removedVals)
					//tmpIdx := int(rand.Intn(len(insVals)))
					//insVal := insVals[tmpIdx]
					//isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
					//testingpkg.SimpleAssert(t, isDeleted == true)
					//if len(insVals) == 1 {
					//	// make empty
					//	insVals = make([]int32, 0)
					//} else if len(insVals) == tmpIdx+1 {
					//	insVals = insVals[:len(insVals)-1]
					//} else {
					//	insVals = append(insVals[:tmpIdx], insVals[tmpIdx+1:]...)
					//}
					//removedVals = append(removedVals, insVal)
				}
			}
		case 2: // Get
			if len(insVals) > 0 {
				tmpIdx := int(rand.Intn(len(insVals)))
				gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVals[tmpIdx]))))
				if gotVal != uint32(insVals[tmpIdx]) {
					fmt.Println(gotVal)
					common.RuntimeStack()
				}
				testingpkg.SimpleAssert(t, gotVal == uint32(insVals[tmpIdx]))
			}
		}
	}

	shi.Finalize(false)
}

func TestSkipLisMixOpPageBackedOnMem(t *testing.T) {
	testSkipLisMixOpPageBackedOnMemInner(t, 100, uint8(150), uint8(10), uint16(0))
	testSkipLisMixOpPageBackedOnMemInner(t, 100, uint8(150), uint8(10), uint16(300))
	testSkipLisMixOpPageBackedOnMemInner(t, 100, uint8(150), uint8(10), uint16(600))
	testSkipLisMixOpPageBackedOnMemInner(t, 100, uint8(200), uint8(5), uint16(10))
	testSkipLisMixOpPageBackedOnMemInner(t, 100, uint8(250), uint8(5), uint16(10))
	testSkipLisMixOpPageBackedOnMemInner(t, 100, uint8(250), uint8(4), uint16(0))
	testSkipLisMixOpPageBackedOnMemInner(t, 100, uint8(250), uint8(3), uint16(0))

	testSkipLisMixOpPageBackedOnMemInner(t, 50, uint8(150), uint8(10), uint16(0))
	testSkipLisMixOpPageBackedOnMemInner(t, 50, uint8(150), uint8(10), uint16(300))
	testSkipLisMixOpPageBackedOnMemInner(t, 50, uint8(150), uint8(10), uint16(600))
	testSkipLisMixOpPageBackedOnMemInner(t, 50, uint8(200), uint8(5), uint16(10))
	testSkipLisMixOpPageBackedOnMemInner(t, 50, uint8(250), uint8(5), uint16(10))
	testSkipLisMixOpPageBackedOnMemInner(t, 50, uint8(250), uint8(4), uint16(0))
	testSkipLisMixOpPageBackedOnMemInner(t, 50, uint8(250), uint8(3), uint16(0))

	testSkipLisMixOpPageBackedOnMemInner(t, 1, uint8(150), uint8(10), uint16(0))
	testSkipLisMixOpPageBackedOnMemInner(t, 1, uint8(150), uint8(10), uint16(300))
	testSkipLisMixOpPageBackedOnMemInner(t, 1, uint8(150), uint8(10), uint16(600))
	testSkipLisMixOpPageBackedOnMemInner(t, 1, uint8(200), uint8(5), uint16(10))
	testSkipLisMixOpPageBackedOnMemInner(t, 1, uint8(250), uint8(5), uint16(10))
	testSkipLisMixOpPageBackedOnMemInner(t, 1, uint8(250), uint8(4), uint16(0))
	testSkipLisMixOpPageBackedOnMemInner(t, 1, uint8(250), uint8(3), uint16(0))
}
