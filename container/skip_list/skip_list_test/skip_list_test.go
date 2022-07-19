package skip_list_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/container/skip_list"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"math/rand"
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

//func confirmSkipListContent(t *testing.T, sl *skip_list.SkipList, step int32) int32 {
//	entryCnt := int32(0)
//	lastKeyVal := int32(-1)
//	dupCheckMap := make(map[int32]int32)
//	itr := sl.Iterator(nil, nil)
//	for done, _, key, _ := itr.Next(); !done; done, _, key, _ = itr.Next() {
//		curVal := key.ToInteger()
//		fmt.Printf("lastKeyVal=%d curVal=%d entryCnt=%d\n", lastKeyVal, curVal, entryCnt)
//		_, ok := dupCheckMap[curVal]
//		if step != -1 {
//			if !(lastKeyVal == -1 || (lastKeyVal <= curVal && (curVal-lastKeyVal == step))) {
//				fmt.Println("!!! curVal or lastKeyVal is invalid !!!")
//			} else if ok {
//				fmt.Println("!!! curVal is duplicated !!!")
//			}
//		}
//		//testingpkg.SimpleAssert(t, lastKeyVal == -1 || (lastKeyVal <= key.ToInteger() && (key.ToInteger()-lastKeyVal == step)))
//		//testingpkg.SimpleAssert(t, lastKeyVal != key.ToInteger())
//		lastKeyVal = curVal
//		dupCheckMap[lastKeyVal] = lastKeyVal
//		entryCnt++
//	}
//
//	fmt.Printf("entryCnt=%d\n", entryCnt)
//	return entryCnt
//}

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

//func insertRandom(sl *skip_list.SkipList, num int32, insVals *[]int32, checkDupMap map[int32]int32) {
//	if int32(len(*insVals))+num < MAX_ENTRIES {
//		for ii := 0; ii < int(num); ii++ {
//			//insVal := rand.Int31()
//			insVal := rand.Int31()
//			for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
//				insVal = rand.Int31()
//			}
//			checkDupMap[insVal] = insVal
//
//			if insVal == TARGET_KEY_RELATED_BUG {
//				fmt.Printf("!!!insert of TARGET_KEY_RELATED_BUG!!!  ii=%d, insVal=%d len(*insVals)=%d\n", ii, insVal, len(*insVals))
//				isInserted = true
//			}
//			sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//			fmt.Printf("sl.Insert at insertRandom: ii=%d, insVal=%d len(*insVals)=%d\n", ii, insVal, len(*insVals))
//			if isInserted && !isExistKeyOnList(sl, TARGET_KEY_RELATED_BUG, false) {
//				fmt.Printf("TARGET_KEY_RELATED_BUG does not visible with iterator!\n")
//			}
//			tmpInsVals := append(*insVals, insVal)
//			*insVals = tmpInsVals
//		}
//	}
//}

const MAX_ENTRIES = 700
const TARGET_KEY_RELATED_BUG = 2041122064

var isInserted bool = false
var entriesOnListNum int32 = 0
var removedEntriesNum int32 = 0
var insVals []int32
var removedVals []int32

////for debug
//func isAlreadyRemoved(removedVals []int32, checkVal int32) bool {
//	for _, val := range removedVals {
//		if val == checkVal {
//			return true
//		}
//	}
//	return false
//}

//for debug
func isAlreadyRemoved2(checkVal int32) bool {
	for _, val := range removedVals {
		if val == checkVal {
			return true
		}
	}
	return false
}

// for debug
func isExistKeyOnList(sl *skip_list.SkipList, checkKey int32, isPrint bool) bool {
	itr := sl.Iterator(nil, nil)
	fmt.Printf("isExistKeyOnList:")
	for done, _, key, _ := itr.Next(); !done; done, _, key, _ = itr.Next() {
		if isPrint {
			fmt.Printf(" %d", key.ToInteger())
		}
		if key.ToInteger() == checkKey {
			if isPrint {
				fmt.Println("")
			}
			return true
		}
	}

	if isPrint {
		fmt.Println("")
	}
	return false
}

func countSkipListContent(sl *skip_list.SkipList) int32 {
	entryCnt := int32(0)
	itr := sl.Iterator(nil, nil)
	for done, _, _, _ := itr.Next(); !done; done, _, _, _ = itr.Next() {
		entryCnt++
	}

	//fmt.Printf("entryCnt=%d\n", entryCnt)
	return entryCnt
}

//func removeRandom(t *testing.T, sl *skip_list.SkipList, opStep int32, num int32, insVals *[]int32, removedVals *[]int32) {
//	if int32(len(*insVals))-num > 0 {
//		for ii := 0; ii < int(num); ii++ {
//			tmpIdx := int(rand.Intn(len(*insVals)))
//			insValsPointed := *insVals
//			insVal := insValsPointed[tmpIdx]
//			if insVal == TARGET_KEY_RELATED_BUG {
//				fmt.Println(TARGET_KEY_RELATED_BUG)
//			}
//			isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//			fmt.Printf("sl.Remove at removeRandom: ii=%d, insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, insVal, len(*insVals), len(*removedVals))
//			if isInserted && !isExistKeyOnList(sl, TARGET_KEY_RELATED_BUG, false) {
//				fmt.Printf("TARGET_KEY_RELATED_BUG does not visible with iterator!\n")
//			}
//			if isDeleted != true && !isAlreadyRemoved(*removedVals, insVal) {
//				fmt.Printf("isDeleted should be true! opStep=%d, ii=%d tmpIdx=%d insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", opStep, ii, tmpIdx, insVal, len(*insVals), len(*removedVals))
//				common.RuntimeStack()
//			}
//			testingpkg.SimpleAssert(t, isDeleted == true || isAlreadyRemoved(*removedVals, insVal))
//			if len(*insVals) == 1 {
//				// make empty
//				insValsTmp := make([]int32, 0)
//				*insVals = insValsTmp
//			} else if len(*insVals) == tmpIdx+1 {
//				insValsPointed := *insVals
//				insValsPointed = insValsPointed[:len(*insVals)-1]
//				*insVals = insValsPointed
//			} else {
//				insValsPointed := *insVals
//				insValsPointed = append(insValsPointed[:tmpIdx], insValsPointed[tmpIdx+1:]...)
//				*insVals = insValsPointed
//			}
//			removedValsPointed := *removedVals
//			removedValsPointed = append(removedValsPointed, insVal)
//			*removedVals = removedValsPointed
//		}
//	}
//}
//
//func testSkipLisMixOpPageBackedOnMemInner(t *testing.T, bulkSize int32, opTimes uint8, skipRand uint8, initialEntryNum uint16) {
//	os.Remove("test.db")
//	os.Remove("test.log")
//
//	//fmt.Println("fuzzing test now!")
//	checkDupMap := make(map[int32]int32)
//
//	shi := samehada.NewSamehadaInstanceForTesting()
//	sl := skip_list.NewSkipList(shi.GetBufferPoolManager(), types.Integer)
//
//	// override global rand seed (seed has been set on NewSkipList)
//	rand.Seed(3)
//
//	tmpSkipRand := skipRand
//	// skip random value series
//	for tmpSkipRand > 0 {
//		rand.Int31()
//		tmpSkipRand--
//	}
//
//	insVals := make([]int32, 0)
//
//	// initial entries
//	useInitialEntryNum := int(initialEntryNum)
//	for ii := 0; ii < useInitialEntryNum; ii++ {
//		if len(insVals) < MAX_ENTRIES {
//			// avoid duplication
//			insVal := rand.Int31()
//			for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
//				insVal = rand.Int31()
//			}
//			checkDupMap[insVal] = insVal
//
//			sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//			insVals = append(insVals, insVal)
//		}
//	}
//
//	// entries num on SkipList should be same with this variable
//	entriesOnListNum := int32(useInitialEntryNum)
//	removedEntriesNum := int32(0)
//
//	// check num of stored entries on sl is same with num of initial entries (if differ, there are bug)
//	if entriesOnListNum != countSkipListContent(sl) {
//		fmt.Println("initial entries are invalid!")
//		common.RuntimeStack()
//	}
//
//	removedVals := make([]int32, 0)
//	//useOpTimes := int(opTimes * 4)
//	useOpTimes := int(opTimes)
//	for ii := 0; ii < useOpTimes; ii++ {
//		// get 0-2
//		opType := rand.Intn(3)
//		switch opType {
//		case 0: // Insert
//			if int32(len(insVals))+bulkSize < MAX_ENTRIES {
//				//insVal := rand.Int31()
//				insertRandom(sl, bulkSize, &insVals, checkDupMap)
//				//sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//				//insVals = append(insVals, insVal)
//				entriesOnListNum += bulkSize
//				if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
//					fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
//					common.RuntimeStack()
//				}
//			}
//		case 1: // Delete
//			// get 0-5 value
//			tmpRand := rand.Intn(6)
//			if tmpRand == 0 {
//				//// 20% is Remove to not existing entry
//				//if len(removedVals) != 0 {
//				//	tmpIdx := int(rand.Intn(len(removedVals)))
//				//	tmpVal := removedVals[tmpIdx]
//				//	isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(tmpVal))), uint32(tmpVal))
//				//	testingpkg.SimpleAssert(t, isDeleted == false)
//				//	if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
//				//		fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
//				//		common.RuntimeStack()
//				//	}
//				//}
//			} else {
//				// 80% is Remove to existing entry
//				if int32(len(insVals))-bulkSize > 0 {
//					removeRandom(t, sl, int32(ii), bulkSize, &insVals, &removedVals)
//					entriesOnListNum -= bulkSize
//					removedEntriesNum += bulkSize
//					if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
//						fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
//						common.RuntimeStack()
//					}
//
//					//tmpIdx := int(rand.Intn(len(insVals)))
//					//insVal := insVals[tmpIdx]
//					//isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//					//testingpkg.SimpleAssert(t, isDeleted == true)
//					//if len(insVals) == 1 {
//					//	// make empty
//					//	insVals = make([]int32, 0)
//					//} else if len(insVals) == tmpIdx+1 {
//					//	insVals = insVals[:len(insVals)-1]
//					//} else {
//					//	insVals = append(insVals[:tmpIdx], insVals[tmpIdx+1:]...)
//					//}
//					//removedVals = append(removedVals, insVal)
//				}
//			}
//		case 2: // Get
//			if len(insVals) > 0 {
//				tmpIdx := int(rand.Intn(len(insVals)))
//				gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVals[tmpIdx]))))
//				if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
//					fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
//					common.RuntimeStack()
//				}
//				if gotVal != uint32(insVals[tmpIdx]) {
//					fmt.Println(gotVal)
//					common.RuntimeStack()
//				}
//				testingpkg.SimpleAssert(t, gotVal == uint32(insVals[tmpIdx]))
//			}
//		}
//	}
//
//	shi.Finalize(false)
//}
//
//func Disabled_TestSkipLisMixOpPageBackedOnMem(t *testing.T) {
//	testSkipLisMixOpPageBackedOnMemInner(t, 100, uint8(150), uint8(10), uint16(0))
//	testSkipLisMixOpPageBackedOnMemInner(t, 100, uint8(150), uint8(10), uint16(300))
//	testSkipLisMixOpPageBackedOnMemInner(t, 100, uint8(150), uint8(10), uint16(600))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 100, uint8(200), uint8(5), uint16(10))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 100, uint8(250), uint8(5), uint16(10))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 100, uint8(250), uint8(4), uint16(0))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 100, uint8(250), uint8(3), uint16(0))
//	//
//	//testSkipLisMixOpPageBackedOnMemInner(t, 50, uint8(150), uint8(10), uint16(0))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 50, uint8(150), uint8(10), uint16(300))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 50, uint8(150), uint8(10), uint16(600))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 50, uint8(200), uint8(5), uint16(10))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 50, uint8(250), uint8(5), uint16(10))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 50, uint8(250), uint8(4), uint16(0))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 50, uint8(250), uint8(3), uint16(0))
//	//
//	//testSkipLisMixOpPageBackedOnMemInner(t, 1, uint8(150), uint8(10), uint16(0))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 1, uint8(150), uint8(10), uint16(300))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 1, uint8(150), uint8(10), uint16(600))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 1, uint8(200), uint8(5), uint16(10))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 1, uint8(250), uint8(5), uint16(10))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 1, uint8(250), uint8(4), uint16(0))
//	//testSkipLisMixOpPageBackedOnMemInner(t, 1, uint8(250), uint8(3), uint16(0))
//}

func insertRandom2(sl *skip_list.SkipList, num int32, checkDupMap map[int32]int32) {
	if int32(len(insVals))+num < MAX_ENTRIES {
		for ii := 0; ii < int(num); ii++ {
			//insVal := rand.Int31()
			insVal := rand.Int31()
			for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
				insVal = rand.Int31()
			}
			checkDupMap[insVal] = insVal

			if insVal == TARGET_KEY_RELATED_BUG {
				isInserted = true
			}
			if insVal == 1431672847 {
				fmt.Println("")
			}
			sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
			fmt.Printf("sl.Insert at insertRandom: ii=%d, insVal=%d len(*insVals)=%d\n", ii, insVal, len(insVals))
			if isInserted && !isExistKeyOnList(sl, TARGET_KEY_RELATED_BUG, true) {
				panic("TARGET_KEY_RELATED_BUG does not visible with iterator!")
			}
			insVals = append(insVals, insVal)
		}
	}
}

func removeRandom2(t *testing.T, sl *skip_list.SkipList, opStep int32, num int32) {
	if int32(len(insVals))-num > 0 {
		for ii := 0; ii < int(num); ii++ {
			tmpIdx := int(rand.Intn(len(insVals)))
			insVal := insVals[tmpIdx]
			if insVal == TARGET_KEY_RELATED_BUG {
				isInserted = false
			}
			//sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
			isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
			fmt.Printf("sl.Remove at removeRandom: ii=%d, insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, insVal, len(insVals), len(removedVals))
			if isInserted && !isExistKeyOnList(sl, TARGET_KEY_RELATED_BUG, true) {
				panic("TARGET_KEY_RELATED_BUG does not visible with iterator!")
			}
			if isAlreadyRemoved2(insVal) {
				fmt.Printf("delete duplicated value should not be occur! opStep=%d, ii=%d tmpIdx=%d insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", opStep, ii, tmpIdx, insVal, len(insVals), len(removedVals))
				panic("delete duplicated value should not be occur!")
			}
			//if isDeleted != true && !isAlreadyRemoved2(insVal) {
			if isDeleted != true {
				fmt.Printf("isDeleted should be true! opStep=%d, ii=%d tmpIdx=%d insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", opStep, ii, tmpIdx, insVal, len(insVals), len(removedVals))
				panic("isDeleted should be true!")
				//common.RuntimeStack()
			}
			testingpkg.SimpleAssert(t, isDeleted == true || isAlreadyRemoved2(insVal))
			if len(insVals) == 1 {
				// make empty
				insVals = make([]int32, 0)
			} else if len(insVals) == tmpIdx+1 {
				insVals = insVals[:len(insVals)-1]
			} else {
				insVals = append(insVals[:tmpIdx], insVals[tmpIdx+1:]...)
			}
			removedVals = append(removedVals, insVal)
		}
	}
}

func testSkipLisMixOpPageBackedOnMemInner2(t *testing.T, bulkSize int32, opTimes uint8, skipRand uint8, initialEntryNum uint16) {
	fmt.Println("")
	fmt.Println("")
	fmt.Printf("start of testSkipLisMixOpPageBackedOnMemInner2 bulkSize=%d opTimes=%d skipRand=%d initialEntryNum=%d ====================================================\n",
		bulkSize, opTimes, skipRand, initialEntryNum)

	//os.Remove("test.db")
	//os.Remove("test.log")

	//fmt.Println("fuzzing test now!")
	checkDupMap := make(map[int32]int32)

	//shi := samehada.NewSamehadaInstanceForTesting()
	//sl := skip_list.NewSkipList(shi.GetBufferPoolManager(), types.Integer)
	sl := skip_list.NewSkipList(nil, types.Integer)

	// override global rand seed (seed has been set on NewSkipList)
	rand.Seed(3)

	tmpSkipRand := skipRand
	// skip random value series
	for tmpSkipRand > 0 {
		rand.Int31()
		tmpSkipRand--
	}

	insVals = make([]int32, 0)
	removedVals = make([]int32, 0)
	isInserted = false
	entriesOnListNum = 0

	// initial entries
	useInitialEntryNum := int(initialEntryNum)
	for ii := 0; ii < useInitialEntryNum; ii++ {
		if entriesOnListNum+1 < MAX_ENTRIES {
			// avoid duplication
			insVal := rand.Int31()
			for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
				insVal = rand.Int31()
			}
			checkDupMap[insVal] = insVal
			if insVal == TARGET_KEY_RELATED_BUG {
				isInserted = true
			}

			fmt.Printf("sl.Insert at testSkipLisMixOpPageBackedOnMemInner2 for initial entry: ii=%d, insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, insVal, len(insVals), len(removedVals))
			sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
			insVals = append(insVals, insVal)
			entriesOnListNum++
		}
	}

	// entries num on SkipList should be same with this variable
	//entriesOnListNum = int32(useInitialEntryNum)
	removedEntriesNum = int32(0)

	// check num of stored entries on sl is same with num of initial entries (if differ, there are bug)
	if entriesOnListNum != countSkipListContent(sl) {
		fmt.Println("initial entries num are strange!")
		panic("initial entries count are strange!")
		//common.RuntimeStack()
	}

	//useOpTimes := int(opTimes * 4)
	useOpTimes := int(opTimes)
	for ii := 0; ii < useOpTimes; ii++ {
		// get 0-2
		opType := rand.Intn(3)
		switch opType {
		case 0: // Insert
			if int32(len(insVals))+bulkSize < MAX_ENTRIES {
				//insVal := rand.Int31()
				insertRandom2(sl, bulkSize, checkDupMap)
				//sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
				//insVals = append(insVals, insVal)
				entriesOnListNum += bulkSize
				if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
					fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
					//common.RuntimeStack()
					panic("entries num on list is strange!")
				}
			}
		case 1: // Delete
			// get 0-5 value
			tmpRand := rand.Intn(6)
			if tmpRand == 0 {
				//// 20% is Remove to not existing entry
				//if len(removedVals) != 0 {
				//	tmpIdx := int(rand.Intn(len(removedVals)))
				//	tmpVal := removedVals[tmpIdx]
				//	isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(tmpVal))), uint32(tmpVal))
				//	testingpkg.SimpleAssert(t, isDeleted == false)
				//	if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
				//		fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
				//		common.RuntimeStack()
				//	}
				//}
			} else {
				// 80% is Remove to existing entry
				if entriesOnListNum-bulkSize > 0 {
					removeRandom2(t, sl, int32(ii), bulkSize)
					entriesOnListNum -= bulkSize
					removedEntriesNum += bulkSize
					if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
						fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
						panic("entries num on list is strange!")
						//common.RuntimeStack()
					}
				}
			}
		case 2: // Get
			if len(insVals) > 0 {
				tmpIdx := int(rand.Intn(len(insVals)))
				if insVals[tmpIdx] == TARGET_KEY_RELATED_BUG {
					fmt.Println("")
				}
				fmt.Printf("sl.GetValue at testSkipLisMixOpPageBackedOnMemInner2: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(removedVals))
				gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVals[tmpIdx]))))
				if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
					fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
					panic("entries num on list is strange!")
					//common.RuntimeStack()
				}
				if gotVal == math.MaxUint32 {
					fmt.Printf("%d is not found!\n", insVals[tmpIdx])
					panic("sl.GetValue could not target key!")
				}
				if gotVal != uint32(insVals[tmpIdx]) {
					fmt.Printf("gotVal is not match! %d != %d\n", gotVal, insVals[tmpIdx])
					panic("gotVal is not match!")
					//common.RuntimeStack()
				}
				testingpkg.SimpleAssert(t, gotVal == uint32(insVals[tmpIdx]))
			}
		}
	}

	//shi.Finalize(false)
}

func TestSkipLisMixOpPageBackedOnMem2(t *testing.T) {
	testSkipLisMixOpPageBackedOnMemInner2(t, 1, uint8(150), uint8(10), uint16(0))
	testSkipLisMixOpPageBackedOnMemInner2(t, 1, uint8(150), uint8(10), uint16(300))
	testSkipLisMixOpPageBackedOnMemInner2(t, 1, uint8(150), uint8(10), uint16(600))
	testSkipLisMixOpPageBackedOnMemInner2(t, 1, uint8(200), uint8(5), uint16(10))
	testSkipLisMixOpPageBackedOnMemInner2(t, 1, uint8(250), uint8(5), uint16(10))
	testSkipLisMixOpPageBackedOnMemInner2(t, 1, uint8(250), uint8(4), uint16(0))
	testSkipLisMixOpPageBackedOnMemInner2(t, 1, uint8(250), uint8(3), uint16(0))

	testSkipLisMixOpPageBackedOnMemInner2(t, 50, uint8(150), uint8(10), uint16(0))
	testSkipLisMixOpPageBackedOnMemInner2(t, 50, uint8(150), uint8(10), uint16(300))
	testSkipLisMixOpPageBackedOnMemInner2(t, 50, uint8(150), uint8(10), uint16(600))
	testSkipLisMixOpPageBackedOnMemInner2(t, 50, uint8(200), uint8(5), uint16(10))
	testSkipLisMixOpPageBackedOnMemInner2(t, 50, uint8(250), uint8(5), uint16(10))
	testSkipLisMixOpPageBackedOnMemInner2(t, 50, uint8(250), uint8(4), uint16(0))
	testSkipLisMixOpPageBackedOnMemInner2(t, 50, uint8(250), uint8(3), uint16(0))

	testSkipLisMixOpPageBackedOnMemInner2(t, 100, uint8(150), uint8(10), uint16(0))
	testSkipLisMixOpPageBackedOnMemInner2(t, 100, uint8(150), uint8(10), uint16(300))
	testSkipLisMixOpPageBackedOnMemInner2(t, 100, uint8(150), uint8(10), uint16(600))
	testSkipLisMixOpPageBackedOnMemInner2(t, 100, uint8(200), uint8(5), uint16(10))
	testSkipLisMixOpPageBackedOnMemInner2(t, 100, uint8(250), uint8(5), uint16(10))
	testSkipLisMixOpPageBackedOnMemInner2(t, 100, uint8(250), uint8(4), uint16(0))
	testSkipLisMixOpPageBackedOnMemInner2(t, 100, uint8(250), uint8(3), uint16(0))
}
