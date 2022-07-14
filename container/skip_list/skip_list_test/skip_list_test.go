package skip_list_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/container/skip_list"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"math/rand"
	"os"
	"testing"
)

//func TestSkipListOnMem(t *testing.T) {
//	val := types.NewInteger(0)
//	sl := skip_list.NewSkipListOnMem(1, &val, math.MaxUint32, true)
//
//	insVals := make([]int32, 0)
//	for i := 0; i < 250; i++ {
//		insVals = append(insVals, int32(i*11))
//	}
//	// shuffle value list for inserting
//	rand.Shuffle(len(insVals), func(i, j int) { insVals[i], insVals[j] = insVals[j], insVals[i] })
//	for _, insVal := range insVals {
//		//fmt.Println(insVal)
//		sl.InsertOnMem(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//		res := sl.GetValueOnMem(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))))
//		if res == math.MaxUint32 {
//			t.Errorf("result should not be nil")
//		} else {
//			testingpkg.SimpleAssert(t, uint32(insVal) == res)
//		}
//	}
//
//	// delete some values
//	for i := 0; i < 100; i++ {
//		// check existance before delete
//		res := sl.GetValueOnMem(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
//		testingpkg.SimpleAssert(t, res == uint32(i*11))
//
//		// check no existance after delete
//		sl.RemoveOnMem(samehada_util.GetPonterOfValue(types.NewInteger(int32(i*11))), uint32(i*11))
//		res = sl.GetValueOnMem(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
//		testingpkg.SimpleAssert(t, math.MaxUint32 == res)
//	}
//}
//
//func TestSkipListItrOnMem(t *testing.T) {
//	val := types.NewInteger(0)
//	sl := skip_list.NewSkipListOnMem(1, &val, math.MaxUint32, true)
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
//		sl.InsertOnMem(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
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
//	itr1 := sl.IteratorOnMem(startValP, endValP)
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
//	itr2 := sl.IteratorOnMem(startValP, endValP)
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
//	itr3 := sl.IteratorOnMem(startValP, endValP)
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
//	itr4 := sl.IteratorOnMem(startValP, endValP)
//	for done, _, key, _ := itr4.Next(); !done; done, _, key, _ = itr4.Next() {
//		testingpkg.SimpleAssert(t, lastKeyVal <= key.ToInteger())
//		//fmt.Println(key.ToInteger())
//		lastKeyVal = key.ToInteger()
//		nodeCnt++
//	}
//
//	testingpkg.SimpleAssert(t, nodeCnt == 250)
//}

func TestBSearchOfSkipLisBlockPageBackedOnMem(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.log")
	shi := samehada.NewSamehadaInstanceForTesting()
	bpm := shi.GetBufferPoolManager()

	bpage := skip_list_page.NewSkipListBlockPage(bpm, 1, &skip_list_page.SkipListPair{
		Key:   types.NewInteger(-1),
		Value: 0,
	})

	// ------- when element num is even number -----
	bpage.Entries = make([]*skip_list_page.SkipListPair, 0)
	bpage.Entries = append(bpage.Entries, &skip_list_page.SkipListPair{
		Key:   types.NewInteger(-1),
		Value: 0,
	})
	// set entries
	for ii := 1; ii < 50; ii++ {
		bpage.Entries = append(bpage.Entries, &skip_list_page.SkipListPair{types.NewInteger(int32(ii * 10)), uint32(ii * 10)})
	}
	bpage.EntryCnt = int32(len(bpage.Entries))

	for ii := 1; ii < 100; ii++ {
		key := types.NewInteger(int32(ii * 5))
		found, entry, idx := bpage.FindEntryByKey(&key)
		//fmt.Println(ii)
		if ii%2 == 0 {
			testingpkg.SimpleAssert(t, found == true && entry.Value == uint32(key.ToInteger()))
		} else {
			testingpkg.SimpleAssert(t, found == false && uint32(key.ToInteger())-bpage.ValueAt(idx) == 5)
		}
	}

	// ------- when element num is odd number -----
	bpage.Entries = make([]*skip_list_page.SkipListPair, 0)
	bpage.Entries = append(bpage.Entries, &skip_list_page.SkipListPair{
		Key:   types.NewInteger(-1),
		Value: 0,
	})
	// set entries
	for ii := 1; ii < 51; ii++ {
		bpage.Entries = append(bpage.Entries, &skip_list_page.SkipListPair{types.NewInteger(int32(ii * 10)), uint32(ii * 10)})
	}
	bpage.EntryCnt = int32(len(bpage.Entries))

	for ii := 1; ii < 102; ii++ {
		key := types.NewInteger(int32(ii * 5))
		found, entry, idx := bpage.FindEntryByKey(&key)
		//fmt.Println(ii)
		if ii%2 == 0 {
			testingpkg.SimpleAssert(t, found == true && entry.Value == uint32(key.ToInteger()))
		} else {
			testingpkg.SimpleAssert(t, found == false && uint32(key.ToInteger())-bpage.ValueAt(idx) == 5)
		}
	}
}

func confirmSkipListContent(t *testing.T, sl *skip_list.SkipList, step int32) int32 {
	entryCnt := int32(0)
	lastKeyVal := int32(-1)
	dupCheckMap := make(map[int32]int32)
	itr := sl.Iterator(nil, nil)
	for done, _, key, _ := itr.Next(); !done; done, _, key, _ = itr.Next() {
		curVal := key.ToInteger()
		fmt.Printf("lastKeyVal=%d curVal=%d nodeCnt=%d\n", lastKeyVal, curVal, entryCnt)
		_, ok := dupCheckMap[curVal]
		if !(lastKeyVal == -1 || (lastKeyVal <= curVal && (curVal-lastKeyVal == step))) {
			fmt.Println("!!! curVal or lastKeyVal is invalid !!!")
		} else if ok {
			fmt.Println("!!! curVal is duplicated !!!")
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

func TestSkipLisPageBackedOnMem(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.log")

	shi := samehada.NewSamehadaInstanceForTesting()
	sl := skip_list.NewSkipList(shi.GetBufferPoolManager(), types.Integer)

	// override global rand seed (seed has been set on NewSkipList)
	rand.Seed(3)

	insVals := make([]int32, 0)
	for i := 0; i < 250; i++ {
		insVals = append(insVals, int32(i*11))
	}
	// shuffle value list for inserting
	rand.Shuffle(len(insVals), func(i, j int) { insVals[i], insVals[j] = insVals[j], insVals[i] })

	// Insert entries
	insCnt := 0
	for _, insVal := range insVals {
		fmt.Printf("insCnt: %d\n", insCnt)
		insCnt++
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
	}

	confirmSkipListContent(t, sl, 11)

	// Get entries
	for i := 0; i < 250; i++ {
		fmt.Printf("get entry i=%d key=%d\n", i, i*11)
		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		if res == math.MaxUint32 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.SimpleAssert(t, uint32(i*11) == res)
		}
	}

	// delete some values
	for i := 0; i < 100; i++ {
		// check existance before delete
		res := uint32(0)
		if i == 74 {
			res = sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		} else {
			res = sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		}
		fmt.Printf("check existance before delete : i=%d\n", i)
		testingpkg.SimpleAssert(t, res == uint32(i*11))

		// check no existance after delete
		sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(i*11))), uint32(i*11))

		res = sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		testingpkg.SimpleAssert(t, math.MaxUint32 == res)
	}
}

func TestSkipListItrPageBackedOnMem(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.log")

	shi := samehada.NewSamehadaInstanceForTesting()
	sl := skip_list.NewSkipList(shi.GetBufferPoolManager(), types.Integer)

	insVals := make([]int32, 0)
	for i := 0; i < 250; i++ {
		insVals = append(insVals, int32(i*11))
	}
	// shuffle value list for inserting
	rand.Shuffle(len(insVals), func(i, j int) { insVals[i], insVals[j] = insVals[j], insVals[i] })

	for _, insVal := range insVals {
		//fmt.Println(insVal)
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
	}

	//fmt.Println("--------------")
	//sl.CheckElemListOnMem()

	//fmt.Println("--------------")
	lastKeyVal := int32(0)
	startVal := int32(77777)
	endVal := int32(math.MaxInt32 / 2)
	startValP := samehada_util.GetPonterOfValue(types.NewInteger(startVal))
	endValP := samehada_util.GetPonterOfValue(types.NewInteger(endVal))

	itr1 := sl.Iterator(startValP, endValP)
	for done, _, key, _ := itr1.Next(); !done; done, _, key, _ = itr1.Next() {
		testingpkg.SimpleAssert(t, startVal <= key.ToInteger() && key.ToInteger() <= endVal && lastKeyVal <= key.ToInteger())
		//fmt.Println(key.ToInteger())
		lastKeyVal = key.ToInteger()
	}

	//fmt.Println("--------------")
	lastKeyVal = int32(0)
	startValP = nil
	endValP = samehada_util.GetPonterOfValue(types.NewInteger(endVal))
	itr2 := sl.Iterator(startValP, endValP)
	for done, _, key, _ := itr2.Next(); !done; done, _, key, _ = itr2.Next() {
		testingpkg.SimpleAssert(t, key.ToInteger() <= endVal && lastKeyVal <= key.ToInteger())
		//fmt.Println(key.ToInteger())
		lastKeyVal = key.ToInteger()
	}

	//fmt.Println("--------------")
	lastKeyVal = int32(0)
	startValP = samehada_util.GetPonterOfValue(types.NewInteger(startVal))
	endValP = nil
	itr3 := sl.Iterator(startValP, endValP)
	for done, _, key, _ := itr3.Next(); !done; done, _, key, _ = itr3.Next() {
		testingpkg.SimpleAssert(t, startVal <= key.ToInteger() && lastKeyVal <= key.ToInteger())
		//fmt.Println(key.ToInteger())
		lastKeyVal = key.ToInteger()
	}

	//fmt.Println("--------------")
	lastKeyVal = int32(0)
	startValP = nil
	endValP = nil
	nodeCnt := 0
	itr4 := sl.Iterator(startValP, endValP)
	for done, _, key, _ := itr4.Next(); !done; done, _, key, _ = itr4.Next() {
		fmt.Printf("lastKeyVal=%d curVal=%d nodeCnt=%d\n", lastKeyVal, key.ToInteger(), nodeCnt)
		testingpkg.SimpleAssert(t, lastKeyVal <= key.ToInteger())
		lastKeyVal = key.ToInteger()
		nodeCnt++
	}

	fmt.Printf("nodeCnt=%d\n", nodeCnt)
	testingpkg.SimpleAssert(t, nodeCnt == 250)
}
