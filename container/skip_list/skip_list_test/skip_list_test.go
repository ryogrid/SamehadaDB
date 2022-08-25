package skip_list_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/container/skip_list"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/buffer"
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"math/rand"
	"os"
	"testing"
)

func TestSerializationOfSkipLisBlockPage(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.log")
	shi := samehada.NewSamehadaInstanceForTesting()
	bpm := shi.GetBufferPoolManager()

	bpage := skip_list_page.NewSkipListBlockPage(bpm, 1, skip_list_page.SkipListPair{
		Key:   types.NewInteger(math.MinInt32),
		Value: 0,
	})

	bpage.SetPageId(7)
	bpage.SetEntryCnt(1)
	bpage.SetLevel(4)
	bpage.SetForwardEntry(5, types.PageID(11))
	bpage.SetFreeSpacePointer(common.PageSize - 9)
	// EntryCnt is incremented to 2
	// freeSpacePointer is decremented size of entry (1+2+7+4 => 14)
	bpage.SetEntry(1, &skip_list_page.SkipListPair{types.NewVarchar("abcdeff"), 12345})

	testingpkg.SimpleAssert(t, bpage.GetPageId() == 7)
	testingpkg.SimpleAssert(t, bpage.GetEntryCnt() == 2)
	testingpkg.SimpleAssert(t, bpage.GetLevel() == 4)
	testingpkg.SimpleAssert(t, bpage.GetForwardEntry(5) == types.PageID(11))

	testingpkg.SimpleAssert(t, bpage.GetFreeSpacePointer() == (common.PageSize-9-14))
	entry := bpage.GetEntry(1, types.Varchar)
	testingpkg.SimpleAssert(t, entry.Key.CompareEquals(types.NewVarchar("abcdeff")))
	testingpkg.SimpleAssert(t, entry.Value == 12345)

	shi.Shutdown(false)
}

func TestInnerInsertDeleteOfBlockPageSimple(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.log")
	shi := samehada.NewSamehadaInstanceForTesting()
	bpm := shi.GetBufferPoolManager()

	// ---------- test RemoveInner --------
	// setup a page
	bpage1 := skip_list_page.NewSkipListBlockPage(bpm, 1, skip_list_page.SkipListPair{
		Key:   types.NewVarchar("abcd"),
		Value: 1,
	})

	initialEntries := make([]*skip_list_page.SkipListPair, 0)
	initialEntries = append(initialEntries, bpage1.GetEntry(0, types.Varchar))
	initialEntries = append(initialEntries, &skip_list_page.SkipListPair{
		Key:   types.NewVarchar("abcde"),
		Value: 2,
	})
	initialEntries = append(initialEntries, &skip_list_page.SkipListPair{
		Key:   types.NewVarchar("abcdef"),
		Value: 3,
	})
	initialEntries = append(initialEntries, &skip_list_page.SkipListPair{
		Key:   types.NewVarchar("abcdefg"),
		Value: 4,
	})
	initialEntries = append(initialEntries, &skip_list_page.SkipListPair{
		Key:   types.NewVarchar("abcdefgh"),
		Value: 5,
	})
	bpage1.SetEntries(initialEntries)

	// remove entries
	bpage1.RemoveInner(0)
	bpage1.RemoveInner(2)

	// check entry datas
	testingpkg.SimpleAssert(t, bpage1.GetEntryCnt() == 3)
	testingpkg.SimpleAssert(t, bpage1.GetEntry(0, types.Varchar).Key.CompareEquals(types.NewVarchar("abcde")))
	testingpkg.SimpleAssert(t, bpage1.GetEntry(1, types.Varchar).Key.CompareEquals(types.NewVarchar("abcdef")))
	testingpkg.SimpleAssert(t, bpage1.GetEntry(2, types.Varchar).Key.CompareEquals(types.NewVarchar("abcdefgh")))

	bpm.UnpinPage(bpage1.GetPageId(), true)

	// ---------- test InsertInner --------
	// setup a page
	bpage2 := skip_list_page.NewSkipListBlockPage(bpm, 1, skip_list_page.SkipListPair{
		Key:   types.NewVarchar("abcd"),
		Value: 0,
	})

	initialEntries = make([]*skip_list_page.SkipListPair, 0)
	initialEntries = append(initialEntries, bpage2.GetEntry(0, types.Varchar))
	initialEntries = append(initialEntries, &skip_list_page.SkipListPair{
		Key:   types.NewVarchar("abcde"),
		Value: 1,
	})
	initialEntries = append(initialEntries, &skip_list_page.SkipListPair{
		Key:   types.NewVarchar("abcdef"),
		Value: 2,
	})
	bpage2.SetEntries(initialEntries)

	// insert entries
	bpage2.InsertInner(-1, &skip_list_page.SkipListPair{
		Key:   types.NewVarchar("abc"),
		Value: 0,
	})
	bpage2.InsertInner(2, &skip_list_page.SkipListPair{
		Key:   types.NewVarchar("abcdee"),
		Value: 22,
	})
	bpage2.InsertInner(4, &skip_list_page.SkipListPair{
		Key:   types.NewVarchar("abcdeff"),
		Value: 33,
	})

	// check entry datas
	entryCnt := bpage2.GetEntryCnt()
	testingpkg.SimpleAssert(t, entryCnt == 6)
	entry := bpage2.GetEntry(0, types.Varchar)
	testingpkg.SimpleAssert(t, entry.Key.CompareEquals(types.NewVarchar("abc")))
	entry = bpage2.GetEntry(1, types.Varchar)
	testingpkg.SimpleAssert(t, entry.Key.CompareEquals(types.NewVarchar("abcd")))
	entry = bpage2.GetEntry(2, types.Varchar)
	testingpkg.SimpleAssert(t, entry.Key.CompareEquals(types.NewVarchar("abcde")))
	entry = bpage2.GetEntry(3, types.Varchar)
	testingpkg.SimpleAssert(t, entry.Key.CompareEquals(types.NewVarchar("abcdee")))
	entry = bpage2.GetEntry(4, types.Varchar)
	testingpkg.SimpleAssert(t, entry.Key.CompareEquals(types.NewVarchar("abcdef")))
	entry = bpage2.GetEntry(5, types.Varchar)
	testingpkg.SimpleAssert(t, entry.Key.CompareEquals(types.NewVarchar("abcdeff")))

	bpm.UnpinPage(bpage2.GetPageId(), true)

	shi.Shutdown(false)
}

func TestBSearchOfSkipLisBlockPage(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.log")
	shi := samehada.NewSamehadaInstanceForTesting()
	bpm := shi.GetBufferPoolManager()

	bpage := skip_list_page.NewSkipListBlockPage(bpm, 1, skip_list_page.SkipListPair{
		Key:   types.NewInteger(math.MinInt32),
		Value: 0,
	})

	// ------- when element num is even number -----
	bpage.SetEntries(make([]*skip_list_page.SkipListPair, 0))
	bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{
		Key:   types.NewInteger(math.MinInt32),
		Value: 0,
	}))
	// set entries
	for ii := 1; ii < 50; ii++ {
		bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{types.NewInteger(int32(ii * 10)), uint32(ii * 10)}))
	}
	bpage.SetEntryCnt(int32(len(bpage.GetEntries(types.Integer))))

	for ii := 1; ii < 100; ii++ {
		key := types.NewInteger(int32(ii * 5))
		found, entry, idx := bpage.FindEntryByKey(&key)
		//fmt.Println(ii)
		if ii%2 == 0 {
			testingpkg.SimpleAssert(t, found == true && entry.Value == uint32(key.ToInteger()))
		} else {
			testingpkg.SimpleAssert(t, found == false && uint32(key.ToInteger())-bpage.ValueAt(idx, types.Integer) == 5)
		}
	}

	// ------- when element num is odd number -----
	bpage.SetEntries(make([]*skip_list_page.SkipListPair, 0))
	bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{
		Key:   types.NewInteger(math.MinInt32),
		Value: 0,
	}))
	// set entries
	for ii := 1; ii < 51; ii++ {
		bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{types.NewInteger(int32(ii * 10)), uint32(ii * 10)}))
	}
	bpage.SetEntryCnt(int32(len(bpage.GetEntries(types.Integer))))

	for ii := 1; ii < 102; ii++ {
		key := types.NewInteger(int32(ii * 5))
		found, entry, idx := bpage.FindEntryByKey(&key)
		//fmt.Println(ii)
		if ii%2 == 0 {
			testingpkg.SimpleAssert(t, found == true && entry.Value == uint32(key.ToInteger()))
		} else {
			testingpkg.SimpleAssert(t, found == false && uint32(key.ToInteger())-bpage.ValueAt(idx, types.Integer) == 5)
		}
	}

	shi.Shutdown(false)
}

func TestBSearchOfSkipLisBlockPage2(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.log")
	shi := samehada.NewSamehadaInstanceForTesting()
	bpm := shi.GetBufferPoolManager()

	bpage := skip_list_page.NewSkipListBlockPage(bpm, 1, skip_list_page.SkipListPair{
		Key:   types.NewInteger(math.MinInt32),
		Value: 0,
	})

	// ------- when element num is even number -----
	bpage.SetEntries(make([]*skip_list_page.SkipListPair, 0))
	bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{
		Key:   types.NewInteger(math.MinInt32),
		Value: 0,
	}))
	// set entries
	for ii := 1; ii < 50; ii++ {
		bpage.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(ii*10))), uint32(ii*10), bpm, nil, 1)
		//bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{types.NewInteger(int32(ii * 10)), uint32(ii * 10)}))
	}
	bpage.SetEntryCnt(int32(len(bpage.GetEntries(types.Integer))))

	for ii := 1; ii < 100; ii++ {
		key := types.NewInteger(int32(ii * 5))
		found, entry, idx := bpage.FindEntryByKey(&key)
		//fmt.Println(ii)
		if ii%2 == 0 {
			testingpkg.SimpleAssert(t, found == true && entry.Value == uint32(key.ToInteger()))
		} else {
			testingpkg.SimpleAssert(t, found == false && uint32(key.ToInteger())-bpage.ValueAt(idx, types.Integer) == 5)
		}
	}

	// ------- when element num is odd number -----
	bpage.SetEntries(make([]*skip_list_page.SkipListPair, 0))
	bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{
		Key:   types.NewInteger(math.MinInt32),
		Value: 0,
	}))
	// set entries
	for ii := 1; ii < 51; ii++ {
		bpage.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(ii*10))), uint32(ii*10), bpm, nil, 1)
		//bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{types.NewInteger(int32(ii * 10)), uint32(ii * 10)}))
	}
	bpage.SetEntryCnt(int32(len(bpage.GetEntries(types.Integer))))

	for ii := 1; ii < 102; ii++ {
		key := types.NewInteger(int32(ii * 5))
		found, entry, idx := bpage.FindEntryByKey(&key)
		//fmt.Println(ii)
		if ii%2 == 0 {
			testingpkg.SimpleAssert(t, found == true && entry.Value == uint32(key.ToInteger()))
		} else {
			testingpkg.SimpleAssert(t, found == false && uint32(key.ToInteger())-bpage.ValueAt(idx, types.Integer) == 5)
		}
	}

	shi.Shutdown(false)
}

func confirmSkipListContent(t *testing.T, sl *skip_list.SkipList, step int32) int32 {
	entryCnt := int32(0)
	lastKeyVal := int32(-1)
	dupCheckMap := make(map[int32]int32)
	itr := sl.Iterator(nil, nil)
	for done, _, key, _ := itr.Next(); !done; done, _, key, _ = itr.Next() {
		curVal := key.ToInteger()
		//fmt.Printf("lastKeyVal=%d curVal=%d entryCnt=%d\n", lastKeyVal, curVal, entryCnt)
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

	//fmt.Printf("entryCnt=%d\n", entryCnt)
	return entryCnt
}

func TestSkipListSimple(t *testing.T) {
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
		//fmt.Printf("insCnt: %d\n", insCnt)
		insCnt++
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
	}

	//confirmSkipListContent(t, sl, 11)

	// Get entries
	for i := 0; i < 250; i++ {
		//fmt.Printf("get entry i=%d key=%d\n", i, i*11)
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
		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		if res == math.MaxUint32 {
			panic("result should not be nil")
		} else {
			testingpkg.SimpleAssert(t, uint32(i*11) == res)
		}

		// check no existance after delete
		sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(i*11))), uint32(i*11))

		res = sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		testingpkg.SimpleAssert(t, math.MaxUint32 == res)
		//fmt.Println("contents listing after delete")
		//confirmSkipListContent(t, sl, -1)
	}

	shi.Shutdown(false)
}

func TestSkipListInsertAndDeleteAll(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.log")

	shi := samehada.NewSamehadaInstance("test", 100)
	sl := skip_list.NewSkipList(shi.GetBufferPoolManager(), types.Integer)

	// override global rand seed (seed has been set on NewSkipList)
	rand.Seed(3)

	insVals := make([]int32, 0)
	for i := 0; i < 2000; i++ {
		insVals = append(insVals, int32(i*11))
	}
	// shuffle value list for inserting
	rand.Shuffle(len(insVals), func(i, j int) { insVals[i], insVals[j] = insVals[j], insVals[i] })

	//////////// remove from tail ///////

	// Insert entries
	insCnt := 0
	for _, insVal := range insVals {
		//fmt.Printf("insCnt: %d\n", insCnt)
		insCnt++
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
	}

	//confirmSkipListContent(t, sl, 11)

	// Get entries
	for i := 0; i < 2000; i++ {
		//fmt.Printf("get entry i=%d key=%d\n", i, i*11)
		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		if res == math.MaxUint32 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.SimpleAssert(t, uint32(i*11) == res)
		}
	}

	// delete all values
	for i := (2000 - 1); i >= 0; i-- {
		// delete
		isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(i*11))), uint32(i*11))
		common.ShPrintf(common.DEBUG, "i=%d i*11=%d\n", i, i*11)
		testingpkg.SimpleAssert(t, isDeleted == true)

		// check no existance after delete
		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		common.ShPrintf(common.DEBUG, "i=%d i*11=%d res=%d\n", i, i*11, res)
		testingpkg.SimpleAssert(t, math.MaxUint32 == res)
	}

	//////////// remove from head ///////

	// Re-Insert entries
	insCnt = 0
	for _, insVal := range insVals {
		//fmt.Printf("insCnt: %d\n", insCnt)
		insCnt++
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
	}

	// Get entries
	for i := 0; i < 2000; i++ {
		//fmt.Printf("get entry i=%d key=%d\n", i, i*11)
		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		if res == math.MaxUint32 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.SimpleAssert(t, uint32(i*11) == res)
		}
	}

	// delete all values
	for i := 0; i < 2000; i++ {
		// delete
		isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(i*11))), uint32(i*11))
		common.ShPrintf(common.DEBUG, "i=%d i*11=%d\n", i, i*11)
		testingpkg.SimpleAssert(t, isDeleted == true)

		// check no existance after delete
		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		common.ShPrintf(common.DEBUG, "i=%d i*11=%d res=%d\n", i, i*11, res)
		testingpkg.SimpleAssert(t, math.MaxUint32 == res)
	}

	shi.Shutdown(false)
}

func TestSkipListItr(t *testing.T) {
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
		//fmt.Printf("lastKeyVal=%d curVal=%d nodeCnt=%d\n", lastKeyVal, key.ToInteger(), nodeCnt)
		testingpkg.SimpleAssert(t, lastKeyVal <= key.ToInteger())
		lastKeyVal = key.ToInteger()
		nodeCnt++
	}

	//fmt.Printf("nodeCnt=%d\n", nodeCnt)
	testingpkg.SimpleAssert(t, nodeCnt == 250)
}

func FuzzSkipLisMix(f *testing.F) {
	f.Add(int32(100), int32(150), int32(10), int32(300))
	f.Fuzz(func(t *testing.T, bulkSize int32, opTimes int32, skipRand int32, initialEntryNum int32) {
		if bulkSize < 0 || opTimes < 0 || skipRand < 0 || initialEntryNum < 0 {
			return
		}

		os.Remove("test.db")
		os.Remove("test.log")

		shi := samehada.NewSamehadaInstanceForTesting()
		//shi := samehada.NewSamehadaInstance("test", 10*1024) // buffer is about 40MB
		bpm := shi.GetBufferPoolManager()

		testSkipListMix(t, bpm, bulkSize, opTimes, skipRand, initialEntryNum)

		shi.CloseFilesForTesting()
	})
}

//func TestFuzzerUnexpectedExitParam(t *testing.T) {
//	os.Remove("test.db")
//	os.Remove("test.log")
//
//	shi := samehada.NewSamehadaInstanceForTesting()
//	//shi := samehada.NewSamehadaInstance("test", 10*1024) // buffer is about 40MB
//	bpm := shi.GetBufferPoolManager()
//
//	fmt.Printf("param of TestFuzzerUnexpectedExitParam: %d %d %d %d\n", int32(rune('Į')), int32(rune('Ď')), int32(rune('T')), int32(rune('Ć')))
//	testSkipListMix(t, bpm, rune('Į'), rune('Ď'), rune('T'), rune('Ć'))
//
//	shi.CloseFilesForTesting()
//}

const MAX_ENTRIES = 100000

//var entriesOnListNum int32 = 0
//var removedEntriesNum int32 = 0

//var insVals []int32
//var removedVals []int32
//var insValsS []string
//var removedValsS []string
//
//// for debug
//func isAlreadyRemoved(checkVal int32) bool {
//	for _, val := range removedVals {
//		if val == checkVal {
//			return true
//		}
//	}
//	return false
//}
//
//// for debug
//func isAlreadyRemovedS(checkVal string) bool {
//	for _, val := range removedValsS {
//		if val == checkVal {
//			return true
//		}
//	}
//	return false
//}
//
////func isExistKeyOnList(sl *skip_list.SkipList, checkKey int32, isPrint bool) bool {
////	itr := sl.Iterator(nil, nil)
////	fmt.Printf("isExistKeyOnList:")
////	for done, _, key, _ := itr.Next(); !done; done, _, key, _ = itr.Next() {
////		if isPrint {
////			fmt.Printf(" %d", key.ToInteger())
////		}
////		if key.ToInteger() == checkKey {
////			if isPrint {
////				fmt.Println("")
////			}
////			return true
////		}
////	}
////
////	if isPrint {
////		fmt.Println("")
////	}
////	return false
////}
//
//func countSkipListContent(sl *skip_list.SkipList) int32 {
//	entryCnt := int32(0)
//	itr := sl.Iterator(nil, nil)
//	for done, _, _, _ := itr.Next(); !done; done, _, _, _ = itr.Next() {
//		entryCnt++
//	}
//
//	//fmt.Printf("entryCnt=%d\n", entryCnt)
//	return entryCnt
//}
//
//func insertRandom(sl *skip_list.SkipList, num int32, checkDupMap map[int32]int32) {
//	if int32(len(insVals))+num < MAX_ENTRIES {
//		for ii := 0; ii < int(num); ii++ {
//			insVal := rand.Int31()
//			for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
//				insVal = rand.Int31()
//			}
//			checkDupMap[insVal] = insVal
//
//			sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//			//fmt.Printf("sl.Insert at insertRandom: ii=%d, insVal=%d len(*insVals)=%d\n", ii, insVal, len(insVals))
//			insVals = append(insVals, insVal)
//		}
//	}
//}
//
//func insertRandomS(sl *skip_list.SkipList, num int32, checkDupMap map[string]string) {
//	if int32(len(insValsS))+num < MAX_ENTRIES {
//		for ii := 0; ii < int(num); ii++ {
//			insVal := *samehada_util.GetRandomStr(20) //rand.Int31()
//			for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
//				insVal = *samehada_util.GetRandomStr(20)
//			}
//			checkDupMap[insVal] = insVal
//
//			sl.Insert(samehada_util.GetPonterOfValue(types.NewVarchar(insVal)), uint32(len(insVal)))
//			//fmt.Printf("sl.Insert at insertRandom: ii=%d, insVal=%d len(*insVals)=%d\n", ii, insVal, len(insVals))
//			insValsS = append(insValsS, insVal)
//		}
//	}
//}
//
//func removeRandom(t *testing.T, sl *skip_list.SkipList, opStep int32, num int32) {
//	if int32(len(insVals))-num > 0 {
//		for ii := 0; ii < int(num); ii++ {
//			tmpIdx := int(rand.Intn(len(insVals)))
//			insVal := insVals[tmpIdx]
//			//sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//			//if insVal == 1933250583 {
//			//	fmt.Println("")
//			//}
//			isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//			//fmt.Printf("sl.Remove at removeRandom: ii=%d, insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, insVal, len(insVals), len(removedVals))
//			if isAlreadyRemoved(insVal) {
//				fmt.Printf("delete duplicated value should not be occur! opStep=%d, ii=%d tmpIdx=%d insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", opStep, ii, tmpIdx, insVal, len(insVals), len(removedVals))
//				panic("delete duplicated value should not be occur!")
//			}
//			//if isDeleted != true && !isAlreadyRemoved(insVal) {
//			if isDeleted != true {
//				fmt.Printf("isDeleted should be true! opStep=%d, ii=%d tmpIdx=%d insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", opStep, ii, tmpIdx, insVal, len(insVals), len(removedVals))
//				panic("isDeleted should be true!")
//				//common.RuntimeStack()
//			}
//			testingpkg.SimpleAssert(t, isDeleted == true || isAlreadyRemoved(insVal))
//			if len(insVals) == 1 {
//				// make empty
//				insVals = make([]int32, 0)
//			} else if len(insVals) == tmpIdx+1 {
//				insVals = insVals[:len(insVals)-1]
//			} else {
//				insVals = append(insVals[:tmpIdx], insVals[tmpIdx+1:]...)
//			}
//			removedVals = append(removedVals, insVal)
//		}
//	}
//}
//
//func removeRandomS(t *testing.T, sl *skip_list.SkipList, opStep int32, num int32) {
//	if int32(len(insValsS))-num > 0 {
//		for ii := 0; ii < int(num); ii++ {
//			tmpIdx := int(rand.Intn(len(insValsS)))
//			insVal := insValsS[tmpIdx]
//			//sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//			//if insVal == 1933250583 {
//			//	fmt.Println("")
//			//}
//			isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewVarchar(insVal)), uint32(len(insVal)))
//			//fmt.Printf("sl.Remove at removeRandom: ii=%d, insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, insVal, len(insVals), len(removedVals))
//			if isAlreadyRemovedS(insVal) {
//				fmt.Printf("delete duplicated value should not be occur! opStep=%d, ii=%d tmpIdx=%d insVal=%s len(*insValsS)=%d len(*removedValsS)=%d\n", opStep, ii, tmpIdx, insVal, len(insValsS), len(removedValsS))
//				panic("delete duplicated value should not be occur!")
//			}
//			//if isDeleted != true && !isAlreadyRemoved(insVal) {
//			if isDeleted != true {
//				fmt.Printf("isDeleted should be true! opStep=%d, ii=%d tmpIdx=%d insVal=%s len(*insValsS)=%d len(*removedValsS)=%d\n", opStep, ii, tmpIdx, insVal, len(insValsS), len(removedValsS))
//				panic("isDeleted should be true!")
//				//common.RuntimeStack()
//			}
//			testingpkg.SimpleAssert(t, isDeleted == true || isAlreadyRemovedS(insVal))
//			if len(insValsS) == 1 {
//				// make empty
//				insValsS = make([]string, 0)
//			} else if len(insValsS) == tmpIdx+1 {
//				insValsS = insValsS[:len(insValsS)-1]
//			} else {
//				insValsS = append(insValsS[:tmpIdx], insValsS[tmpIdx+1:]...)
//			}
//			removedValsS = append(removedValsS, insVal)
//		}
//	}
//}
//
//func testSkipListMix(t *testing.T, bpm *buffer.BufferPoolManager, bulkSize int32, opTimes int32, skipRand int32, initialEntryNum int32) {
//	common.ShPrintf(common.DEBUG, "start of testSkipListMix bulkSize=%d opTimes=%d skipRand=%d initialEntryNum=%d ====================================================\n",
//		bulkSize, opTimes, skipRand, initialEntryNum)
//
//	//os.Remove("test.db")
//	//os.Remove("test.log")
//	//
//	//shi := samehada.NewSamehadaInstanceForTesting()
//	//bpm := shi.GetBufferPoolManager()
//
//	checkDupMap := make(map[int32]int32)
//
//	sl := skip_list.NewSkipList(bpm, types.Integer)
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
//	insVals = make([]int32, 0)
//	removedVals = make([]int32, 0)
//	entriesOnListNum = 0
//
//	// initial entries
//	useInitialEntryNum := int(initialEntryNum)
//	for ii := 0; ii < useInitialEntryNum; ii++ {
//		if entriesOnListNum+1 < MAX_ENTRIES {
//			// avoid duplication
//			insVal := rand.Int31()
//			for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
//				insVal = rand.Int31()
//			}
//			checkDupMap[insVal] = insVal
//
//			//fmt.Printf("sl.Insert at testSkipListMix for initial entry: ii=%d, insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, insVal, len(insVals), len(removedVals))
//			sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//			insVals = append(insVals, insVal)
//			entriesOnListNum++
//		}
//	}
//
//	// entries num on SkipList should be same with this variable
//	//entriesOnListNum = int32(useInitialEntryNum)
//	removedEntriesNum = int32(0)
//
//	// check num of stored entries on sl is same with num of initial entries (if differ, there are bug)
//	if entriesOnListNum != countSkipListContent(sl) {
//		fmt.Println("initial entries num are strange!")
//		panic("initial entries count are strange!")
//		//common.RuntimeStack()
//	}
//
//	useOpTimes := int(opTimes)
//	for ii := 0; ii < useOpTimes; ii++ {
//		// get 0-2
//		opType := rand.Intn(3)
//		switch opType {
//		case 0: // Insert
//			if int32(len(insVals))+bulkSize < MAX_ENTRIES {
//				//insVal := rand.Int31()
//				insertRandom(sl, bulkSize, checkDupMap)
//				//sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//				//insVals = append(insVals, insVal)
//				entriesOnListNum += bulkSize
//				if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
//					fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
//					//common.RuntimeStack()
//					panic("entries num on list is strange!")
//				}
//			}
//		case 1: // Delete
//			// get 0-5 value
//			tmpRand := rand.Intn(5)
//			if tmpRand == 0 {
//				// 20% is Remove to not existing entry
//				if len(removedVals) != 0 {
//					tmpIdx := int(rand.Intn(len(removedVals)))
//					tmpVal := removedVals[tmpIdx]
//					isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(tmpVal))), uint32(tmpVal))
//					testingpkg.SimpleAssert(t, isDeleted == false)
//					if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
//						fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
//						common.RuntimeStack()
//					}
//				}
//			} else {
//				// 80% is Remove to existing entry
//				if entriesOnListNum-bulkSize > 0 {
//					removeRandom(t, sl, int32(ii), bulkSize)
//					entriesOnListNum -= bulkSize
//					removedEntriesNum += bulkSize
//					if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
//						fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
//						panic("entries num on list is strange!")
//						//common.RuntimeStack()
//					}
//				}
//			}
//		case 2: // Get
//			if len(insVals) > 0 {
//				tmpIdx := int(rand.Intn(len(insVals)))
//				//fmt.Printf("sl.GetValue at testSkipListMix: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(removedVals))
//				gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVals[tmpIdx]))))
//				if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
//					fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
//					panic("entries num on list is strange!")
//					//common.RuntimeStack()
//				}
//				if gotVal == math.MaxUint32 {
//					fmt.Printf("%d is not found!\n", insVals[tmpIdx])
//					panic("sl.GetValue could not target key!")
//				}
//				if gotVal != uint32(insVals[tmpIdx]) {
//					fmt.Printf("gotVal is not match! %d != %d\n", gotVal, insVals[tmpIdx])
//					panic("gotVal is not match!")
//					//common.RuntimeStack()
//				}
//				testingpkg.SimpleAssert(t, gotVal == uint32(insVals[tmpIdx]))
//			}
//		}
//	}
//
//	//shi.Shutdown(false)
//}
//
//func testSkipListMixS(t *testing.T, bpm *buffer.BufferPoolManager, bulkSize int32, opTimes int32, skipRand int32, initialEntryNum int32) {
//	common.ShPrintf(common.DEBUG, "start of testSkipListMixS bulkSize=%d opTimes=%d skipRand=%d initialEntryNum=%d ====================================================\n",
//		bulkSize, opTimes, skipRand, initialEntryNum)
//
//	//os.Remove("test.db")
//	//os.Remove("test.log")
//	//
//	//shi := samehada.NewSamehadaInstanceForTesting()
//	//bpm := shi.GetBufferPoolManager()
//
//	checkDupMapS := make(map[string]string)
//
//	sl := skip_list.NewSkipList(bpm, types.Varchar)
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
//	insValsS = make([]string, 0)
//	removedValsS = make([]string, 0)
//	entriesOnListNum = 0
//
//	// initial entries
//	useInitialEntryNum := int(initialEntryNum)
//	for ii := 0; ii < useInitialEntryNum; ii++ {
//		if entriesOnListNum+1 < MAX_ENTRIES {
//			// avoid duplication
//			insVal := *samehada_util.GetRandomStr(20)
//			for _, exist := checkDupMapS[insVal]; exist; _, exist = checkDupMapS[insVal] {
//				insVal = *samehada_util.GetRandomStr(20)
//			}
//			checkDupMapS[insVal] = insVal
//
//			//fmt.Printf("sl.Insert at testSkipListMix for initial entry: ii=%d, insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, insVal, len(insVals), len(removedVals))
//			sl.Insert(samehada_util.GetPonterOfValue(types.NewVarchar(insVal)), uint32(len(insVal)))
//			insValsS = append(insValsS, insVal)
//			entriesOnListNum++
//		}
//	}
//
//	// entries num on SkipList should be same with this variable
//	//entriesOnListNum = int32(useInitialEntryNum)
//	removedEntriesNum = int32(0)
//
//	// check num of stored entries on sl is same with num of initial entries (if differ, there are bug)
//	if entriesOnListNum != countSkipListContent(sl) {
//		fmt.Println("initial entries num are strange!")
//		panic("initial entries count are strange!")
//		//common.RuntimeStack()
//	}
//
//	useOpTimes := int(opTimes)
//	for ii := 0; ii < useOpTimes; ii++ {
//		// get 0-2
//		opType := rand.Intn(3)
//		switch opType {
//		case 0: // Insert
//			if int32(len(insValsS))+bulkSize < MAX_ENTRIES {
//				//insVal := rand.Int31()
//				insertRandomS(sl, bulkSize, checkDupMapS)
//				//sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
//				//insVals = append(insVals, insVal)
//				entriesOnListNum += bulkSize
//				if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insValsS)) || removedEntriesNum != int32(len(removedValsS)) {
//					fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insValsS), removedEntriesNum, len(removedValsS))
//					//common.RuntimeStack()
//					panic("entries num on list is strange!")
//				}
//			}
//		case 1: // Delete
//			// get 0-5 value
//			tmpRand := rand.Intn(5)
//			if tmpRand == 0 {
//				// 20% is Remove to not existing entry
//				if len(removedValsS) != 0 {
//					tmpIdx := int(rand.Intn(len(removedValsS)))
//					tmpVal := removedValsS[tmpIdx]
//					isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewVarchar(tmpVal)), uint32(len(tmpVal)))
//					testingpkg.SimpleAssert(t, isDeleted == false)
//					if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insValsS)) || removedEntriesNum != int32(len(removedValsS)) {
//						fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insValsS), removedEntriesNum, len(removedValsS))
//						common.RuntimeStack()
//					}
//				}
//			} else {
//				// 80% is Remove to existing entry
//				if entriesOnListNum-bulkSize > 0 {
//					removeRandomS(t, sl, int32(ii), bulkSize)
//					entriesOnListNum -= bulkSize
//					removedEntriesNum += bulkSize
//					if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insValsS)) || removedEntriesNum != int32(len(removedValsS)) {
//						fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insValsS), removedEntriesNum, len(removedValsS))
//						panic("entries num on list is strange!")
//						//common.RuntimeStack()
//					}
//				}
//			}
//		case 2: // Get
//			if len(insValsS) > 0 {
//				tmpIdx := int(rand.Intn(len(insValsS)))
//				//fmt.Printf("sl.GetValue at testSkipListMix: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(removedVals))
//				gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewVarchar(insValsS[tmpIdx])))
//				if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insValsS)) || removedEntriesNum != int32(len(removedValsS)) {
//					fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insValsS), removedEntriesNum, len(removedValsS))
//					panic("entries num on list is strange!")
//					//common.RuntimeStack()
//				}
//				if gotVal == math.MaxUint32 {
//					fmt.Printf("%s is not found!\n", insValsS[tmpIdx])
//					panic("sl.GetValue could not target key!")
//				}
//				if gotVal != uint32(len(insValsS[tmpIdx])) {
//					fmt.Printf("gotVal is not match! %d != %d\n", gotVal, len(insValsS[tmpIdx]))
//					panic("gotVal is not match!")
//					//common.RuntimeStack()
//				}
//				testingpkg.SimpleAssert(t, gotVal == uint32(len(insValsS[tmpIdx])))
//			}
//		}
//	}
//
//	//shi.Shutdown(false)
//}
//
////var bpm *buffer.BufferPoolManager
//
//func TestSkipListMix(t *testing.T) {
//	os.Remove("test.db")
//	os.Remove("test.log")
//
//	shi := samehada.NewSamehadaInstanceForTesting()
//	//shi := samehada.NewSamehadaInstance("test", 10*1024) // buffer is about 40MB
//	bpm := shi.GetBufferPoolManager()
//
//	testSkipListMix(t, bpm, 1, int32(150), int32(10), int32(0))
//	testSkipListMix(t, bpm, 1, int32(150), int32(10), int32(300))
//	testSkipListMix(t, bpm, 1, int32(150), int32(10), int32(600))
//	testSkipListMix(t, bpm, 1, int32(200), int32(5), int32(10))
//	testSkipListMix(t, bpm, 1, int32(250), int32(5), int32(10))
//	testSkipListMix(t, bpm, 1, int32(250), int32(4), int32(0))
//	testSkipListMix(t, bpm, 1, int32(250), int32(3), int32(0))
//
//	testSkipListMix(t, bpm, 50, int32(150), int32(10), int32(0))
//	testSkipListMix(t, bpm, 50, int32(150), int32(10), int32(300))
//	testSkipListMix(t, bpm, 50, int32(150), int32(10), int32(600))
//	testSkipListMix(t, bpm, 50, int32(200), int32(5), int32(10))
//	testSkipListMix(t, bpm, 50, int32(250), int32(5), int32(10))
//	testSkipListMix(t, bpm, 50, int32(250), int32(4), int32(0))
//	testSkipListMix(t, bpm, 50, int32(250), int32(3), int32(0))
//
//	testSkipListMix(t, bpm, 100, int32(150), int32(10), int32(0))
//	testSkipListMix(t, bpm, 100, int32(150), int32(10), int32(300))
//	testSkipListMix(t, bpm, 100, int32(150), int32(10), int32(600))
//	testSkipListMix(t, bpm, 100, int32(200), int32(5), int32(10))
//	testSkipListMix(t, bpm, 100, int32(250), int32(5), int32(10))
//	testSkipListMix(t, bpm, 100, int32(250), int32(4), int32(0))
//	testSkipListMix(t, bpm, 100, int32(250), int32(3), int32(0))
//
//	shi.Shutdown(true)
//}
//
//func TestSkipListMixS(t *testing.T) {
//	os.Remove("test.db")
//	os.Remove("test.log")
//
//	shi := samehada.NewSamehadaInstanceForTesting()
//	//shi := samehada.NewSamehadaInstance("test", 10*1024) // buffer is about 40MB
//	bpm := shi.GetBufferPoolManager()
//
//	testSkipListMixS(t, bpm, 1, int32(150), int32(10), int32(0))
//	testSkipListMixS(t, bpm, 1, int32(150), int32(10), int32(300))
//	testSkipListMixS(t, bpm, 1, int32(150), int32(10), int32(600))
//	testSkipListMixS(t, bpm, 1, int32(200), int32(5), int32(10))
//	testSkipListMixS(t, bpm, 1, int32(250), int32(5), int32(10))
//	testSkipListMixS(t, bpm, 1, int32(250), int32(4), int32(0))
//	testSkipListMixS(t, bpm, 1, int32(250), int32(3), int32(0))
//
//	testSkipListMixS(t, bpm, 50, int32(150), int32(10), int32(0))
//	testSkipListMixS(t, bpm, 50, int32(150), int32(10), int32(300))
//	testSkipListMixS(t, bpm, 50, int32(150), int32(10), int32(600))
//	testSkipListMixS(t, bpm, 50, int32(200), int32(5), int32(10))
//	testSkipListMixS(t, bpm, 50, int32(250), int32(5), int32(10))
//	testSkipListMixS(t, bpm, 50, int32(250), int32(4), int32(0))
//	testSkipListMixS(t, bpm, 50, int32(250), int32(3), int32(0))
//
//	testSkipListMixS(t, bpm, 100, int32(150), int32(10), int32(0))
//	testSkipListMixS(t, bpm, 100, int32(150), int32(10), int32(300))
//	testSkipListMixS(t, bpm, 100, int32(150), int32(10), int32(600))
//	testSkipListMixS(t, bpm, 100, int32(200), int32(5), int32(10))
//	testSkipListMixS(t, bpm, 100, int32(250), int32(5), int32(10))
//	testSkipListMixS(t, bpm, 100, int32(250), int32(4), int32(0))
//	testSkipListMixS(t, bpm, 100, int32(250), int32(3), int32(0))
//
//	shi.Shutdown(true)
//}

//var insVals []int32
//var removedVals []int32
//var insValsS []string
//var removedValsS []string

//// for debug
//func isAlreadyRemoved(checkVal int32) bool {
//	for _, val := range removedVals {
//		if val == checkVal {
//			return true
//		}
//	}
//	return false
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

func insertRandom(sl *skip_list.SkipList, num int32, checkDupMap map[int32]int32) {
	if int32(len(insVals))+num < MAX_ENTRIES {
		for ii := 0; ii < int(num); ii++ {
			insVal := rand.Int31()
			for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
				insVal = rand.Int31()
			}
			checkDupMap[insVal] = insVal

			sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
			//fmt.Printf("sl.Insert at insertRandom: ii=%d, insVal=%d len(*insVals)=%d\n", ii, insVal, len(insVals))
			insVals = append(insVals, insVal)
		}
	}
}

func removeRandom(t *testing.T, sl *skip_list.SkipList, opStep int32, num int32) {
	if int32(len(insVals))-num > 0 {
		for ii := 0; ii < int(num); ii++ {
			tmpIdx := int(rand.Intn(len(insVals)))
			insVal := insVals[tmpIdx]
			//sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
			//if insVal == 1933250583 {
			//	fmt.Println("")
			//}
			isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
			//fmt.Printf("sl.Remove at removeRandom: ii=%d, insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, insVal, len(insVals), len(removedVals))
			if isAlreadyRemoved(insVal) {
				fmt.Printf("delete duplicated value should not be occur! opStep=%d, ii=%d tmpIdx=%d insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", opStep, ii, tmpIdx, insVal, len(insVals), len(removedVals))
				panic("delete duplicated value should not be occur!")
			}
			//if isDeleted != true && !isAlreadyRemoved(insVal) {
			if isDeleted != true {
				fmt.Printf("isDeleted should be true! opStep=%d, ii=%d tmpIdx=%d insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", opStep, ii, tmpIdx, insVal, len(insVals), len(removedVals))
				panic("isDeleted should be true!")
				//common.RuntimeStack()
			}
			testingpkg.SimpleAssert(t, isDeleted == true || isAlreadyRemoved(insVal))
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

func getRandomPrimitiveVal[T comparable](keyType types.TypeID) interface{} {
	switch keyType {
	case types.Integer:
		return rand.Int31()
	case types.Varchar:
		return samehada_util.GetRandomStr(20)
	}
}

func testSkipListMix[T comparable](t *testing.T, bpm *buffer.BufferPoolManager, keyType types.TypeID, bulkSize int32, opTimes int32, skipRand int32, initialEntryNum int32) {
	common.ShPrintf(common.DEBUG, "start of testSkipListMix bulkSize=%d opTimes=%d skipRand=%d initialEntryNum=%d ====================================================\n",
		bulkSize, opTimes, skipRand, initialEntryNum)

	//os.Remove("test.db")
	//os.Remove("test.log")
	//
	//shi := samehada.NewSamehadaInstanceForTesting()
	//bpm := shi.GetBufferPoolManager()

	checkDupMap := make(map[T]T)

	sl := skip_list.NewSkipList(bpm, types.Integer)

	// override global rand seed (seed has been set on NewSkipList)
	rand.Seed(3)

	tmpSkipRand := skipRand
	// skip random value series
	for tmpSkipRand > 0 {
		rand.Int31()
		tmpSkipRand--
	}

	insVals := make([]T, 0)
	removedVals := make([]T, 0)
	entriesOnListNum := 0

	// initial entries
	useInitialEntryNum := int(initialEntryNum)
	for ii := 0; ii < useInitialEntryNum; ii++ {
		if entriesOnListNum+1 < MAX_ENTRIES {
			// avoid duplication
			//insVal := rand.Int31()
			insVal := getRandomPrimitiveVal[T](keyType).(T)
			for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
				//insVal = rand.Int31()
				insVal = getRandomPrimitiveVal[T](keyType).(T)
			}
			checkDupMap[insVal] = insVal

			//fmt.Printf("sl.Insert at testSkipListMix for initial entry: ii=%d, insVal=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, insVal, len(insVals), len(removedVals))
			sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
			insVals = append(insVals, insVal)
			entriesOnListNum++
		}
	}

	// entries num on SkipList should be same with this variable
	//entriesOnListNum = int32(useInitialEntryNum)
	removedEntriesNum := int32(0)

	// check num of stored entries on sl is same with num of initial entries (if differ, there are bug)
	if entriesOnListNum != countSkipListContent(sl) {
		fmt.Println("initial entries num are strange!")
		panic("initial entries count are strange!")
		//common.RuntimeStack()
	}

	useOpTimes := int(opTimes)
	for ii := 0; ii < useOpTimes; ii++ {
		// get 0-2
		opType := rand.Intn(3)
		switch opType {
		case 0: // Insert
			if int32(len(insVals))+bulkSize < MAX_ENTRIES {
				//insVal := rand.Int31()
				insertRandom(sl, bulkSize, checkDupMap)
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
			tmpRand := rand.Intn(5)
			if tmpRand == 0 {
				// 20% is Remove to not existing entry
				if len(removedVals) != 0 {
					tmpIdx := int(rand.Intn(len(removedVals)))
					tmpVal := removedVals[tmpIdx]
					isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(tmpVal))), uint32(tmpVal))
					testingpkg.SimpleAssert(t, isDeleted == false)
					if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
						fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
						common.RuntimeStack()
					}
				}
			} else {
				// 80% is Remove to existing entry
				if entriesOnListNum-bulkSize > 0 {
					removeRandom(t, sl, int32(ii), bulkSize)
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
				//fmt.Printf("sl.GetValue at testSkipListMix: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(removedVals))
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

	//shi.Shutdown(false)
}

func testSkipListMixRoot[T comparable](t *testing.T, keyType types.TypeID) {
	os.Remove("test.db")
	os.Remove("test.log")

	shi := samehada.NewSamehadaInstanceForTesting()
	//shi := samehada.NewSamehadaInstance("test", 10*1024) // buffer is about 40MB
	bpm := shi.GetBufferPoolManager()

	testSkipListMix[T](t, bpm, keyType, 1, int32(150), int32(10), int32(0))
	testSkipListMix[T](t, bpm, keyType, 1, int32(150), int32(10), int32(300))
	testSkipListMix[T](t, bpm, keyType, 1, int32(150), int32(10), int32(600))
	testSkipListMix[T](t, bpm, keyType, 1, int32(200), int32(5), int32(10))
	testSkipListMix[T](t, bpm, keyType, 1, int32(250), int32(5), int32(10))
	testSkipListMix[T](t, bpm, keyType, 1, int32(250), int32(4), int32(0))
	testSkipListMix[T](t, bpm, keyType, 1, int32(250), int32(3), int32(0))

	testSkipListMix[T](t, bpm, keyType, 50, int32(150), int32(10), int32(0))
	testSkipListMix[T](t, bpm, keyType, 50, int32(150), int32(10), int32(300))
	testSkipListMix[T](t, bpm, keyType, 50, int32(150), int32(10), int32(600))
	testSkipListMix[T](t, bpm, keyType, 50, int32(200), int32(5), int32(10))
	testSkipListMix[T](t, bpm, keyType, 50, int32(250), int32(5), int32(10))
	testSkipListMix[T](t, bpm, keyType, 50, int32(250), int32(4), int32(0))
	testSkipListMix[T](t, bpm, keyType, 50, int32(250), int32(3), int32(0))

	testSkipListMix[T](t, bpm, keyType, 100, int32(150), int32(10), int32(0))
	testSkipListMix[T](t, bpm, keyType, 100, int32(150), int32(10), int32(300))
	testSkipListMix[T](t, bpm, keyType, 100, int32(150), int32(10), int32(600))
	testSkipListMix[T](t, bpm, keyType, 100, int32(200), int32(5), int32(10))
	testSkipListMix[T](t, bpm, keyType, 100, int32(250), int32(5), int32(10))
	testSkipListMix[T](t, bpm, keyType, 100, int32(250), int32(4), int32(0))
	testSkipListMix[T](t, bpm, keyType, 100, int32(250), int32(3), int32(0))

	shi.Shutdown(true)
}

func TestSkipListMixInteger(t *testing.T) {
	testSkipListMixRoot[int32](t, types.Integer)
}
