package skip_list_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/container/skip_list"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"math/rand"
	"os"
	"sync"
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
	bpage.SetLSN(9)
	bpage.SetEntryCnt(1)
	bpage.SetLevel(4)
	bpage.SetForwardEntry(5, types.PageID(11))
	bpage.SetFreeSpacePointer(common.PageSize - 9)
	// EntryCnt is incremented to 2
	// freeSpacePointer is decremented size of entry (1+2+7+4 => 14)
	bpage.SetEntry(1, &skip_list_page.SkipListPair{types.NewVarchar("abcdeff"), 12345})

	testingpkg.SimpleAssert(t, bpage.GetPageId() == 7)
	testingpkg.SimpleAssert(t, bpage.GetLSN() == 9)
	testingpkg.SimpleAssert(t, bpage.GetEntryCnt() == 2)
	testingpkg.SimpleAssert(t, bpage.GetLevel() == 4)
	testingpkg.SimpleAssert(t, bpage.GetForwardEntry(5) == types.PageID(11))

	testingpkg.SimpleAssert(t, bpage.GetFreeSpacePointer() == (common.PageSize-9-14))
	entry := bpage.GetEntry(1, types.Varchar)
	testingpkg.SimpleAssert(t, entry.Key.CompareEquals(types.NewVarchar("abcdeff")))
	testingpkg.SimpleAssert(t, entry.Value == 12345)

	shi.Shutdown(false)
}

func TestSerializationOfSkipLisHeaderPage(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.log")
	shi := samehada.NewSamehadaInstanceForTesting()
	bpm := shi.GetBufferPoolManager()

	hpageId := skip_list_page.NewSkipListHeaderPage(bpm, types.Integer)
	hpage := skip_list_page.FetchAndCastToHeaderPage(bpm, hpageId)

	hpage.SetPageId(7)
	hpage.SetLSN(7)
	hpage.SetListStartPageId(7)
	hpage.SetKeyType(types.Varchar)

	testingpkg.SimpleAssert(t, hpage.GetPageId() == 7)
	testingpkg.SimpleAssert(t, hpage.GetLSN() == 7)
	testingpkg.SimpleAssert(t, hpage.GetListStartPageId() == 7)
	testingpkg.SimpleAssert(t, hpage.GetKeyType() == types.Varchar)

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
		bpage.WLatch()
		bpage.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(ii*10))), uint32(ii*10), bpm, nil, 1)
		//bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{types.NewInteger(int32(ii * 10)), uint32(ii * 10)}))
	}
	bpage.WLatch()
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
	bpage.WUnlatch()

	// ------- when element num is odd number -----
	bpage.WLatch()
	bpage.SetEntries(make([]*skip_list_page.SkipListPair, 0))
	bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{
		Key:   types.NewInteger(math.MinInt32),
		Value: 0,
	}))
	bpage.WUnlatch()
	// set entries
	for ii := 1; ii < 51; ii++ {
		bpage.WLatch()
		bpage.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(ii*10))), uint32(ii*10), bpm, nil, 1)
		//bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{types.NewInteger(int32(ii * 10)), uint32(ii * 10)}))
	}
	bpage.WLatch()
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
	bpage.WUnlatch()

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

	//shi := samehada.NewSamehadaInstance("test", 100)
	shi := samehada.NewSamehadaInstance("test", 5)
	sl := skip_list.NewSkipList(shi.GetBufferPoolManager(), types.Integer)

	// override global rand seed (seed has been set on NewSkipList)
	rand.Seed(3)

	insVals := make([]int32, 0)
	for i := 0; i < 5000; i++ {
		insVals = append(insVals, int32(i*11))
	}
	// shuffle value list for inserting
	rand.Shuffle(len(insVals), func(i, j int) { insVals[i], insVals[j] = insVals[j], insVals[i] })

	//////////// remove from tail ///////

	// Insert entries
	insCnt := 0
	for _, insVal := range insVals {
		common.ShPrintf(common.DEBUG, "insCnt: %d\n", insCnt)
		insCnt++
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint32(insVal))
	}

	//confirmSkipListContent(t, sl, 11)

	// Get entries
	for i := 0; i < 5000; i++ {
		//fmt.Printf("get entry i=%d key=%d\n", i, i*11)
		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		if res == math.MaxUint32 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.SimpleAssert(t, uint32(i*11) == res)
		}
	}

	// delete all values
	for i := (5000 - 1); i >= 0; i-- {
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
	for i := 0; i < 5000; i++ {
		//fmt.Printf("get entry i=%d key=%d\n", i, i*11)
		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		if res == math.MaxUint32 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.SimpleAssert(t, uint32(i*11) == res)
		}
	}

	// delete all values
	for i := 0; i < 5000; i++ {
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

//func FuzzSkipLisMixInteger(f *testing.F) {
//	f.Add(int32(100), int32(150), int32(10), int32(300))
//	f.Fuzz(func(t *testing.T, bulkSize int32, opTimes int32, skipRand int32, initialEntryNum int32) {
//		if bulkSize < 0 || opTimes < 0 || skipRand < 0 || initialEntryNum < 0 {
//			return
//		}
//
//		os.Remove("test.db")
//		os.Remove("test.log")
//
//		shi := samehada.NewSamehadaInstanceForTesting()
//		//shi := samehada.NewSamehadaInstance("test", 10*1024) // buffer is about 40MB
//		bpm := shi.GetBufferPoolManager()
//
//		testSkipListMix[int32](t, types.Integer, bulkSize, opTimes, skipRand, initialEntryNum)
//
//		shi.CloseFilesForTesting()
//	})
//}

//func FuzzSkipLisMixVarchar(f *testing.F) {
//	f.Add(int32(100), int32(150), int32(10), int32(300))
//	f.Fuzz(func(t *testing.T, bulkSize int32, opTimes int32, skipRand int32, initialEntryNum int32) {
//		if bulkSize < 0 || opTimes < 0 || skipRand < 0 || initialEntryNum < 0 {
//			return
//		}
//
//		//os.Remove("test.db")
//		//os.Remove("test.log")
//		randStr := *samehada_util.GetRandomStr(20)
//		//fnameNum := strconv.Itoa(int(bulkSize + opTimes + skipRand + initialEntryNum))
//		//randStr := fnameNum + ".db"
//
//		//shi := samehada.NewSamehadaInstanceForTesting()
//		//shi := samehada.NewSamehadaInstance(*randStr, 10) // 10 frames (1 page = 4096bytes)
//		shi := samehada.NewSamehadaInstance(randStr, 10*1024) // 10 * 1024 frames (1 page = 4096bytes)
//		bpm := shi.GetBufferPoolManager()
//
//		testSkipListMix[string](t, types.Varchar, bulkSize, opTimes, skipRand, initialEntryNum)
//
//		//shi.CloseFilesForTesting()
//		shi.Shutdown(true)
//	})
//}

//func TestFuzzerUnexpectedExitParam(t *testing.T) {
//	os.Remove("test.db")
//	os.Remove("test.log")
//
//	//shi := samehada.NewSamehadaInstanceForTesting()
//	shi := samehada.NewSamehadaInstance("test", 10) // 10 pages
//	bpm := shi.GetBufferPoolManager()
//
//	fmt.Printf("param of TestFuzzerUnexpectedExitParam: %d %d %d %d\n", int32('Î'), int32('['), int32('Y'), int32('Ú'))
//	testSkipListMix[string](t, types.Varchar, rune('Î'), rune('['), rune('Y'), rune('Ú'))
//
//	shi.CloseFilesForTesting()
//}

const MAX_ENTRIES = 100000

func GetValueForSkipListEntry(val interface{}) uint32 {
	var ret uint32
	switch val.(type) {
	case int32:
		ret = uint32(val.(int32))
	case float32:
		ret = uint32(val.(float32))
	case string:
		ret = uint32(len(val.(string)))
	default:
		panic("unsupported type!")
	}
	return ret
}

func getRandomPrimitiveVal[T int32 | float32 | string](keyType types.TypeID) T {
	switch keyType {
	case types.Integer:
		var ret interface{} = rand.Int31()
		return ret.(T)
	case types.Float:
		var ret interface{} = rand.Float32()
		return ret.(T)
	case types.Varchar:
		var ret interface{} = *samehada_util.GetRandomStr(20)
		return ret.(T)
	default:
		panic("not supported keyType")
	}
}

// for debug
func isAlreadyRemoved[T int32 | float32 | string](checkVal T, removedVals []T) bool {
	for _, val := range removedVals {
		if val == checkVal {
			return true
		}
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

func insertRandom[T int32 | float32 | string](sl *skip_list.SkipList, num int32, checkDupMap map[T]T, insVals *[]T, keyType types.TypeID) {
	if int32(len(*insVals))+num < MAX_ENTRIES {
		for ii := 0; ii < int(num); ii++ {
			insVal := getRandomPrimitiveVal[T](keyType)
			for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
				insVal = getRandomPrimitiveVal[T](keyType)
			}
			checkDupMap[insVal] = insVal

			pairVal := GetValueForSkipListEntry(insVal)

			sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), pairVal)
			//fmt.Printf("sl.Insert at insertRandom: ii=%d, insVal=%d len(*insVals)=%d\n", ii, insVal, len(insVals))
			*insVals = append(*insVals, insVal)
		}
	}
}

func removeRandom[T int32 | float32 | string](t *testing.T, sl *skip_list.SkipList, opStep int32, num int32, insVals *[]T, removedVals *[]T) {
	if int32(len(*insVals))-num > 0 {
		for ii := 0; ii < int(num); ii++ {
			tmpIdx := int(rand.Intn(len(*insVals)))
			insVal := (*insVals)[tmpIdx]

			pairVal := GetValueForSkipListEntry(insVal)

			isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(insVal)), pairVal)
			if isAlreadyRemoved(insVal, *removedVals) {
				fmt.Printf("delete duplicated value should not be occur! opStep=%d, ii=%d tmpIdx=%d insVal=%v len(*insVals)=%d len(*removedVals)=%d\n", opStep, ii, tmpIdx, insVal, len(*insVals), len(*removedVals))
				panic("delete duplicated value should not be occur!")
			}
			if isDeleted != true {
				fmt.Printf("isDeleted should be true! opStep=%d, ii=%d tmpIdx=%d insVal=%v len(*insVals)=%d len(*removedVals)=%d\n", opStep, ii, tmpIdx, insVal, len(*insVals), len(*removedVals))
				panic("isDeleted should be true!")
				//common.RuntimeStack()
			}
			testingpkg.SimpleAssert(t, isDeleted == true || isAlreadyRemoved(insVal, *removedVals))
			if len(*insVals) == 1 {
				// make empty
				*insVals = make([]T, 0)
			} else if len(*insVals) == tmpIdx+1 {
				*insVals = (*insVals)[:len(*insVals)-1]
			} else {
				*insVals = append((*insVals)[:tmpIdx], (*insVals)[tmpIdx+1:]...)
			}
			*removedVals = append(*removedVals, insVal)
		}
	}
}

func testSkipListMix[T int32 | float32 | string](t *testing.T, keyType types.TypeID, bulkSize int32, opTimes int32, skipRand int32, initialEntryNum int32) {
	common.ShPrintf(common.DEBUG, "start of testSkipListMix bulkSize=%d opTimes=%d skipRand=%d initialEntryNum=%d ====================================================\n",
		bulkSize, opTimes, skipRand, initialEntryNum)

	os.Remove("test.db")
	os.Remove("test.log")

	shi := samehada.NewSamehadaInstance("test", 10)
	//shi := samehada.NewSamehadaInstanceForTesting()
	//shi := samehada.NewSamehadaInstance("test", 10*1024) // buffer is about 40MB
	bpm := shi.GetBufferPoolManager()

	checkDupMap := make(map[T]T)

	sl := skip_list.NewSkipList(bpm, keyType)

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
	entriesOnListNum := int32(0)

	// initial entries
	useInitialEntryNum := int(initialEntryNum)
	for ii := 0; ii < useInitialEntryNum; ii++ {
		if entriesOnListNum+1 < MAX_ENTRIES {
			// avoid duplication
			//insVal := rand.Int31()
			insVal := getRandomPrimitiveVal[T](keyType)
			for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
				//insVal = rand.Int31()
				insVal = getRandomPrimitiveVal[T](keyType)
			}
			checkDupMap[insVal] = insVal

			pairVal := GetValueForSkipListEntry(insVal)

			sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), pairVal)
			insVals = append(insVals, insVal)
			entriesOnListNum++
		}
	}

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
				insertRandom(sl, bulkSize, checkDupMap, &insVals, keyType)
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
					isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(tmpVal)), GetValueForSkipListEntry(tmpVal))
					testingpkg.SimpleAssert(t, isDeleted == false)
					if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
						fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
						common.RuntimeStack()
					}
				}
			} else {
				// 80% is Remove to existing entry
				if entriesOnListNum-bulkSize > 0 {
					removeRandom(t, sl, int32(ii), bulkSize, &insVals, &removedVals)
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
				gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewValue(insVals[tmpIdx])))
				if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
					fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
					panic("entries num on list is strange!")
					//common.RuntimeStack()
				}
				if gotVal == math.MaxUint32 {
					fmt.Printf("%v is not found!\n", insVals[tmpIdx])
					panic("sl.GetValue could not target key!")
				}
				correctVal := GetValueForSkipListEntry(insVals[tmpIdx])
				if gotVal != correctVal {
					fmt.Printf("gotVal is not match! %d != %d\n", gotVal, correctVal)
					panic("gotVal is not match!")
					//common.RuntimeStack()
				}
				testingpkg.SimpleAssert(t, gotVal == correctVal)
			}
		}
	}

	//shi.Shutdown(false)
	shi.CloseFilesForTesting()
}

func testSkipListMixParallel[T int32 | float32 | string](t *testing.T, sl *skip_list.SkipList, keyType types.TypeID, opTimes int32, skipRand int32, initialEntryNum int32) {
	common.ShPrintf(common.DEBUG, "start of testSkipListMix opTimes=%d skipRand=%d initialEntryNum=%d ====================================================\n",
		opTimes, skipRand, initialEntryNum)

	checkDupMap := make(map[T]T)

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
	//entriesOnListNum := int32(0)

	// initial entries
	useInitialEntryNum := int(initialEntryNum)
	for ii := 0; ii < useInitialEntryNum; ii++ {
		// avoid duplication
		//insVal := rand.Int31()
		insVal := getRandomPrimitiveVal[T](keyType)
		for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
			//insVal = rand.Int31()
			insVal = getRandomPrimitiveVal[T](keyType)
		}
		checkDupMap[insVal] = insVal

		pairVal := GetValueForSkipListEntry(insVal)

		sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), pairVal)
		insVals = append(insVals, insVal)
	}

	//removedEntriesNum := int32(0)

	insValsMutex := new(sync.RWMutex)
	removedValsMutex := new(sync.RWMutex)
	checkDupMapMutex := new(sync.RWMutex)

	useOpTimes := int(opTimes)
	for ii := 0; ii < useOpTimes; ii++ {
		// get 0-2
		opType := rand.Intn(3)
		switch opType {
		case 0: // Insert
			go func() {
				insVal := getRandomPrimitiveVal[T](keyType)
				checkDupMapMutex.RLock()
				for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
					insVal = getRandomPrimitiveVal[T](keyType)
				}
				checkDupMapMutex.RUnlock()
				checkDupMapMutex.Lock()
				checkDupMap[insVal] = insVal
				checkDupMapMutex.Unlock()

				pairVal := GetValueForSkipListEntry(insVal)

				fmt.Println("Insert op start.")
				sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), pairVal)
				//fmt.Printf("sl.Insert at insertRandom: ii=%d, insVal=%d len(*insVals)=%d\n", ii, insVal, len(insVals))
				insValsMutex.Lock()
				insVals = append(insVals, insVal)
				insValsMutex.Unlock()
			}()
		case 1: // Delete
			// get 0-5 value
			tmpRand := rand.Intn(5)
			if tmpRand == 0 {
				go func() {
					// 20% is Remove to not existing entry
					removedValsMutex.RLock()
					if len(removedVals) == 0 {
						removedValsMutex.RUnlock()
						return
					}

					tmpIdx := int(rand.Intn(len(removedVals)))
					tmpVal := removedVals[tmpIdx]

					removedValsMutex.RUnlock()
					fmt.Println("Remove(fail) op start.")
					isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(tmpVal)), GetValueForSkipListEntry(tmpVal))
					common.SH_Assert(isDeleted == false, "delete should be fail!")
				}()
			} else {
				go func() {
					// 80% is Remove to existing entry
					insValsMutex.RLock()
					if len(insVals)-1 < 0 {
						insValsMutex.RUnlock()
						return
					}
					tmpIdx := int(rand.Intn(len(insVals)))
					insVal := (insVals)[tmpIdx]
					insValsMutex.RUnlock()

					pairVal := GetValueForSkipListEntry(insVal)

					fmt.Println("Remove(success) op start.")
					isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(insVal)), pairVal)
					if isDeleted == true {
						removedValsMutex.Lock()
						removedVals = append(removedVals, insVal)
						removedValsMutex.Unlock()
					} else {
						removedValsMutex.RLock()
						if ok := isAlreadyRemoved(insVal, removedVals); !ok {
							removedValsMutex.RUnlock()
							panic("remove op test failed!")
						}
					}
					//common.SH_Assert(isDeleted == true, "remove should be success!")
				}()
			}
		case 2: // Get
			go func() {
				insValsMutex.RLock()
				if len(insVals) == 0 {
					insValsMutex.RUnlock()
					return
				}
				tmpIdx := int(rand.Intn(len(insVals)))
				//fmt.Printf("sl.GetValue at testSkipListMix: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(removedVals))
				getTgtBase := insVals[tmpIdx]
				getTgt := types.NewValue(getTgtBase)
				correctVal := GetValueForSkipListEntry(insVals[tmpIdx])
				insValsMutex.RUnlock()

				fmt.Println("Get op start.")
				gotVal := sl.GetValue(&getTgt)
				if gotVal == math.MaxUint32 {
					removedValsMutex.RLock()
					if ok := isAlreadyRemoved(getTgtBase, removedVals); !ok {
						removedValsMutex.RUnlock()
						panic("get op test failed!")
					}
				} else if gotVal != correctVal {
					panic("returned value of get of is wrong!")
				}
				//common.SH_Assert(, "gotVal is not collect!")
			}()
		}
	}
}

func testSkipListMixRoot[T int32 | float32 | string](t *testing.T, keyType types.TypeID) {
	//os.Remove("test.db")
	//os.Remove("test.log")
	//
	//shi := samehada.NewSamehadaInstance("test", 10)
	////shi := samehada.NewSamehadaInstanceForTesting()
	////shi := samehada.NewSamehadaInstance("test", 10*1024) // buffer is about 40MB
	//bpm := shi.GetBufferPoolManager()

	testSkipListMix[T](t, keyType, 1, int32(150), int32(10), int32(0))
	testSkipListMix[T](t, keyType, 1, int32(150), int32(10), int32(300))
	testSkipListMix[T](t, keyType, 1, int32(150), int32(10), int32(600))
	testSkipListMix[T](t, keyType, 1, int32(200), int32(5), int32(10))
	testSkipListMix[T](t, keyType, 1, int32(250), int32(5), int32(10))
	testSkipListMix[T](t, keyType, 1, int32(250), int32(4), int32(0))
	testSkipListMix[T](t, keyType, 1, int32(250), int32(3), int32(0))

	testSkipListMix[T](t, keyType, 50, int32(150), int32(10), int32(0))
	testSkipListMix[T](t, keyType, 50, int32(150), int32(10), int32(300))
	testSkipListMix[T](t, keyType, 50, int32(150), int32(10), int32(600))
	testSkipListMix[T](t, keyType, 50, int32(200), int32(5), int32(10))
	testSkipListMix[T](t, keyType, 50, int32(250), int32(5), int32(10))
	testSkipListMix[T](t, keyType, 50, int32(250), int32(4), int32(0))
	testSkipListMix[T](t, keyType, 50, int32(250), int32(3), int32(0))

	testSkipListMix[T](t, keyType, 100, int32(150), int32(10), int32(0))
	testSkipListMix[T](t, keyType, 100, int32(150), int32(10), int32(300))
	testSkipListMix[T](t, keyType, 100, int32(150), int32(10), int32(600))
	testSkipListMix[T](t, keyType, 100, int32(200), int32(5), int32(10))
	testSkipListMix[T](t, keyType, 100, int32(250), int32(5), int32(10))
	testSkipListMix[T](t, keyType, 100, int32(250), int32(4), int32(0))
	testSkipListMix[T](t, keyType, 100, int32(250), int32(3), int32(0))

	////shi.Shutdown(true)
	//shi.CloseFilesForTesting()
}

func testSkipListMixParallelRoot[T int32 | float32 | string](t *testing.T, keyType types.TypeID) {
	os.Remove("test.db")
	os.Remove("test.log")

	shi := samehada.NewSamehadaInstance("test", 30)
	//shi := samehada.NewSamehadaInstanceForTesting()
	//shi := samehada.NewSamehadaInstance("test", 10*1024) // buffer is about 40MB
	bpm := shi.GetBufferPoolManager()
	sl := skip_list.NewSkipList(bpm, keyType)

	testSkipListMixParallel[T](t, sl, keyType, int32(300000), int32(10), int32(1000))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(150), int32(10), int32(300))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(150), int32(10), int32(600))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(200), int32(5), int32(10))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(250), int32(5), int32(10))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(250), int32(4), int32(0))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(250), int32(3), int32(0))

	////shi.Shutdown(true)
	shi.CloseFilesForTesting()
}

func TestSkipListMixInteger(t *testing.T) {
	testSkipListMixRoot[int32](t, types.Integer)
}

func TestSkipListMixFloat(t *testing.T) {
	testSkipListMixRoot[float32](t, types.Float)
}

func TestSkipListMixVarchar(t *testing.T) {
	testSkipListMixRoot[string](t, types.Varchar)
}

//func TestSkipListMixParallelInteger(t *testing.T) {
//	testSkipListMixParallelRoot[int32](t, types.Integer)
//}

func testSkipListInsertGetEven(t *testing.T, sl *skip_list.SkipList, ch chan string) {
	for ii := int32(0); ii < 1000; ii = ii + 2 {
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(ii)), uint32(ii))
		gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(ii)))
		if gotVal == math.MaxUint32 {
			t.Fail()
			fmt.Printf("value %d is not found!\n", ii)
			panic("inserted value not found!")
		}
	}
	fmt.Println("even finished.")
	ch <- ""
}

func testSkipListInsertGetOdd(t *testing.T, sl *skip_list.SkipList, ch chan string) {
	for ii := int32(1); ii < 1000; ii = ii + 2 {
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(ii)), uint32(ii))
		gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(ii)))
		if gotVal == math.MaxUint32 {
			fmt.Printf("value %d is not found!\n", ii)
		}
	}
	fmt.Println("odd finished.")
	ch <- ""
}

func TestSkipListParallelSimpleInteger(t *testing.T) {
	os.Remove("test.db")
	os.Remove("test.log")

	//shi := samehada.NewSamehadaInstance("test", 400)
	shi := samehada.NewSamehadaInstance("test", 30)
	bpm := shi.GetBufferPoolManager()
	sl := skip_list.NewSkipList(bpm, types.Integer)

	//var wg sync.WaitGroup
	//wg.Add(2)

	ch1 := make(chan string)
	ch2 := make(chan string)

	go testSkipListInsertGetEven(t, sl, ch1)
	go testSkipListInsertGetOdd(t, sl, ch2)

	//wg.Wait()
	ch1Ret := <-ch1
	t.Logf("%s\n", ch1Ret)
	t.Logf("ch1 received\n")
	ch2Ret := <-ch2
	t.Logf("%s\n", ch2Ret)
	t.Logf("ch2 received\n")

	shi.CloseFilesForTesting()
}
