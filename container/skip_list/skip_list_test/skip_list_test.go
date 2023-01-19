package skip_list_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/container/skip_list"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"
)

/*
import (
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/samehada/samehada_util"
	"github.com/ryogrid/SamehadaDB/storage/page/skip_list_page"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"github.com/ryogrid/SamehadaDB/types"
	"math"
	"os"
	"testing"
)
*/

/*
func TestSerializationOfSkipLisBlockPage(t *testing.T) {
	t.Parallel()
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}
	//shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
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
	// freeSpacePointer is decremented size of entry (1+2+7+8 => 18)
	bpage.SetEntry(1, &skip_list_page.SkipListPair{types.NewVarchar("abcdeff"), 12345})

	testingpkg.SimpleAssert(t, bpage.GetPageId() == 7)
	testingpkg.SimpleAssert(t, bpage.GetLSN() == 9)
	testingpkg.SimpleAssert(t, bpage.GetEntryCnt() == 2)
	testingpkg.SimpleAssert(t, bpage.GetLevel() == 4)
	testingpkg.SimpleAssert(t, bpage.GetForwardEntry(5) == types.PageID(11))

	testingpkg.SimpleAssert(t, bpage.GetFreeSpacePointer() == (common.PageSize-9-18))
	entry := bpage.GetEntry(1, types.Varchar)
	testingpkg.SimpleAssert(t, entry.Key.CompareEquals(types.NewVarchar("abcdeff")))
	testingpkg.SimpleAssert(t, entry.Value == 12345)

	shi.Shutdown(false)
}

func TestSerializationOfSkipLisHeaderPage(t *testing.T) {
	t.Parallel()
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}
	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	bpm := shi.GetBufferPoolManager()

	hpage, _, _ := skip_list_page.NewSkipListHeaderPage(bpm, types.Integer)
	//hpage := skip_list_page.FetchAndCastToHeaderPage(bpm, hpageId)

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
	t.Parallel()
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}
	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
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
	t.Parallel()
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}
	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
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
		bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{types.NewInteger(int32(ii * 10)), uint64(ii * 10)}))
	}
	bpage.SetEntryCnt(int32(len(bpage.GetEntries(types.Integer))))

	for ii := 1; ii < 100; ii++ {
		key := types.NewInteger(int32(ii * 5))
		found, entry, idx := bpage.FindEntryByKey(&key)
		//fmt.Println(ii)
		if ii%2 == 0 {
			testingpkg.SimpleAssert(t, found == true && entry.Value == uint64(key.ToInteger()))
		} else {
			testingpkg.SimpleAssert(t, found == false && uint64(key.ToInteger())-bpage.ValueAt(idx, types.Integer) == 5)
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
		bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{types.NewInteger(int32(ii * 10)), uint64(ii * 10)}))
	}
	bpage.SetEntryCnt(int32(len(bpage.GetEntries(types.Integer))))

	for ii := 1; ii < 102; ii++ {
		key := types.NewInteger(int32(ii * 5))
		found, entry, idx := bpage.FindEntryByKey(&key)
		//fmt.Println(ii)
		if ii%2 == 0 {
			testingpkg.SimpleAssert(t, found == true && entry.Value == uint64(key.ToInteger()))
		} else {
			testingpkg.SimpleAssert(t, found == false && uint64(key.ToInteger())-bpage.ValueAt(idx, types.Integer) == 5)
		}
	}

	shi.Shutdown(false)
}

func TestBSearchOfSkipLisBlockPage2(t *testing.T) {
	t.Parallel()
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}
	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
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
		bpm.IncPinOfPage(bpage)
		bpage.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(ii*10))), uint64(ii*10), bpm, nil, 1)
		//bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{types.NewInteger(int32(ii * 10)), uint64(ii * 10)}))
	}
	bpage.WLatch()
	bpage.SetEntryCnt(int32(len(bpage.GetEntries(types.Integer))))

	for ii := 1; ii < 100; ii++ {
		key := types.NewInteger(int32(ii * 5))
		found, entry, idx := bpage.FindEntryByKey(&key)
		//fmt.Println(ii)
		if ii%2 == 0 {
			testingpkg.SimpleAssert(t, found == true && entry.Value == uint64(key.ToInteger()))
		} else {
			testingpkg.SimpleAssert(t, found == false && uint64(key.ToInteger())-bpage.ValueAt(idx, types.Integer) == 5)
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
		bpm.IncPinOfPage(bpage)
		bpage.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(ii*10))), uint64(ii*10), bpm, nil, 1)
		//bpage.SetEntries(append(bpage.GetEntries(types.Integer), &skip_list_page.SkipListPair{types.NewInteger(int32(ii * 10)), uint64(ii * 10)}))
	}
	bpage.WLatch()
	bpage.SetEntryCnt(int32(len(bpage.GetEntries(types.Integer))))

	for ii := 1; ii < 102; ii++ {
		key := types.NewInteger(int32(ii * 5))
		found, entry, idx := bpage.FindEntryByKey(&key)
		//fmt.Println(ii)
		if ii%2 == 0 {
			testingpkg.SimpleAssert(t, found == true && entry.Value == uint64(key.ToInteger()))
		} else {
			testingpkg.SimpleAssert(t, found == false && uint64(key.ToInteger())-bpage.ValueAt(idx, types.Integer) == 5)
		}
	}
	bpage.WUnlatch()

	shi.Shutdown(false)
}

*/

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

/*
func TestSkipListSimple(t *testing.T) {
	t.Parallel()
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
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
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint64(insVal))
	}

	//confirmSkipListContent(t, sl, 11)

	// Get entries
	for i := 0; i < 250; i++ {
		//fmt.Printf("get entry i=%d key=%d\n", i, i*11)
		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		if res == math.MaxUint32 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.SimpleAssert(t, uint64(i*11) == res)
		}
	}

	// delete some values
	for i := 0; i < 100; i++ {
		// check existance before delete
		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		if res == math.MaxUint32 {
			panic("result should not be nil")
		} else {
			testingpkg.SimpleAssert(t, uint64(i*11) == res)
		}

		// check no existance after delete
		sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(i*11))), uint64(i*11))

		res = sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		testingpkg.SimpleAssert(t, math.MaxUint64 == res)
		//fmt.Println("contents listing after delete")
		//confirmSkipListContent(t, sl, -1)
	}

	shi.Shutdown(false)
}

func TestSkipListInsertAndDeleteAll(t *testing.T) {
	t.Parallel()
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	//shi := samehada.NewSamehadaInstance(t.Name(), 100)
	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
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
		common.ShPrintf(common.DEBUG_INFO, "insCnt: %d\n", insCnt)
		insCnt++
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint64(insVal))
	}

	//confirmSkipListContent(t, sl, 11)

	// Get entries
	for i := 0; i < 5000; i++ {
		//fmt.Printf("get entry i=%d key=%d\n", i, i*11)
		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		if res == math.MaxUint32 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.SimpleAssert(t, uint64(i*11) == res)
		}
	}

	// delete all values
	for i := (5000 - 1); i >= 0; i-- {
		// delete
		isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(i*11))), uint64(i*11))
		common.ShPrintf(common.DEBUG_INFO, "i=%d i*11=%d\n", i, i*11)
		testingpkg.SimpleAssert(t, isDeleted == true)

		// check no existance after delete
		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		common.ShPrintf(common.DEBUG_INFO, "i=%d i*11=%d res=%d\n", i, i*11, res)
		testingpkg.SimpleAssert(t, math.MaxUint64 == res)
	}

	//////////// remove from head ///////

	// Re-Insert entries
	insCnt = 0
	for _, insVal := range insVals {
		//fmt.Printf("insCnt: %d\n", insCnt)
		insCnt++
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint64(insVal))
	}

	// Get entries
	for i := 0; i < 5000; i++ {
		//fmt.Printf("get entry i=%d key=%d\n", i, i*11)
		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		if res == math.MaxUint32 {
			t.Errorf("result should not be nil")
		} else {
			testingpkg.SimpleAssert(t, uint64(i*11) == res)
		}
	}

	// delete all values
	for i := 0; i < 5000; i++ {
		// delete
		isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(int32(i*11))), uint64(i*11))
		common.ShPrintf(common.DEBUG_INFO, "i=%d i*11=%d\n", i, i*11)
		testingpkg.SimpleAssert(t, isDeleted == true)

		// check no existance after delete
		res := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(int32(i * 11))))
		common.ShPrintf(common.DEBUG_INFO, "i=%d i*11=%d res=%d\n", i, i*11, res)
		testingpkg.SimpleAssert(t, math.MaxUint64 == res)
	}

	shi.Shutdown(false)
}

func TestSkipListItr(t *testing.T) {
	t.Parallel()
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	sl := skip_list.NewSkipList(shi.GetBufferPoolManager(), types.Integer)

	insVals := make([]int32, 0)
	for i := 0; i < 250; i++ {
		insVals = append(insVals, int32(i*11))
	}
	// shuffle value list for inserting
	rand.Shuffle(len(insVals), func(i, j int) { insVals[i], insVals[j] = insVals[j], insVals[i] })

	for _, insVal := range insVals {
		//fmt.Println(insVal)
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(int32(insVal))), uint64(insVal))
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
*/

//func FuzzSkipLisMixInteger(f *testing.F) {
//	if testing.Short() {
//		f.Skip("skip this in short mode.")
//	}
//	//f.Add(int32(100), int32(150), int32(10), int32(300))
//	f.Add(int32(100), int32(50), int32(10), int32(300))
//	f.Fuzz(func(t *testing.T, bulkSize int32, opTimes int32, skipRand int32, initialEntryNum int32) {
//		if bulkSize < 0 || opTimes < 0 || skipRand < 0 || initialEntryNum < 0 {
//			return
//		}
//
//		//if !common.EnableOnMemStorage {
//		//	os.Remove(t.Name() + ".db")
//		//	os.Remove(t.Name() + ".log")
//		//}
//
//		//shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
//		////shi := samehada.NewSamehadaInstance(t.Name(), 10*1024) // buffer is about 40MB
//		//bpm := shi.GetBufferPoolManager()
//
//		testSkipListMix[int32](t, types.Integer, bulkSize, opTimes, skipRand, initialEntryNum, true)
//
//		//shi.CloseFilesForTesting()
//	})
//}
//
//func FuzzSkipLisMixVarchar(f *testing.F) {
//	if testing.Short() {
//		f.Skip("skip this in short mode.")
//	}
//	f.Add(int32(100), int32(150), int32(10), int32(300))
//	f.Fuzz(func(t *testing.T, bulkSize int32, opTimes int32, skipRand int32, initialEntryNum int32) {
//		if bulkSize < 0 || opTimes < 0 || skipRand < 0 || initialEntryNum < 0 {
//			return
//		}
//
//		//if !common.EnableOnMemStorage {
//		//	os.Remove(t.Name() + ".db")
//		//	os.Remove(t.Name() + ".log")
//		//}
//		//randStr := *samehada_util.GetRandomStr(20)
//		////fnameNum := strconv.Itoa(int(bulkSize + opTimes + skipRand + initialEntryNum))
//		////randStr := fnameNum + ".db"
//		//
//		////shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
//		////shi := samehada.NewSamehadaInstance(*randStr, 10) // 10 frames (1 page = 4096bytes)
//		//shi := samehada.NewSamehadaInstance(randStr, 10*1024) // 10 * 1024 frames (1 page = 4096bytes)
//		//bpm := shi.GetBufferPoolManager()
//
//		testSkipListMix[string](t, types.Varchar, bulkSize, opTimes, skipRand, initialEntryNum, true)
//
//		////shi.CloseFilesForTesting()
//		//shi.Shutdown(true)
//	})
//}
//
//func FuzzSkipLisMixParallelStrideVarchar(f *testing.F) {
//	if testing.Short() {
//		f.Skip("skip this in short mode.")
//	}
//	f.Add(int32(30), int32(100), int32(10), int32(0))
//	f.Add(int32(1), int32(200), int32(11), int32(100))
//	f.Fuzz(func(t *testing.T, stride int32, opTimes int32, skipRand int32, initialEntryNum int32) {
//		//startTime := time.Now().UnixNano()
//
//		if stride < 1 || stride > 60 || opTimes < 1 || opTimes > 200 || skipRand < 0 || initialEntryNum < 0 || initialEntryNum > 20 {
//			return
//		}
//
//		testSkipListMixParallelStride[string](t, types.Varchar, stride, opTimes, skipRand, initialEntryNum, 500)
//
//		//// launch test with passed paramaters
//		//go func(stride_ int32, opTimes_ int32, skipRand_ int32, initialEntryNum_ int32) {
//		//	testSkipListMixParallelStride[string](t, types.Varchar, stride_, opTimes_, skipRand_, initialEntryNum_, 500, false)
//		//}(stride, opTimes, skipRand, initialEntryNum)
//		//
//		//// check timeout
//		//for {
//		//	elapsedTime := time.Now().UnixNano() - startTime
//		//	if elapsedTime > 1000*1000*800 { //800ms
//		//		return
//		//	}
//		//	time.Sleep(time.Millisecond * 10) // 5ms
//		//}
//	})
//}

func memset(buffer []byte, value int) {
	for i := range buffer {
		buffer[i] = byte(value)
	}
}

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
	for ii := 0; ii < int(num); ii++ {
		insVal := samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
			insVal = samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		}
		checkDupMap[insVal] = insVal

		pairVal := samehada_util.GetValueForSkipListEntry(insVal)

		sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), uint64(pairVal))
		//fmt.Printf("sl.Insert at insertRandom: ii=%d, insVal=%d len(*insVals)=%d\n", ii, insVal, len(insVals))
		*insVals = append(*insVals, insVal)
	}
}

func removeRandom[T int32 | float32 | string](t *testing.T, sl *skip_list.SkipList, opStep int32, num int32, insVals *[]T, removedVals *[]T) {
	if int32(len(*insVals))-num > 0 {
		for ii := 0; ii < int(num); ii++ {
			tmpIdx := int(rand.Intn(len(*insVals)))
			insVal := (*insVals)[tmpIdx]

			pairVal := samehada_util.GetValueForSkipListEntry(insVal)

			isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(insVal)), uint64(pairVal))
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

			// check removed val does not exist
			isDeleted = sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(insVal)), uint64(pairVal))
			if isDeleted != false {
				fmt.Printf("isDeleted should not be true! opStep=%d, ii=%d tmpIdx=%d insVal=%v len(*insVals)=%d len(*removedVals)=%d\n", opStep, ii, tmpIdx, insVal, len(*insVals), len(*removedVals))
				panic("isDeleted should be false!")
				//common.RuntimeStack()
			}

			if len(*insVals) == 1 {
				// make empty
				*insVals = make([]T, 0)
			} else if len(*insVals) == tmpIdx+1 {
				*insVals = (*insVals)[:len(*insVals)-1]
			} else {
				*insVals = append((*insVals)[:tmpIdx], (*insVals)[tmpIdx+1:]...)
			}
			*removedVals = append(*removedVals, insVal)
			//if int32(len(*insVals)) != countSkipListContent(sl) {
			//	fmt.Printf("entries num on list is strange! len(*insVals)=%d ii=%d\n", len(*insVals), ii)
			//	panic("entries num on list is strange!")
			//	//common.RuntimeStack()
			//}
		}
	}
}

func testSkipListMix[T int32 | float32 | string](t *testing.T, keyType types.TypeID, bulkSize int32, opTimes int32, skipRand int32, initialEntryNum int32, isFuzz bool) {
	common.ShPrintf(common.DEBUG_INFO, "start of testSkipListMix bulkSize=%d opTimes=%d skipRand=%d initialEntryNum=%d ====================================================\n",
		bulkSize, opTimes, skipRand, initialEntryNum)

	var startTime int64
	if isFuzz {
		startTime = time.Now().UnixNano()
	}
	//if !common.EnableOnMemStorage {
	//	os.Remove(t.Name() + ".db")
	//	os.Remove(t.Name() + ".log")
	//}

	//shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	//shi := samehada.NewSamehadaInstance(t.Name(), 10)

	shi := samehada.NewSamehadaInstance(t.Name(), 300)
	//shi := samehada.NewSamehadaInstance(t.Name(), 10)
	//shi := samehada.NewSamehadaInstance(t.Name(), 10*1024) // buffer is about 40MB

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
		// avoid duplication
		//insVal := rand.Int31()
		insVal := samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
			//insVal = rand.Int31()
			insVal = samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		}
		checkDupMap[insVal] = insVal

		pairVal := samehada_util.GetValueForSkipListEntry(insVal)

		sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), uint64(pairVal))
		insVals = append(insVals, insVal)
		entriesOnListNum++
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
		if isFuzz { // for avoiding over 1sec
			elapsedTime := time.Now().UnixNano() - startTime
			if elapsedTime > 1000*850 { //850ms
				return
			}
		}

		// get 0-2
		opType := rand.Intn(3)
		switch opType {
		case 0: // Insert
			insertRandom(sl, bulkSize, checkDupMap, &insVals, keyType)
			entriesOnListNum += bulkSize
			if entriesOnListNum != countSkipListContent(sl) || entriesOnListNum != int32(len(insVals)) || removedEntriesNum != int32(len(removedVals)) {
				fmt.Printf("entries num on list is strange! %d != (%d or %d) / %d != %d\n", entriesOnListNum, countSkipListContent(sl), len(insVals), removedEntriesNum, len(removedVals))
				//common.RuntimeStack()
				panic("entries num on list is strange!")
			}
		case 1: // Delete
			// get 0-5 value
			tmpRand := rand.Intn(5)
			if tmpRand == 0 {
				// 20% is Remove to not existing entry
				if len(removedVals) != 0 {
					tmpIdx := int(rand.Intn(len(removedVals)))
					tmpVal := removedVals[tmpIdx]
					isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(tmpVal)), samehada_util.GetValueForSkipListEntry(tmpVal))
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
				correctVal := samehada_util.GetValueForSkipListEntry(insVals[tmpIdx])
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

func testSkipListMixParallel[T int32 | float32 | string](t *testing.T, keyType types.TypeID, opTimes int32, skipRand int32, initialEntryNum int32) {
	common.ShPrintf(common.DEBUG_INFO, "start of testSkipListMix opTimes=%d skipRand=%d initialEntryNum=%d ====================================================\n",
		opTimes, skipRand, initialEntryNum)

	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	//shi := samehada.NewSamehadaInstance(t.Name(), 30)
	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	//shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	//shi := samehada.NewSamehadaInstance(t.Name(), 10*1024) // buffer is about 40MB
	bpm := shi.GetBufferPoolManager()
	sl := skip_list.NewSkipList(bpm, keyType)

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
		insVal := samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
			//insVal = rand.Int31()
			insVal = samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		}
		checkDupMap[insVal] = insVal

		pairVal := samehada_util.GetValueForSkipListEntry(insVal)

		sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), uint64(pairVal))
		insVals = append(insVals, insVal)
	}

	//removedEntriesNum := int32(0)

	insValsMutex := new(sync.RWMutex)
	removedValsMutex := new(sync.RWMutex)
	checkDupMapMutex := new(sync.RWMutex)

	ch := make(chan int32)

	useOpTimes := int(opTimes)
	exitedThCnt := 0
	for ii := 0; ii <= useOpTimes; ii++ {
		// wait 20 groroutine exited
		if ii == useOpTimes {
			for exitedThCnt < 20 {
				<-ch
				exitedThCnt++
				common.ShPrintf(common.DEBUGGING, "exitedThCnt=%d\n", exitedThCnt)
			}
			break
		} else if ii%20 == 0 && ii != 0 {
			for exitedThCnt < 20 {
				<-ch
				exitedThCnt++
				common.ShPrintf(common.DEBUGGING, "exitedThCnt=%d\n", exitedThCnt)
			}
		}
		common.ShPrintf(common.DEBUGGING, "ii=%d\n", ii)
		exitedThCnt = 0

		// get 0-2
		opType := rand.Intn(3)
		switch opType {
		case 0: // Insert
			go func() {
				insVal := samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
				checkDupMapMutex.RLock()
				for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
					insVal = samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
				}
				checkDupMapMutex.RUnlock()
				checkDupMapMutex.Lock()
				checkDupMap[insVal] = insVal
				checkDupMapMutex.Unlock()

				pairVal := samehada_util.GetValueForSkipListEntry(insVal)

				common.ShPrintf(common.DEBUGGING, "Insert op start.")
				sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), uint64(pairVal))
				//fmt.Printf("sl.Insert at insertRandom: ii=%d, insVal=%d len(*insVals)=%d\n", ii, insVal, len(insVals))
				insValsMutex.Lock()
				insVals = append(insVals, insVal)
				insValsMutex.Unlock()
				ch <- 1
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
						ch <- 1
						return
					}

					tmpIdx := int(rand.Intn(len(removedVals)))
					tmpVal := removedVals[tmpIdx]

					removedValsMutex.RUnlock()
					common.ShPrintf(common.DEBUGGING, "Remove(fail) op start.")
					isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(tmpVal)), samehada_util.GetValueForSkipListEntry(tmpVal))
					common.SH_Assert(isDeleted == false, "delete should be fail!")
					ch <- 1
				}()
			} else {
				go func() {
					// 80% is Remove to existing entry
					insValsMutex.RLock()
					if len(insVals)-1 < 0 {
						insValsMutex.RUnlock()
						ch <- 1
						return
					}
					tmpIdx := int(rand.Intn(len(insVals)))
					insVal := insVals[tmpIdx]
					insValsMutex.RUnlock()

					pairVal := samehada_util.GetValueForSkipListEntry(insVal)

					common.ShPrintf(common.DEBUGGING, "Remove(success) op start.")
					isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(insVal)), uint64(pairVal))
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
						removedValsMutex.RUnlock()
					}
					ch <- 1
					//common.SH_Assert(isDeleted == true, "remove should be success!")
				}()
			}
		case 2: // Get
			go func() {
				insValsMutex.RLock()
				if len(insVals) == 0 {
					insValsMutex.RUnlock()
					ch <- 1
					return
				}
				tmpIdx := int(rand.Intn(len(insVals)))
				//fmt.Printf("sl.GetValue at testSkipListMix: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(removedVals))
				getTgtBase := insVals[tmpIdx]
				getTgt := types.NewValue(getTgtBase)
				correctVal := samehada_util.GetValueForSkipListEntry(insVals[tmpIdx])
				insValsMutex.RUnlock()

				common.ShPrintf(common.DEBUGGING, "Get op start.")
				gotVal := sl.GetValue(&getTgt)
				if gotVal == math.MaxUint64 {
					removedValsMutex.RLock()
					if ok := isAlreadyRemoved(getTgtBase, removedVals); !ok {
						removedValsMutex.RUnlock()
						panic("get op test failed!")
					}
					removedValsMutex.RUnlock()
				} else if gotVal != correctVal {
					panic("returned value of get of is wrong!")
				}
				ch <- 1
				//common.SH_Assert(, "gotVal is not collect!")
			}()
		}
	}
	shi.CloseFilesForTesting()
}

func testSkipListMixParallelBulk[T int32 | float32 | string](t *testing.T, keyType types.TypeID, bulkSize int32, opTimes int32, skipRand int32, initialEntryNum int32) {
	common.ShPrintf(common.DEBUG_INFO, "start of testSkipListMix opTimes=%d skipRand=%d initialEntryNum=%d ====================================================\n",
		opTimes, skipRand, initialEntryNum)

	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	//shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	//shi := samehada.NewSamehadaInstance(t.Name(), 10*1024) // buffer is about 40MB
	bpm := shi.GetBufferPoolManager()
	sl := skip_list.NewSkipList(bpm, keyType)

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

	threadNum := 20
	// initial entries
	useInitialEntryNum := int(initialEntryNum)
	for ii := 0; ii < useInitialEntryNum; ii++ {
		// avoid duplication
		//insVal := rand.Int31()
		insVal := samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
			//insVal = rand.Int31()
			insVal = samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		}
		checkDupMap[insVal] = insVal

		pairVal := samehada_util.GetValueForSkipListEntry(insVal)

		sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), uint64(pairVal))
		insVals = append(insVals, insVal)
	}

	//removedEntriesNum := int32(0)

	insValsMutex := new(sync.RWMutex)
	removedValsMutex := new(sync.RWMutex)
	checkDupMapMutex := new(sync.RWMutex)

	ch := make(chan int32)

	useOpTimes := int(opTimes)
	exitedThCnt := 0
	for ii := 0; ii <= useOpTimes; ii++ {
		// wait 20 groroutine exited
		if ii == useOpTimes {
			for exitedThCnt < threadNum*int(bulkSize) {
				<-ch
				exitedThCnt++
				common.ShPrintf(common.DEBUGGING, "exitedThCnt=%d\n", exitedThCnt)
			}
			break
		} else if ii%threadNum == 0 && ii != 0 {
			for exitedThCnt < threadNum*int(bulkSize) {
				<-ch
				exitedThCnt++
				common.ShPrintf(common.DEBUGGING, "exitedThCnt=%d\n", exitedThCnt)
			}
		}
		common.ShPrintf(common.DEBUGGING, "ii=%d\n", ii)
		exitedThCnt = 0

		// get 0-3
		opType := rand.Intn(4)
		switch opType {
		case 0: // Insert
			go func() {
				for ii := int32(0); ii < bulkSize; ii++ {
					insVal := samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
					checkDupMapMutex.RLock()
					for _, exist := checkDupMap[insVal]; exist; _, exist = checkDupMap[insVal] {
						insVal = samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
					}
					checkDupMapMutex.RUnlock()
					checkDupMapMutex.Lock()
					checkDupMap[insVal] = insVal
					checkDupMapMutex.Unlock()

					pairVal := samehada_util.GetValueForSkipListEntry(insVal)

					common.ShPrintf(common.DEBUGGING, "Insert op start.")
					sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), uint64(pairVal))
					//fmt.Printf("sl.Insert at insertRandom: ii=%d, insVal=%d len(*insVals)=%d\n", ii, insVal, len(insVals))
					insValsMutex.Lock()
					insVals = append(insVals, insVal)
					insValsMutex.Unlock()
					ch <- 1
				}
			}()
		case 1, 2: // Delete
			// get 0-1 value
			tmpRand := rand.Intn(2)
			if tmpRand == 0 {
				// 50% is Remove to not existing entry
				go func() {
					for ii := int32(0); ii < bulkSize; ii++ {
						removedValsMutex.RLock()
						if len(removedVals) == 0 {
							removedValsMutex.RUnlock()
							ch <- 1
							continue
							//return
						}

						tmpIdx := int(rand.Intn(len(removedVals)))
						tmpVal := removedVals[tmpIdx]

						removedValsMutex.RUnlock()
						common.ShPrintf(common.DEBUGGING, "Remove(fail) op start.")
						isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(tmpVal)), samehada_util.GetValueForSkipListEntry(tmpVal))
						common.SH_Assert(isDeleted == false, "delete should be fail!")
						ch <- 1
					}
				}()
			} else {
				// 50% is Remove to existing entry
				go func() {
					for ii := int32(0); ii < bulkSize; ii++ {
						insValsMutex.RLock()
						if len(insVals)-1 < 0 {
							insValsMutex.RUnlock()
							ch <- 1
							continue
							//return
						}
						tmpIdx := int(rand.Intn(len(insVals)))
						insVal := (insVals)[tmpIdx]
						insValsMutex.RUnlock()

						pairVal := samehada_util.GetValueForSkipListEntry(insVal)

						common.ShPrintf(common.DEBUGGING, "Remove(success) op start.")
						isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(insVal)), uint64(pairVal))
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
							removedValsMutex.RUnlock()
						}
						ch <- 1
						//common.SH_Assert(isDeleted == true, "remove should be success!")
					}
				}()
			}
		case 3: // Get
			go func() {
				for ii := int32(0); ii < bulkSize; ii++ {
					insValsMutex.RLock()
					if len(insVals) == 0 {
						insValsMutex.RUnlock()
						ch <- 1
						continue
						//return
					}
					tmpIdx := int(rand.Intn(len(insVals)))
					//fmt.Printf("sl.GetValue at testSkipListMix: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*removedVals)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(removedVals))
					getTgtBase := insVals[tmpIdx]
					getTgt := types.NewValue(getTgtBase)
					correctVal := samehada_util.GetValueForSkipListEntry(insVals[tmpIdx])
					insValsMutex.RUnlock()

					common.ShPrintf(common.DEBUGGING, "Get op start.")
					gotVal := sl.GetValue(&getTgt)
					if gotVal == math.MaxUint64 {
						removedValsMutex.RLock()
						if ok := isAlreadyRemoved(getTgtBase, removedVals); !ok {
							removedValsMutex.RUnlock()
							panic("get op test failed!")
						}
						removedValsMutex.RUnlock()
					} else if gotVal != correctVal {
						panic("returned value of get of is wrong!")
					}
					ch <- 1
					//common.SH_Assert(, "gotVal is not collect!")
				}
			}()
		}
	}
	shi.CloseFilesForTesting()
}

func testSkipListMixParallelStride[T int32 | float32 | string](t *testing.T, keyType types.TypeID, stride int32, opTimes int32, seedVal int32, initialEntryNum int32, bpoolSize int32) {
	common.ShPrintf(common.DEBUG_INFO, "start of testSkipListMixParallelStride stride=%d opTimes=%d seedVal=%d initialEntryNum=%d ====================================================\n",
		stride, opTimes, seedVal, initialEntryNum)

	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	const THREAD_NUM = 20

	shi := samehada.NewSamehadaInstance(t.Name(), int(bpoolSize))
	//shi := samehada.NewSamehadaInstance(t.Name(), 30)
	//shi := samehada.NewSamehadaInstance(t.Name(), 60)

	//shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	//shi := samehada.NewSamehadaInstance(t.Name(), 10*1024) // buffer is about 40MB
	bpm := shi.GetBufferPoolManager()
	sl := skip_list.NewSkipList(bpm, keyType)

	checkDupMap := make(map[T]T)

	// override global rand seed (seed has been set on NewSkipList)
	//rand.Seed(3)
	rand.Seed(int64(seedVal))

	//tmpSkipRand := seedVal
	//// skip random value series
	//for tmpSkipRand > 0 {
	//	rand.Int31()
	//	tmpSkipRand--
	//}

	insVals := make([]T, 0)
	removedValsForGetAndRemove := make(map[T]T, 0)
	removedValsForRemove := make(map[T]T, 0)

	// initial entries
	useInitialEntryNum := int(initialEntryNum)
	for ii := 0; ii < useInitialEntryNum; ii++ {
		// avoid duplication
		insValBase := samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		for _, exist := checkDupMap[insValBase]; exist; _, exist = checkDupMap[insValBase] {
			insValBase = samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		}
		checkDupMap[insValBase] = insValBase

		for ii := int32(0); ii < stride; ii++ {
			insVal := samehada_util.StrideAdd(samehada_util.StrideMul(insValBase, stride), ii)
			pairVal := samehada_util.GetValueForSkipListEntry(insVal)
			checkDupMap[insVal.(T)] = insVal.(T)
			common.ShPrintf(common.DEBUGGING, "Insert op start.")
			sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), uint64(pairVal))
			//fmt.Printf("sl.Insert at insertRandom: ii=%d, insValBase=%d len(*insVals)=%d\n", ii, insValBase, len(insVals))
		}

		insVals = append(insVals, insValBase)
	}

	insValsMutex := new(sync.RWMutex)
	removedValsForGetMutex := new(sync.RWMutex)
	removedValsForRemoveMutex := new(sync.RWMutex)
	checkDupMapMutex := new(sync.RWMutex)

	ch := make(chan int32)

	useOpTimes := int(opTimes)
	runningThCnt := 0
	for ii := 0; ii <= useOpTimes; ii++ {
		// wait last go routines finishes
		if ii == useOpTimes {
			for runningThCnt > 0 {
				<-ch
				runningThCnt--
				common.ShPrintf(common.DEBUGGING, "runningThCnt=%d\n", runningThCnt)
			}
			break
		}

		// wait for keeping THREAD_NUM groroutine existing
		for runningThCnt >= THREAD_NUM {
			//for runningThCnt > 0 { // serial execution
			<-ch
			runningThCnt--

			common.ShPrintf(common.DEBUGGING, "runningThCnt=%d\n", runningThCnt)
		}
		common.ShPrintf(common.DEBUGGING, "ii=%d\n", ii)
		//runningThCnt = 0

		// get 0-3
		opType := rand.Intn(4)
		switch opType {
		case 0: // Insert
			go func() {
				//checkDupMapMutex.RLock()
				checkDupMapMutex.Lock()
				insValBase := samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
				for _, exist := checkDupMap[insValBase]; exist; _, exist = checkDupMap[insValBase] {
					insValBase = samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
				}
				//checkDupMapMutex.RUnlock()
				//checkDupMapMutex.Lock()
				checkDupMap[insValBase] = insValBase
				checkDupMapMutex.Unlock()

				for ii := int32(0); ii < stride; ii++ {
					insVal := samehada_util.StrideAdd(samehada_util.StrideMul(insValBase, stride), ii)
					pairVal := samehada_util.GetValueForSkipListEntry(insVal)

					checkDupMapMutex.Lock()
					checkDupMap[insVal.(T)] = insVal.(T)
					checkDupMapMutex.Unlock()

					common.ShPrintf(common.DEBUGGING, "Insert op start.")
					sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), uint64(pairVal))
					//fmt.Printf("sl.Insert at insertRandom: ii=%d, insValBase=%d len(*insVals)=%d\n", ii, insValBase, len(insVals))
				}
				insValsMutex.Lock()
				insVals = append(insVals, insValBase)
				insValsMutex.Unlock()
				ch <- 1
			}()
		case 1, 2: // Delete
			// get 0-1 value
			tmpRand := rand.Intn(2)
			if tmpRand == 0 {
				// 50% is Remove to not existing entry
				go func() {
					removedValsForRemoveMutex.RLock()
					if len(removedValsForRemove) == 0 {
						removedValsForRemoveMutex.RUnlock()
						ch <- 1
						//continue
						return
					}
					removedValsForRemoveMutex.RUnlock()

					for ii := int32(0); ii < stride; ii++ {
						removedValsForRemoveMutex.RLock()
						delVal := samehada_util.ChoiceValFromMap(removedValsForRemove)
						removedValsForRemoveMutex.RUnlock()

						common.ShPrintf(common.DEBUGGING, "Remove(fail) op start.")
						isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(delVal)), samehada_util.GetValueForSkipListEntry(delVal))
						common.SH_Assert(isDeleted == false, "delete should be fail!")
					}
					ch <- 1
				}()
			} else {
				// 50% is Remove to existing entry
				go func() {
					insValsMutex.Lock()
					if len(insVals)-1 < 0 {
						insValsMutex.Unlock()
						ch <- 1
						//continue
						return
					}
					tmpIdx := int(rand.Intn(len(insVals)))
					delValBase := insVals[tmpIdx]
					if len(insVals) == 1 {
						// make empty
						insVals = make([]T, 0)
					} else if len(insVals)-1 == tmpIdx {
						insVals = insVals[:len(insVals)-1]
					} else {
						insVals = append(insVals[:tmpIdx], insVals[tmpIdx+1:]...)
					}
					insValsMutex.Unlock()

					for ii := int32(0); ii < stride; ii++ {
						delVal := samehada_util.StrideAdd(samehada_util.StrideMul(delValBase, stride), ii).(T)
						pairVal := samehada_util.GetValueForSkipListEntry(delVal)
						common.ShPrintf(common.DEBUGGING, "Remove(success) op start.")

						// append to map before doing remove op for other get op thread
						removedValsForGetMutex.Lock()
						removedValsForGetAndRemove[delVal] = delVal
						removedValsForGetMutex.Unlock()

						isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(delVal)), uint64(pairVal))
						if isDeleted == true {
							// append to map after doing remove op for other fail remove op thread
							removedValsForRemoveMutex.Lock()
							removedValsForRemove[delVal] = delVal
							removedValsForRemoveMutex.Unlock()

						} else {
							removedValsForGetMutex.RLock()
							if _, ok := removedValsForGetAndRemove[delVal]; !ok {
								removedValsForGetMutex.RUnlock()
								panic("remove op test failed!")
							}
							removedValsForGetMutex.RUnlock()
							//panic("remove op test failed!")
						}
					}
					ch <- 1
					//common.SH_Assert(isDeleted == true, "remove should be success!")
				}()
			}
		case 3: // Get
			go func() {
				insValsMutex.RLock()
				if len(insVals) == 0 {
					insValsMutex.RUnlock()
					ch <- 1
					//continue
					return
				}
				tmpIdx := int(rand.Intn(len(insVals)))
				//fmt.Printf("sl.GetValue at testSkipListMix: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*removedValsForGetAndRemove)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(removedValsForGetAndRemove))
				getTgtBase := insVals[tmpIdx]
				insValsMutex.RUnlock()
				for ii := int32(0); ii < stride; ii++ {
					getTgt := samehada_util.StrideAdd(samehada_util.StrideMul(getTgtBase, stride), ii).(T)
					getTgtVal := types.NewValue(getTgt)
					correctVal := samehada_util.GetValueForSkipListEntry(getTgt)

					common.ShPrintf(common.DEBUGGING, "Get op start.")
					gotVal := sl.GetValue(&getTgtVal)
					if gotVal == math.MaxUint64 {
						removedValsForGetMutex.RLock()
						if _, ok := removedValsForGetAndRemove[getTgt]; !ok {
							removedValsForGetMutex.RUnlock()
							panic("get op test failed!")
						}
						removedValsForGetMutex.RUnlock()
					} else if gotVal != correctVal {
						panic("returned value of get of is wrong!")
					}
				}
				ch <- 1
				//common.SH_Assert(, "gotVal is not collect!")
			}()
		}
		runningThCnt++
	}
	shi.CloseFilesForTesting()
}

func testSkipListMixParallelStrideAddedIterator[T int32 | float32 | string](t *testing.T, keyType types.TypeID, stride int32, opTimes int32, seedVal int32, initialEntryNum int32, bpoolSize int32) {
	common.ShPrintf(common.DEBUG_INFO, "start of testSkipListMixParallelStride stride=%d opTimes=%d seedVal=%d initialEntryNum=%d ====================================================\n",
		stride, opTimes, seedVal, initialEntryNum)

	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	const THREAD_NUM = 20

	shi := samehada.NewSamehadaInstance(t.Name(), int(bpoolSize))
	//shi := samehada.NewSamehadaInstance(t.Name(), 30)
	//shi := samehada.NewSamehadaInstance(t.Name(), 60)

	//shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	//shi := samehada.NewSamehadaInstance(t.Name(), 10*1024) // buffer is about 40MB
	bpm := shi.GetBufferPoolManager()
	sl := skip_list.NewSkipList(bpm, keyType)

	checkDupMap := make(map[T]T)

	// override global rand seed (seed has been set on NewSkipList)
	//rand.Seed(3)
	rand.Seed(int64(seedVal))

	//tmpSkipRand := seedVal
	//// skip random value series
	//for tmpSkipRand > 0 {
	//	rand.Int31()
	//	tmpSkipRand--
	//}

	insVals := make([]T, 0)
	removedValsForGetAndRemove := make(map[T]T, 0)
	removedValsForRemove := make(map[T]T, 0)

	// initial entries
	useInitialEntryNum := int(initialEntryNum)
	for ii := 0; ii < useInitialEntryNum; ii++ {
		// avoid duplication
		insValBase := samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		for _, exist := checkDupMap[insValBase]; exist; _, exist = checkDupMap[insValBase] {
			insValBase = samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
		}
		checkDupMap[insValBase] = insValBase

		for ii := int32(0); ii < stride; ii++ {
			insVal := samehada_util.StrideAdd(samehada_util.StrideMul(insValBase, stride), ii)
			pairVal := samehada_util.GetValueForSkipListEntry(insVal)

			common.ShPrintf(common.DEBUGGING, "Insert op start.")
			sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), uint64(pairVal))
			//fmt.Printf("sl.Insert at insertRandom: ii=%d, insValBase=%d len(*insVals)=%d\n", ii, insValBase, len(insVals))
		}

		insVals = append(insVals, insValBase)
	}

	insValsMutex := new(sync.RWMutex)
	removedValsForGetMutex := new(sync.RWMutex)
	removedValsForRemoveMutex := new(sync.RWMutex)
	checkDupMapMutex := new(sync.RWMutex)

	ch := make(chan int32)

	useOpTimes := int(opTimes)
	runningThCnt := 0
	for ii := 0; ii <= useOpTimes; ii++ {
		// wait last go routines finishes
		if ii == useOpTimes {
			for runningThCnt > 0 {
				<-ch
				runningThCnt--
				common.ShPrintf(common.DEBUGGING, "runningThCnt=%d\n", runningThCnt)
			}
			break
		}

		// wait for keeping THREAD_NUM groroutine existing
		for runningThCnt >= THREAD_NUM {
			//for runningThCnt > 0 { // serial execution
			<-ch
			runningThCnt--

			common.ShPrintf(common.DEBUGGING, "runningThCnt=%d\n", runningThCnt)
		}
		common.ShPrintf(common.DEBUGGING, "ii=%d\n", ii)
		//runningThCnt = 0

		// get 0-4
		opType := rand.Intn(5)
		switch opType {
		case 0: // Insert
			go func() {
				//checkDupMapMutex.RLock()
				checkDupMapMutex.RLock()
				insValBase := samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
				for _, exist := checkDupMap[insValBase]; exist; _, exist = checkDupMap[insValBase] {
					insValBase = samehada_util.GetRandomPrimitiveVal[T](keyType, nil)
				}
				checkDupMapMutex.RUnlock()
				checkDupMapMutex.Lock()
				checkDupMap[insValBase] = insValBase
				checkDupMapMutex.Unlock()

				for ii := int32(0); ii < stride; ii++ {
					insVal := samehada_util.StrideAdd(samehada_util.StrideMul(insValBase, stride), ii)
					pairVal := samehada_util.GetValueForSkipListEntry(insVal)

					common.ShPrintf(common.DEBUGGING, "Insert op start.")
					sl.Insert(samehada_util.GetPonterOfValue(types.NewValue(insVal)), uint64(pairVal))
					//fmt.Printf("sl.Insert at insertRandom: ii=%d, insValBase=%d len(*insVals)=%d\n", ii, insValBase, len(insVals))
				}
				insValsMutex.Lock()
				insVals = append(insVals, insValBase)
				insValsMutex.Unlock()
				ch <- 1
			}()
		case 1, 2: // Delete
			// get 0-1 value
			tmpRand := rand.Intn(2)
			if tmpRand == 0 {
				// 50% is Remove to not existing entry
				go func() {
					removedValsForRemoveMutex.RLock()
					if len(removedValsForRemove) == 0 {
						removedValsForRemoveMutex.RUnlock()
						ch <- 1
						//continue
						return
					}
					removedValsForRemoveMutex.RUnlock()

					for ii := int32(0); ii < stride; ii++ {
						removedValsForRemoveMutex.RLock()
						delVal := samehada_util.ChoiceValFromMap(removedValsForRemove)
						removedValsForRemoveMutex.RUnlock()

						common.ShPrintf(common.DEBUGGING, "Remove(fail) op start.")
						isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(delVal)), samehada_util.GetValueForSkipListEntry(delVal))
						common.SH_Assert(isDeleted == false, "delete should be fail!")
					}
					ch <- 1
				}()
			} else {
				// 50% is Remove to existing entry
				go func() {
					insValsMutex.Lock()
					if len(insVals)-1 < 0 {
						insValsMutex.Unlock()
						ch <- 1
						//continue
						return
					}
					tmpIdx := int(rand.Intn(len(insVals)))
					delValBase := insVals[tmpIdx]
					if len(insVals) == 1 {
						// make empty
						insVals = make([]T, 0)
					} else if len(insVals)-1 == tmpIdx {
						insVals = insVals[:len(insVals)-1]
					} else {
						insVals = append(insVals[:tmpIdx], insVals[tmpIdx+1:]...)
					}
					insValsMutex.Unlock()

					for ii := int32(0); ii < stride; ii++ {
						delVal := samehada_util.StrideAdd(samehada_util.StrideMul(delValBase, stride), ii).(T)
						pairVal := samehada_util.GetValueForSkipListEntry(delVal)
						common.ShPrintf(common.DEBUGGING, "Remove(success) op start.")

						// append to map before doing remove op for other get op thread
						removedValsForGetMutex.Lock()
						removedValsForGetAndRemove[delVal] = delVal
						removedValsForGetMutex.Unlock()

						isDeleted := sl.Remove(samehada_util.GetPonterOfValue(types.NewValue(delVal)), uint64(pairVal))
						if isDeleted == true {
							// append to map after doing remove op for other fail remove op thread
							removedValsForRemoveMutex.Lock()
							removedValsForRemove[delVal] = delVal
							removedValsForRemoveMutex.Unlock()

						} else {
							removedValsForGetMutex.RLock()
							if _, ok := removedValsForGetAndRemove[delVal]; !ok {
								removedValsForGetMutex.RUnlock()
								panic("remove op test failed!")
							}
							removedValsForGetMutex.RUnlock()
							//panic("remove op test failed!")
						}
					}
					ch <- 1
					//common.SH_Assert(isDeleted == true, "remove should be success!")
				}()
			}
		case 3: // Get
			go func() {
				insValsMutex.RLock()
				if len(insVals) == 0 {
					insValsMutex.RUnlock()
					ch <- 1
					//continue
					return
				}
				tmpIdx := int(rand.Intn(len(insVals)))
				//fmt.Printf("sl.GetValue at testSkipListMix: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*removedValsForGetAndRemove)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(removedValsForGetAndRemove))
				getTgtBase := insVals[tmpIdx]
				insValsMutex.RUnlock()
				for ii := int32(0); ii < stride; ii++ {
					getTgt := samehada_util.StrideAdd(samehada_util.StrideMul(getTgtBase, stride), ii).(T)
					getTgtVal := types.NewValue(getTgt)
					correctVal := samehada_util.GetValueForSkipListEntry(getTgt)

					common.ShPrintf(common.DEBUGGING, "Get op start.")
					gotVal := sl.GetValue(&getTgtVal)
					if gotVal == math.MaxUint64 {
						removedValsForGetMutex.RLock()
						if _, ok := removedValsForGetAndRemove[getTgt]; !ok {
							removedValsForGetMutex.RUnlock()
							panic("get op test failed!")
						}
						removedValsForGetMutex.RUnlock()
					} else if gotVal != correctVal {
						panic("returned value of get of is wrong!")
					}
				}
				ch <- 1
				//common.SH_Assert(, "gotVal is not collect!")
			}()
		case 4: //GetRangeScanIterator
			go func() {
				insValsMutex.RLock()
				if len(insVals) == 0 {
					insValsMutex.RUnlock()
					ch <- 1
					//continue
					return
				}
				tmpIdx := int(rand.Intn(len(insVals)))
				//fmt.Printf("sl.GetValue at testSkipListMix: ii=%d, tmpIdx=%d insVals[tmpIdx]=%d len(*insVals)=%d len(*removedValsForGetAndRemove)=%d\n", ii, tmpIdx, insVals[tmpIdx], len(insVals), len(removedValsForGetAndRemove))
				rangeStartBase := insVals[tmpIdx]
				insValsMutex.RUnlock()
				rangeStartVal := types.NewValue(rangeStartBase)
				rangeEndBase := samehada_util.StrideAdd(rangeStartBase, stride).(T)
				rangeEndVal := types.NewValue(rangeEndBase)
				itr := sl.Iterator(&rangeStartVal, &rangeEndVal)
				for done, _, _, _ := itr.Next(); !done; done, _, _, _ = itr.Next() {
				}

				ch <- 1
			}()
		}
		runningThCnt++
	}
	shi.CloseFilesForTesting()
}

func testSkipListMixRoot[T int32 | float32 | string](t *testing.T, keyType types.TypeID) {
	//if !common.EnableOnMemStorage {
	//	os.Remove(t.Name() + ".db")
	//	os.Remove(t.Name() + ".log")
	//}
	//
	//shi := samehada.NewSamehadaInstance(t.Name(), 10)
	////shi := samehada.NewSamehadaInstance(t.Name(), common.BufferPoolMaxFrameNumForTest)
	////shi := samehada.NewSamehadaInstance(t.Name(), 10*1024) // buffer is about 40MB
	//bpm := shi.GetBufferPoolManager()

	testSkipListMix[T](t, keyType, 1, int32(150), int32(10), int32(0), false)
	testSkipListMix[T](t, keyType, 1, int32(150), int32(10), int32(300), false)
	testSkipListMix[T](t, keyType, 1, int32(150), int32(10), int32(600), false)
	testSkipListMix[T](t, keyType, 1, int32(200), int32(5), int32(10), false)
	testSkipListMix[T](t, keyType, 1, int32(250), int32(5), int32(10), false)
	testSkipListMix[T](t, keyType, 1, int32(250), int32(4), int32(0), false)
	testSkipListMix[T](t, keyType, 1, int32(250), int32(3), int32(0), false)

	//testSkipListMix[T](t, keyType, 50, int32(150), int32(10), int32(0), false)
	//testSkipListMix[T](t, keyType, 50, int32(150), int32(10), int32(300), false)
	//testSkipListMix[T](t, keyType, 50, int32(150), int32(10), int32(600), false)
	//testSkipListMix[T](t, keyType, 50, int32(200), int32(5), int32(10), false)
	testSkipListMix[T](t, keyType, 50, int32(250), int32(5), int32(10), false)
	testSkipListMix[T](t, keyType, 50, int32(250), int32(4), int32(0), false)
	testSkipListMix[T](t, keyType, 50, int32(250), int32(3), int32(0), false)

	//testSkipListMix[T](t, keyType, 100, int32(150), int32(10), int32(0), false)
	//testSkipListMix[T](t, keyType, 100, int32(150), int32(10), int32(300), false)
	//testSkipListMix[T](t, keyType, 100, int32(150), int32(10), int32(600), false)
	//testSkipListMix[T](t, keyType, 100, int32(200), int32(5), int32(10), false)
	//testSkipListMix[T](t, keyType, 100, int32(250), int32(5), int32(10), false)
	//testSkipListMix[T](t, keyType, 100, int32(250), int32(4), int32(0), false)
	//testSkipListMix[T](t, keyType, 100, int32(250), int32(3), int32(0), false)

	////shi.Shutdown(true)
	//shi.CloseFilesForTesting()
}

func testSkipListMixParallelRoot[T int32 | float32 | string](t *testing.T, keyType types.TypeID) {
	// 4th arg should be multiple of 20
	testSkipListMixParallel[T](t, keyType, int32(200000), int32(10), int32(1000))
	testSkipListMixParallel[T](t, keyType, int32(200000), int32(11), int32(1000))
	testSkipListMixParallel[T](t, keyType, int32(200000), int32(12), int32(1000))
	testSkipListMixParallel[T](t, keyType, int32(200000), int32(13), int32(1000))

	fmt.Println("test finished.")

	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(150), int32(10), int32(300))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(150), int32(10), int32(600))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(200), int32(5), int32(10))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(250), int32(5), int32(10))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(250), int32(4), int32(0))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(250), int32(3), int32(0))
}

func testSkipListMixParallelBulkRoot[T int32 | float32 | string](t *testing.T, keyType types.TypeID) {
	// 4th arg should be multiple of 20
	testSkipListMixParallelBulk[T](t, keyType, 200, 1000, 11, 800)
	//testSkipListMixParallel[T](t, keyType, int32(200000), int32(11), int32(1000))
	//testSkipListMixParallel[T](t, keyType, int32(200000), int32(12), int32(1000))
	//testSkipListMixParallel[T](t, keyType, int32(200000), int32(13), int32(1000))

	fmt.Println("test finished.")

	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(150), int32(10), int32(300))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(150), int32(10), int32(600))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(200), int32(5), int32(10))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(250), int32(5), int32(10))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(250), int32(4), int32(0))
	//testSkipListMixParallel[T](t, sl, keyType, 100, int32(250), int32(3), int32(0))
}

func testSkipListMixParallelStrideRoot[T int32 | float32 | string](t *testing.T, keyType types.TypeID) {
	bpoolSize := int32(500)

	if keyType == types.Float {
		// check of Float case is soft

		//testSkipListMixParallelStride[T](t, keyType, 20, 1000, 12, 0, bpoolSize)
		testSkipListMixParallelStride[T](t, keyType, 240, 1000, 12, 0, bpoolSize)
		fmt.Println("test finished 0/5.")
	} else {
		// 4th arg should be multiple of 20
		testSkipListMixParallelStride[T](t, keyType, 800, 1000, 12, 800, bpoolSize)
		fmt.Println("test finished 1/5.")
		testSkipListMixParallelStride[T](t, keyType, 1, 100000, 12, 800, bpoolSize)
		fmt.Println("test finished 2/5.")
		testSkipListMixParallelStride[T](t, keyType, 300, 1000, 14, 800, bpoolSize)
		fmt.Println("test finished 3/5.")
		testSkipListMixParallelStride[T](t, keyType, 300, 1000, 15, 0, bpoolSize)
		fmt.Println("test finished 4/5.")
		testSkipListMixParallelStride[T](t, keyType, 8, 100000, 13, 200, bpoolSize)
		fmt.Println("test finished 5/5.")
	}
}

func testSkipListMixParallelStrideAddedIteratorRoot[T int32 | float32 | string](t *testing.T, keyType types.TypeID) {
	bpoolSize := int32(500)

	// 4th arg should be multiple of 20
	testSkipListMixParallelStrideAddedIterator[T](t, keyType, 800, 1000, 12, 800, bpoolSize)
	fmt.Println("test finished 1/5.")
	testSkipListMixParallelStrideAddedIterator[T](t, keyType, 1, 100000, 12, 800, bpoolSize)
	fmt.Println("test finished 2/5.")
	testSkipListMixParallelStrideAddedIterator[T](t, keyType, 300, 1000, 14, 800, bpoolSize)
	fmt.Println("test finished 3/5.")
	testSkipListMixParallelStrideAddedIterator[T](t, keyType, 300, 1000, 15, 0, bpoolSize)
	fmt.Println("test finished 4/5.")
	testSkipListMixParallelStrideAddedIterator[T](t, keyType, 8, 100000, 13, 200, bpoolSize)
	fmt.Println("test finished 5/5.")
}

/*
func TestSkipListMixInteger(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testSkipListMixRoot[int32](t, types.Integer)
}

func TestSkipListMixFloat(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testSkipListMixRoot[float32](t, types.Float)
}

func TestSkipListMixVarchar(t *testing.T) {
	t.Parallel()
	testSkipListMixRoot[string](t, types.Varchar)
}

func TestSkipListMixParallelInteger(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testSkipListMixParallelRoot[int32](t, types.Integer)
}

func TestSkipListMixParallelVarchar(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testSkipListMixParallelRoot[string](t, types.Varchar)
}

func TestSkipListMixParallelBulkInteger(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testSkipListMixParallelBulkRoot[int32](t, types.Integer)
}

func TestSkipListMixParallelBulkVarchar(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testSkipListMixParallelBulkRoot[string](t, types.Varchar)
}

func TestSkipListMixParallelStrideInteger(t *testing.T) {
	//t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testSkipListMixParallelStrideRoot[int32](t, types.Integer)
}
*/

func TestSkipListMixParallelStrideFloat(t *testing.T) {
	//t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testSkipListMixParallelStrideRoot[float32](t, types.Float)
}

/*
func TestSkipListMixParallelStrideVarchar(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testSkipListMixParallelStrideRoot[string](t, types.Varchar)
}

//func TestSkipListMixParallsStrideVarcharLongRun(t *testing.T) {
//	//t.Parallel()
//	if testing.Short() {
//		t.Skip("skip this in short mode.")
//	}
//	// assumed running time is about 12h
//	testSkipListMixParallelStride[string](t, types.Varchar, 300, 3000*12, 15, 0, 500)
//}

func TestSkipListMixParallelStrideAddedIteratorInteger(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testSkipListMixParallelStrideAddedIteratorRoot[int32](t, types.Integer)
}

func TestSkipListMixParallelStrideAddedIteratorVarchar(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	testSkipListMixParallelStrideAddedIteratorRoot[string](t, types.Varchar)
}

//// 24h running had been succeeded at 220923
//// \SamehadaDB\container\skip_list\skip_list_test>go test . -race -timeout 24h -v 2>&1 > fuzzlikelog-parallel2.txt
//func TestSkipListMixParallsStrideVarcharLikeFuzzer(t *testing.T) {
//	//t.Parallel()
//	if testing.Short() {
//		t.Skip("skip this in short mode.")
//	}
//
//	runtime.GOMAXPROCS(50)
//
//	const THREAD_NUM = 15
//
//	startTime := time.Now()
//
//	ch := make(chan int32)
//	finishedCase := 0
//	runningThCnt := 0
//	for {
//		// wait for keeping THREAD_NUM groroutine existing
//		for runningThCnt >= THREAD_NUM {
//			//for runningThCnt > 0 { // serial execution
//			<-ch
//			runningThCnt--
//			finishedCase++
//		}
//
//		if finishedCase%10 == 0 {
//			d := time.Since(startTime)
//			fmt.Printf("elapse %v: %d case executed\n", d, finishedCase)
//		}
//
//		stride := 1 + (rand.Uint32() % 60)
//		opTimes := 1 + (rand.Uint32() % 200)
//		seedVal := rand.Uint32() % 1000
//		initialEntryNum := 1 + (rand.Uint32() % 20)
//
//		//fmt.Printf("%d %d %d %d\n", stride, opTimes, seedVal, initialEntryNum)
//		go func(stride_ int32, opTimes_ int32, seedVal_ int32, initialEntryNum_ int32) {
//
//			testSkipListMixParallelStride[string](t, types.Varchar, stride_, opTimes_, seedVal_, initialEntryNum_, 500)
//			ch <- 1
//		}(int32(stride), int32(opTimes), int32(seedVal), int32(initialEntryNum))
//
//		runningThCnt++
//	}
//}

func testSkipListInsertGetEven(t *testing.T, sl *skip_list.SkipList, ch chan string) {
	for ii := int32(0); ii < 10000; ii = ii + 2 {
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(ii)), uint64(ii))
		gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(ii)))
		if gotVal == math.MaxUint64 {
			t.Fail()
			fmt.Printf("value %d is not found!\n", ii)
			panic("inserted value not found!")
		}
	}
	fmt.Println("even finished.")
	ch <- ""
}

func testSkipListInsertGetOdd(t *testing.T, sl *skip_list.SkipList, ch chan string) {
	for ii := int32(1); ii < 10000; ii = ii + 2 {
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(ii)), uint64(ii))
		gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(ii)))
		if gotVal == math.MaxUint64 {
			fmt.Printf("value %d is not found!\n", ii)
		}
	}
	fmt.Println("odd finished.")
	ch <- ""
}

func testSkipListInsertGetEvenSeparate(t *testing.T, sl *skip_list.SkipList, ch chan string) {
	for ii := int32(0); ii < 100000; ii = ii + 2 {
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(ii)), uint64(ii))
	}
	for ii := int32(0); ii < 100000; ii = ii + 2 {
		gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(ii)))
		if gotVal == math.MaxUint64 {
			t.Fail()
			fmt.Printf("value %d is not found!\n", ii)
			panic("inserted value not found!")
		}
	}
	fmt.Println("even finished.")
	ch <- ""
}

func testSkipListInsertGetOddSeparate(t *testing.T, sl *skip_list.SkipList, ch chan string) {
	for ii := int32(1); ii < 100000; ii = ii + 2 {
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(ii)), uint64(ii))
	}

	for ii := int32(1); ii < 100000; ii = ii + 2 {
		gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(ii)))
		if gotVal == math.MaxUint64 {
			fmt.Printf("value %d is not found!\n", ii)
		}
	}

	fmt.Println("odd finished.")
	ch <- ""
}

func testSkipListInsertGetInsert3stride1and3(t *testing.T, sl *skip_list.SkipList, ch chan string) {
	// insert 012345678...
	//        ^  ^  ^
	for ii := int32(0); ii < 10000; ii++ {
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(3*ii)), uint64(3*ii))
		gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(3 * ii)))
		if gotVal == math.MaxUint64 {
			fmt.Printf("value %d is not found!\n", ii)
		}
	}
	// insert 012345678...
	//          ^  ^  ^
	for ii := int32(0); ii < 10000; ii++ {
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(3*ii+2)), uint64(3*ii+2))
		gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(3*ii + 2)))
		if gotVal == math.MaxUint64 {
			fmt.Printf("value %d is not found!\n", ii)
		}
	}
	fmt.Println("1and3 finished.")
	ch <- ""
}

func testSkipListInsertGetRemove3stride2(t *testing.T, sl *skip_list.SkipList, ch chan string) {
	// insert 012345678...
	//         ^  ^  ^
	for ii := int32(0); ii < 10000; ii++ {
		sl.Insert(samehada_util.GetPonterOfValue(types.NewInteger(3*ii+1)), uint64(3*ii+1))
		gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(3*ii + 1)))
		if gotVal == math.MaxUint64 {
			fmt.Printf("value %d is not found!\n", ii)
			panic("inserted value not found!")
		}
	}
	// remove 012345678... from tail
	//        ^^ ^^ ^^
	for ii := int32(10000 - 1); ii >= 0; ii-- {
		sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(3*ii+1)), uint64(3*ii+1))
		gotVal := sl.GetValue(samehada_util.GetPonterOfValue(types.NewInteger(3*ii + 1)))
		if gotVal != math.MaxUint64 {
			fmt.Printf("value %d should be not found!\n", 3*ii+1)
			panic("remove should be failed!")
		}
		sl.Remove(samehada_util.GetPonterOfValue(types.NewInteger(3*ii)), uint64(3*ii))
		// no check because another thread have not finished insert
	}

	fmt.Println("2 finished.")
	ch <- ""
}

func TestSkipListParallelSimpleInteger(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}

	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	//shi := samehada.NewSamehadaInstance(t.Name(), 400)
	shi := samehada.NewSamehadaInstance(t.Name(), 30)
	bpm := shi.GetBufferPoolManager()
	sl := skip_list.NewSkipList(bpm, types.Integer)

	ch1 := make(chan string)
	ch2 := make(chan string)

	go testSkipListInsertGetEven(t, sl, ch1)
	go testSkipListInsertGetOdd(t, sl, ch2)

	ch1Ret := <-ch1
	t.Logf("%s\n", ch1Ret)
	t.Logf("ch1 received\n")
	ch2Ret := <-ch2
	t.Logf("%s\n", ch2Ret)
	t.Logf("ch2 received\n")

	shi.CloseFilesForTesting()
}

func TestSkipListParallelSimpleInteger2(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	//shi := samehada.NewSamehadaInstance(t.Name(), 400)
	shi := samehada.NewSamehadaInstance(t.Name(), 30)
	bpm := shi.GetBufferPoolManager()
	sl := skip_list.NewSkipList(bpm, types.Integer)

	ch1 := make(chan string)
	ch2 := make(chan string)

	go testSkipListInsertGetEvenSeparate(t, sl, ch1)
	go testSkipListInsertGetOddSeparate(t, sl, ch2)

	ch1Ret := <-ch1
	t.Logf("%s\n", ch1Ret)
	t.Logf("ch1 received\n")
	ch2Ret := <-ch2
	t.Logf("%s\n", ch2Ret)
	t.Logf("ch2 received\n")

	shi.CloseFilesForTesting()
}

func TestSkipListParallelSimpleInteger3Stride(t *testing.T) {
	t.Parallel()
	if testing.Short() {
		t.Skip("skip this in short mode.")
	}

	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	//shi := samehada.NewSamehadaInstance(t.Name(), 400)
	shi := samehada.NewSamehadaInstance(t.Name(), 30)
	bpm := shi.GetBufferPoolManager()
	sl := skip_list.NewSkipList(bpm, types.Integer)

	ch1 := make(chan string)
	ch2 := make(chan string)

	go testSkipListInsertGetInsert3stride1and3(t, sl, ch1)
	go testSkipListInsertGetRemove3stride2(t, sl, ch2)

	//wg.Wait()
	ch1Ret := <-ch1
	t.Logf("%s\n", ch1Ret)
	t.Logf("ch1 received\n")
	ch2Ret := <-ch2
	t.Logf("%s\n", ch2Ret)
	t.Logf("ch2 received\n")

	shi.CloseFilesForTesting()
}
*/
