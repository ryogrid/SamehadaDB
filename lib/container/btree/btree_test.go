package btree

import (
	"bytes"
	"encoding/binary"
	"fmt"
	blink_tree "github.com/ryogrid/bltree-go-for-embedding"
	"math/rand"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ryogrid/SamehadaDB/lib/recovery"
	"github.com/ryogrid/SamehadaDB/lib/storage/buffer"
	"github.com/ryogrid/SamehadaDB/lib/storage/disk"
)

func TestBLTree_insert_and_find_embedding(t *testing.T) {
	poolSize := uint32(10)

	dm := disk.NewDiskManagerTest()
	lmgr := recovery.NewLogManager(&dm)
	bpm := NewParentBufMgrImpl(buffer.NewBufferPoolManager(poolSize, dm, lmgr))

	mgr := blink_tree.NewBufMgr(12, 20, bpm, nil)
	bltree := blink_tree.NewBLTree(mgr)
	if valLen, _, _ := bltree.FindKey([]byte{1, 1, 1, 1}, blink_tree.BtId); valLen >= 0 {
		t.Errorf("FindKey() = %v, want %v", valLen, -1)
	}

	if err := bltree.InsertKey([]byte{1, 1, 1, 1}, 0, [blink_tree.BtId]byte{0, 0, 0, 0, 0, 1}, true); err != blink_tree.BLTErrOk {
		t.Errorf("InsertKey() = %v, want %v", err, blink_tree.BLTErrOk)
	}

	_, foundKey, _ := bltree.FindKey([]byte{1, 1, 1, 1}, blink_tree.BtId)
	if bytes.Compare(foundKey, []byte{1, 1, 1, 1}) != 0 {
		t.Errorf("FindKey() = %v, want %v", foundKey, []byte{1, 1, 1, 1})
	}
}

func TestBLTree_insert_and_find_many_embedding(t *testing.T) {
	poolSize := uint32(100)

	dm := disk.NewDiskManagerTest()
	lmgr := recovery.NewLogManager(&dm)
	bpm := NewParentBufMgrImpl(buffer.NewBufferPoolManager(poolSize, dm, lmgr))

	mgr := blink_tree.NewBufMgr(12, 36, bpm, nil)
	bltree := blink_tree.NewBLTree(mgr)

	num := uint64(160000)

	for i := uint64(0); i < num; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.InsertKey(bs, 0, [blink_tree.BtId]byte{}, true); err != blink_tree.BLTErrOk {
			t.Errorf("InsertKey() = %v, want %v", err, blink_tree.BLTErrOk)
		}
	}

	for i := uint64(0); i < num; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if _, foundKey, _ := bltree.FindKey(bs, blink_tree.BtId); bytes.Compare(foundKey, bs) != 0 {
			t.Errorf("FindKey() = %v, want %v", foundKey, bs)
		}
	}
}

func TestBLTree_insert_and_find_concurrently_embedding(t *testing.T) {
	_ = os.Remove("TestBLTree_insert_and_find_concurrently_embedding.db")

	poolSize := uint32(300)

	//dm := disk.NewDiskManagerImpl("TestBLTree_insert_and_find_concurrently_embedding.db")
	dm := disk.NewVirtualDiskManagerImpl("TestBLTree_insert_and_find_concurrently_embedding.db")
	lmgr := recovery.NewLogManager(&dm)
	bpm := NewParentBufMgrImpl(buffer.NewBufferPoolManager(poolSize, dm, lmgr))

	mgr := blink_tree.NewBufMgr(12, blink_tree.HASH_TABLE_ENTRY_CHAIN_LEN*7, bpm, nil)

	keyTotal := 1600000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	blink_tree.InsertAndFindConcurrently(t, 7, mgr, keys)
}

func TestBLTree_deleteMany_embedding(t *testing.T) {
	poolSize := uint32(300)

	dm := disk.NewVirtualDiskManagerImpl("TestBLTree_deleteMany_embedding.db")
	lmgr := recovery.NewLogManager(&dm)
	bpm := NewParentBufMgrImpl(buffer.NewBufferPoolManager(poolSize, dm, lmgr))

	mgr := blink_tree.NewBufMgr(12, blink_tree.HASH_TABLE_ENTRY_CHAIN_LEN*7, bpm, nil)
	bltree := blink_tree.NewBLTree(mgr)

	keyTotal := 160000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	for i := range keys {
		if err := bltree.InsertKey(keys[i], 0, [blink_tree.BtId]byte{0, 0, 0, 0, 0, 0}, true); err != blink_tree.BLTErrOk {
			t.Errorf("InsertKey() = %v, want %v", err, blink_tree.BLTErrOk)
		}
		if i%2 == 0 {
			if err := bltree.DeleteKey(keys[i], 0); err != blink_tree.BLTErrOk {
				t.Errorf("DeleteKey() = %v, want %v", err, blink_tree.BLTErrOk)
			}
		}
	}

	for i := range keys {
		if i%2 == 0 {
			if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != -1 {
				t.Errorf("FindKey() = %v, want %v, key %v", found, -1, keys[i])
			}
		} else {
			if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != 6 {
				t.Errorf("FindKey() = %v, want %v, key %v", found, 6, keys[i])
			}
		}
	}
}

func TestBLTree_deleteAll_embedding(t *testing.T) {
	poolSize := uint32(300)

	dm := disk.NewVirtualDiskManagerImpl("TestBLTree_deleteAll_embedding.db")
	lmgr := recovery.NewLogManager(&dm)
	bpm := NewParentBufMgrImpl(buffer.NewBufferPoolManager(poolSize, dm, lmgr))
	mgr := blink_tree.NewBufMgr(12, blink_tree.HASH_TABLE_ENTRY_CHAIN_LEN*7, bpm, nil)
	bltree := blink_tree.NewBLTree(mgr)

	keyTotal := 1600000

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	for i := range keys {
		if err := bltree.InsertKey(keys[i], 0, [blink_tree.BtId]byte{0, 0, 0, 0, 0, 0}, true); err != blink_tree.BLTErrOk {
			t.Errorf("InsertKey() = %v, want %v", err, blink_tree.BLTErrOk)
		}
	}

	for i := range keys {
		if err := bltree.DeleteKey(keys[i], 0); err != blink_tree.BLTErrOk {
			t.Errorf("DeleteKey() = %v, want %v", err, blink_tree.BLTErrOk)
		}
		if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != -1 {
			t.Errorf("FindKey() = %v, want %v, key %v", found, -1, keys[i])
		}
	}
}

func TestBLTree_deleteManyConcurrently_embedding(t *testing.T) {
	_ = os.Remove("TestBLTree_deleteManyConcurrently_embedding.db")

	poolSize := uint32(300)

	dm := disk.NewVirtualDiskManagerImpl("TestBLTree_deleteManyConcurrently_embedding.db")
	lmgr := recovery.NewLogManager(&dm)
	bpm := NewParentBufMgrImpl(buffer.NewBufferPoolManager(poolSize, dm, lmgr))
	mgr := blink_tree.NewBufMgr(12, blink_tree.HASH_TABLE_ENTRY_CHAIN_LEN*16, bpm, nil)

	keyTotal := 1600000
	routineNum := 16 //7

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	wg := sync.WaitGroup{}
	wg.Add(routineNum)

	start := time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := blink_tree.NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if err := bltree.InsertKey(keys[i], 0, [blink_tree.BtId]byte{}, true); err != blink_tree.BLTErrOk {
					t.Errorf("in goroutine%d InsertKey() = %v, want %v", n, err, blink_tree.BLTErrOk)
				}

				if i%2 == (n % 2) {
					if err := bltree.DeleteKey(keys[i], 0); err != blink_tree.BLTErrOk {
						t.Errorf("DeleteKey() = %v, want %v", err, blink_tree.BLTErrOk)
					}
				}

				if i%2 == (n % 2) {
					if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != -1 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, -1, keys[i])
						panic("FindKey() != -1")
					}
				} else {
					if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != 6 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, 6, keys[i])
						panic("FindKey() != 6")
					}
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()
	t.Logf("insert %d keys and delete skip one concurrently. duration =  %v", keyTotal, time.Since(start))

	wg = sync.WaitGroup{}
	wg.Add(routineNum)

	start = time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := blink_tree.NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if i%2 == (n % 2) {
					if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != -1 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, -1, keys[i])
					}
				} else {
					if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != 6 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, 6, keys[i])
					}
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()

	t.Logf("find %d keys. duration = %v", keyTotal, time.Since(start))
}

func TestBLTree_deleteInsertRangeScanConcurrently_embedding(t *testing.T) {
	_ = os.Remove("TestBLTree_deleteInsertRangeScanConcurrently_embedding.db")

	poolSize := uint32(300)

	dm := disk.NewVirtualDiskManagerImpl("TestBLTree_deleteInsertRangeScanConcurrently_embedding.db")
	lmgr := recovery.NewLogManager(&dm)
	bpm := NewParentBufMgrImpl(buffer.NewBufferPoolManager(poolSize, dm, lmgr))
	mgr := blink_tree.NewBufMgr(12, blink_tree.HASH_TABLE_ENTRY_CHAIN_LEN*16, bpm, nil)

	keyTotal := 1600000
	routineNum := 16

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	wg := sync.WaitGroup{}
	wg.Add(routineNum)

	start := time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := blink_tree.NewBLTree(mgr)

			rangeScanCheck := func(startKey []byte) {
				//elemNum, keyArr, _ := bltree.RangeScan(startKey, nil)
				elemNum, keyArr, _ := bltree.RangeScan(nil, nil)
				if elemNum != len(keyArr) {
					panic("elemNum != len(keyArr)")
				}
				// check result keys are ordered
				curNum := uint64(0)
				keyInts := make([]uint64, 0)
				for idx := 0; idx < elemNum; idx++ {
					buf := bytes.NewBuffer(keyArr[idx])
					var foundKey uint64
					binary.Read(buf, binary.BigEndian, &foundKey)
					keyInts = append(keyInts, foundKey)
					if foundKey < curNum {
						panic("foundKey < curNum")
					}
					curNum = foundKey
				}
				//fmt.Println(keyInts)
			}

			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if err := bltree.InsertKey(keys[i], 0, [blink_tree.BtId]byte{}, true); err != blink_tree.BLTErrOk {
					t.Errorf("in goroutine%d InsertKey() = %v, want %v", n, err, blink_tree.BLTErrOk)
				}

				if i%2 == (n % 2) {
					if err := bltree.DeleteKey(keys[i], 0); err != blink_tree.BLTErrOk {
						t.Errorf("DeleteKey() = %v, want %v", err, blink_tree.BLTErrOk)
					}
				}

				if i%2 == (n % 2) {
					if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != -1 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, -1, keys[i])
						panic("FindKey() != -1")
					}
					rangeScanCheck(keys[i])
				} else {
					if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != 6 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, 6, keys[i])
						panic("FindKey() != 6")
					}
					rangeScanCheck(keys[i])
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()
	t.Logf("insert %d keys and delete skip one concurrently. duration =  %v", keyTotal, time.Since(start))

	wg = sync.WaitGroup{}
	wg.Add(routineNum)

	start = time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := blink_tree.NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if i%2 == (n % 2) {
					// find a entry or range scan
					if i%2 == 0 {
						if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != -1 {
							t.Errorf("FindKey() = %v, want %v, key %v", found, -1, keys[i])
						}
					}
				} else {
					if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != 6 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, 6, keys[i])
					}
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()

	t.Logf("find %d keys. duration = %v", keyTotal, time.Since(start))
}

func TestBLTree_deleteManyConcurrentlyShuffle_embedding(t *testing.T) {
	_ = os.Remove("TestBLTree_deleteManyConcurrently_shuffle_embedding.db")

	poolSize := uint32(300)

	dm := disk.NewVirtualDiskManagerImpl("TestBLTree_deleteManyConcurrently_shuffle_embedding.db")
	lmgr := recovery.NewLogManager(&dm)
	bpm := NewParentBufMgrImpl(buffer.NewBufferPoolManager(poolSize, dm, lmgr))
	mgr := blink_tree.NewBufMgr(12, blink_tree.HASH_TABLE_ENTRY_CHAIN_LEN*16, bpm, nil)

	keyTotal := 1600000
	routineNum := 16

	keys := make([][]byte, keyTotal)
	for i := 0; i < keyTotal; i++ {
		bs := make([]byte, 8)
		binary.LittleEndian.PutUint64(bs, uint64(i))
		keys[i] = bs
	}

	// shuffle keys
	randGen := rand.New(rand.NewSource(time.Now().UnixNano()))
	randGen.Shuffle(len(keys), func(i, j int) { keys[i], keys[j] = keys[j], keys[i] })

	wg := sync.WaitGroup{}
	wg.Add(routineNum)

	start := time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := blink_tree.NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if err := bltree.InsertKey(keys[i], 0, [blink_tree.BtId]byte{}, true); err != blink_tree.BLTErrOk {
					t.Errorf("in goroutine%d InsertKey() = %v, want %v", n, err, blink_tree.BLTErrOk)
				}

				if i%2 == (n % 2) {
					if err := bltree.DeleteKey(keys[i], 0); err != blink_tree.BLTErrOk {
						t.Errorf("DeleteKey() = %v, want %v", err, blink_tree.BLTErrOk)
					}
				}

				if i%2 == (n % 2) {
					if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != -1 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, -1, keys[i])
						panic("FindKey() != -1")
					}
				} else {
					if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != 6 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, 6, keys[i])
						panic("FindKey() != 6")
					}
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()
	t.Logf("insert %d keys and delete skip one concurrently. duration =  %v", keyTotal, time.Since(start))

	wg = sync.WaitGroup{}
	wg.Add(routineNum)

	start = time.Now()
	for r := 0; r < routineNum; r++ {
		go func(n int) {
			bltree := blink_tree.NewBLTree(mgr)
			for i := 0; i < keyTotal; i++ {
				if i%routineNum != n {
					continue
				}
				if i%2 == (n % 2) {
					if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != -1 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, -1, keys[i])
					}
				} else {
					if found, _, _ := bltree.FindKey(keys[i], blink_tree.BtId); found != 6 {
						t.Errorf("FindKey() = %v, want %v, key %v", found, 6, keys[i])
					}
				}
			}

			wg.Done()
		}(r)
	}
	wg.Wait()

	t.Logf("find %d keys. duration = %v", keyTotal, time.Since(start))
}

func TestBLTree_restart_embedding(t *testing.T) {
	_ = os.Remove("TestBLTree_restart_embedding.db")

	poolSize := uint32(100)

	// use virtual disk manager which does file I/O on memory
	dm := disk.NewVirtualDiskManagerImpl("TestBLTree_restart_embedding.db")
	lmgr := recovery.NewLogManager(&dm)
	orgBpm := buffer.NewBufferPoolManager(poolSize, dm, lmgr)
	bpm := NewParentBufMgrImpl(orgBpm)

	mgr := blink_tree.NewBufMgr(12, blink_tree.HASH_TABLE_ENTRY_CHAIN_LEN*2, bpm, nil)
	bltree := blink_tree.NewBLTree(mgr)

	firstNum := uint64(100000)

	for i := uint64(0); i <= firstNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.InsertKey(bs, 0, [blink_tree.BtId]byte{}, true); err != blink_tree.BLTErrOk {
			t.Errorf("InsertKey() = %v, want %v", err, blink_tree.BLTErrOk)
		}
	}

	// delete half of inserted keys
	for i := uint64(0); i < firstNum/2; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.DeleteKey(bs, 0); err != blink_tree.BLTErrOk {
			t.Errorf("InsertKey() = %v, want %v", err, blink_tree.BLTErrOk)
		}
	}

	// keep page ID mapping info on memory for testing
	idMappingsBeforeShutdown := mgr.GetPageIdConvMap()

	// shutdown BLTree
	// includes perpetuation of page ID mappings and free page IDs
	mgr.Close()

	// shutdown embedding db which own parent buffer manager of BufMgr
	pageZeroShId := mgr.GetMappedShPageIdOfPageZero()
	orgBpm.FlushAllPages()
	//dm.ShutDown()

	//dm = disk.NewDiskManagerImpl("TestBLTree_restart_embedding.db")
	lmgr = recovery.NewLogManager(&dm)
	bpm = NewParentBufMgrImpl(buffer.NewBufferPoolManager(poolSize, dm, lmgr))
	mgr = blink_tree.NewBufMgr(12, 48, bpm, &pageZeroShId)
	bltree = blink_tree.NewBLTree(mgr)

	secondNum := firstNum * 2

	idMappingReloaded := mgr.GetPageIdConvMap()

	idMappingCnt := 0
	// check reloaded pageID mapping info
	idMappingsBeforeShutdown.Range(func(key, value interface{}) bool {
		pageId := key.(blink_tree.Uid)
		if shPageId, ok := idMappingReloaded.Load(pageId); !ok {
			//fmt.Println("pageId mapping may be removed as freed page ID: ", pageId)
			idMappingCnt++
			return true
		} else if value.(int32) != shPageId.(int32) {
			t.Errorf("pageId mapping entry is broken.")
			return false
		}
		idMappingCnt++
		return true
	})
	fmt.Println("id mapping reloaded:", idMappingCnt)

	// check behavior of BLTree after relaunch
	for i := firstNum; i <= secondNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if err := bltree.InsertKey(bs, 0, [blink_tree.BtId]byte{}, true); err != blink_tree.BLTErrOk {
			t.Errorf("InsertKey() = %v, want %v", err, blink_tree.BLTErrOk)
		}
	}

	for i := firstNum / 2; i <= secondNum; i++ {
		bs := make([]byte, 8)
		binary.BigEndian.PutUint64(bs, i)
		if _, foundKey, _ := bltree.FindKey(bs, blink_tree.BtId); bytes.Compare(foundKey, bs) != 0 {
			t.Errorf("FindKey() = %v, want %v", foundKey, bs)
		}
	}
}

func TestBLTree_insert_and_range_scan_embedding(t *testing.T) {
	poolSize := uint32(10)

	dm := disk.NewDiskManagerTest()
	lmgr := recovery.NewLogManager(&dm)
	bpm := NewParentBufMgrImpl(buffer.NewBufferPoolManager(poolSize, dm, lmgr))

	mgr := blink_tree.NewBufMgr(12, 20, bpm, nil)
	bltree := blink_tree.NewBLTree(mgr)

	keyTotal := 10
	keys := make([][]byte, keyTotal)
	for i := 1; i < keyTotal; i++ {
		key := make([]byte, 8)
		binary.LittleEndian.PutUint64(key, uint64(i))
		keys[i-1] = key
	}

	// insert in shuffled order
	for i := 0; i < keyTotal-1; i++ {
		//key := keysRandom[i]
		key := keys[i]
		val := make([]byte, 4)
		binary.LittleEndian.PutUint32(val, uint32(i+1))
		if err := bltree.InsertKey(key, 0, [blink_tree.BtId]byte{val[0], val[1], val[2], val[3], 0, 1}, true); err != blink_tree.BLTErrOk {
			t.Errorf("InsertKey() = %v, want %v", err, blink_tree.BLTErrOk)
		}
	}

	_, _, _ = bltree.RangeScan(nil, nil)
	//num, keyArr, valArr := bltree.RangeScan(nil, nil)
	//fmt.Println(num, keyArr, valArr)
}
