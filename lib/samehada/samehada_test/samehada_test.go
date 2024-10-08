package samehada_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/lib/common"
	"github.com/ryogrid/SamehadaDB/lib/samehada"
	"github.com/ryogrid/SamehadaDB/lib/samehada/samehada_util"
	testingpkg "github.com/ryogrid/SamehadaDB/lib/testing/testing_assert"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"
)

// TODO: (SDB) need to check query result (TestInsertAndMultiItemPredicateSelect)
func TestInsertAndMultiItemPredicateSelect(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true

	// clear all state of DB
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage == true {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	db := samehada.NewSamehadaDB(t.Name(), 200)
	db.ExecuteSQLRetValues("CREATE TABLE name_age_list(name VARCHAR(256), age INT);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('鈴木', 20);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('青木', 22);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('山田', 25);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('加藤', 18);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('木村', 18);")
	_, results1 := db.ExecuteSQLRetValues("SELECT * FROM name_age_list WHERE age >= 20;")
	samehada_util.PrintExecuteResults(results1)
	_, results2 := db.ExecuteSQLRetValues("SELECT age FROM name_age_list WHERE age >= 20;")
	samehada_util.PrintExecuteResults(results2)
	_, results3 := db.ExecuteSQLRetValues("SELECT name, age FROM name_age_list WHERE age >= 20;")
	samehada_util.PrintExecuteResults(results3)
	_, results4 := db.ExecuteSQLRetValues("SELECT name, age FROM name_age_list WHERE age <= 23 AND age >= 20;")
	samehada_util.PrintExecuteResults(results4)
	_, results5 := db.ExecuteSQLRetValues("SELECT * FROM name_age_list WHERE (age = 18 OR age >= 22) AND age < 25;")
	samehada_util.PrintExecuteResults(results5)

	common.TempSuppressOnMemStorage = false
	db.Shutdown()
	common.TempSuppressOnMemStorageMutex.Unlock()
}

// TODO: (SDB) need to check query result (TestHasJoinSelect)
func TestHasJoinSelect(t *testing.T) {
	// clear all state of DB
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	db := samehada.NewSamehadaDB(t.Name(), 200)
	db.ExecuteSQLRetValues("CREATE TABLE id_name_list(id INT, name VARCHAR(256));")
	db.ExecuteSQLRetValues("INSERT INTO id_name_list(id, name) VALUES (1, '鈴木');")
	db.ExecuteSQLRetValues("INSERT INTO id_name_list(id, name) VALUES (2, '青木');")
	db.ExecuteSQLRetValues("INSERT INTO id_name_list(id, name) VALUES (3, '山田');")
	db.ExecuteSQLRetValues("INSERT INTO id_name_list(id, name) VALUES (4, '加藤');")
	db.ExecuteSQLRetValues("INSERT INTO id_name_list(id, name) VALUES (5, '木村');")
	db.ExecuteSQLRetValues("CREATE TABLE id_buppin_list(id INT, buppin VARCHAR(256));")
	db.ExecuteSQLRetValues("INSERT INTO id_buppin_list(id, buppin) VALUES (1, 'Desktop PC');")
	db.ExecuteSQLRetValues("INSERT INTO id_buppin_list(id, buppin) VALUES (1, 'Laptop PC');")
	db.ExecuteSQLRetValues("INSERT INTO id_buppin_list(id, buppin) VALUES (2, '3D Printer');")
	db.ExecuteSQLRetValues("INSERT INTO id_buppin_list(id, buppin) VALUES (4, 'Scanner');")
	db.ExecuteSQLRetValues("INSERT INTO id_buppin_list(id, buppin) VALUES (4, 'Network Switch');")
	_, results1 := db.ExecuteSQLRetValues("SELECT * FROM id_name_list JOIN id_buppin_list ON id_name_list.id = id_buppin_list.id;")
	samehada_util.PrintExecuteResults(results1)
	_, results2 := db.ExecuteSQLRetValues("SELECT id_buppin_list.id, id_buppin_list.buppin FROM id_name_list JOIN id_buppin_list ON id_name_list.id = id_buppin_list.id;")
	samehada_util.PrintExecuteResults(results2)
	_, results3 := db.ExecuteSQLRetValues("SELECT * FROM id_name_list JOIN id_buppin_list ON id_name_list.id = id_buppin_list.id WHERE id_name_list.id > 1;")
	samehada_util.PrintExecuteResults(results3)
	_, results4 := db.ExecuteSQLRetValues("SELECT id_name_list.id, id_buppin_list.buppin FROM id_name_list JOIN id_buppin_list ON id_name_list.id = id_buppin_list.id WHERE id_name_list.id > 1 AND id_buppin_list.id < 4;")
	samehada_util.PrintExecuteResults(results4)

	db.Shutdown()
}

func TestSimpleDelete(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true
	// clear all state of DB
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage == true {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	db := samehada.NewSamehadaDB(t.Name(), 200)
	db.ExecuteSQLRetValues("CREATE TABLE name_age_list(name VARCHAR(256), age INT);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('鈴木', 20);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('青木', 22);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('山田', 25);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('加藤', 18);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('木村', 18);")

	db.ExecuteSQLRetValues("DELETE FROM name_age_list WHERE age > 20;")
	_, results1 := db.ExecuteSQLRetValues("SELECT * FROM name_age_list;")
	testingpkg.SimpleAssert(t, len(results1) == 3)

	common.TempSuppressOnMemStorage = false
	db.Shutdown()
	common.TempSuppressOnMemStorageMutex.Unlock()
}

func TestSimpleUpdate(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true
	// clear all state of DB
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage == true {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	db := samehada.NewSamehadaDB(t.Name(), 200)
	db.ExecuteSQLRetValues("CREATE TABLE name_age_list(name VARCHAR(256), age INT);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('鈴木', 20);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('青木', 22);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('山田', 25);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('加藤', 18);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('木村', 18);")

	db.ExecuteSQLRetValues("UPDATE name_age_list SET name = '鮫肌' WHERE age <= 20;")
	_, results1 := db.ExecuteSQLRetValues("SELECT * FROM name_age_list WHERE name = '鮫肌';")
	samehada_util.PrintExecuteResults(results1)
	testingpkg.SimpleAssert(t, len(results1) == 3)

	common.TempSuppressOnMemStorage = false
	db.Shutdown()
	common.TempSuppressOnMemStorageMutex.Unlock()
}

//func TestRebootWithLoadAndRecovery(t *testing.T) {
//	common.TempSuppressOnMemStorageMutex.Lock()
//	common.TempSuppressOnMemStorage = true
//
//	// clear all state of DB
//	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage {
//		os.Remove(t.Name() + ".db")
//		os.Remove(t.Name() + ".log")
//	}
//
//	db := samehada.NewSamehadaDB("TestRebootWithLoadAndRecovery", 200)
//	db.ExecuteSQLRetValues("CREATE TABLE name_age_list(name VARCHAR(256), age INT);")
//	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('鈴木', 20);")
//	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('青木', 22);")
//	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('山田', 25);")
//	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('加藤', 18);")
//	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('木村', 18);")
//
//	db.ExecuteSQLRetValues("UPDATE name_age_list SET name = '鮫肌' WHERE age <= 20;")
//	_, results1 := db.ExecuteSQLRetValues("SELECT * FROM name_age_list WHERE name = '鮫肌';")
//	samehada.PrintExecuteResults(results1)
//	testingpkg.SimpleAssert(t, len(results1) == 3)
//
//	// close db and log file
//	db.ShutdownForTescase()
//
//	// relaunch using TestRebootWithLoadAndRecovery.log files
//	// load of db file and redo/undo process runs
//	// and remove needless log data
//	db2 := samehada.NewSamehadaDB(t.Name(), 200)
//	db2.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('鮫肌', 18);")
//	_, results2 := db2.ExecuteSQLRetValues("SELECT * FROM name_age_list WHERE name = '鮫肌';")
//	samehada.PrintExecuteResults(results2)
//	testingpkg.SimpleAssert(t, len(results2) == 4)
//
//	// close db and log file
//	db2.ShutdownForTescase()
//
//	// relaunch using TestRebootWithLoadAndRecovery.db and TestRebootWithLoadAndRecovery.log files
//	// load of db file and redo/undo process runs
//	// and remove needless log data
//	db3 := samehada.NewSamehadaDB(t.Name(), 200)
//	db3.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('鮫肌', 15);")
//	_, results3 := db3.ExecuteSQLRetValues("SELECT * FROM name_age_list WHERE name = '鮫肌';")
//	samehada.PrintExecuteResults(results3)
//	testingpkg.SimpleAssert(t, len(results3) == 5)
//
//	common.TempSuppressOnMemStorage = false
//	// close db and log file
//	db3.Shutdown()
//	common.TempSuppressOnMemStorageMutex.Unlock()
//}
//
//func TestRebootAndReturnIFValues(t *testing.T) {
//	common.TempSuppressOnMemStorageMutex.Lock()
//	common.TempSuppressOnMemStorage = true
//
//	// clear all state of DB
//	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage == true {
//		os.Remove(t.Name() + ".db")
//		os.Remove(t.Name() + ".log")
//	}
//
//	db := samehada.NewSamehadaDB(t.Name(), 200)
//	db.ExecuteSQL("CREATE TABLE name_age_list(name VARCHAR(256), age INT);")
//	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('鈴木', 20);")
//	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('青木', 22);")
//	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('山田', 25);")
//	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('加藤', 18);")
//	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('木村', 18);")
//
//	db.ExecuteSQL("UPDATE name_age_list SET name = '鮫肌' WHERE age <= 20;")
//	_, results1 := db.ExecuteSQL("SELECT * FROM name_age_list WHERE name = '鮫肌';")
//	fmt.Println("---")
//	for _, resultRow := range results1 {
//		fmt.Printf("%s %d\n", resultRow[0].(string), resultRow[1].(int32))
//	}
//	testingpkg.SimpleAssert(t, len(results1) == 3)
//
//	// close db and log file
//	db.ShutdownForTescase()
//
//	// relaunch
//	// load of db file and redo/undo process runs
//	// and remove needless log data
//	db2 := samehada.NewSamehadaDB(t.Name(), 200)
//	db2.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('鮫肌', 18);")
//	_, results2 := db2.ExecuteSQL("SELECT * FROM name_age_list WHERE name = '鮫肌';")
//	fmt.Println("---")
//	for _, resultRow := range results2 {
//		fmt.Printf("%s %d\n", resultRow[0].(string), resultRow[1].(int32))
//	}
//	testingpkg.SimpleAssert(t, len(results2) == 4)
//
//	// close db and log file
//	db2.ShutdownForTescase()
//
//	// relaunch
//	// load of db file and redo/undo process runs
//	// and remove needless log data
//	db3 := samehada.NewSamehadaDB(t.Name(), 200)
//	db3.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('鮫肌', 15);")
//	_, results3 := db3.ExecuteSQL("SELECT * FROM name_age_list WHERE name = '鮫肌';")
//	fmt.Println("---")
//	for _, resultRow := range results3 {
//		fmt.Printf("%s %d\n", resultRow[0].(string), resultRow[1].(int32))
//	}
//	testingpkg.SimpleAssert(t, len(results3) == 5)
//
//	common.TempSuppressOnMemStorage = false
//	db3.Shutdown()
//	common.TempSuppressOnMemStorageMutex.Unlock()
//}

func TestParallelQueryIssue(t *testing.T) {
	// clear all state of DB
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	db := samehada.NewSamehadaDB(t.Name(), 500) // 500KB
	opTimes := 10000

	queryVals := make([]int32, 0)

	for ii := 0; ii < opTimes; ii++ {
		randVal := rand.Int31()
		queryVals = append(queryVals, randVal)
	}

	err, _ := db.ExecuteSQL("CREATE TABLE k_v_list(k INT, v INT);")
	testingpkg.Assert(t, err == nil, "failed to create table")

	insCh := make(chan int32)
	for ii := 0; ii < opTimes; ii++ {
		go func(val int32) {
			err, _ = db.ExecuteSQL(fmt.Sprintf("INSERT INTO k_v_list(k, v) VALUES (%d, %d);", val, val))
			testingpkg.Assert(t, err == nil, "failed to insert val: "+strconv.Itoa(int(val)))
			insCh <- val
		}(queryVals[ii])
	}
	for ii := 0; ii < opTimes; ii++ {
		<-insCh
	}

	// shuffle query vals array elements
	rand.Shuffle(len(queryVals), func(i, j int) { queryVals[i], queryVals[j] = queryVals[j], queryVals[i] })

	fmt.Println("records insertion done.")

	THREAD_NUM := common.KernelThreadNum
	runtime.GOMAXPROCS(THREAD_NUM)

	ch := make(chan [2]int32)

	runningThCnt := 0
	allCnt := 0
	commitedCnt := 0
	abotedCnt := 0

	startTime := time.Now()
	for ii := 0; ii < opTimes; ii++ {
		// wait last go routines finishes
		if ii == opTimes-1 {
			for runningThCnt > 0 {
				recvRslt := <-ch
				allCnt++
				if recvRslt[1] == -1 {
					abotedCnt++
				} else {
					commitedCnt++
					testingpkg.Assert(t, recvRslt[0] == recvRslt[1], "failed to select val: "+strconv.Itoa(int(recvRslt[0])))
				}
				runningThCnt--
			}
			break
		}

		// wait for keeping THREAD_NUM * 2 groroutine existing
		for runningThCnt >= THREAD_NUM*2 {
			recvRslt := <-ch
			runningThCnt--
			allCnt++
			if allCnt%500 == 0 {
				fmt.Printf(strconv.Itoa(allCnt) + " queries done\n")
			}
			if recvRslt[1] == -1 {
				abotedCnt++
			} else {
				commitedCnt++
				testingpkg.Assert(t, recvRslt[0] == recvRslt[1], "failed to select val: "+strconv.Itoa(int(recvRslt[0])))
			}
		}

		go func(queryVal int32) {
			err_, results := db.ExecuteSQL(fmt.Sprintf("SELECT v FROM k_v_list WHERE k = %d;", queryVal))
			if err != nil {
				fmt.Println(err_)
				ch <- [2]int32{queryVal, -1}
				return
			}

			gotValue := results[0][0].(int32)
			ch <- [2]int32{queryVal, gotValue}
		}(queryVals[ii])

		runningThCnt++
	}

	fmt.Println("allCnt: " + strconv.Itoa(allCnt))
	fmt.Println("abotedCnt: " + strconv.Itoa(abotedCnt))
	fmt.Println("commitedCnt: " + strconv.Itoa(commitedCnt))
	d := time.Since(startTime)
	fmt.Printf("%f qps: elapsed %f sec\n", float32(opTimes)/float32(d.Seconds()), d.Seconds())
	db.Shutdown()
}

func TestParallelQueryIssueSelectUpdate(t *testing.T) {
	// clear all state of DB
	if !common.EnableOnMemStorage {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	db := samehada.NewSamehadaDB(t.Name(), 500) // 500KB
	opTimes := 10000

	queryVals := make([]int32, 0)

	for ii := 0; ii < opTimes; ii++ {
		randVal := rand.Int31()
		queryVals = append(queryVals, randVal)
	}

	err, _ := db.ExecuteSQL("CREATE TABLE k_v_list(k INT, v INT);")
	testingpkg.Assert(t, err == nil, "failed to create table")

	insCh := make(chan int32)
	for ii := 0; ii < opTimes; ii++ {
		go func(val int32) {
			err, _ = db.ExecuteSQL(fmt.Sprintf("INSERT INTO k_v_list(k, v) VALUES (%d, %d);", val, val))
			testingpkg.Assert(t, err == nil, "failed to insert val: "+strconv.Itoa(int(val)))
			insCh <- val
		}(queryVals[ii])
	}
	for ii := 0; ii < opTimes; ii++ {
		<-insCh
	}

	queryVals = queryVals[:len(queryVals)/2]
	// shuffle query vals array elements
	rand.Shuffle(len(queryVals), func(i, j int) { queryVals[i], queryVals[j] = queryVals[j], queryVals[i] })
	tmpQueryVals := make([]int32, 0)
	for _, val := range queryVals {
		tmpQueryVals = append(tmpQueryVals, val)
		tmpQueryVals = append(tmpQueryVals, val)
	}
	queryVals = tmpQueryVals

	fmt.Println("records insertion done.")

	THREAD_NUM := common.KernelThreadNum
	runtime.GOMAXPROCS(THREAD_NUM)

	ch := make(chan [2]int32)

	runningThCnt := 0
	allCnt := 0
	commitedCnt := 0
	abotedCnt := 0

	startTime := time.Now()
	for ii := 0; ii < opTimes; ii++ {
		// wait last go routines finishes
		if ii == opTimes-1 {
			for runningThCnt > 0 {
				recvRslt := <-ch
				allCnt++
				if recvRslt[1] == -1 {
					// may be update
					commitedCnt++
				} else {
					commitedCnt++
					testingpkg.Assert(t, recvRslt[0] == recvRslt[1], "failed to select val: "+strconv.Itoa(int(recvRslt[0])))
				}
				runningThCnt--
			}
			break
		}

		// wait for keeping THREAD_NUM * 2 groroutine existing
		for runningThCnt >= THREAD_NUM*2 {
			recvRslt := <-ch
			runningThCnt--
			allCnt++
			if allCnt%500 == 0 {
				fmt.Printf(strconv.Itoa(allCnt) + " queries done\n")
			}
			if recvRslt[1] == -1 {
				// may be update
				commitedCnt++
			} else {
				commitedCnt++
				testingpkg.Assert(t, recvRslt[0] == recvRslt[1], "failed to select val: "+strconv.Itoa(int(recvRslt[0])))
			}
		}

		go func(queryVal int32) {
			var err_ error
			var results [][]interface{}
			//rndVal := rand.Int31()
			err_, results = db.ExecuteSQL(fmt.Sprintf("UPDATE k_v_list SET k = %d, v = %d WHERE k = %d;", queryVal, queryVal, queryVal))
			//if rndVal%2 == 0 {
			//	err_, results = db.ExecuteSQL(fmt.Sprintf("SELECT v FROM k_v_list WHERE k = %d;", queryVal))
			//} else {
			//	err_, results = db.ExecuteSQL(fmt.Sprintf("UPDATE k_v_list SET k = %d, v = %d WHERE k = %d;", queryVal, queryVal, queryVal))
			//}

			if err_ != nil {
				fmt.Println(err_)
				ch <- [2]int32{queryVal, -1}
				return
			}

			if results != nil && len(results) > 0 { // may be select
				//fmt.Println(results)
				gotValue := results[0][0].(int32)
				ch <- [2]int32{queryVal, gotValue}
			} else { // may be update
				ch <- [2]int32{queryVal, -1}
			}
		}(queryVals[ii])

		runningThCnt++
	}

	fmt.Println("allCnt: " + strconv.Itoa(allCnt))
	fmt.Println("abotedCnt: " + strconv.Itoa(abotedCnt))
	fmt.Println("commitedCnt: " + strconv.Itoa(commitedCnt))
	d := time.Since(startTime)
	fmt.Printf("%f qps: elapsed %f sec\n", float32(opTimes)/float32(d.Seconds()), d.Seconds())
	db.Shutdown()
}

func TestRebootAndReturnIFValuesWithCheckpoint(t *testing.T) {
	common.TempSuppressOnMemStorageMutex.Lock()
	common.TempSuppressOnMemStorage = true

	// clear all state of DB
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage == true {
		os.Remove(t.Name() + ".db")
		os.Remove(t.Name() + ".log")
	}

	db := samehada.NewSamehadaDB(t.Name(), 10*1024)
	db.ExecuteSQL("CREATE TABLE name_age_list(name VARCHAR(256), age INT);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('鈴木', 20);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('saklasjさいあｐしえあｓｄｋあｌｋ;ぢえああ', 22);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('山田', 25);")

	// wait until checkpointing thread runs
	time.Sleep(70 * time.Second)

	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('加藤', 18);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('木村', 18);")
	db.ExecuteSQL("DELETE from name_age_list WHERE age = 20;")

	db.ExecuteSQL("UPDATE name_age_list SET name = '鮫肌' WHERE age <= 20;")
	db.ExecuteSQL("UPDATE name_age_list SET name = 'lksaｊぁｓあいえあいえじゃｓｌｋｆｄじゃか' WHERE name = 'saklasjさいあｐしえあｓｄｋあｌｋ;ぢえああ';")
	_, results1 := db.ExecuteSQL("SELECT * FROM name_age_list WHERE name = '鮫肌';")
	fmt.Println("---")
	for _, resultRow := range results1 {
		fmt.Printf("%s %d\n", resultRow[0].(string), resultRow[1].(int32))
	}
	testingpkg.SimpleAssert(t, len(results1) == 2)

	// close db and stop checkpointing thread
	db.ShutdownForTescase()

	// relaunch
	// load of db file and redo/undo process runs
	// and remove needless log data
	db2 := samehada.NewSamehadaDB(t.Name(), 10*1024)
	db2.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('鮫肌', 18);")
	_, results2 := db2.ExecuteSQL("SELECT * FROM name_age_list WHERE name = '鮫肌';")
	fmt.Println("---")
	for _, resultRow := range results2 {
		fmt.Printf("%s %d\n", resultRow[0].(string), resultRow[1].(int32))
	}
	testingpkg.SimpleAssert(t, len(results2) == 3)

	// close db and log file
	db2.ShutdownForTescase()

	// relaunch
	// load of db file and redo/undo process runs
	// and remove needless log data
	db3 := samehada.NewSamehadaDB(t.Name(), 10*1024)
	db3.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('鮫肌', 15);")
	_, results3 := db3.ExecuteSQL("SELECT * FROM name_age_list WHERE name = '鮫肌';")
	fmt.Println("---")
	for _, resultRow := range results3 {
		fmt.Printf("%s %d\n", resultRow[0].(string), resultRow[1].(int32))
	}
	testingpkg.SimpleAssert(t, len(results3) == 4)

	db3.ExecuteSQL("UPDATE name_age_list SET name = '鈴木' WHERE name = '青木';")
	_, results5 := db3.ExecuteSQL("SELECT * FROM name_age_list WHERE name != '鮫肌';")
	fmt.Println("---")
	for _, resultRow := range results5 {
		fmt.Printf("%s %d\n", resultRow[0].(string), resultRow[1].(int32))
	}
	testingpkg.SimpleAssert(t, len(results5) == 2)

	common.TempSuppressOnMemStorage = false
	db3.Shutdown()
	common.TempSuppressOnMemStorageMutex.Unlock()
}
