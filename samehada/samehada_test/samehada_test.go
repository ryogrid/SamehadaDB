package samehada_test

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/common"
	"github.com/ryogrid/SamehadaDB/samehada"
	testingpkg "github.com/ryogrid/SamehadaDB/testing"
	"os"
	"testing"
)

// TODO: (SDB) need to check query result (TestInsertAndMultiItemPredicateSelect)
func TestInsertAndMultiItemPredicateSelect(t *testing.T) {
	common.TempSuppressOnMemStorage = true

	// clear all state of DB
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage == true {
		os.Remove("example.db")
		os.Remove("example.log")
	}

	db := samehada.NewSamehadaDB("example", 200)
	db.ExecuteSQLRetValues("CREATE TABLE name_age_list(name VARCHAR(256), age INT);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('鈴木', 20);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('青木', 22);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('山田', 25);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('加藤', 18);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('木村', 18);")
	_, results1 := db.ExecuteSQLRetValues("SELECT * FROM name_age_list WHERE age >= 20;")
	samehada.PrintExecuteResults(results1)
	_, results2 := db.ExecuteSQLRetValues("SELECT age FROM name_age_list WHERE age >= 20;")
	samehada.PrintExecuteResults(results2)
	_, results3 := db.ExecuteSQLRetValues("SELECT name, age FROM name_age_list WHERE age >= 20;")
	samehada.PrintExecuteResults(results3)
	_, results4 := db.ExecuteSQLRetValues("SELECT name, age FROM name_age_list WHERE age <= 23 AND age >= 20;")
	samehada.PrintExecuteResults(results4)
	_, results5 := db.ExecuteSQLRetValues("SELECT * FROM name_age_list WHERE (age = 18 OR age >= 22) AND age < 25;")
	samehada.PrintExecuteResults(results5)

	db.Shutdown()
	common.TempSuppressOnMemStorage = false
}

// TODO: (SDB) need to check query result (TestHasJoinSelect)
func TestHasJoinSelect(t *testing.T) {
	// clear all state of DB
	if !common.EnableOnMemStorage {
		os.Remove("example.db")
		os.Remove("example.log")
	}

	db := samehada.NewSamehadaDB("example", 200)
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
	samehada.PrintExecuteResults(results1)
	_, results2 := db.ExecuteSQLRetValues("SELECT id_buppin_list.id, id_buppin_list.buppin FROM id_name_list JOIN id_buppin_list ON id_name_list.id = id_buppin_list.id;")
	samehada.PrintExecuteResults(results2)
	_, results3 := db.ExecuteSQLRetValues("SELECT * FROM id_name_list JOIN id_buppin_list ON id_name_list.id = id_buppin_list.id WHERE id_name_list.id > 1;")
	samehada.PrintExecuteResults(results3)
	_, results4 := db.ExecuteSQLRetValues("SELECT id_name_list.id, id_buppin_list.buppin FROM id_name_list JOIN id_buppin_list ON id_name_list.id = id_buppin_list.id WHERE id_name_list.id > 1 AND id_buppin_list.id < 4;")
	samehada.PrintExecuteResults(results4)

	db.Shutdown()
}

func TestSimpleDelete(t *testing.T) {
	common.TempSuppressOnMemStorage = true
	// clear all state of DB
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage == true {
		os.Remove("example.db")
		os.Remove("example.log")
	}

	db := samehada.NewSamehadaDB("example", 200)
	db.ExecuteSQLRetValues("CREATE TABLE name_age_list(name VARCHAR(256), age INT);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('鈴木', 20);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('青木', 22);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('山田', 25);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('加藤', 18);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('木村', 18);")

	db.ExecuteSQLRetValues("DELETE FROM name_age_list WHERE age > 20;")
	_, results1 := db.ExecuteSQLRetValues("SELECT * FROM name_age_list;")
	testingpkg.SimpleAssert(t, len(results1) == 3)

	db.Shutdown()
}

func TestSimpleUpdate(t *testing.T) {
	common.TempSuppressOnMemStorage = true
	// clear all state of DB
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage == true {
		os.Remove("example.db")
		os.Remove("example.log")
	}

	db := samehada.NewSamehadaDB("example", 200)
	db.ExecuteSQLRetValues("CREATE TABLE name_age_list(name VARCHAR(256), age INT);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('鈴木', 20);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('青木', 22);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('山田', 25);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('加藤', 18);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('木村', 18);")

	db.ExecuteSQLRetValues("UPDATE name_age_list SET name = '鮫肌' WHERE age <= 20;")
	_, results1 := db.ExecuteSQLRetValues("SELECT * FROM name_age_list WHERE name = '鮫肌';")
	samehada.PrintExecuteResults(results1)
	testingpkg.SimpleAssert(t, len(results1) == 3)

	db.Shutdown()
	common.TempSuppressOnMemStorage = false
}

func TestRebootWithLoadAndRecovery(t *testing.T) {
	common.TempSuppressOnMemStorage = true

	// clear all state of DB
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage == true {
		os.Remove("/tmp/todo.db")
		os.Remove("/tmp/todo.log")
	}

	db := samehada.NewSamehadaDB("/tmp/todo", 200)
	db.ExecuteSQLRetValues("CREATE TABLE name_age_list(name VARCHAR(256), age INT);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('鈴木', 20);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('青木', 22);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('山田', 25);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('加藤', 18);")
	db.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('木村', 18);")

	db.ExecuteSQLRetValues("UPDATE name_age_list SET name = '鮫肌' WHERE age <= 20;")
	_, results1 := db.ExecuteSQLRetValues("SELECT * FROM name_age_list WHERE name = '鮫肌';")
	samehada.PrintExecuteResults(results1)
	testingpkg.SimpleAssert(t, len(results1) == 3)

	// close db and log file
	db.Shutdown()

	// relaunch using /tmp/todo.db and /tmp/todo.log files
	// load of db file and redo/undo process runs
	// and remove needless log data
	db2 := samehada.NewSamehadaDB("/tmp/todo", 200)
	db2.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('鮫肌', 18);")
	_, results2 := db2.ExecuteSQLRetValues("SELECT * FROM name_age_list WHERE name = '鮫肌';")
	samehada.PrintExecuteResults(results2)
	testingpkg.SimpleAssert(t, len(results2) == 4)

	// close db and log file
	db2.Shutdown()

	// relaunch using /tmp/todo.db and /tmp/todo.log files
	// load of db file and redo/undo process runs
	// and remove needless log data
	db3 := samehada.NewSamehadaDB("/tmp/todo", 200)
	db3.ExecuteSQLRetValues("INSERT INTO name_age_list(name, age) VALUES ('鮫肌', 15);")
	_, results3 := db3.ExecuteSQLRetValues("SELECT * FROM name_age_list WHERE name = '鮫肌';")
	samehada.PrintExecuteResults(results3)
	testingpkg.SimpleAssert(t, len(results3) == 5)

	// close db and log file
	db3.Shutdown()

	common.TempSuppressOnMemStorage = false
}

func TestRebootAndReturnIFValues(t *testing.T) {
	common.TempSuppressOnMemStorage = true

	// clear all state of DB
	if !common.EnableOnMemStorage || common.TempSuppressOnMemStorage == true {
		os.Remove("/tmp/todo.db")
		os.Remove("/tmp/todo.log")
	}

	db := samehada.NewSamehadaDB("/tmp/todo", 200)
	db.ExecuteSQL("CREATE TABLE name_age_list(name VARCHAR(256), age INT);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('鈴木', 20);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('青木', 22);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('山田', 25);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('加藤', 18);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('木村', 18);")

	db.ExecuteSQL("UPDATE name_age_list SET name = '鮫肌' WHERE age <= 20;")
	_, results1 := db.ExecuteSQL("SELECT * FROM name_age_list WHERE name = '鮫肌';")
	fmt.Println("---")
	for _, resultRow := range results1 {
		fmt.Printf("%s %d\n", resultRow[0].(string), resultRow[1].(int32))
	}
	testingpkg.SimpleAssert(t, len(results1) == 3)

	// close db and log file
	db.Shutdown()

	// relaunch using /tmp/todo.db and /tmp/todo.log files
	// load of db file and redo/undo process runs
	// and remove needless log data
	db2 := samehada.NewSamehadaDB("/tmp/todo", 200)
	db2.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('鮫肌', 18);")
	_, results2 := db2.ExecuteSQL("SELECT * FROM name_age_list WHERE name = '鮫肌';")
	fmt.Println("---")
	for _, resultRow := range results2 {
		fmt.Printf("%s %d\n", resultRow[0].(string), resultRow[1].(int32))
	}
	testingpkg.SimpleAssert(t, len(results2) == 4)

	// close db and log file
	db2.Shutdown()

	// relaunch using /tmp/todo.db and /tmp/todo.log files
	// load of db file and redo/undo process runs
	// and remove needless log data
	db3 := samehada.NewSamehadaDB("/tmp/todo", 200)
	db3.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('鮫肌', 15);")
	_, results3 := db3.ExecuteSQL("SELECT * FROM name_age_list WHERE name = '鮫肌';")
	fmt.Println("---")
	for _, resultRow := range results3 {
		fmt.Printf("%s %d\n", resultRow[0].(string), resultRow[1].(int32))
	}
	testingpkg.SimpleAssert(t, len(results3) == 5)

	db3.Shutdown()

	common.TempSuppressOnMemStorage = false
}
