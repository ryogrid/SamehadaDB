package main

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/samehada"
	"github.com/ryogrid/SamehadaDB/types"
	"os"
)

func PrintExecuteResults(results [][]*types.Value) {
	for _, valList := range results {
		for _, val := range valList {
			fmt.Printf("%s ", val.ToString())
		}
		fmt.Println("")
	}
}

func main() {
	// clear all state of DB
	os.Remove("example.db")
	os.Remove("example.log")

	db := samehada.NewSamehadaDB("example")
	db.ExecuteSQL("CREATE TABLE name_age_list(name VARCHAR(256), age INT);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('鈴木', 20);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('青木', 22);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('山田', 25);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('加藤', 18);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('木村', 18);")
	_, results1 := db.ExecuteSQL("SELECT * FROM name_age_list WHERE age >= 20;")
	PrintExecuteResults(results1)
	_, results2 := db.ExecuteSQL("SELECT age FROM name_age_list WHERE age >= 20;")
	PrintExecuteResults(results2)
	_, results3 := db.ExecuteSQL("SELECT name, age FROM name_age_list WHERE age >= 20;")
	PrintExecuteResults(results3)
	_, results4 := db.ExecuteSQL("SELECT name FROM name_age_list WHERE age >= 20;")
	PrintExecuteResults(results4)
}
