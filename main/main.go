package main

import (
	"fmt"
	"github.com/ryogrid/SamehadaDB/samehada"
	"os"
)

func main() {
	// clear all state of DB
	os.Remove("example.db")
	os.Remove("example.log")

	db := samehada.NewSamehadaDB("example")
	db.ExecuteSQL("CREATE TABLE name_age_list(name VARCHAR(256), age INT);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('鈴木', 20);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('青木', 22);")
	db.ExecuteSQL("INSERT INTO name_age_list(name, age) VALUES ('山田', 25);")
	_, results := db.ExecuteSQL("SELECT * FROM name_age_list WHERE name = '山田';")
	for _, valList := range results {
		for _, val := range valList {
			fmt.Printf("%s ", val.ToString())
		}
		fmt.Println("")
	}
}
