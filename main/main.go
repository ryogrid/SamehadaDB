package main

import "github.com/ryogrid/SamehadaDB/parser"

func main() {
	//fmt.Println("hello world!")
	sqlStr := "CREATE INDEX testTbl_index USING hash ON testTbl (name);"
	parser.PrintParsedNodes(&sqlStr)
}
