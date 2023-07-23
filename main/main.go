package main

import "github.com/ryogrid/SamehadaDB/parser"

// current SamehadaDB can be used as an embedded DB form only.
// so, this entry point is used for running snippet code for debugging now...
func main() {
	//fmt.Println("hello world!")
	sqlStr := "CREATE INDEX testTbl_index USING hash ON testTbl (name);"
	parser.PrintParsedNodes(&sqlStr)
}
