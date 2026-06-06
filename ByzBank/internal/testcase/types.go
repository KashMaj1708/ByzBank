package testcase

import "github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"

// Transaction is one (x,y,amt) transfer.
type Transaction struct {
	X   int
	Y   int
	Amt int64
}

// Set is one graded test set from the CSV file.
type Set struct {
	Number   int
	Txns     []Transaction
	Live     []config.ServerID
	Contact  []config.ServerID
	Byzantine []config.ServerID
}

// File is a parsed Lab4 test file.
type File struct {
	Path string
	Sets []Set
}
