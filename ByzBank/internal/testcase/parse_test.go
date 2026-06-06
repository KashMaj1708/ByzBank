package testcase_test

import (
	"path/filepath"
	"testing"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/testcase"
)

func TestParseTestset1(t *testing.T) {
	path := filepath.Join("..", "..", "test", "Lab4_Testset_1_36node.csv")
	f, err := testcase.ParseFile(path)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(f.Sets) != 6 {
		t.Fatalf("want 6 sets, got %d", len(f.Sets))
	}
	if f.Sets[0].Number != 1 || len(f.Sets[0].Txns) != 3 {
		t.Fatalf("set 1: %+v", f.Sets[0])
	}
	if len(f.Sets[1].Txns) != 4 {
		t.Fatalf("set 2 txns: %d", len(f.Sets[1].Txns))
	}
	if len(f.Sets[4].Live) != 30 {
		t.Fatalf("set 5 live count: %d want 30", len(f.Sets[4].Live))
	}
	contact, err := testcase.ContactFor(f.Sets[0], 1)
	if err != nil || contact != 1 {
		t.Fatalf("contact C1: %v %d", err, contact)
	}
}

func TestParseTestset2(t *testing.T) {
	path := filepath.Join("..", "..", "test", "Lab4_Testset_2_36node.csv")
	f, err := testcase.ParseFile(path)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(f.Sets) != 6 {
		t.Fatalf("want 6 sets, got %d", len(f.Sets))
	}
	if f.Sets[0].Txns[0].X != 299 || f.Sets[0].Txns[0].Y != 1999 {
		t.Fatalf("set1 txn0: %+v", f.Sets[0].Txns[0])
	}
}
