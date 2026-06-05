package store

import (
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func openTestStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := Open(Options{
		Path:           filepath.Join(dir, "test.db"),
		InitialBalance: 10,
		TotalItems:     3000,
	})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestInitialBalances(t *testing.T) {
	s := openTestStore(t)
	cases := []int{1, 5, 1000, 1500, 2001, 3000}
	for _, item := range cases {
		if got := s.GetBalance(item); got != 10 {
			t.Errorf("bal[%d] = %d, want 10", item, got)
		}
	}
}

func TestApplyTransfer(t *testing.T) {
	s := openTestStore(t)
	if err := s.ApplyTransfer(5, 7, 3); err != nil {
		t.Fatalf("ApplyTransfer: %v", err)
	}
	if got := s.GetBalance(5); got != 7 {
		t.Errorf("bal[5] = %d, want 7", got)
	}
	if got := s.GetBalance(7); got != 13 {
		t.Errorf("bal[7] = %d, want 13", got)
	}
}

func TestApplyTransferInsufficient(t *testing.T) {
	s := openTestStore(t)
	if err := s.ApplyTransfer(5, 7, 100); err == nil {
		t.Fatal("expected insufficient balance error")
	}
	if s.GetBalance(5) != 10 || s.GetBalance(7) != 10 {
		t.Fatal("balances should be unchanged after failed transfer")
	}
}

func TestLockAcquireReacquire(t *testing.T) {
	s := openTestStore(t)
	if !s.AcquireLock(42, 1) {
		t.Fatal("first AcquireLock should succeed")
	}
	if s.AcquireLock(42, 2) {
		t.Fatal("second AcquireLock on same item should fail")
	}
	if !s.IsLocked(42) {
		t.Fatal("item 42 should be locked")
	}
	if got := s.LockSeq(42); got != 1 {
		t.Fatalf("LockSeq = %d, want 1", got)
	}
	s.ReleaseLock(42)
	if s.IsLocked(42) {
		t.Fatal("item 42 should be unlocked after release")
	}
	if !s.AcquireLock(42, 3) {
		t.Fatal("AcquireLock after release should succeed")
	}
}

func TestWALUndoRestoresBalance(t *testing.T) {
	s := openTestStore(t)
	const txnID = "cross-5-1500-4"

	oldX := s.GetBalance(5)
	if err := s.ApplyDebitOnly(5, 4); err != nil {
		t.Fatalf("ApplyDebitOnly: %v", err)
	}
	if s.GetBalance(5) != oldX-4 {
		t.Fatalf("balance after debit = %d, want %d", s.GetBalance(5), oldX-4)
	}

	pre := NewWALPreimage(map[int]int64{5: oldX})
	if err := s.WALWrite(txnID, pre); err != nil {
		t.Fatalf("WALWrite: %v", err)
	}
	if err := s.WALUndo(txnID); err != nil {
		t.Fatalf("WALUndo: %v", err)
	}
	if got := s.GetBalance(5); got != oldX {
		t.Errorf("balance after undo = %d, want %d", got, oldX)
	}
	if !s.WALExists(txnID) {
		t.Fatal("WAL entry should still exist after undo")
	}
	if err := s.WALDelete(txnID); err != nil {
		t.Fatalf("WALDelete: %v", err)
	}
	if s.WALExists(txnID) {
		t.Fatal("WAL entry should be gone after delete")
	}
}

func TestCrossShardDebitCreditWAL(t *testing.T) {
	s := openTestStore(t)
	const txnID = "cross-5-1500-4"

	xOld := s.GetBalance(5)
	yOld := s.GetBalance(1500)

	_ = s.ApplyDebitOnly(5, 4)
	_ = s.ApplyCreditOnly(1500, 4)
	_ = s.WALWrite(txnID, NewWALPreimage(map[int]int64{5: xOld, 1500: yOld}))

	if s.GetBalance(5) != 6 || s.GetBalance(1500) != 14 {
		t.Fatalf("prepare balances wrong: x=%d y=%d", s.GetBalance(5), s.GetBalance(1500))
	}

	if err := s.WALUndo(txnID); err != nil {
		t.Fatal(err)
	}
	if s.GetBalance(5) != 10 || s.GetBalance(1500) != 10 {
		t.Fatalf("undo failed: x=%d y=%d", s.GetBalance(5), s.GetBalance(1500))
	}
}

func TestAppendDatastoreAndPrint(t *testing.T) {
	s := openTestStore(t)
	entries := []DatastoreEntry{
		{Type: TxnIntra, Phase: PhaseCommit, X: 100, Y: 501, Amt: 8, BallotOrViewSeq: 0, Outcome: OutcomeCommit},
		{Type: TxnIntra, Phase: PhaseCommit, X: 101, Y: 301, Amt: 9, BallotOrViewSeq: 1, Outcome: OutcomeCommit},
	}
	for _, e := range entries {
		if err := s.AppendDatastore(e); err != nil {
			t.Fatalf("AppendDatastore: %v", err)
		}
	}

	got, err := s.Datastore()
	if err != nil {
		t.Fatalf("Datastore: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("len = %d, want 2", len(got))
	}
	if got[0].Seq != 0 || got[1].Seq != 1 {
		t.Fatalf("seqs = %d,%d, want 0,1", got[0].Seq, got[1].Seq)
	}

	out := s.PrintDatastore()
	if !strings.Contains(out, "(100,501,8)") || !strings.Contains(out, "(101,301,9)") {
		t.Fatalf("PrintDatastore missing entries:\n%s", out)
	}
}

func TestPrintBalance(t *testing.T) {
	s := openTestStore(t)
	_ = s.ApplyTransfer(5, 7, 3)
	if got := s.PrintBalance(5); got != "bal[5] = 7" {
		t.Fatalf("PrintBalance = %q", got)
	}
}

func TestClientTimestamp(t *testing.T) {
	s := openTestStore(t)
	if s.GetClientTS("client-A") != 0 {
		t.Fatal("unseen client should return 0")
	}
	if err := s.SetClientTS("client-A", 42); err != nil {
		t.Fatal(err)
	}
	if s.GetClientTS("client-A") != 42 {
		t.Fatalf("ts = %d, want 42", s.GetClientTS("client-A"))
	}
}

func TestConcurrentAccess(t *testing.T) {
	s := openTestStore(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			item := 100 + i
			_ = s.AcquireLock(item, int64(i+1))
			_ = s.ApplyCreditOnly(item, 1)
			s.ReleaseLock(item)
		}(i)
	}
	wg.Wait()
}
