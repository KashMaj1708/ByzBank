package twopc_test

import (
	"testing"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
)

func TestCrossShardCommitPhase(t *testing.T) {
	h := startDualClusters(t)
	defer h.cleanup()

	req := pbft.Request{ClientID: "c1", TS: 1, X: 5, Y: 1500, Amt: 4}
	h.submitToCoord(t, req)

	waitUntil(t, 30*time.Second, func() bool {
		return h.replies.MatchingCount(req, "committed") >= h.topo.ClientQuorum()
	})
	waitUntil(t, 30*time.Second, func() bool {
		return participantCommitDone(h, req)
	})

	for _, id := range clusterServerIDs(h.topo, 1) {
		rep := h.replicas[id]
		if got := rep.Store.GetBalance(5); got != 6 {
			t.Errorf("C1 %s bal[5]=%d want 6", id, got)
		}
		if rep.Store.IsLocked(5) {
			t.Errorf("C1 %s: lock on x should be released", id)
		}
		if rep.Store.WALExists(pbft.TxnID(req)) {
			t.Errorf("C1 %s: WAL should be deleted", id)
		}
		entries, err := rep.Store.Datastore()
		if err != nil {
			t.Fatalf("datastore %s: %v", id, err)
		}
		if len(entries) != 2 {
			t.Errorf("C1 %s: want 2 datastore entries, got %d", id, len(entries))
			continue
		}
		if entries[0].Phase != store.PhasePrepare || entries[1].Phase != store.PhaseCommit {
			t.Errorf("C1 %s: want prepare+commit entries, got %v", id, entries)
		}
	}
	for _, id := range clusterServerIDs(h.topo, 2) {
		rep := h.replicas[id]
		if got := rep.Store.GetBalance(1500); got != 14 {
			t.Errorf("C2 %s bal[1500]=%d want 14", id, got)
		}
		if rep.Store.IsLocked(1500) {
			t.Errorf("C2 %s: lock on y should be released", id)
		}
		if rep.Store.WALExists(pbft.TxnID(req)) {
			t.Errorf("C2 %s: WAL should be deleted", id)
		}
		entries, err := rep.Store.Datastore()
		if err != nil {
			t.Fatalf("datastore %s: %v", id, err)
		}
		if len(entries) != 2 {
			t.Errorf("C2 %s: want 2 datastore entries, got %d", id, len(entries))
			continue
		}
		if entries[0].Phase != store.PhasePrepare || entries[1].Phase != store.PhaseCommit {
			t.Errorf("C2 %s: want prepare+commit entries, got %v", id, entries)
		}
	}

	if !h.acks.WaitForQuorum(req, h.topo.ClientQuorum(), 30*time.Second) {
		t.Fatalf("coordinator collected %d acks, want >= %d", h.acks.Count(req), h.topo.ClientQuorum())
	}
}

func participantCommitDone(h *dualHarness, req pbft.Request) bool {
	for _, id := range clusterServerIDs(h.topo, 2) {
		rep := h.replicas[id]
		if rep.Store.IsLocked(1500) || rep.Store.WALExists(pbft.TxnID(req)) {
			return false
		}
		entries, err := rep.Store.Datastore()
		if err != nil || len(entries) < 2 {
			return false
		}
	}
	return true
}

func TestCrossShardAbortUndo(t *testing.T) {
	h := startDualClusters(t)
	defer h.cleanup()

	for _, id := range clusterServerIDs(h.topo, 2) {
		if !h.replicas[id].Store.AcquireLock(1500, 99) {
			t.Fatalf("failed to pre-lock y on %s", id)
		}
	}

	req := pbft.Request{ClientID: "c1", TS: 1, X: 5, Y: 1500, Amt: 4}
	h.submitToCoord(t, req)

	waitUntil(t, 30*time.Second, func() bool {
		return h.replies.MatchingCount(req, "abort") >= h.topo.ClientQuorum()
	})

	for _, id := range clusterServerIDs(h.topo, 1) {
		rep := h.replicas[id]
		if got := rep.Store.GetBalance(5); got != 10 {
			t.Errorf("C1 %s bal[5]=%d want 10 after WAL undo", id, got)
		}
		if rep.Store.IsLocked(5) {
			t.Errorf("C1 %s: lock on x should be released", id)
		}
		if rep.Store.WALExists(pbft.TxnID(req)) {
			t.Errorf("C1 %s: WAL should be deleted", id)
		}
		entries, err := rep.Store.Datastore()
		if err != nil {
			t.Fatalf("datastore %s: %v", id, err)
		}
		if len(entries) != 2 {
			t.Errorf("C1 %s: want 2 datastore entries, got %d", id, len(entries))
			continue
		}
		if entries[1].Outcome != store.OutcomeAbort {
			t.Errorf("C1 %s: commit entry should be abort, got %v", id, entries[1])
		}
	}
	for _, id := range clusterServerIDs(h.topo, 2) {
		if got := h.replicas[id].Store.GetBalance(1500); got != 10 {
			t.Errorf("C2 %s bal[1500]=%d want 10", id, got)
		}
	}
}
