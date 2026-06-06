package server_test

import (
	"context"
	"testing"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
)

// Figure 4 symbolic items (all in C1 except E).
const (
	figItemA = 10
	figItemB = 11
	figItemC = 12
	figItemD = 13
	figItemE = 1500
)

func TestFigure4Interleaving(t *testing.T) {
	h := startHarness(t, harnessOptions{clusters: []config.ClusterID{1, 2}, disableCommit: true})
	defer h.cleanup()

	// Seed balances so (A,B,20) is valid with InitialBalance=10.
	for _, item := range []int{figItemA, figItemB, figItemC, figItemD} {
		seedBalance(t, h, item, 20)
	}

	intraAB := pbft.Request{ClientID: "fig-intra-ab", TS: 1, X: figItemA, Y: figItemB, Amt: 20}
	crossAE := pbft.Request{ClientID: "fig-cross-ae", TS: 1, X: figItemA, Y: figItemE, Amt: 10}
	intraCD := pbft.Request{ClientID: "fig-intra-cd", TS: 1, X: figItemC, Y: figItemD, Amt: 5}

	// Step 1: intra (A,B,20) commits.
	if err := h.driver.Fire(intraAB); err != nil {
		t.Fatalf("fire intra AB: %v", err)
	}
	if _, err := h.driver.WaitFor(intraAB, "committed", 20*time.Second); err != nil {
		t.Fatalf("intra AB: %v", err)
	}
	assertBalanceAll(t, h, figItemA, 10)
	assertBalanceAll(t, h, figItemB, 50)
	assertLockedAll(t, h, figItemA, false)
	assertLockedAll(t, h, figItemB, false)

	// Step 2: cross (A,E,10) prepare — lock A, WAL, debit.
	if err := h.driver.Fire(crossAE); err != nil {
		t.Fatalf("fire cross AE: %v", err)
	}
	waitUntil(t, 30*time.Second, func() bool {
		return h.replica(1, 0).Store.GetBalance(figItemA) == 0
	})
	assertBalanceAll(t, h, figItemA, 0)
	assertLockedAll(t, h, figItemA, true)
	assertWALAll(t, h, crossAE, true)
	rep := h.replica(1, 0)
	if n := crossDatastoreEntries(t, rep, crossAE); n != 1 {
		t.Fatalf("after prepare want 1 cross datastore entry, got %d", n)
	}
	entries, _ := rep.Store.Datastore()
	if entries[len(entries)-1].Phase != store.PhasePrepare {
		t.Fatalf("last entry should be prepare, got %v", entries[len(entries)-1])
	}
	if h.driver.MatchingCount(crossAE, "committed") > 0 {
		t.Fatal("client reply must not be sent during prepare")
	}

	// Step 3: intra (C,D,5) interleaved before cross commit.
	if err := h.driver.Fire(intraCD); err != nil {
		t.Fatalf("fire intra CD: %v", err)
	}
	if _, err := h.driver.WaitFor(intraCD, "committed", 20*time.Second); err != nil {
		t.Fatalf("intra CD: %v", err)
	}
	assertBalanceAll(t, h, figItemC, 25)
	assertBalanceAll(t, h, figItemD, 35)
	assertLockedAll(t, h, figItemA, true)
	assertWALAll(t, h, crossAE, true)

	// Step 4: cross commit — release A, delete WAL, client reply.
	commitReq := crossAE
	commitReq.Op = pbft.OpCoordCommit
	h.coordPrimary().PBFT.StartConsensus(context.Background(), commitReq)

	if _, err := h.driver.WaitFor(crossAE, "committed", 30*time.Second); err != nil {
		t.Fatalf("cross commit: %v", err)
	}
	waitUntil(t, 30*time.Second, func() bool {
		return !h.replica(2, 0).Store.IsLocked(figItemE)
	})

	assertLockedAll(t, h, figItemA, false)
	assertWALAll(t, h, crossAE, false)
	assertBalanceAll(t, h, figItemA, 0)
	assertBalanceAll(t, h, figItemE, 20)

	for _, rep := range h.clusterReplicas(1) {
		if n := crossDatastoreEntries(t, rep, crossAE); n != 2 {
			t.Errorf("%s: want 2 cross datastore entries, got %d", rep.Self, n)
		}
	}
	for _, rep := range h.clusterReplicas(2) {
		if n := crossDatastoreEntries(t, rep, crossAE); n != 2 {
			t.Errorf("%s: want 2 cross datastore entries, got %d", rep.Self, n)
		}
	}
}
