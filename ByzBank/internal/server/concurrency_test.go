package server_test

import (
	"testing"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
)

func TestConcurrentContention(t *testing.T) {
	t.Run("DisjointCrossShardBothCommit", func(t *testing.T) {
		h := startHarness(t, harnessOptions{clusters: []config.ClusterID{1, 2, 3}})
		defer h.cleanup()

		// Disjoint cluster pairs: C1→C2 and C3→C1 (no shared coordinator).
		req1 := pbft.Request{ClientID: "d1", TS: 1, X: 5, Y: 1500, Amt: 2}
		req2 := pbft.Request{ClientID: "d2", TS: 1, X: 2005, Y: 10, Amt: 3}

		if err := h.driver.FireOpenLoop([]pbft.Request{req1, req2}); err != nil {
			t.Fatalf("fire open-loop: %v", err)
		}
		waitCrossShardDone(t, h, req1, 45*time.Second)
		waitCrossShardDone(t, h, req2, 45*time.Second)

		assertBalanceAll(t, h, 5, 8)
		assertBalanceAll(t, h, 10, 13)
		assertBalanceAll(t, h, 1500, 12)
		assertBalanceAll(t, h, 2005, 7)
		assertLockedAll(t, h, 5, false)
		assertLockedAll(t, h, 10, false)
		assertLockedAll(t, h, 1500, false)
		assertLockedAll(t, h, 2005, false)
	})

	t.Run("SameSenderItemSerializes", func(t *testing.T) {
		h := startHarness(t, harnessOptions{clusters: []config.ClusterID{1, 2}})
		defer h.cleanup()

		req1 := pbft.Request{ClientID: "win", TS: 1, X: 5, Y: 1500, Amt: 4}
		req2 := pbft.Request{ClientID: "lose", TS: 2, X: 5, Y: 1600, Amt: 3}

		h.driver.FireConcurrent([]pbft.Request{req1, req2})

		waitCrossShardDone(t, h, req1, 45*time.Second)
		waitCrossShardDone(t, h, req2, 45*time.Second)

		if h.driver.MatchingCount(req1, "committed") < h.topo.ClientQuorum() {
			t.Fatal("first transaction did not commit")
		}
		if h.driver.MatchingCount(req2, "committed") < h.topo.ClientQuorum() {
			t.Fatal("second transaction did not commit after waiting for lock")
		}
		assertBalanceAll(t, h, 5, 3)
		assertLockedAll(t, h, 5, false)
	})

	t.Run("CrossShardLockBlocksIntraOnSameItem", func(t *testing.T) {
		h := startHarness(t, harnessOptions{clusters: []config.ClusterID{1, 2}, disableCommit: true})
		defer h.cleanup()

		cross := pbft.Request{ClientID: "cross", TS: 1, X: 5, Y: 1500, Amt: 4}
		intra := pbft.Request{ClientID: "intra", TS: 1, X: 5, Y: 6, Amt: 2}

		if err := h.driver.Fire(cross); err != nil {
			t.Fatalf("fire cross: %v", err)
		}
		waitUntil(t, 30*time.Second, func() bool {
			return h.replica(1, 0).Store.GetBalance(5) == 6
		})
		assertLockedAll(t, h, 5, true)

		if err := h.driver.Fire(intra); err != nil {
			t.Fatalf("fire intra: %v", err)
		}
		time.Sleep(3 * time.Second)

		if n := h.driver.MatchingCount(intra, "committed"); n > 0 {
			t.Fatalf("intra should be ignored while x is locked, got %d replies", n)
		}
		assertBalanceAll(t, h, 5, 6)
		assertBalanceAll(t, h, 6, 10)
	})
}
