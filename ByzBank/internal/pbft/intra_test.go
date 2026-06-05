package pbft_test

import (
	"testing"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
)

func TestIntraShardHappy(t *testing.T) {
	c := startCluster(t, 1)
	defer c.cleanup()

	req := pbft.Request{ClientID: "c1", TS: 1, X: 5, Y: 7, Amt: 3}
	c.submit(t, req)

	n := waitForConsensus(t, c, req)
	if n < c.topo.ClientQuorum() {
		t.Fatalf("client replies = %d, want >= %d", n, c.topo.ClientQuorum())
	}

	waitUntil(t, 5*time.Second, func() bool {
		return c.replicas[1].Store.GetBalance(5) == 7
	})

	assertBalancesOnAll(t, c, 5, 7)
	assertBalancesOnAll(t, c, 7, 13)
	assertLocksReleased(t, c, 5, 7)
}

func TestInsufficientBalanceIgnored(t *testing.T) {
	c := startCluster(t, 1)
	defer c.cleanup()

	first := pbft.Request{ClientID: "c1", TS: 1, X: 5, Y: 7, Amt: 3}
	c.submit(t, first)
	waitForConsensus(t, c, first)
	waitUntil(t, 5*time.Second, func() bool {
		return c.replicas[1].Store.GetBalance(5) == 7
	})

	beforeDS := datastoreLen(t, c.replicas[1])
	second := pbft.Request{ClientID: "c1", TS: 2, X: 5, Y: 8, Amt: 100}
	c.submit(t, second)

	time.Sleep(2 * time.Second)

	if n := c.collector.MatchingCount(second, "committed"); n > 0 {
		t.Fatalf("insufficient txn should not commit, got %d replies", n)
	}
	assertBalancesOnAll(t, c, 5, 7)
	assertBalancesOnAll(t, c, 8, 10)
	if after := datastoreLen(t, c.replicas[1]); after != beforeDS {
		t.Fatalf("datastore grew from %d to %d on ignored txn", beforeDS, after)
	}
}

func TestAllReplicasExecuteIdenticalState(t *testing.T) {
	c := startCluster(t, 1)
	defer c.cleanup()

	req := pbft.Request{ClientID: "c1", TS: 1, X: 100, Y: 501, Amt: 8}
	c.submit(t, req)
	waitForConsensus(t, c, req)
	waitUntil(t, 5*time.Second, func() bool {
		return c.replicas[1].Store.GetBalance(100) == 2
	})

	for id, rep := range c.replicas {
		if got := rep.Store.GetBalance(100); got != 2 {
			t.Errorf("%s bal[100]=%d want 2", id, got)
		}
		if got := rep.Store.GetBalance(501); got != 18 {
			t.Errorf("%s bal[501]=%d want 18", id, got)
		}
		entries, err := rep.Store.Datastore()
		if err != nil {
			t.Fatalf("%s datastore: %v", id, err)
		}
		if len(entries) != 1 {
			t.Errorf("%s datastore len=%d want 1", id, len(entries))
		}
	}
}

func TestClientQuorumFourMatchingReplies(t *testing.T) {
	c := startCluster(t, 1)
	defer c.cleanup()

	req := pbft.Request{ClientID: "c-quorum", TS: 42, X: 10, Y: 20, Amt: 1}
	c.submit(t, req)
	n := waitForConsensus(t, c, req)
	if n < 4 {
		t.Fatalf("got %d matching replies, want >= 4 (f+1)", n)
	}
	if n > c.topo.ClusterSize {
		t.Fatalf("got %d replies, impossible for cluster size %d", n, c.topo.ClusterSize)
	}
}

func TestLocksReleasedAfterCommit(t *testing.T) {
	c := startCluster(t, 1)
	defer c.cleanup()

	req := pbft.Request{ClientID: "c1", TS: 1, X: 50, Y: 60, Amt: 2}
	c.submit(t, req)
	waitForConsensus(t, c, req)
	waitUntil(t, 5*time.Second, func() bool {
		return !c.replicas[1].Store.IsLocked(50)
	})
	assertLocksReleased(t, c, 50, 60)
}

func TestSequentialTransactions(t *testing.T) {
	c := startCluster(t, 1)
	defer c.cleanup()

	r1 := pbft.Request{ClientID: "c1", TS: 1, X: 5, Y: 7, Amt: 3}
	c.submit(t, r1)
	waitForConsensus(t, c, r1)
	waitUntil(t, 5*time.Second, func() bool {
		return c.replicas[1].Store.GetBalance(5) == 7
	})

	r2 := pbft.Request{ClientID: "c1", TS: 2, X: 7, Y: 5, Amt: 1}
	c.submit(t, r2)
	waitForConsensus(t, c, r2)
	waitUntil(t, 5*time.Second, func() bool {
		return c.replicas[1].Store.GetBalance(7) == 12
	})

	assertBalancesOnAll(t, c, 5, 8)
	assertBalancesOnAll(t, c, 7, 12)
	for _, rep := range c.replicas {
		if n := datastoreLen(t, rep); n != 2 {
			t.Errorf("datastore len=%d want 2", n)
		}
	}
}

func TestCrossShardRequestIgnored(t *testing.T) {
	c := startCluster(t, 1)
	defer c.cleanup()

	req := pbft.Request{ClientID: "c1", TS: 1, X: 5, Y: 1500, Amt: 4}
	c.submit(t, req)
	time.Sleep(2 * time.Second)

	if n := c.collector.MatchingCount(req, "committed"); n > 0 {
		t.Fatalf("cross-shard request should not commit on C1 primary, got %d replies", n)
	}
	assertBalancesOnAll(t, c, 5, 10)
	assertBalancesOnAll(t, c, 1500, 10)
}

func TestReplayAttackIgnored(t *testing.T) {
	c := startCluster(t, 1)
	defer c.cleanup()

	req := pbft.Request{ClientID: "c1", TS: 10, X: 5, Y: 7, Amt: 1}
	c.submit(t, req)
	waitForConsensus(t, c, req)
	waitUntil(t, 5*time.Second, func() bool {
		return c.replicas[1].Store.GetBalance(5) == 9
	})

	c.submit(t, req) // same TS replay
	time.Sleep(1500 * time.Millisecond)

	assertBalancesOnAll(t, c, 5, 9)
	assertBalancesOnAll(t, c, 7, 11)
	if n := c.collector.MatchingCount(req, "committed"); n != 12 {
		// exactly one round of 12 executions, not 24
		if n > 12 {
			t.Fatalf("replay produced extra replies: %d", n)
		}
	}
}

func TestInitialBalancesUntouchedOnIgnoredTxn(t *testing.T) {
	c := startCluster(t, 1)
	defer c.cleanup()

	req := pbft.Request{ClientID: "c1", TS: 1, X: 5, Y: 8, Amt: 50}
	c.submit(t, req)
	time.Sleep(2 * time.Second)

	for id, rep := range c.replicas {
		if rep.Store.GetBalance(5) != 10 || rep.Store.GetBalance(8) != 10 {
			t.Errorf("%s balances changed on ignored txn", id)
		}
	}
}

func TestDigestStability(t *testing.T) {
	req := pbft.Request{ClientID: "a", TS: 1, X: 1, Y: 2, Amt: 3}
	d1 := pbft.Digest(req)
	d2 := pbft.Digest(req)
	if string(d1) != string(d2) {
		t.Fatal("digest not stable")
	}
	req2 := req
	req2.Amt = 4
	if string(pbft.Digest(req2)) == string(d1) {
		t.Fatal("digest should change when request changes")
	}
}

func TestTopologyQuorumsUsed(t *testing.T) {
	topo := config.Default()
	if topo.F() != 3 {
		t.Fatalf("f=%d want 3", topo.F())
	}
	if topo.CollectorQuorum() != 9 {
		t.Fatalf("collector=%d want 9", topo.CollectorQuorum())
	}
	if topo.Quorum() != 7 {
		t.Fatalf("quorum=%d want 7", topo.Quorum())
	}
	if topo.ClientQuorum() != 4 {
		t.Fatalf("client quorum=%d want 4", topo.ClientQuorum())
	}
}
