package pbft_test

import (
	"testing"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
)

func TestViewChange(t *testing.T) {
	topo := config.Default()
	primary := topo.PrimaryOf(1, 0)
	faults := map[config.ServerID]pbft.FaultConfig{
		primary: {Alive: true, ByzantineLeader: true},
	}
	c := startClusterWithFaults(t, 1, faults)
	defer c.cleanup()
	for _, rep := range c.replicas {
		rep.PBFT.SetViewChangeTimeout(300 * time.Millisecond)
	}

	req := pbft.Request{ClientID: "vc", TS: 1, X: 5, Y: 7, Amt: 3}
	n := c.submitWithRetry(t, req)
	if n < c.topo.ClientQuorum() {
		t.Fatalf("client replies = %d, want >= %d", n, c.topo.ClientQuorum())
	}

	waitUntil(t, 10*time.Second, func() bool {
		return c.replicas[2].Store.GetBalance(5) == 7
	})

	assertBalancesOnAll(t, c, 5, 7)
	assertBalancesOnAll(t, c, 7, 13)
	assertLocksReleased(t, c, 5, 7)

	newPrimary := topo.PrimaryOf(1, 1)
	if c.replicas[newPrimary].PBFT.View() < 1 {
		t.Fatalf("expected view >= 1 on new primary %s, got %d", newPrimary, c.replicas[newPrimary].PBFT.View())
	}

	if log := c.replicas[primary].PBFT.PrintLog(); log == "" {
		t.Fatalf("Byzantine primary should log VIEW-CHANGE messages")
	}
}

func TestByzantineBackup(t *testing.T) {
	faults := map[config.ServerID]pbft.FaultConfig{
		3: {Alive: true, ByzantineBackup: true},
		4: {Alive: true, ByzantineBackup: true},
		5: {Alive: true, ByzantineBackup: true},
	}
	c := startClusterWithFaults(t, 1, faults)
	defer c.cleanup()

	req := pbft.Request{ClientID: "bb", TS: 1, X: 5, Y: 7, Amt: 3}
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
}

func TestNoConsensusSixFaulty(t *testing.T) {
	// With prepare quorum 2f+1=7, fewer than 7 honest replicas cannot commit.
	faults := map[config.ServerID]pbft.FaultConfig{
		3: {Alive: true, ByzantineBackup: true},
		4: {Alive: true, ByzantineBackup: true},
		5: {Alive: true, ByzantineBackup: true},
		6: {Alive: true, ByzantineBackup: true},
		7: {Alive: true, ByzantineBackup: true},
		8: {Alive: true, ByzantineBackup: true},
	}
	c := startClusterWithFaults(t, 1, faults)
	defer c.cleanup()

	req := pbft.Request{ClientID: "nf", TS: 1, X: 5, Y: 7, Amt: 3}
	c.submit(t, req)

	time.Sleep(5 * time.Second)
	if n := c.collector.MatchingCount(req, "committed"); n > 0 {
		t.Fatalf("expected no consensus with 6 Byzantine backups, got %d replies", n)
	}
	assertBalancesOnAll(t, c, 5, 10)
	assertBalancesOnAll(t, c, 7, 10)
}

func TestPrintStatusAndView(t *testing.T) {
	c := startCluster(t, 1)
	defer c.cleanup()

	req := pbft.Request{ClientID: "dbg", TS: 1, X: 5, Y: 7, Amt: 1}
	c.submit(t, req)
	waitForConsensus(t, c, req)
	waitUntil(t, 5*time.Second, func() bool {
		return c.replicas[1].PBFT.StatusCode(1) == "E"
	})

	status := c.replicas[1].PBFT.PrintStatus(1)
	if status == "" {
		t.Fatal("PrintStatus returned empty")
	}
	_ = c.replicas[1].PBFT.PrintView()
	_ = c.replicas[1].PBFT.PrintLog()
}
