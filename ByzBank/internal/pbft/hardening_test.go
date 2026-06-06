package pbft_test

import (
	"testing"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
)

func TestNoQuorumSixLiveNoClientReply(t *testing.T) {
	topo := config.Default()
	faults := make(map[config.ServerID]pbft.FaultConfig)
	for _, srv := range topo.ServersInCluster(1) {
		if srv.ID > 6 {
			faults[srv.ID] = pbft.FaultConfig{Alive: false}
		}
	}
	c := startClusterWithFaults(t, 1, faults)
	defer c.cleanup()

	req := pbft.Request{ClientID: "no-quorum", TS: 1, X: 10, Y: 20, Amt: 1}
	c.submit(t, req)
	time.Sleep(3 * time.Second)

	if n := c.collector.MatchingCount(req, "committed"); n > 0 {
		t.Fatalf("intra txn committed with only 6 live replicas (need 7), got %d replies", n)
	}
	for _, srv := range topo.ServersInCluster(1) {
		if srv.ID <= 6 {
			if got := c.replicas[srv.ID].Store.GetBalance(10); got != 10 {
				t.Errorf("%s bal[10]=%d want 10 (no commit)", srv.ID, got)
			}
		}
	}
}
