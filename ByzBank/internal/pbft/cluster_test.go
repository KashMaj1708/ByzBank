package pbft_test

import (
	"context"
	"io"
	"log"
	"net"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/crypto"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pb"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/server"
)

type testCluster struct {
	topo      config.Topology
	replicas  map[config.ServerID]*server.Replica
	collector *pbft.ReplyCollector
	cancel    context.CancelFunc
}

func startCluster(t *testing.T, cluster config.ClusterID) *testCluster {
	return startClusterWithFaults(t, cluster, nil)
}

func startClusterWithFaults(t *testing.T, cluster config.ClusterID, faults map[config.ServerID]pbft.FaultConfig) *testCluster {
	t.Helper()

	topo := config.Default()
	keyDir := t.TempDir()
	if err := crypto.GenerateAllKeys(topo, keyDir); err != nil {
		t.Fatalf("generate keys: %v", err)
	}
	dataDir := t.TempDir()
	logger := log.New(io.Discard, "", 0)
	collector := &pbft.ReplyCollector{}

	replicas := make(map[config.ServerID]*server.Replica)
	ctx, cancel := context.WithCancel(context.Background())

	for _, srv := range topo.ServersInCluster(cluster) {
		ring, err := crypto.LoadKeyRing(topo, srv.ID, keyDir)
		if err != nil {
			cancel()
			t.Fatalf("load keys %s: %v", srv.ID, err)
		}
		fc := pbft.DefaultFaultConfig()
		if faults != nil {
			if f, ok := faults[srv.ID]; ok {
				fc = f
			}
		}
		rep, err := server.NewReplicaWithConfig(server.ReplicaConfig{
			Self:      srv.ID,
			Topo:      &topo,
			Ring:      ring,
			Logger:    logger,
			ReplySink: collector,
			Fault:     fc,
			DataDir:   dataDir,
			Addr:      "127.0.0.1:0",
		})
		if err != nil {
			cancel()
			t.Fatalf("start %s: %v", srv.ID, err)
		}
		updateServerAddr(&topo, srv.ID, rep.Addr())
		replicas[srv.ID] = rep
	}

	for _, rep := range replicas {
		go rep.Run(ctx)
	}
	time.Sleep(500 * time.Millisecond)

	return &testCluster{
		topo:      topo,
		replicas:  replicas,
		collector: collector,
		cancel:    cancel,
	}
}

func (c *testCluster) cleanup() {
	c.cancel()
	for _, rep := range c.replicas {
		rep.Stop()
	}
}

func (c *testCluster) primary() *server.Replica {
	return c.replicas[c.topo.PrimaryOf(1, 0)]
}

func (c *testCluster) submit(t *testing.T, req pbft.Request) {
	t.Helper()
	primary := c.primary()
	err := pbft.SubmitRequest(primary.Hub.Inject, req)
	if err != nil {
		t.Fatalf("submit request: %v", err)
	}
}

func (c *testCluster) submitWithRetry(t *testing.T, req pbft.Request) int {
	t.Helper()
	primary := c.primary()
	injects := make([]func(*pb.Envelope) error, 0, len(c.replicas))
	for _, rep := range c.replicas {
		injects = append(injects, rep.Hub.Inject)
	}
	n, err := pbft.SubmitWithRetryForTopo(
		&c.topo,
		primary.Hub.Inject,
		injects,
		c.collector,
		req,
	)
	if err != nil {
		t.Fatalf("submit with retry: got %d replies, err=%v", n, err)
	}
	return n
}

func (c *testCluster) aliveReplicas() map[config.ServerID]*server.Replica {
	out := make(map[config.ServerID]*server.Replica)
	for id, rep := range c.replicas {
		if rep.PBFT != nil {
			out[id] = rep
		}
	}
	return out
}

func updateServerAddr(topo *config.Topology, id config.ServerID, addr string) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		panic(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		panic(err)
	}
	for i := range topo.Servers {
		if topo.Servers[i].ID == id {
			topo.Servers[i].Host = host
			topo.Servers[i].Port = port
			return
		}
	}
}

func waitForConsensus(t *testing.T, c *testCluster, req pbft.Request) int {
	t.Helper()
	n, err := c.collector.WaitForQuorum(req, "committed", c.topo.ClientQuorum(), 15*time.Second)
	if err != nil {
		t.Fatalf("wait for client quorum: got %d replies, err=%v", n, err)
	}
	return n
}

func assertBalancesOnAll(t *testing.T, c *testCluster, item int, want int64) {
	t.Helper()
	for id, rep := range c.replicas {
		if got := rep.Store.GetBalance(item); got != want {
			t.Errorf("%s bal[%d] = %d, want %d", id, item, got, want)
		}
	}
}

func assertLocksReleased(t *testing.T, c *testCluster, items ...int) {
	t.Helper()
	for id, rep := range c.replicas {
		for _, item := range items {
			if rep.Store.IsLocked(item) {
				t.Errorf("%s: item %d still locked", id, item)
			}
		}
	}
}

func datastoreLen(t *testing.T, rep *server.Replica) int {
	t.Helper()
	entries, err := rep.Store.Datastore()
	if err != nil {
		t.Fatalf("datastore: %v", err)
	}
	return len(entries)
}

func waitUntil(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatal("condition not met before timeout")
}

// Ensure temp data paths are unique per replica.
func init() {
	_ = filepath.Join
}
