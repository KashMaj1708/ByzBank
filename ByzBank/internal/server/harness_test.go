package server_test

import (
	"context"
	"io"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/client"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/crypto"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/server"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/twopc"
)

type harness struct {
	topo      config.Topology
	replicas  map[config.ServerID]*server.Replica
	replies   *pbft.ReplyCollector
	prepared  *twopc.PreparedCollector
	acks      *twopc.AckCollector
	driver    *client.Driver
	cancel    context.CancelFunc
}

type harnessOptions struct {
	clusters      []config.ClusterID
	disableCommit bool
}

func startHarness(t *testing.T, opts harnessOptions) *harness {
	t.Helper()
	if len(opts.clusters) == 0 {
		opts.clusters = []config.ClusterID{1, 2, 3}
	}

	topo := config.Default()
	keyDir := t.TempDir()
	if err := crypto.GenerateAllKeys(topo, keyDir); err != nil {
		t.Fatalf("generate keys: %v", err)
	}
	dataDir := t.TempDir()
	logger := log.New(io.Discard, "", 0)
	replies := &pbft.ReplyCollector{}
	prepared := twopc.NewPreparedCollector()
	acks := twopc.NewAckCollector()

	replicas := make(map[config.ServerID]*server.Replica)
	ctx, cancel := context.WithCancel(context.Background())

	for _, cluster := range opts.clusters {
		for _, srv := range topo.ServersInCluster(cluster) {
			ring, err := crypto.LoadKeyRing(topo, srv.ID, keyDir)
			if err != nil {
				cancel()
				t.Fatalf("load keys %s: %v", srv.ID, err)
			}
			rep, err := server.NewReplicaWithConfig(server.ReplicaConfig{
				Self:                  srv.ID,
				Topo:                  &topo,
				Ring:                  ring,
				Logger:                logger,
				ReplySink:             replies,
				PreparedCollector:     prepared,
				AckCollector:          acks,
				Disable2PCCommitPhase: opts.disableCommit,
				Enable2PC:             true,
				Fault:                 pbft.DefaultFaultConfig(),
				DataDir:               dataDir,
				Addr:                  "127.0.0.1:0",
			})
			if err != nil {
				cancel()
				t.Fatalf("start %s: %v", srv.ID, err)
			}
			patchAddr(&topo, srv.ID, rep.Addr())
			replicas[srv.ID] = rep
		}
	}

	for _, rep := range replicas {
		go rep.Run(ctx)
	}
	time.Sleep(2 * time.Second)

	injectors := make(map[config.ServerID]client.InjectFunc, len(replicas))
	for id, rep := range replicas {
		injectors[id] = rep.Hub.Inject
	}

	return &harness{
		topo:     topo,
		replicas: replicas,
		replies:  replies,
		prepared: prepared,
		acks:     acks,
		driver:   client.NewDriver(&topo, injectors, replies),
		cancel:   cancel,
	}
}

func (h *harness) cleanup() {
	h.cancel()
	for _, rep := range h.replicas {
		rep.Stop()
	}
}

func (h *harness) coordPrimary() *server.Replica {
	return h.replicas[h.topo.PrimaryOf(1, 0)]
}

func (h *harness) replica(cluster config.ClusterID, offset int) *server.Replica {
	servers := h.topo.ServersInCluster(cluster)
	if offset < 0 || offset >= len(servers) {
		panic("bad offset")
	}
	return h.replicas[servers[offset].ID]
}

func (h *harness) clusterReplicas(cluster config.ClusterID) []*server.Replica {
	out := make([]*server.Replica, 0, h.topo.ClusterSize)
	for _, srv := range h.topo.ServersInCluster(cluster) {
		out = append(out, h.replicas[srv.ID])
	}
	return out
}

func patchAddr(topo *config.Topology, id config.ServerID, addr string) {
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

func seedBalance(t *testing.T, h *harness, item int, credit int64) {
	t.Helper()
	cluster := h.topo.ClusterOf(item)
	for _, rep := range h.clusterReplicas(cluster) {
		if err := rep.Store.ApplyCreditOnly(item, credit); err != nil {
			t.Fatalf("seed item %d on %s: %v", item, rep.Self, err)
		}
	}
}

func assertBalanceAll(t *testing.T, h *harness, item int, want int64) {
	t.Helper()
	cluster := h.topo.ClusterOf(item)
	for _, rep := range h.clusterReplicas(cluster) {
		if got := rep.Store.GetBalance(item); got != want {
			t.Errorf("%s bal[%d]=%d want %d", rep.Self, item, got, want)
		}
	}
}

func assertLockedAll(t *testing.T, h *harness, item int, want bool) {
	t.Helper()
	cluster := h.topo.ClusterOf(item)
	for _, rep := range h.clusterReplicas(cluster) {
		if got := rep.Store.IsLocked(item); got != want {
			t.Errorf("%s item %d locked=%v want %v", rep.Self, item, got, want)
		}
	}
}

func assertWALAll(t *testing.T, h *harness, req pbft.Request, want bool) {
	t.Helper()
	txnID := pbft.TxnID(req)
	cluster := h.topo.ClusterOf(req.X)
	for _, rep := range h.clusterReplicas(cluster) {
		if got := rep.Store.WALExists(txnID); got != want {
			t.Errorf("%s WAL exists=%v want %v", rep.Self, got, want)
		}
	}
}

func waitCrossShardDone(t *testing.T, h *harness, req pbft.Request, timeout time.Duration) {
	t.Helper()
	waitUntil(t, timeout, func() bool {
		if h.driver.MatchingCount(req, "committed") < h.topo.ClientQuorum() {
			return false
		}
		for _, rep := range h.clusterReplicas(h.topo.ClusterOf(req.X)) {
			if rep.Store.IsLocked(req.X) {
				return false
			}
		}
		if !h.topo.SameCluster(req.X, req.Y) {
			for _, rep := range h.clusterReplicas(h.topo.ClusterOf(req.Y)) {
				if rep.Store.IsLocked(req.Y) {
					return false
				}
			}
		}
		return true
	})
}

func crossDatastoreEntries(t *testing.T, rep *server.Replica, req pbft.Request) int {
	t.Helper()
	entries, err := rep.Store.Datastore()
	if err != nil {
		t.Fatalf("datastore %s: %v", rep.Self, err)
	}
	n := 0
	for _, e := range entries {
		if e.Type == store.TxnCross && e.X == req.X && e.Y == req.Y && e.Amt == req.Amt {
			n++
		}
	}
	return n
}
