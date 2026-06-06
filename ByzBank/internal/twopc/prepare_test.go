package twopc_test

import (
	"context"
	"io"
	"log"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/crypto"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/server"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/twopc"
)

type dualHarness struct {
	topo      config.Topology
	replicas  map[config.ServerID]*server.Replica
	replies   *pbft.ReplyCollector
	prepared  *twopc.PreparedCollector
	acks      *twopc.AckCollector
	cancel    context.CancelFunc
}

func startClusters(t *testing.T, clusters ...config.ClusterID) *dualHarness {
	return startClustersWithOptions(t, false, clusters...)
}

func startClustersPrepareOnly(t *testing.T, clusters ...config.ClusterID) *dualHarness {
	return startClustersWithOptions(t, true, clusters...)
}

func startClustersWithOptions(t *testing.T, prepareOnly bool, clusters ...config.ClusterID) *dualHarness {
	t.Helper()
	if len(clusters) == 0 {
		clusters = []config.ClusterID{1, 2}
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

	for _, cluster := range clusters {
		for _, srv := range topo.ServersInCluster(cluster) {
			ring, err := crypto.LoadKeyRing(topo, srv.ID, keyDir)
			if err != nil {
				cancel()
				t.Fatalf("load keys %s: %v", srv.ID, err)
			}
			rep, err := server.NewReplicaWithConfig(server.ReplicaConfig{
				Self:              srv.ID,
				Topo:              &topo,
				Ring:              ring,
				Logger:            logger,
				ReplySink:         replies,
				PreparedCollector: prepared,
				AckCollector:          acks,
				Disable2PCCommitPhase: prepareOnly,
				Enable2PC:             true,
				Fault:             pbft.DefaultFaultConfig(),
				DataDir:           dataDir,
				Addr:              "127.0.0.1:0",
			})
			if err != nil {
				cancel()
				t.Fatalf("start %s: %v", srv.ID, err)
			}
			updateAddr(&topo, srv.ID, rep.Addr())
			replicas[srv.ID] = rep
		}
	}

	for _, cluster := range clusters {
		for _, srv := range topo.ServersInCluster(cluster) {
			go replicas[srv.ID].Run(ctx)
		}
	}
	time.Sleep(2 * time.Second)

	return &dualHarness{
		topo:     topo,
		replicas: replicas,
		replies:  replies,
		prepared: prepared,
		acks:     acks,
		cancel:   cancel,
	}
}

func (h *dualHarness) cleanup() {
	h.cancel()
	for _, rep := range h.replicas {
		rep.Stop()
	}
}

func (h *dualHarness) coordPrimary() *server.Replica {
	return h.replicas[h.topo.PrimaryOf(1, 0)]
}

func (h *dualHarness) submitToCoord(t *testing.T, req pbft.Request) {
	t.Helper()
	primary := h.coordPrimary()
	if err := pbft.SubmitRequest(primary.Hub.Inject, req); err != nil {
		t.Fatalf("submit: %v", err)
	}
}

func updateAddr(topo *config.Topology, id config.ServerID, addr string) {
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

func startDualClusters(t *testing.T) *dualHarness {
	return startClusters(t, 1, 2)
}

func TestCrossShardPreparePhase(t *testing.T) {
	h := startClustersPrepareOnly(t, 1, 2)
	defer h.cleanup()

	req := pbft.Request{ClientID: "c1", TS: 1, X: 5, Y: 1500, Amt: 4}
	h.submitToCoord(t, req)

	waitUntil(t, 20*time.Second, func() bool {
		return h.replicas[1].Store.GetBalance(5) == 6
	})
	waitUntil(t, 20*time.Second, func() bool {
		return h.replicas[13].Store.GetBalance(1500) == 14
	})

	for _, id := range clusterServerIDs(h.topo, 1) {
		rep := h.replicas[id]
		if got := rep.Store.GetBalance(5); got != 6 {
			t.Errorf("C1 %s bal[5]=%d want 6", id, got)
		}
		if !rep.Store.IsLocked(5) {
			t.Errorf("C1 %s: x should stay locked after prepare", id)
		}
		if !rep.Store.WALExists(pbft.TxnID(req)) {
			t.Errorf("C1 %s: WAL missing", id)
		}
		entries, err := rep.Store.Datastore()
		if err != nil {
			t.Fatalf("datastore %s: %v", id, err)
		}
		if len(entries) != 1 || entries[0].Phase != store.PhasePrepare {
			t.Errorf("C1 %s: want one prepare datastore entry, got %v", id, entries)
		}
	}
	for _, id := range clusterServerIDs(h.topo, 2) {
		rep := h.replicas[id]
		if got := rep.Store.GetBalance(1500); got != 14 {
			t.Errorf("C2 %s bal[1500]=%d want 14", id, got)
		}
		if !rep.Store.IsLocked(1500) {
			t.Errorf("C2 %s: y should stay locked after prepare", id)
		}
		if !rep.Store.WALExists(pbft.TxnID(req)) {
			t.Errorf("C2 %s: WAL missing", id)
		}
		entries, err := rep.Store.Datastore()
		if err != nil {
			t.Fatalf("datastore %s: %v", id, err)
		}
		if len(entries) != 1 || entries[0].Phase != store.PhasePrepare {
			t.Errorf("C2 %s: want one prepare datastore entry, got %v", id, entries)
		}
	}

	if reply, ok := h.prepared.WaitForOne(req, store.OutcomeCommit, 1*time.Second); !ok {
		t.Fatal("coordinator did not receive participant prepared certificate")
	} else if !twopc.VerifyClusterCert(
		h.replicas[1].Ring, &h.topo, 2, reply.CommitCert, "COMMIT", h.topo.Quorum(),
	) {
		t.Fatal("participant certificate failed verification")
	}

	if n := h.replies.MatchingCount(req, "committed"); n > 0 {
		t.Fatalf("client should not receive reply during prepare phase, got %d", n)
	}
}

func TestCrossShardInsufficientBalanceIgnored(t *testing.T) {
	h := startDualClusters(t)
	defer h.cleanup()

	req := pbft.Request{ClientID: "c1", TS: 1, X: 5, Y: 1500, Amt: 100}
	h.submitToCoord(t, req)
	time.Sleep(4 * time.Second)

	for _, id := range clusterServerIDs(h.topo, 1) {
		if got := h.replicas[id].Store.GetBalance(5); got != 10 {
			t.Errorf("C1 %s bal[5]=%d want 10", id, got)
		}
	}
	for _, id := range clusterServerIDs(h.topo, 2) {
		if got := h.replicas[id].Store.GetBalance(1500); got != 10 {
			t.Errorf("C2 %s bal[1500]=%d want 10", id, got)
		}
	}
}

func TestCrossShardParticipantAbort(t *testing.T) {
	h := startClustersPrepareOnly(t, 1, 2)
	defer h.cleanup()

	for _, id := range clusterServerIDs(h.topo, 2) {
		if !h.replicas[id].Store.AcquireLock(1500, 99) {
			t.Fatalf("failed to pre-lock y on %s", id)
		}
	}

	req := pbft.Request{ClientID: "c1", TS: 1, X: 5, Y: 1500, Amt: 4}
	h.submitToCoord(t, req)

	waitUntil(t, 20*time.Second, func() bool {
		_, ok := h.prepared.WaitForOne(req, store.OutcomeAbort, 50*time.Millisecond)
		return ok
	})

	if reply, ok := h.prepared.WaitForOne(req, store.OutcomeAbort, 1*time.Second); !ok {
		t.Fatal("coordinator did not receive participant abort certificate")
	} else if !twopc.VerifyClusterCert(
		h.replicas[1].Ring, &h.topo, 2, reply.CommitCert, "COMMIT", h.topo.Quorum(),
	) {
		t.Fatal("abort certificate failed verification")
	}

	for _, id := range clusterServerIDs(h.topo, 1) {
		rep := h.replicas[id]
		if got := rep.Store.GetBalance(5); got != 6 {
			t.Errorf("C1 %s bal[5]=%d want 6 after coord prepare", id, got)
		}
		if !rep.Store.IsLocked(5) {
			t.Errorf("C1 %s: x should remain locked", id)
		}
	}
	for _, id := range clusterServerIDs(h.topo, 2) {
		if got := h.replicas[id].Store.GetBalance(1500); got != 10 {
			t.Errorf("C2 %s bal[1500]=%d want 10 after participant abort", id, got)
		}
	}
}

func clusterServerIDs(topo config.Topology, cluster config.ClusterID) []config.ServerID {
	ids := make([]config.ServerID, 0, topo.ClusterSize)
	for _, srv := range topo.ServersInCluster(cluster) {
		ids = append(ids, srv.ID)
	}
	return ids
}
