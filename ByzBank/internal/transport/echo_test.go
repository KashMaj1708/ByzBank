package transport_test

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
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/server"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/transport"
)

func TestSignedEcho(t *testing.T) {
	topo := config.Default()
	if topo.TotalServers() != 36 || topo.ClusterSize != 12 {
		t.Fatalf("expected 36-server topology, got %d servers (cluster size %d)",
			topo.TotalServers(), topo.ClusterSize)
	}

	keyDir := t.TempDir()
	if err := crypto.GenerateAllKeys(topo, keyDir); err != nil {
		t.Fatalf("generate keys: %v", err)
	}

	env := startClusterReplicas(t, &topo, keyDir, 1)
	defer env.cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	primary := env.replicas[1]
	ping := transport.NewEnvelope(1, transport.TypePing, []byte("phase1-echo"))
	primary.Hub.Sign(ping)

	if err := primary.BroadcastCluster(ctx, ping); err != nil {
		t.Fatalf("broadcast PING: %v", err)
	}

	wantPongs := topo.ClusterSize - 1 // S2..S12
	seen := make(map[config.ServerID]struct{}, wantPongs)
	deadline := time.After(10 * time.Second)

	for len(seen) < wantPongs {
		select {
		case pong := <-primary.PongWait():
			from := config.ServerID(pong.SenderId)
			if pong.Type != transport.TypePong {
				t.Fatalf("expected PONG, got %q from %s", pong.Type, from)
			}
			if !primary.Hub.Verify(pong) {
				t.Fatalf("invalid PONG signature from %s", from)
			}
			if from == 1 {
				t.Fatalf("replica should not pong itself")
			}
			if _, dup := seen[from]; dup {
				t.Fatalf("duplicate PONG from %s", from)
			}
			seen[from] = struct{}{}
		case <-deadline:
			t.Fatalf("timed out waiting for PONGs: got %d, want %d (missing backups)", len(seen), wantPongs)
		}
	}

	// Corrupted signature must be rejected at delivery time.
	bad := transport.NewEnvelope(2, transport.TypePing, []byte("bad"))
	primary.Hub.Sign(bad)
	if len(bad.Signature) > 0 {
		bad.Signature[0] ^= 0xff
	}
	if err := env.replicas[2].Hub.Deliver(bad); err == nil {
		t.Fatal("corrupted signature should be rejected")
	}
}

type clusterEnv struct {
	replicas map[config.ServerID]*server.Replica
	cancel   context.CancelFunc
	cleanup  func()
}

func startClusterReplicas(t *testing.T, topo *config.Topology, keyDir string, cluster config.ClusterID) *clusterEnv {
	t.Helper()

	logger := log.New(io.Discard, "", 0)
	replicas := make(map[config.ServerID]*server.Replica)
	ctx, cancel := context.WithCancel(context.Background())

	for _, srv := range topo.ServersInCluster(cluster) {
		ring, err := crypto.LoadKeyRing(*topo, srv.ID, keyDir)
		if err != nil {
			cancel()
			t.Fatalf("load keys for %s: %v", srv.ID, err)
		}
		rep, err := server.NewReplicaWithConfig(server.ReplicaConfig{
			Self:    srv.ID,
			Topo:    topo,
			Ring:    ring,
			Logger:  logger,
			DataDir: keyDir,
			Addr:    "127.0.0.1:0",
		})
		if err != nil {
			cancel()
			t.Fatalf("start %s: %v", srv.ID, err)
		}
		updateServerAddr(topo, srv.ID, rep.Addr())
		replicas[srv.ID] = rep
	}

	for _, rep := range replicas {
		go rep.Run(ctx)
	}

	return &clusterEnv{
		replicas: replicas,
		cancel:   cancel,
		cleanup: func() {
			cancel()
			for _, rep := range replicas {
				rep.Stop()
			}
		},
	}
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
