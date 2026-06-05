// Command server boots a single replica process with signed gRPC transport.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/crypto"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/server"
)

func main() {
	idFlag := flag.String("id", "", "server id, e.g. S1")
	keysDir := flag.String("keys", "config/keys", "directory containing per-server ed25519 key JSON files")
	flag.Parse()

	if *idFlag == "" {
		log.Fatal("missing required --id flag (e.g. --id S1)")
	}

	id, err := config.ParseServerID(*idFlag)
	if err != nil {
		log.Fatalf("bad --id: %v", err)
	}

	topo := config.Load()
	self, ok := topo.ServerByID(id)
	if !ok {
		log.Fatalf("server %s not found in topology (have %d servers)", id, topo.TotalServers())
	}

	logger := log.New(os.Stdout, fmt.Sprintf("[%s] ", self.ID), log.LstdFlags|log.Lmsgprefix)

	ring, err := crypto.LoadOrCreateKeyRing(topo, self.ID, *keysDir)
	if err != nil {
		logger.Fatalf("load keys: %v", err)
	}

	replica, err := server.NewReplica(self.ID, &topo, ring, logger)
	if err != nil {
		logger.Fatalf("start replica: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go replica.Run(ctx)

	// Keep a lightweight HTTP health endpoint on port+10000 so the Phase 0
	// orchestration harness can still probe liveness without speaking gRPC.
	healthAddr := fmt.Sprintf("%s:%d", self.Host, self.Port+10000)
	healthMux := http.NewServeMux()
	healthMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "OK %s cluster=%s grpc=%s\n", self.ID, self.Cluster, replica.Addr())
	})
	healthSrv := &http.Server{Addr: healthAddr, Handler: healthMux}
	go func() {
		ln, err := net.Listen("tcp", healthAddr)
		if err != nil {
			logger.Printf("health listener error: %v", err)
			return
		}
		if err := healthSrv.Serve(ln); err != nil && err != http.ErrServerClosed {
			logger.Printf("health serve error: %v", err)
		}
	}()

	logger.Printf("%s gRPC listening on %s (cluster %s, f=%d, quorum=%d); health on %s",
		self.ID, replica.Addr(), self.Cluster, topo.F(), topo.Quorum(), healthAddr)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop
	logger.Printf("shutdown signal received, draining...")

	cancel()
	replica.Stop()

	hctx, hcancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer hcancel()
	_ = healthSrv.Shutdown(hctx)

	logger.Printf("%s stopped cleanly", self.ID)
}
