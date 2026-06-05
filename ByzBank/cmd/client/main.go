// Command client is the single client driver and interactive menu.
//
// Phase 0 only ships a --healthcheck mode that probes every server's /health
// endpoint, so the bring-up/tear-down harness can be verified end to end. The
// CSV test-case runner and interactive menu are added in later phases.
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
)

func main() {
	health := flag.Bool("healthcheck", false, "probe every server's /health endpoint and report")
	flag.Parse()

	topo := config.Load()

	if *health {
		os.Exit(runHealthcheck(topo))
	}

	fmt.Printf("2pcbyz client (Phase 0 stub)\n")
	fmt.Printf("topology: %d clusters x %d nodes = %d servers, f=%d, quorum=%d\n",
		topo.NumClusters, topo.ClusterSize, topo.TotalServers(), topo.F(), topo.Quorum())
	fmt.Printf("run with --healthcheck to probe all servers\n")
}

func runHealthcheck(topo config.Topology) int {
	client := &http.Client{Timeout: 2 * time.Second}
	up, down := 0, 0
	for _, s := range topo.Servers {
		// Servers expose HTTP health on grpc_port+10000 (Phase 1).
		url := fmt.Sprintf("http://%s:%d/health", s.Host, s.Port+10000)
		resp, err := client.Get(url)
		if err != nil {
			fmt.Printf("  %-4s %-20s DOWN (%v)\n", s.ID, s.Addr(), err)
			down++
			continue
		}
		resp.Body.Close()
		if resp.StatusCode == http.StatusOK {
			fmt.Printf("  %-4s %-20s UP\n", s.ID, s.Addr())
			up++
		} else {
			fmt.Printf("  %-4s %-20s BAD (%d)\n", s.ID, s.Addr(), resp.StatusCode)
			down++
		}
	}
	fmt.Printf("\nhealthcheck: %d up, %d down (of %d)\n", up, down, topo.TotalServers())
	if down > 0 {
		return 1
	}
	return 0
}
