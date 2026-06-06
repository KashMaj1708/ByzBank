// Package client implements the open-loop transaction driver used by tests and
// the interactive client (Phase 8). A single process fires requests without
// waiting for prior transactions to finish; servers enforce per-client
// last-timestamp ordering.
package client

import (
	"fmt"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pb"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
)

// InjectFunc delivers a signed envelope into one replica's inbound queue.
type InjectFunc func(*pb.Envelope) error

// Driver fires client requests open-loop to coordinator primaries.
type Driver struct {
	topo       *config.Topology
	injectors  map[config.ServerID]InjectFunc
	collector  *pbft.ReplyCollector
	view       int
}

// NewDriver constructs an open-loop driver. injectors must cover every server
// the driver may target (typically all replicas in the harness).
func NewDriver(topo *config.Topology, injectors map[config.ServerID]InjectFunc, collector *pbft.ReplyCollector) *Driver {
	return &Driver{
		topo:      topo,
		injectors: injectors,
		collector: collector,
		view:      0,
	}
}

// SetView updates the view used for primary lookup (view-change tests).
func (d *Driver) SetView(view int) { d.view = view }

// Fire sends one request to the primary of ClusterOf(x) without waiting.
func (d *Driver) Fire(req pbft.Request) error {
	cluster := d.topo.ClusterOf(req.X)
	primary := d.topo.PrimaryOf(cluster, d.view)
	inject, ok := d.injectors[primary]
	if !ok {
		return fmt.Errorf("no injector for primary %s", primary)
	}
	return pbft.SubmitRequest(inject, req)
}

// FireOpenLoop submits every request in order without waiting between them.
func (d *Driver) FireOpenLoop(reqs []pbft.Request) error {
	for _, req := range reqs {
		if err := d.Fire(req); err != nil {
			return err
		}
	}
	return nil
}

// FireConcurrent submits requests in separate goroutines (stress contention).
func (d *Driver) FireConcurrent(reqs []pbft.Request) {
	for _, req := range reqs {
		req := req
		go func() { _ = d.Fire(req) }()
	}
}

// WaitFor blocks until f+1 matching replies arrive.
func (d *Driver) WaitFor(req pbft.Request, result string, timeout time.Duration) (int, error) {
	return d.collector.WaitForQuorum(req, result, d.topo.ClientQuorum(), timeout)
}

// MatchingCount returns how many replies match a request and result string.
func (d *Driver) MatchingCount(req pbft.Request, result string) int {
	return d.collector.MatchingCount(req, result)
}

// Collector exposes the underlying reply sink for advanced tests.
func (d *Driver) Collector() *pbft.ReplyCollector { return d.collector }
