package pbft

import (
	"context"
	"sync"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pb"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/transport"
)

// ReplyCollector records client replies from executing replicas.
type ReplyCollector struct {
	mu      sync.Mutex
	replies []Reply
}

// Record implements ReplySink.
func (c *ReplyCollector) Record(r Reply) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.replies = append(c.replies, r)
}

// All returns a snapshot of collected replies.
func (c *ReplyCollector) All() []Reply {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Reply, len(c.replies))
	copy(out, c.replies)
	return out
}

// MatchingCount returns how many replies match the request fields and result.
func (c *ReplyCollector) MatchingCount(req Request, result string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, r := range c.replies {
		if r.ClientID == req.ClientID && r.TS == req.TS &&
			r.X == req.X && r.Y == req.Y && r.Amt == req.Amt && r.Result == result {
			n++
		}
	}
	return n
}

// WaitForQuorum blocks until at least quorum matching replies arrive or timeout.
func (c *ReplyCollector) WaitForQuorum(req Request, result string, quorum int, timeout time.Duration) (int, error) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if n := c.MatchingCount(req, result); n >= quorum {
			return n, nil
		}
		time.Sleep(20 * time.Millisecond)
	}
	return c.MatchingCount(req, result), context.DeadlineExceeded
}

// SubmitRequest injects a client request envelope to the primary.
func SubmitRequest(inject func(*pb.Envelope) error, req Request) error {
	payload, err := marshal(req)
	if err != nil {
		return err
	}
	return inject(&pb.Envelope{Type: transport.TypeClientRequest, Payload: payload})
}

// SubmitToAll injects the same client request to every replica inject function.
func SubmitToAll(injectAll []func(*pb.Envelope) error, req Request) error {
	payload, err := marshal(req)
	if err != nil {
		return err
	}
	env := &pb.Envelope{Type: transport.TypeClientRequest, Payload: payload}
	for _, inject := range injectAll {
		if err := inject(env); err != nil {
			return err
		}
	}
	return nil
}

// SubmitWithRetryForTopo uses topology-scaled client timers.
func SubmitWithRetryForTopo(
	topo *config.Topology,
	primaryInject func(*pb.Envelope) error,
	allInjects []func(*pb.Envelope) error,
	collector *ReplyCollector,
	req Request,
) (int, error) {
	t := DefaultTunables(*topo)
	return SubmitWithRetry(primaryInject, allInjects, collector, req, topo.ClientQuorum(), t.ClientPrimaryWait, t.ClientTotalWait)
}

// SubmitWithRetry sends to the primary, waits for client quorum, then broadcasts
// to all replicas on timeout (Lab 2 client timer behaviour).
func SubmitWithRetry(
	primaryInject func(*pb.Envelope) error,
	allInjects []func(*pb.Envelope) error,
	collector *ReplyCollector,
	req Request,
	quorum int,
	primaryWait time.Duration,
	totalWait time.Duration,
) (int, error) {
	if err := SubmitRequest(primaryInject, req); err != nil {
		return 0, err
	}
	if n, err := collector.WaitForQuorum(req, resultCommitted, quorum, primaryWait); err == nil {
		return n, nil
	}
	if err := SubmitToAll(allInjects, req); err != nil {
		return collector.MatchingCount(req, resultCommitted), err
	}
	return collector.WaitForQuorum(req, resultCommitted, quorum, totalWait)
}
