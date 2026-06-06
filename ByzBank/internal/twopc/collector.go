package twopc

import (
	"sync"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
)

// PreparedCollector records participant prepare-phase certificates at the coordinator.
type PreparedCollector struct {
	mu    sync.Mutex
	certs []ParticipantReplyMsg
}

// NewPreparedCollector creates an empty collector.
func NewPreparedCollector() *PreparedCollector {
	return &PreparedCollector{}
}

// Has reports whether any participant reply was recorded for this client txn.
func (c *PreparedCollector) Has(req pbft.Request) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, m := range c.certs {
		if sameClientTxn(m.Req, req) {
			return true
		}
	}
	return false
}

// Record stores one participant reply.
func (c *PreparedCollector) Record(msg ParticipantReplyMsg) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.certs = append(c.certs, msg)
}

// Matching returns replies for the same client transaction and outcome.
func (c *PreparedCollector) Matching(req pbft.Request, outcome store.Outcome) []ParticipantReplyMsg {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]ParticipantReplyMsg, 0)
	for _, m := range c.certs {
		if sameClientTxn(m.Req, req) && m.Outcome == outcome {
			out = append(out, m)
		}
	}
	return out
}

// WaitForOne blocks until at least one matching reply arrives.
func (c *PreparedCollector) WaitForOne(req pbft.Request, outcome store.Outcome, timeout time.Duration) (ParticipantReplyMsg, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if matches := c.Matching(req, outcome); len(matches) > 0 {
			return matches[0], true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return ParticipantReplyMsg{}, false
}

func sameClientTxn(a, b pbft.Request) bool {
	return a.ClientID == b.ClientID && a.TS == b.TS && a.X == b.X && a.Y == b.Y && a.Amt == b.Amt
}
