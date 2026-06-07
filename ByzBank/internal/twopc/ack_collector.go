package twopc

import (
	"sync"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
)

// AckCollector records participant commit-phase acks at the coordinator.
type AckCollector struct {
	mu   sync.Mutex
	acks map[string]map[config.ServerID]struct{}
}

// NewAckCollector creates an empty ack collector.
func NewAckCollector() *AckCollector {
	return &AckCollector{acks: make(map[string]map[config.ServerID]struct{})}
}

// Reset clears all recorded participant acks.
func (c *AckCollector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.acks = make(map[string]map[config.ServerID]struct{})
}

// Record stores one ack from a participant server.
func (c *AckCollector) Record(req pbft.Request, from config.ServerID) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := pbft.TxnID(req)
	if c.acks[key] == nil {
		c.acks[key] = make(map[config.ServerID]struct{})
	}
	c.acks[key][from] = struct{}{}
}

// Count returns distinct participant servers that acked one transaction.
func (c *AckCollector) Count(req pbft.Request) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.acks[pbft.TxnID(req)])
}

// WaitForQuorum blocks until at least quorum distinct acks arrive.
func (c *AckCollector) WaitForQuorum(req pbft.Request, quorum int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if c.Count(req) >= quorum {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}
