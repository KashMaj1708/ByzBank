package testcase

import (
	"fmt"
	"sync"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
)

// Metrics records client-side throughput and latency.
type Metrics struct {
	mu      sync.Mutex
	records []txnRecord
}

type txnRecord struct {
	Req       pbft.Request
	SentAt    time.Time
	RepliedAt time.Time
	Result    string
}

// NewMetrics creates an empty metrics collector.
func NewMetrics() *Metrics { return &Metrics{} }

// RecordSend notes when a request was fired.
func (m *Metrics) RecordSend(req pbft.Request, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.records = append(m.records, txnRecord{Req: req, SentAt: at})
}

// RecordReply attaches a reply result to a matching pending send.
func (m *Metrics) RecordReply(req pbft.Request, result string, at time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for i := range m.records {
		r := &m.records[i]
		if sameReq(r.Req, req) && r.RepliedAt.IsZero() {
			r.RepliedAt = at
			r.Result = result
			return
		}
	}
}

// Performance renders throughput and mean latency for committed transactions.
// Throughput = committed / wall-clock from first send to last committed reply.
// Latency = mean(reply_time - send_time) over committed transactions.
func (m *Metrics) Performance() string {
	committed, wall, meanLat := m.Snapshot()
	if committed == 0 {
		return "Performance: no committed transactions"
	}
	throughput := float64(committed) / wall.Seconds()
	return fmt.Sprintf("Performance: committed=%d throughput=%.2f txns/sec mean_latency=%s",
		committed, throughput, meanLat.Round(time.Millisecond))
}

// Snapshot returns committed count, wall-clock seconds, and mean latency.
func (m *Metrics) Snapshot() (committed int, wall time.Duration, meanLat time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.records) == 0 {
		return 0, 0, 0
	}
	var totalLatency time.Duration
	var firstSend, lastReply time.Time
	for _, r := range m.records {
		if firstSend.IsZero() || r.SentAt.Before(firstSend) {
			firstSend = r.SentAt
		}
		if r.Result == "committed" && !r.RepliedAt.IsZero() {
			committed++
			totalLatency += r.RepliedAt.Sub(r.SentAt)
			if lastReply.IsZero() || r.RepliedAt.After(lastReply) {
				lastReply = r.RepliedAt
			}
		}
	}
	wall = lastReply.Sub(firstSend)
	if wall <= 0 {
		wall = time.Millisecond
	}
	if committed > 0 {
		meanLat = totalLatency / time.Duration(committed)
	}
	return committed, wall, meanLat
}

func sameReq(a, b pbft.Request) bool {
	return a.ClientID == b.ClientID && a.TS == b.TS && a.X == b.X && a.Y == b.Y && a.Amt == b.Amt
}
