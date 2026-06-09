package smallbank

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
)

// Record captures one completed operation sample.
type Record struct {
	Kind       Kind
	CrossShard bool
	Committed  bool
	SentAt     time.Time
	RepliedAt  time.Time
	Latency    time.Duration
}

// Metrics collects SmallBank workload statistics.
type Metrics struct {
	mu        sync.Mutex
	records   []Record
	penalties int64
}

// NewMetrics creates an empty collector.
func NewMetrics() *Metrics { return &Metrics{} }

// AddPenalty records a WriteCheck penalty applied to the system.
func (m *Metrics) AddPenalty(amt int64) {
	m.mu.Lock()
	m.penalties += amt
	m.mu.Unlock()
}

// Penalties returns total penalties recorded.
func (m *Metrics) Penalties() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.penalties
}

// Record adds one sample.
func (m *Metrics) Record(r Record) {
	m.mu.Lock()
	m.records = append(m.records, r)
	m.mu.Unlock()
}

type bucketStats struct {
	Count      int
	Committed  int
	Mean       time.Duration
	P50        time.Duration
	P95        time.Duration
	P99        time.Duration
	Throughput float64
	Wall       time.Duration
}

func percentile(latencies []time.Duration, p float64) time.Duration {
	if len(latencies) == 0 {
		return 0
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	idx := int(float64(len(latencies)-1) * p)
	if idx < 0 {
		idx = 0
	}
	return latencies[idx]
}

func (m *Metrics) stats(filter func(Record) bool) bucketStats {
	m.mu.Lock()
	defer m.mu.Unlock()
	var samples []Record
	var first, last time.Time
	for _, r := range m.records {
		if filter != nil && !filter(r) {
			continue
		}
		samples = append(samples, r)
	}
	if len(samples) == 0 {
		return bucketStats{}
	}
	lats := make([]time.Duration, 0, len(samples))
	var total time.Duration
	committed := 0
	for _, r := range samples {
		if first.IsZero() || r.SentAt.Before(first) {
			first = r.SentAt
		}
		if r.Committed {
			committed++
			lats = append(lats, r.Latency)
			total += r.Latency
			if !r.RepliedAt.IsZero() && (last.IsZero() || r.RepliedAt.After(last)) {
				last = r.RepliedAt
			}
		}
	}
	stats := bucketStats{Count: len(samples), Committed: committed}
	if committed == 0 {
		return stats
	}
	stats.Mean = total / time.Duration(committed)
	stats.P50 = percentile(append([]time.Duration(nil), lats...), 0.50)
	stats.P95 = percentile(append([]time.Duration(nil), lats...), 0.95)
	stats.P99 = percentile(append([]time.Duration(nil), lats...), 0.99)
	stats.Wall = last.Sub(first)
	if stats.Wall <= 0 {
		stats.Wall = time.Millisecond
	}
	stats.Throughput = float64(committed) / stats.Wall.Seconds()
	return stats
}

// Report renders intra/cross/overall throughput and latency breakdown.
func (m *Metrics) Report() string {
	all := m.stats(nil)
	intra := m.stats(func(r Record) bool { return !r.CrossShard })
	cross := m.stats(func(r Record) bool { return r.CrossShard })
	writes := m.stats(func(r Record) bool { return r.Kind != KindBalance && r.Committed })
	return fmt.Sprintf(
		"SmallBank metrics:\n"+
			"  overall:  committed=%d/%d throughput=%.3f txns/s wall=%s mean=%s p50=%s p95=%s p99=%s\n"+
			"  writes:   committed=%d (excl. Bal) throughput=%.3f txns/s mean=%s\n"+
			"  intra:    committed=%d/%d throughput=%.3f txns/s mean=%s p50=%s p95=%s p99=%s\n"+
			"  cross:    committed=%d/%d throughput=%.3f txns/s mean=%s p50=%s p95=%s p99=%s\n"+
			"  penalties applied: %d",
		all.Committed, all.Count, all.Throughput, all.Wall.Round(time.Millisecond), all.Mean.Round(time.Millisecond),
		all.P50.Round(time.Millisecond), all.P95.Round(time.Millisecond), all.P99.Round(time.Millisecond),
		writes.Committed, writes.Throughput, writes.Mean.Round(time.Millisecond),
		intra.Committed, intra.Count, intra.Throughput, intra.Mean.Round(time.Millisecond),
		intra.P50.Round(time.Millisecond), intra.P95.Round(time.Millisecond), intra.P99.Round(time.Millisecond),
		cross.Committed, cross.Count, cross.Throughput, cross.Mean.Round(time.Millisecond),
		cross.P50.Round(time.Millisecond), cross.P95.Round(time.Millisecond), cross.P99.Round(time.Millisecond),
		m.Penalties(),
	)
}

// sameReq mirrors testcase metrics matching.
func sameReq(a, b pbft.Request) bool {
	return a.ClientID == b.ClientID && a.TS == b.TS && a.X == b.X && a.Y == b.Y && a.Amt == b.Amt
}
