package testcase

import (
	"testing"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
)

func TestPerformanceMetrics(t *testing.T) {
	m := NewMetrics()
	req1 := pbft.Request{ClientID: "c", TS: 1, X: 1, Y: 2, Amt: 1}
	req2 := pbft.Request{ClientID: "c", TS: 2, X: 3, Y: 4, Amt: 1}

	t0 := time.Now()
	m.RecordSend(req1, t0)
	m.RecordSend(req2, t0.Add(100*time.Millisecond))
	m.RecordReply(req1, "committed", t0.Add(500*time.Millisecond))
	m.RecordReply(req2, "committed", t0.Add(800*time.Millisecond))

	committed, wall, meanLat := m.Snapshot()
	if committed != 2 {
		t.Fatalf("committed=%d want 2", committed)
	}
	if wall < 700*time.Millisecond || wall > 900*time.Millisecond {
		t.Fatalf("wall=%s want ~800ms", wall)
	}
	if meanLat < 400*time.Millisecond || meanLat > 600*time.Millisecond {
		t.Fatalf("meanLat=%s want ~500ms", meanLat)
	}
	out := m.Performance()
	if out == "" || out == "Performance: no committed transactions" {
		t.Fatalf("unexpected output: %q", out)
	}
}
