package server_test

import (
	"testing"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/testcase"
)

func TestMixedWorkloadPerformance(t *testing.T) {
	h := startHarness(t, harnessOptions{clusters: []config.ClusterID{1, 2, 3}})
	defer h.cleanup()

	metrics := testcase.NewMetrics()
	reqs := []pbft.Request{
		{ClientID: "perf", TS: 1, X: 100, Y: 501, Amt: 1},
		{ClientID: "perf", TS: 2, X: 299, Y: 1999, Amt: 1},
		{ClientID: "perf", TS: 3, X: 1001, Y: 1650, Amt: 1},
		{ClientID: "perf", TS: 4, X: 2800, Y: 2150, Amt: 1},
	}

	for _, req := range reqs {
		sent := time.Now()
		if err := h.driver.Fire(req); err != nil {
			t.Fatalf("fire %v: %v", req, err)
		}
		metrics.RecordSend(req, sent)
	}

	for _, req := range reqs {
		waitUntil(t, 45*time.Second, func() bool {
			return h.replies.MatchingCount(req, "committed") >= h.topo.ClientQuorum()
		})
		metrics.RecordReply(req, "committed", time.Now())
	}

	committed, wall, meanLat := metrics.Snapshot()
	if committed != len(reqs) {
		t.Fatalf("committed=%d want %d", committed, len(reqs))
	}
	if wall <= 0 || meanLat <= 0 {
		t.Fatalf("degenerate metrics wall=%s meanLat=%s", wall, meanLat)
	}
	throughput := float64(committed) / wall.Seconds()
	if throughput <= 0 {
		t.Fatalf("non-positive throughput")
	}
	t.Logf("%s", metrics.Performance())
}
