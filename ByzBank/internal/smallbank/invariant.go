package smallbank

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/client"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
)

var balanceLineRe = regexp.MustCompile(`bal\[(\d+)\]\s*=\s*(-?\d+)`)

// SumBalances queries one server per cluster and returns the sum of tracked items.
func SumBalances(ctx context.Context, remote *client.Remote, topo *config.Topology, schema Schema) (int64, error) {
	items := schema.AllTrackedItems()
	seen := make(map[int]int64, len(items))
	for _, cluster := range []config.ClusterID{1, 2, 3} {
		if int(cluster) > topo.NumClusters {
			break
		}
		primary := topo.PrimaryOf(cluster, 0)
		for _, item := range items {
			if topo.ClusterOf(item) != cluster {
				continue
			}
			line, err := remote.PrintBalance(ctx, primary, item)
			if err != nil {
				return 0, fmt.Errorf("balance item %d via %s: %w", item, primary, err)
			}
			bal, err := parseBalanceLine(line)
			if err != nil {
				return 0, err
			}
			seen[item] = bal
		}
	}
	var total int64
	for _, v := range seen {
		total += v
	}
	return total, nil
}

func parseBalanceLine(line string) (int64, error) {
	m := balanceLineRe.FindStringSubmatch(line)
	if m == nil {
		return 0, fmt.Errorf("parse balance line %q", line)
	}
	item, _ := strconv.Atoi(m[1])
	val, _ := strconv.ParseInt(m[2], 10, 64)
	_ = item
	return val, nil
}

// SumBalancesByCluster returns per-cluster totals for diagnostics.
func SumBalancesByCluster(ctx context.Context, remote *client.Remote, topo *config.Topology, schema Schema) (map[config.ClusterID]int64, int64, error) {
	items := schema.AllTrackedItems()
	perCluster := make(map[config.ClusterID]int64)
	for cluster := 1; cluster <= topo.NumClusters; cluster++ {
		cid := config.ClusterID(cluster)
		primary := topo.PrimaryOf(cid, 0)
		var subtotal int64
		for _, item := range items {
			if topo.ClusterOf(item) != cid {
				continue
			}
			line, err := remote.PrintBalance(ctx, primary, item)
			if err != nil {
				return nil, 0, fmt.Errorf("balance item %d via %s: %w", item, primary, err)
			}
			bal, err := parseBalanceLine(line)
			if err != nil {
				return nil, 0, err
			}
			subtotal += bal
		}
		perCluster[cid] = subtotal
	}
	var total int64
	for _, v := range perCluster {
		total += v
	}
	return perCluster, total, nil
}

// ClusterOutstanding aggregates in-flight 2PC state across cluster primaries.
type ClusterOutstanding struct {
	TotalWAL   int
	TotalLocks int
	TxnIDs     []string
	ByServer   map[config.ServerID]store.Outstanding2PC
}

// FetchClusterOutstanding queries every cluster primary for WAL/lock counts.
func FetchClusterOutstanding(ctx context.Context, remote *client.Remote, topo *config.Topology) (ClusterOutstanding, error) {
	out := ClusterOutstanding{ByServer: make(map[config.ServerID]store.Outstanding2PC)}
	seen := make(map[string]struct{})
	for cluster := 1; cluster <= topo.NumClusters; cluster++ {
		primary := topo.PrimaryOf(config.ClusterID(cluster), 0)
		srv, err := remote.FetchOutstanding(ctx, primary)
		if err != nil {
			return out, err
		}
		out.ByServer[primary] = srv
		out.TotalWAL += srv.WALCount
		out.TotalLocks += srv.LockCount
		for _, id := range srv.WALTxnIDs {
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			out.TxnIDs = append(out.TxnIDs, id)
		}
	}
	return out, nil
}

// Wait2PCQuiescence drains replicas and waits until all primaries report zero WAL entries.
// WAL is the stranded-2PC indicator (half-applied cross-shard debits/credits). Locks may
// linger briefly on hot items without WAL; those wedge throughput but do not leak money.
func Wait2PCQuiescence(ctx context.Context, remote *client.Remote, topo *config.Topology, maxWait time.Duration) (ClusterOutstanding, error) {
	poll := 500 * time.Millisecond
	stableFor := 3 * time.Second
	deadline := time.Now().Add(maxWait)
	var last ClusterOutstanding
	walClearSince := time.Time{}
	for time.Now().Before(deadline) {
		for cluster := 1; cluster <= topo.NumClusters; cluster++ {
			primary := topo.PrimaryOf(config.ClusterID(cluster), 0)
			_ = remote.DrainReplica(ctx, primary, 5*time.Second, true)
		}
		out, err := FetchClusterOutstanding(ctx, remote, topo)
		if err != nil {
			return last, err
		}
		last = out
		if out.TotalWAL == 0 {
			if walClearSince.IsZero() {
				walClearSince = time.Now()
			} else if time.Since(walClearSince) >= stableFor {
				return out, nil
			}
		} else {
			walClearSince = time.Time{}
		}
		select {
		case <-ctx.Done():
			return last, ctx.Err()
		case <-time.After(poll):
		}
	}
	return last, fmt.Errorf("2PC WAL not clear within %s: wal=%d locks=%d txns=%v",
		maxWait, last.TotalWAL, last.TotalLocks, last.TxnIDs)
}

// CheckConservation verifies global sum changed only by recorded penalties.
func CheckConservation(initial, final, penalties int64) error {
	want := initial - penalties
	if final != want {
		return fmt.Errorf("conservation violated: initial=%d penalties=%d want=%d got=%d",
			initial, penalties, want, final)
	}
	return nil
}
