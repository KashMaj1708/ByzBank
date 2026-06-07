package smallbank

import (
	"context"
	"fmt"
	"regexp"
	"strconv"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/client"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
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

// CheckConservation verifies global sum changed only by recorded penalties.
func CheckConservation(initial, final, penalties int64) error {
	want := initial - penalties
	if final != want {
		return fmt.Errorf("conservation violated: initial=%d penalties=%d want=%d got=%d",
			initial, penalties, want, final)
	}
	return nil
}
