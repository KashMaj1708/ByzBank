package testcase

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
)

var balanceRe = regexp.MustCompile(`bal\[\d+\]\s*=\s*(-?\d+)`)

// OracleDump is one per-set snapshot for check_oracle.py-compatible comparison.
type OracleDump struct {
	Set                 int               `json:"set"`
	Balances            map[string]int64  `json:"balances"`
	DatastorePerCluster map[string][]string `json:"datastore_per_cluster"`
}

// AllItems returns every item ID referenced in the file (sorted).
func (f *File) AllItems() []int {
	seen := make(map[int]struct{})
	for _, set := range f.Sets {
		for _, txn := range set.Txns {
			seen[txn.X] = struct{}{}
			seen[txn.Y] = struct{}{}
		}
	}
	out := make([]int, 0, len(seen))
	for item := range seen {
		out = append(out, item)
	}
	sort.Ints(out)
	return out
}

func liveInCluster(set Set, cluster config.ClusterID, topo *config.Topology) (config.ServerID, bool) {
	live := toIDSet(set.Live)
	for _, srv := range topo.ServersInCluster(cluster) {
		if _, ok := live[srv.ID]; ok {
			return srv.ID, true
		}
	}
	return 0, false
}

// CollectOracleDump snapshots balances and per-cluster datastores after a set.
func (r *Runner) CollectOracleDump(ctx context.Context, set Set, items []int) (OracleDump, error) {
	dump := OracleDump{
		Set:                 set.Number,
		Balances:            make(map[string]int64, len(items)),
		DatastorePerCluster: make(map[string][]string, 3),
	}
	for _, item := range items {
		cluster := r.Topo.ClusterOf(item)
		rep, ok := liveInCluster(set, cluster, r.Topo)
		if !ok {
			return dump, fmt.Errorf("set %d: no live server in cluster %s for item %d", set.Number, cluster, item)
		}
		line, err := r.Remote.PrintBalance(ctx, rep, item)
		if err != nil {
			return dump, fmt.Errorf("balance item %d via %s: %w", item, rep, err)
		}
		m := balanceRe.FindStringSubmatch(line)
		if m == nil {
			return dump, fmt.Errorf("parse balance %q from %s", line, rep)
		}
		val, _ := strconv.ParseInt(m[1], 10, 64)
		dump.Balances[strconv.Itoa(item)] = val
	}
	for _, cluster := range []config.ClusterID{1, 2, 3} {
		key := fmt.Sprintf("C%d", cluster)
		rep, ok := liveInCluster(set, cluster, r.Topo)
		if !ok {
			dump.DatastorePerCluster[key] = []string{}
			continue
		}
		entries, err := r.Remote.FetchDatastoreOracle(ctx, rep)
		if err != nil {
			return dump, fmt.Errorf("datastore %s via %s: %w", key, rep, err)
		}
		dump.DatastorePerCluster[key] = entries
	}
	return dump, nil
}

type expectedOracle struct {
	Sets []expectedSet `json:"sets"`
}

type expectedSet struct {
	Set                 int    `json:"set"`
	Balances            map[string]int64 `json:"balances"`
	DatastorePerCluster map[string]struct {
		CommittedEntries []string `json:"committed_entries"`
	} `json:"datastore_per_cluster"`
}

func loadExpected(path string) (*expectedOracle, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var exp expectedOracle
	if err := json.Unmarshal(b, &exp); err != nil {
		return nil, err
	}
	return &exp, nil
}

func findExpectedSet(exp *expectedOracle, setNum int) (*expectedSet, error) {
	for i := range exp.Sets {
		if exp.Sets[i].Set == setNum {
			return &exp.Sets[i], nil
		}
	}
	return nil, fmt.Errorf("set %d not found in oracle", setNum)
}

func multisetEqual(a, b []string) bool {
	if len(a) != len(b) && len(a) > 0 && len(b) > 0 {
		// multiset compare — lengths can differ if duplicates handled by counter
	}
	ca := make(map[string]int, len(a))
	cb := make(map[string]int, len(b))
	for _, s := range a {
		ca[s]++
	}
	for _, s := range b {
		cb[s]++
	}
	if len(ca) != len(cb) {
		return false
	}
	for k, va := range ca {
		if cb[k] != va {
			return false
		}
	}
	return true
}

// VerifyDumps compares collected dumps against an expected JSON oracle file.
func VerifyDumps(expectedPath string, dumps []OracleDump) (int, error) {
	exp, err := loadExpected(expectedPath)
	if err != nil {
		return 1, err
	}
	rc := 0
	for _, d := range dumps {
		s, err := findExpectedSet(exp, d.Set)
		if err != nil {
			return 1, err
		}
		fmt.Printf("\n=== SET %d ===\n", d.Set)
		for item, val := range d.Balances {
			expVal, ok := s.Balances[item]
			if !ok {
				fmt.Printf("  [WARN] bal[%s] not in oracle (untouched)\n", item)
				continue
			}
			if val != expVal {
				fmt.Printf("  [FAIL] bal[%s]: actual=%d expected=%d\n", item, val, expVal)
				rc = 1
			} else {
				fmt.Printf("  [OK]   bal[%s] = %d\n", item, val)
			}
		}
		for _, c := range []string{"C1", "C2", "C3"} {
			actual := d.DatastorePerCluster[c]
			expEntries := s.DatastorePerCluster[c].CommittedEntries
			if multisetEqual(actual, expEntries) {
				fmt.Printf("  [OK]   %s datastore: %d entries match (multiset)\n", c, len(actual))
				continue
			}
			fmt.Printf("  [FAIL] %s datastore mismatch:\n", c)
			rc = 1
			ca := make(map[string]int)
			cb := make(map[string]int)
			for _, e := range actual {
				ca[e]++
			}
			for _, e := range expEntries {
				cb[e]++
			}
			for k, n := range ca {
				if cb[k] < n {
					for i := 0; i < n-cb[k]; i++ {
						fmt.Printf("          unexpected: %s\n", k)
					}
				}
			}
			for k, n := range cb {
				if ca[k] < n {
					for i := 0; i < n-ca[k]; i++ {
						fmt.Printf("          missing:    %s\n", k)
					}
				}
			}
		}
	}
	if rc == 0 {
		fmt.Println("\nRESULT: ALL PASS")
	} else {
		fmt.Println("\nRESULT: MISMATCHES FOUND")
	}
	return rc, nil
}
