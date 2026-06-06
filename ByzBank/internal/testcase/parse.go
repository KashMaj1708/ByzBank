package testcase

import (
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
)

var txnRe = regexp.MustCompile(`\((\d+)\s*,\s*(\d+)\s*,\s*(\d+)\)`)
var serverListRe = regexp.MustCompile(`S(\d+)`)

// ParseFile reads and parses a 5-column Lab4 CSV test file.
func ParseFile(path string) (*File, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read csv: %w", err)
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("csv %s: no data rows", path)
	}

	out := &File{Path: path}
	start := 0
	if len(rows) > 0 && len(rows[0]) > 0 {
		if _, err := strconv.Atoi(strings.TrimSpace(rows[0][0])); err != nil {
			start = 1 // optional header row (e.g. "Set Number,...")
		}
	}
	var cur *Set
	for i, row := range rows[start:] {
		i += start
		if len(row) < 2 {
			continue
		}
		if strings.TrimSpace(row[0]) != "" {
			if cur != nil {
				out.Sets = append(out.Sets, *cur)
			}
			num, err := strconv.Atoi(strings.TrimSpace(row[0]))
			if err != nil {
				return nil, fmt.Errorf("row %d set number: %w", i+1, err)
			}
			cur = &Set{Number: num}
			if len(row) >= 3 {
				cur.Live = parseServerList(row[2])
			}
			if len(row) >= 4 {
				cur.Contact = parseServerList(row[3])
			}
			if len(row) >= 5 {
				cur.Byzantine = parseServerList(row[4])
			}
		}
		if cur == nil {
			return nil, fmt.Errorf("row %d: transaction before set header", i+1)
		}
		txn, err := parseTransaction(row[1])
		if err != nil {
			return nil, fmt.Errorf("row %d: %w", i+1, err)
		}
		cur.Txns = append(cur.Txns, txn)
	}
	if cur != nil {
		out.Sets = append(out.Sets, *cur)
	}
	return out, nil
}

func parseTransaction(s string) (Transaction, error) {
	m := txnRe.FindStringSubmatch(strings.TrimSpace(s))
	if m == nil {
		return Transaction{}, fmt.Errorf("invalid transaction %q", s)
	}
	x, _ := strconv.Atoi(m[1])
	y, _ := strconv.Atoi(m[2])
	amt, _ := strconv.ParseInt(m[3], 10, 64)
	return Transaction{X: x, Y: y, Amt: amt}, nil
}

func parseServerList(s string) []config.ServerID {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil
	}
	matches := serverListRe.FindAllStringSubmatch(s, -1)
	out := make([]config.ServerID, 0, len(matches))
	for _, m := range matches {
		n, _ := strconv.Atoi(m[1])
		out = append(out, config.ServerID(n))
	}
	return out
}

// ContactFor returns the contact server for a coordinator cluster.
func ContactFor(set Set, cluster config.ClusterID) (config.ServerID, error) {
	if len(set.Contact) == 0 {
		return 0, fmt.Errorf("set %d: no contact servers", set.Number)
	}
	idx := int(cluster) - 1
	if idx < 0 || idx >= len(set.Contact) {
		return 0, fmt.Errorf("set %d: no contact for cluster %s", set.Number, cluster)
	}
	return set.Contact[idx], nil
}
