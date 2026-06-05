package config

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Load returns the active topology. Cluster count / size / base port can be
// overridden via environment variables so the graded 3x4 default and the 3x12
// benchmark mode are selectable without recompiling:
//
//	BYZ_NUM_CLUSTERS  (default 3)
//	BYZ_CLUSTER_SIZE  (default 12)
//	BYZ_TOTAL_ITEMS   (default 3000)
//	BYZ_BASE_PORT     (default 9000)
//	BYZ_HOST          (default 127.0.0.1)
func Load() Topology {
	d := Default()
	numClusters := envInt("BYZ_NUM_CLUSTERS", d.NumClusters)
	clusterSize := envInt("BYZ_CLUSTER_SIZE", d.ClusterSize)
	totalItems := envInt("BYZ_TOTAL_ITEMS", d.TotalItems)
	basePort := envInt("BYZ_BASE_PORT", d.BasePort)
	host := d.Host
	if v := os.Getenv("BYZ_HOST"); v != "" {
		host = v
	}
	return New(numClusters, clusterSize, totalItems, basePort, host, d.InitialBalance)
}

// JSON renders the topology as indented JSON for inspection/debugging.
func (t Topology) JSON() ([]byte, error) {
	return json.MarshalIndent(t, "", "  ")
}

// ParseServerID parses identifiers like "S5" (case-insensitive) into a
// ServerID. It also accepts a bare integer like "5".
func ParseServerID(s string) (ServerID, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(strings.ToUpper(s), "S")
	n, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("invalid server id %q: %w", s, err)
	}
	return ServerID(n), nil
}

func envInt(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}
