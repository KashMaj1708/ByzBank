// Package config holds the static topology for the BFT sharded 2PC system:
// server identities, host:port addresses, cluster membership, the shard map,
// and the derived PBFT quorum parameters.
//
// Cluster count and cluster size are parameters (not constants) so the same
// binary can run the graded 3x4 (f=1) default or the 3x12 (f=3) benchmark mode
// without code changes. Everything downstream reads from a Topology value so
// there are no magic numbers scattered around the codebase.
package config

import "fmt"

// ClusterID is a 1-based cluster identifier (C1, C2, C3, ...).
type ClusterID int

// ServerID is a 1-based server identifier (S1 .. SN).
type ServerID int

// String renders a ServerID as "S<n>".
func (s ServerID) String() string { return fmt.Sprintf("S%d", int(s)) }

// String renders a ClusterID as "C<n>".
func (c ClusterID) String() string { return fmt.Sprintf("C%d", int(c)) }

// Server describes one replica process.
type Server struct {
	ID      ServerID  `json:"id"`
	Cluster ClusterID `json:"cluster"`
	Host    string    `json:"host"`
	Port    int       `json:"port"`
}

// Addr returns the dialable host:port for this server.
func (s Server) Addr() string { return fmt.Sprintf("%s:%d", s.Host, s.Port) }

// Topology is the complete static description of the system. It is fully
// derivable from a small set of parameters, but is materialised into explicit
// slices/maps so lookups are trivial and the layout can be dumped to JSON.
type Topology struct {
	NumClusters    int    `json:"num_clusters"`
	ClusterSize    int    `json:"cluster_size"`
	TotalItems     int    `json:"total_items"`
	BasePort       int    `json:"base_port"`
	Host           string `json:"host"`
	InitialBalance int64  `json:"initial_balance"`

	Servers []Server `json:"servers"`
}

// Default returns the graded benchmark topology: 3 clusters x 12 nodes = 36
// servers, 3000 data items (1000 per cluster), every account starting at 10.
func Default() Topology {
	return New(3, 12, 3000, 9000, "127.0.0.1", 10)
}

// New builds a Topology from parameters and materialises the server list.
//
// Servers are numbered S1..SN contiguously, and assigned to clusters in
// contiguous blocks of clusterSize. Ports are basePort+ID so S1 -> 9001.
func New(numClusters, clusterSize, totalItems, basePort int, host string, initialBalance int64) Topology {
	t := Topology{
		NumClusters:    numClusters,
		ClusterSize:    clusterSize,
		TotalItems:     totalItems,
		BasePort:       basePort,
		Host:           host,
		InitialBalance: initialBalance,
	}
	total := numClusters * clusterSize
	t.Servers = make([]Server, 0, total)
	for c := 0; c < numClusters; c++ {
		for i := 0; i < clusterSize; i++ {
			id := ServerID(c*clusterSize + i + 1)
			t.Servers = append(t.Servers, Server{
				ID:      id,
				Cluster: ClusterID(c + 1),
				Host:    host,
				Port:    basePort + int(id),
			})
		}
	}
	return t
}

// TotalServers returns the number of servers in the topology.
func (t Topology) TotalServers() int { return t.NumClusters * t.ClusterSize }

// F is the maximum number of Byzantine faults tolerated per cluster: the
// largest f satisfying 3f+1 <= clusterSize.
func (t Topology) F() int { return (t.ClusterSize - 1) / 3 }

// Quorum is the PBFT commit/safety quorum within a cluster: 2f+1.
func (t Topology) Quorum() int { return 2*t.F() + 1 }

// CollectorQuorum is the linear-PBFT collector signature-gathering threshold:
// n-f. For safety arguments use Quorum (2f+1); for the collector use this.
func (t Topology) CollectorQuorum() int { return t.ClusterSize - t.F() }

// ClientQuorum is the number of matching replies a client waits for: f+1.
func (t Topology) ClientQuorum() int { return t.F() + 1 }

// ItemsPerCluster is the contiguous slice of the keyspace each cluster owns.
func (t Topology) ItemsPerCluster() int { return t.TotalItems / t.NumClusters }

// ClusterOf returns the cluster that owns a given data item (1-based item id).
func (t Topology) ClusterOf(item int) ClusterID {
	per := t.ItemsPerCluster()
	idx := (item - 1) / per
	if idx < 0 {
		idx = 0
	}
	if idx >= t.NumClusters {
		idx = t.NumClusters - 1
	}
	return ClusterID(idx + 1)
}

// PrimaryOf returns the primary server of a cluster for a given view.
// Primary = view mod clusterSize, mapped onto that cluster's server block.
func (t Topology) PrimaryOf(cluster ClusterID, view int) ServerID {
	base := (int(cluster) - 1) * t.ClusterSize
	offset := ((view % t.ClusterSize) + t.ClusterSize) % t.ClusterSize
	return ServerID(base + offset + 1)
}

// ServersInCluster returns the servers belonging to a cluster, in ID order.
func (t Topology) ServersInCluster(cluster ClusterID) []Server {
	out := make([]Server, 0, t.ClusterSize)
	for _, s := range t.Servers {
		if s.Cluster == cluster {
			out = append(out, s)
		}
	}
	return out
}

// ServerByID returns the server with the given ID and whether it was found.
func (t Topology) ServerByID(id ServerID) (Server, bool) {
	for _, s := range t.Servers {
		if s.ID == id {
			return s, true
		}
	}
	return Server{}, false
}

// SameCluster reports whether two data items live in the same cluster, i.e.
// whether a transfer between them is intra-shard.
func (t Topology) SameCluster(x, y int) bool {
	return t.ClusterOf(x) == t.ClusterOf(y)
}
