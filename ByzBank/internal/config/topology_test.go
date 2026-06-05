package config

import "testing"

func TestDefaultTopologyShape(t *testing.T) {
	topo := Default()
	if got := topo.TotalServers(); got != 36 {
		t.Fatalf("TotalServers = %d, want 36", got)
	}
	if got := topo.F(); got != 3 {
		t.Fatalf("F = %d, want 3", got)
	}
	if got := topo.Quorum(); got != 7 {
		t.Fatalf("Quorum = %d, want 7 (2f+1)", got)
	}
	if got := topo.CollectorQuorum(); got != 9 {
		t.Fatalf("CollectorQuorum = %d, want 9 (n-f)", got)
	}
	if got := topo.ClientQuorum(); got != 4 {
		t.Fatalf("ClientQuorum = %d, want 4 (f+1)", got)
	}
}

func TestClusterOf(t *testing.T) {
	topo := Default()
	cases := []struct {
		item int
		want ClusterID
	}{
		{1, 1}, {1000, 1},
		{1001, 2}, {2000, 2},
		{2001, 3}, {3000, 3},
	}
	for _, c := range cases {
		if got := topo.ClusterOf(c.item); got != c.want {
			t.Errorf("ClusterOf(%d) = %s, want %s", c.item, got, c.want)
		}
	}
}

func TestSameCluster(t *testing.T) {
	topo := Default()
	if !topo.SameCluster(5, 7) {
		t.Errorf("items 5,7 should be intra-shard (same cluster)")
	}
	if topo.SameCluster(5, 1500) {
		t.Errorf("items 5,1500 should be cross-shard (different clusters)")
	}
}

func TestPrimaryOf(t *testing.T) {
	topo := Default()
	// View 0 -> first server of each cluster.
	if got := topo.PrimaryOf(1, 0); got != 1 {
		t.Errorf("PrimaryOf(C1, v0) = %s, want S1", got)
	}
	if got := topo.PrimaryOf(2, 0); got != 13 {
		t.Errorf("PrimaryOf(C2, v0) = %s, want S13", got)
	}
	if got := topo.PrimaryOf(3, 0); got != 25 {
		t.Errorf("PrimaryOf(C3, v0) = %s, want S25", got)
	}
	// View 1 rotates within the cluster block.
	if got := topo.PrimaryOf(1, 1); got != 2 {
		t.Errorf("PrimaryOf(C1, v1) = %s, want S2", got)
	}
	// Wrap-around: view == clusterSize returns to first server.
	if got := topo.PrimaryOf(1, topo.ClusterSize); got != 1 {
		t.Errorf("PrimaryOf(C1, v=clusterSize) = %s, want S1", got)
	}
}

func TestServersInCluster(t *testing.T) {
	topo := Default()
	c2 := topo.ServersInCluster(2)
	if len(c2) != 12 {
		t.Fatalf("C2 has %d servers, want 12", len(c2))
	}
	if c2[0].ID != 13 || c2[11].ID != 24 {
		t.Errorf("C2 range = %s..%s, want S13..S24", c2[0].ID, c2[11].ID)
	}
}
