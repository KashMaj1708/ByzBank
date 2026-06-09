package smallbank

import (
	"testing"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
)

func TestPickerHotStripedAcrossClusters(t *testing.T) {
	topo := config.Default()
	s := NewSchema(topo)
	p := NewPicker(s.TotalCustomers(), topo.NumClusters, s.CustomersPerCluster, 0.1, 1.0, 42)
	counts := make(map[config.ClusterID]int)
	for i := 0; i < 3000; i++ {
		cust := p.Pick()
		if cust < 1 || cust > s.TotalCustomers() {
			t.Fatalf("cust %d out of range", cust)
		}
		counts[s.CustCluster(cust)]++
	}
	for cluster := 1; cluster <= topo.NumClusters; cluster++ {
		if counts[config.ClusterID(cluster)] == 0 {
			t.Fatalf("cluster %d never picked in hot-only mode", cluster)
		}
	}
	if counts[1] > counts[2]*3 || counts[1] > counts[3]*3 {
		t.Fatalf("hot picks skewed to C1: %v", counts)
	}
}
