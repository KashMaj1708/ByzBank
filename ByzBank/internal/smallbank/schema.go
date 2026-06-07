package smallbank

import (
	"fmt"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
)

// Default customers per cluster (two item ids each → 800 account items).
const CustomersPerCluster = 400

// Schema maps SmallBank customer ids onto the sharded KV item namespace.
type Schema struct {
	Topo               config.Topology
	CustomersPerCluster int
	PenaltyAmount      int64
}

// NewSchema builds the default SmallBank layout for a topology.
func NewSchema(topo config.Topology) Schema {
	return Schema{
		Topo:               topo,
		CustomersPerCluster: CustomersPerCluster,
		PenaltyAmount:      1,
	}
}

// TotalCustomers returns the number of logical customers across all clusters.
func (s Schema) TotalCustomers() int {
	return s.CustomersPerCluster * s.Topo.NumClusters
}

func (s Schema) clusterBase(cluster config.ClusterID) int {
	return (int(cluster)-1)*s.Topo.ItemsPerCluster() + 1
}

// CustCluster returns the cluster that owns a customer id (1-based global).
func (s Schema) CustCluster(custID int) config.ClusterID {
	if custID < 1 || custID > s.TotalCustomers() {
		return 1
	}
	idx := (custID - 1) / s.CustomersPerCluster
	return config.ClusterID(idx + 1)
}

// SavingsItem returns the savings balance item for a customer.
func (s Schema) SavingsItem(custID int) int {
	cluster := s.CustCluster(custID)
	local := ((custID - 1) % s.CustomersPerCluster) + 1
	base := s.clusterBase(cluster)
	return base + (local-1)*2
}

// CheckingItem returns the checking balance item for a customer.
func (s Schema) CheckingItem(custID int) int {
	return s.SavingsItem(custID) + 1
}

// TreasuryItem is the per-cluster liquidity pool for deposits.
func (s Schema) TreasuryItem(cluster config.ClusterID) int {
	return s.clusterBase(cluster) + s.Topo.ItemsPerCluster() - 1
}

// AccountItems returns every savings and checking item id (excludes treasury).
func (s Schema) AccountItems() []int {
	out := make([]int, 0, s.TotalCustomers()*2)
	for cust := 1; cust <= s.TotalCustomers(); cust++ {
		out = append(out, s.SavingsItem(cust), s.CheckingItem(cust))
	}
	return out
}

// AllTrackedItems returns account items plus per-cluster treasury pools.
func (s Schema) AllTrackedItems() []int {
	out := s.AccountItems()
	for c := 1; c <= s.Topo.NumClusters; c++ {
		out = append(out, s.TreasuryItem(config.ClusterID(c)))
	}
	return out
}

// Validate checks customer ids and item placement.
func (s Schema) Validate(custID int) error {
	if custID < 1 || custID > s.TotalCustomers() {
		return fmt.Errorf("custID %d out of range 1..%d", custID, s.TotalCustomers())
	}
	sav := s.SavingsItem(custID)
	chk := s.CheckingItem(custID)
	if !s.Topo.SameCluster(sav, chk) {
		return fmt.Errorf("cust %d spans clusters sav=%d chk=%d", custID, sav, chk)
	}
	return nil
}
