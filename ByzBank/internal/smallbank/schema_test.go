package smallbank

import (
	"testing"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
)

func TestSchemaItemMapping(t *testing.T) {
	topo := config.Default()
	s := NewSchema(topo)
	if err := s.Validate(1); err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(s.TotalCustomers()); err != nil {
		t.Fatal(err)
	}
	sav := s.SavingsItem(1)
	chk := s.CheckingItem(1)
	if !topo.SameCluster(sav, chk) {
		t.Fatalf("cust 1 spans clusters sav=%d chk=%d", sav, chk)
	}
	if topo.ClusterOf(sav) != 1 {
		t.Fatalf("cust 1 expected C1 got %s", topo.ClusterOf(sav))
	}
	cross := s.CheckingItem(401) // first customer cluster 2
	if topo.ClusterOf(cross) != 2 {
		t.Fatalf("cust 401 expected C2")
	}
}

func TestCheckConservation(t *testing.T) {
	if err := CheckConservation(1000, 999, 1); err != nil {
		t.Fatal(err)
	}
	if err := CheckConservation(1000, 1000, 1); err == nil {
		t.Fatal("expected conservation error")
	}
}
