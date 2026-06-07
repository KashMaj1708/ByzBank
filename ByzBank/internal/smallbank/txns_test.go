package smallbank

import (
	"testing"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
)

func TestSendPaymentCrossShard(t *testing.T) {
	topo := config.Default()
	s := NewSchema(topo)
	g := NewGenerator(s, "test")
	op, err := g.SendPayment(1, 401, 3)
	if err != nil {
		t.Fatal(err)
	}
	if !op.CrossShard {
		t.Fatal("expected cross-shard send payment")
	}
	if len(op.Requests) != 1 {
		t.Fatalf("requests=%d", len(op.Requests))
	}
}

func TestUniformKinds(t *testing.T) {
	if len(UniformKinds()) != 6 {
		t.Fatalf("kinds=%d", len(UniformKinds()))
	}
}
