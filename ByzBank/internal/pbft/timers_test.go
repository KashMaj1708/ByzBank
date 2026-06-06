package pbft

import (
	"testing"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
)

func TestDefaultTunablesScaleWithCluster(t *testing.T) {
	topo := config.Default()
	timers := DefaultTunables(topo)
	if timers.ViewChangeTimeout < time.Second {
		t.Fatalf("view change too short: %s", timers.ViewChangeTimeout)
	}
	if timers.LockWaitTimeout < 200*time.Millisecond {
		t.Fatalf("lock wait too short: %s", timers.LockWaitTimeout)
	}
	if timers.SettleDeadline(10) < 30*time.Second {
		t.Fatalf("settle deadline too short for 10 txns")
	}
}
