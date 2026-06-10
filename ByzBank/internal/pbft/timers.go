package pbft

import (
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
)

// Tunables holds protocol timing knobs scaled to the cluster size.
type Tunables struct {
	ViewChangeTimeout   time.Duration
	CoordPrepareTimeout     time.Duration
	CoordPrepareAbortTimeout time.Duration
	LockWaitTimeout     time.Duration
	LockPollInterval   time.Duration
	ClientPrimaryWait  time.Duration
	ClientTotalWait    time.Duration
	AckRetryInterval   time.Duration
	AckRetryDeadline   time.Duration
	SettlePollInterval time.Duration
	SettleDeadlineBase time.Duration
	SettlePerTxn       time.Duration
	SettlePerCrossTxn  time.Duration
	ReclaimDrainWait   time.Duration
}

// DefaultTunables returns timing defaults for a topology.
// Values are sized for n=12/f=3: long enough to avoid spurious view-changes
// and short enough for reasonable throughput under open-loop load.
func DefaultTunables(topo config.Topology) Tunables {
	scale := time.Duration(topo.ClusterSize) * time.Millisecond * 50
	if scale < 500*time.Millisecond {
		scale = 500 * time.Millisecond
	}
	vc := 2*scale + 500*time.Millisecond
	return Tunables{
		ViewChangeTimeout:   vc,
		CoordPrepareTimeout:      12 * vc,
		CoordPrepareAbortTimeout: 45 * time.Second,
		LockWaitTimeout:     scale + 200*time.Millisecond,
		LockPollInterval:   10 * time.Millisecond,
		ClientPrimaryWait:  scale + 3*time.Second,
		ClientTotalWait:    30 * time.Second,
		AckRetryInterval:   300 * time.Millisecond,
		AckRetryDeadline:   45 * time.Second,
		SettlePollInterval: 50 * time.Millisecond,
		SettleDeadlineBase: 15 * time.Second,
		SettlePerTxn:       2 * time.Second,
		SettlePerCrossTxn:  20 * time.Second,
		ReclaimDrainWait:   2*scale + 500*time.Millisecond,
	}
}

// SettleDeadline returns how long the client should wait for one set to finish.
func (t Tunables) SettleDeadline(txnCount int) time.Duration {
	d := t.SettleDeadlineBase + time.Duration(txnCount)*t.SettlePerTxn
	if d > 120*time.Second {
		return 120 * time.Second
	}
	return d
}
