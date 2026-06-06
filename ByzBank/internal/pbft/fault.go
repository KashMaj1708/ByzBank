package pbft

// FaultConfig drives testcase Byzantine and crash behaviour for one replica.
type FaultConfig struct {
	Alive           bool // false = down; ignores all inbound PBFT messages
	ByzantineLeader bool // primary: no pre-prepare broadcast, no NEW-VIEW
	ByzantineBackup bool // backup: never sends PREPARE
	// ClusterHonestLive is the count of live, non-Byzantine replicas in this
	// cluster (set by the testcase runner). Zero means assume full cluster size.
	ClusterHonestLive int `json:"cluster_honest_live,omitempty"`
}

// DefaultFaultConfig returns an honest, live replica configuration.
func DefaultFaultConfig() FaultConfig {
	return FaultConfig{Alive: true}
}
