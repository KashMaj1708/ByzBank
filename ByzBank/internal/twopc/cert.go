package twopc

import (
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/crypto"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
)

// VerifyClusterCert checks a PBFT certificate against one cluster's membership.
func VerifyClusterCert(ring *crypto.KeyRing, topo *config.Topology, cluster config.ClusterID, cert pbft.CertificateMsg, phase string, quorum int) bool {
	if len(cert.Sigs) < quorum {
		return false
	}
	allowed := make(map[config.ServerID]struct{})
	for _, srv := range topo.ServersInCluster(cluster) {
		allowed[srv.ID] = struct{}{}
	}
	seen := make(map[int32]struct{}, len(cert.Sigs))
	for _, ent := range cert.Sigs {
		id := config.ServerID(ent.ServerID)
		if _, ok := allowed[id]; !ok {
			return false
		}
		if _, dup := seen[ent.ServerID]; dup {
			return false
		}
		seen[ent.ServerID] = struct{}{}
		pub, ok := ring.PublicKey(id)
		if !ok {
			return false
		}
		if !crypto.Verify(pub, pbft.PhaseSigningBytes(phase, cert.Seq, cert.View, cert.Digest), ent.Sig) {
			return false
		}
	}
	return true
}
