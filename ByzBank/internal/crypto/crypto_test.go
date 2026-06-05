package crypto

import (
	"testing"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
)

func TestSignVerifyRoundTrip(t *testing.T) {
	kp, err := GenerateKeyPair()
	if err != nil {
		t.Fatal(err)
	}
	msg := []byte("digest-bytes")
	sig := Sign(kp.Private, msg)
	if !Verify(kp.Public, msg, sig) {
		t.Fatal("signature should verify")
	}
	sig[0] ^= 0xff
	if Verify(kp.Public, msg, sig) {
		t.Fatal("corrupted signature should not verify")
	}
}

func TestCertificateQuorum(t *testing.T) {
	topo := config.Default()
	quorum := topo.Quorum()
	digest := []byte("shared-digest")

	keys := make(map[int32]KeyPair, topo.ClusterSize)
	for i := 1; i <= topo.ClusterSize; i++ {
		kp, err := GenerateKeyPair()
		if err != nil {
			t.Fatal(err)
		}
		keys[int32(i)] = kp
	}

	cert := NewCertificate(digest, quorum)
	for id, kp := range keys {
		if !cert.Add(id, config.ServerID(id).String(), kp.Public, Sign(kp.Private, digest)) {
			t.Fatalf("add signature from S%d failed", id)
		}
	}
	if !cert.Complete() {
		t.Fatalf("certificate should be complete at %d sigs", cert.Count())
	}
	if cert.Count() != topo.ClusterSize {
		t.Fatalf("count = %d, want %d", cert.Count(), topo.ClusterSize)
	}
}
