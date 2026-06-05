package crypto

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
)

// KeyFile is the on-disk JSON format for one server's keypair.
type KeyFile struct {
	ServerID  int32  `json:"server_id"`
	PublicHex string `json:"public_hex"`
	PrivateHex string `json:"private_hex,omitempty"`
}

// KeyRing holds this replica's private key and every peer's public key.
type KeyRing struct {
	Self    config.ServerID
	Private ed25519.PrivateKey
	Public  ed25519.PublicKey
	peers   map[config.ServerID]ed25519.PublicKey
}

// GenerateAllKeys writes ed25519 keypairs for every server in the topology.
func GenerateAllKeys(topo config.Topology, dir string) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	for _, s := range topo.Servers {
		kp, err := GenerateKeyPair()
		if err != nil {
			return fmt.Errorf("generate %s: %w", s.ID, err)
		}
		if err := writeKeyFile(dir, s.ID, kp); err != nil {
			return err
		}
	}
	return nil
}

func writeKeyFile(dir string, id config.ServerID, kp KeyPair) error {
	path := keyPath(dir, id)
	f := KeyFile{
		ServerID:   int32(id),
		PublicHex:  PublicKeyHex(kp.Public),
		PrivateHex: PrivateKeyHex(kp.Private),
	}
	b, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o600)
}

func keyPath(dir string, id config.ServerID) string {
	return filepath.Join(dir, fmt.Sprintf("%s.json", id))
}

// LoadKeyRing loads all public keys and the private key for self.
func LoadKeyRing(topo config.Topology, self config.ServerID, dir string) (*KeyRing, error) {
	ring := &KeyRing{
		Self:  self,
		peers: make(map[config.ServerID]ed25519.PublicKey, topo.TotalServers()),
	}
	for _, s := range topo.Servers {
		path := keyPath(dir, s.ID)
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read key %s: %w", s.ID, err)
		}
		var f KeyFile
		if err := json.Unmarshal(b, &f); err != nil {
			return nil, fmt.Errorf("parse key %s: %w", s.ID, err)
		}
		pub, err := ParsePublicKeyHex(f.PublicHex)
		if err != nil {
			return nil, fmt.Errorf("public key %s: %w", s.ID, err)
		}
		ring.peers[s.ID] = pub
		if s.ID == self {
			if f.PrivateHex == "" {
				return nil, fmt.Errorf("missing private key for self %s", self)
			}
			priv, err := ParsePrivateKeyHex(f.PrivateHex)
			if err != nil {
				return nil, fmt.Errorf("private key %s: %w", s.ID, err)
			}
			ring.Private = priv
			ring.Public = pub
		}
	}
	if ring.Private == nil {
		return nil, fmt.Errorf("self key %s not loaded", self)
	}
	return ring, nil
}

// LoadOrCreateKeyRing loads keys from dir, generating them if missing.
func LoadOrCreateKeyRing(topo config.Topology, self config.ServerID, dir string) (*KeyRing, error) {
	if _, err := os.Stat(keyPath(dir, self)); os.IsNotExist(err) {
		if err := GenerateAllKeys(topo, dir); err != nil {
			return nil, err
		}
	}
	return LoadKeyRing(topo, self, dir)
}

// PublicKey returns the public key for a server.
func (k *KeyRing) PublicKey(id config.ServerID) (ed25519.PublicKey, bool) {
	pub, ok := k.peers[id]
	return pub, ok
}

// Sign signs msg with this replica's private key.
func (k *KeyRing) Sign(msg []byte) []byte {
	return Sign(k.Private, msg)
}

// Verify checks a signature from sender.
func (k *KeyRing) Verify(sender config.ServerID, msg, sig []byte) bool {
	pub, ok := k.peers[sender]
	if !ok {
		return false
	}
	return Verify(pub, msg, sig)
}
