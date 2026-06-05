package crypto

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sort"
)

// KeyPair is an ed25519 public/private key pair.
type KeyPair struct {
	Public  ed25519.PublicKey
	Private ed25519.PrivateKey
}

// GenerateKeyPair creates a fresh ed25519 keypair.
func GenerateKeyPair() (KeyPair, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return KeyPair{}, err
	}
	return KeyPair{Public: pub, Private: priv}, nil
}

// Sign returns an ed25519 signature over msg.
func Sign(priv ed25519.PrivateKey, msg []byte) []byte {
	return ed25519.Sign(priv, msg)
}

// Verify checks an ed25519 signature.
func Verify(pub ed25519.PublicKey, msg, sig []byte) bool {
	return ed25519.Verify(pub, msg, sig)
}

// PublicKeyHex encodes a public key as hex (for JSON key files).
func PublicKeyHex(pub ed25519.PublicKey) string {
	return hex.EncodeToString(pub)
}

// PrivateKeyHex encodes a private key seed+pub as hex.
func PrivateKeyHex(priv ed25519.PrivateKey) string {
	return hex.EncodeToString(priv)
}

// ParsePublicKeyHex decodes a hex public key.
func ParsePublicKeyHex(s string) (ed25519.PublicKey, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decode public key: %w", err)
	}
	if len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("public key length %d, want %d", len(b), ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(b), nil
}

// ParsePrivateKeyHex decodes a hex private key.
func ParsePrivateKeyHex(s string) (ed25519.PrivateKey, error) {
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, fmt.Errorf("decode private key: %w", err)
	}
	if len(b) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("private key length %d, want %d", len(b), ed25519.PrivateKeySize)
	}
	return ed25519.PrivateKey(b), nil
}

// CertEntry is one replica's signature over a shared digest.
type CertEntry struct {
	SignerID int32
	Signer   string // "S<n>" for logging
	Sig      []byte
}

// Certificate collects 2f+1 matching signatures over the same digest.
type Certificate struct {
	Digest  []byte
	Quorum  int
	Entries []CertEntry
	seen    map[int32]struct{}
}

// NewCertificate creates an empty certificate expecting quorum signatures.
func NewCertificate(digest []byte, quorum int) *Certificate {
	return &Certificate{
		Digest: digest,
		Quorum: quorum,
		seen:   make(map[int32]struct{}),
	}
}

// Add records a signature from signerID if it verifies and is not a duplicate.
func (c *Certificate) Add(signerID int32, signerLabel string, pub ed25519.PublicKey, sig []byte) bool {
	if _, dup := c.seen[signerID]; dup {
		return false
	}
	if !Verify(pub, c.Digest, sig) {
		return false
	}
	c.seen[signerID] = struct{}{}
	c.Entries = append(c.Entries, CertEntry{
		SignerID: signerID,
		Signer:   signerLabel,
		Sig:      append([]byte(nil), sig...),
	})
	sort.Slice(c.Entries, func(i, j int) bool { return c.Entries[i].SignerID < c.Entries[j].SignerID })
	return true
}

// Complete reports whether the certificate has reached its quorum threshold.
func (c *Certificate) Complete() bool {
	return len(c.Entries) >= c.Quorum
}

// Count returns the number of valid signatures collected.
func (c *Certificate) Count() int { return len(c.Entries) }
