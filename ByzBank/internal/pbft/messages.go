package pbft

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// Request is a client transfer request routed to the coordinator primary.
type Request struct {
	ClientID string `json:"client_id"`
	TS       int64  `json:"ts"`
	X        int    `json:"x"`
	Y        int    `json:"y"`
	Amt      int64  `json:"amt"`
}

// Reply is sent to the client after execution.
type Reply struct {
	ClientID string `json:"client_id"`
	TS       int64  `json:"ts"`
	Seq      int64  `json:"seq"`
	X        int    `json:"x"`
	Y        int    `json:"y"`
	Amt      int64  `json:"amt"`
	Result   string `json:"result"` // "committed"
}

// PrePrepareMsg is the primary's ordered proposal for one sequence.
type PrePrepareMsg struct {
	Seq    int64   `json:"seq"`
	View   int     `json:"view"`
	Digest []byte  `json:"digest"`
	Req    Request `json:"req"`
}

// PrepareMsg is a replica's agreement to a pre-prepare.
type PrepareMsg struct {
	Seq    int64  `json:"seq"`
	View   int     `json:"view"`
	Digest []byte `json:"digest"`
}

// CommitMsg is a replica's agreement after a valid prepare certificate.
type CommitMsg struct {
	Seq    int64  `json:"seq"`
	View   int     `json:"view"`
	Digest []byte `json:"digest"`
}

// SigEntry is one replica signature bundled into a certificate.
type SigEntry struct {
	ServerID int32  `json:"server_id"`
	Sig      []byte `json:"sig"`
}

// CertificateMsg is a quorum of signed phase messages broadcast by the collector.
type CertificateMsg struct {
	Seq    int64      `json:"seq"`
	View   int        `json:"view"`
	Digest []byte     `json:"digest"`
	Sigs   []SigEntry `json:"sigs"`
}

// Digest computes the request digest used across PBFT phases.
func Digest(req Request) []byte {
	h := sha256.New()
	_, _ = fmt.Fprintf(h, "%s|%d|%d|%d|%d", req.ClientID, req.TS, req.X, req.Y, req.Amt)
	return h.Sum(nil)
}

func marshal(v any) ([]byte, error) { return json.Marshal(v) }

func unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// Phase signing bytes for prepare/commit envelopes.
func phaseSigningBytes(phase string, seq int64, view int, digest []byte) []byte {
	return []byte(fmt.Sprintf("%s|%d|%d|%x", phase, seq, view, digest))
}
