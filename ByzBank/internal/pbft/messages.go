package pbft

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// Operation kinds carried inside PBFT requests.
const (
	OpIntra             = "intra"
	OpCoordPrepare      = "coord_prepare"
	OpPartPrepareCommit = "part_prepare_commit"
	OpPartPrepareAbort  = "part_prepare_abort"
	OpCoordCommit       = "coord_commit"
	OpCoordAbort        = "coord_abort"
	OpPartCommit        = "part_commit"
	OpPartAbort         = "part_abort"
)

// Request is a client transfer request routed to the coordinator primary.
type Request struct {
	ClientID string `json:"client_id"`
	TS       int64  `json:"ts"`
	X        int    `json:"x"`
	Y        int    `json:"y"`
	Amt      int64  `json:"amt"`
	Op       string `json:"op,omitempty"`
}

// Client reply result strings.
const (
	ResultCommitted    = "committed"
	ResultAbort        = "abort"
	ResultInsufficient = "insufficient"
)

// Reply is sent to the client after execution.
type Reply struct {
	ClientID string `json:"client_id"`
	TS       int64  `json:"ts"`
	Seq      int64  `json:"seq"`
	X        int    `json:"x"`
	Y        int    `json:"y"`
	Amt      int64  `json:"amt"`
	Result   string `json:"result"`
}

// DiscardSeqMsg tells replicas to drop an abandoned sequence slot.
type DiscardSeqMsg struct {
	Seq  int64 `json:"seq"`
	View int   `json:"view"`
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

// PreparedEntry records a prepared-but-not-executed request carried in VIEW-CHANGE.
type PreparedEntry struct {
	Seq    int64   `json:"seq"`
	View   int     `json:"view"`
	Digest []byte  `json:"digest"`
	Req    Request `json:"req"`
}

// ViewChangeMsg is broadcast when a replica suspects the current primary.
type ViewChangeMsg struct {
	NewView      int             `json:"new_view"`
	LatestStable int64           `json:"latest_stable"`
	Prepared     []PreparedEntry `json:"prepared"`
	PendingReqs  []Request       `json:"pending_reqs"`
}

// NewViewMsg is issued by the new primary after collecting 2f+1 VIEW-CHANGE messages.
type NewViewMsg struct {
	NewView     int             `json:"new_view"`
	PrePrepares []PrePrepareMsg `json:"pre_prepares"`
	ViewChanges []ViewChangeMsg `json:"view_changes"`
}

// Digest computes the request digest used across PBFT phases.
func Digest(req Request) []byte {
	h := sha256.New()
	op := req.Op
	if op == "" {
		op = OpIntra
	}
	if op == OpIntra {
		_, _ = fmt.Fprintf(h, "%s|%d|%d|%d|%d", req.ClientID, req.TS, req.X, req.Y, req.Amt)
	} else {
		_, _ = fmt.Fprintf(h, "%s|%s|%d|%d|%d|%d", req.ClientID, op, req.TS, req.X, req.Y, req.Amt)
	}
	return h.Sum(nil)
}

// TxnID is a stable identifier for WAL entries of one client transaction.
func TxnID(req Request) string { return fmt.Sprintf("%x", Digest(clientReq(req))) }

// clientReq returns the client-facing view of a request (without internal op tags).
func clientReq(req Request) Request {
	out := req
	out.Op = ""
	return out
}

func marshal(v any) ([]byte, error) { return json.Marshal(v) }

func unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

// PhaseSigningBytes returns canonical signing bytes for prepare/commit messages.
func PhaseSigningBytes(phase string, seq int64, view int, digest []byte) []byte {
	return []byte(fmt.Sprintf("%s|%d|%d|%x", phase, seq, view, digest))
}

// phaseSigningBytes is an internal alias.
func phaseSigningBytes(phase string, seq int64, view int, digest []byte) []byte {
	return PhaseSigningBytes(phase, seq, view, digest)
}
