package store

import "fmt"

// TxnKind is intra-shard or cross-shard.
type TxnKind string

const (
	TxnIntra TxnKind = "intra"
	TxnCross TxnKind = "cross"
)

// Phase is the 2PC / commit phase recorded in the datastore.
type Phase string

const (
	PhasePrepare Phase = "prepare"
	PhaseCommit  Phase = "commit"
)

// Outcome is the committed result stored with a datastore entry.
type Outcome string

const (
	OutcomeCommit Outcome = "commit"
	OutcomeAbort  Outcome = "abort"
)

// DatastoreEntry is one append-only committed-transaction record.
type DatastoreEntry struct {
	Seq             int64   `json:"seq"`
	Type            TxnKind `json:"type"`
	Phase           Phase   `json:"phase"`
	X               int     `json:"x"`
	Y               int     `json:"y"`
	Amt             int64   `json:"amt"`
	BallotOrViewSeq int64   `json:"ballot_or_view_seq"`
	Outcome         Outcome `json:"outcome"`
}

// String formats an entry for PrintDatastore output.
func (e DatastoreEntry) String() string {
	return fmt.Sprintf("seq=%d %s (%d,%d,%d) phase=%s outcome=%s view=%d",
		e.Seq, e.Type, e.X, e.Y, e.Amt, e.Phase, e.Outcome, e.BallotOrViewSeq)
}

// OracleString formats an entry like simulate.py / check_oracle.py expect.
func (e DatastoreEntry) OracleString() string {
	switch e.Type {
	case TxnIntra:
		return fmt.Sprintf("INTRA (%d,%d,%d) COMMIT", e.X, e.Y, e.Amt)
	case TxnCross:
		switch e.Phase {
		case PhasePrepare:
			return fmt.Sprintf("CROSS (%d,%d,%d) PREPARE", e.X, e.Y, e.Amt)
		case PhaseCommit:
			if e.Outcome == OutcomeAbort {
				return fmt.Sprintf("CROSS (%d,%d,%d) COMMIT(ABORT)", e.X, e.Y, e.Amt)
			}
			return fmt.Sprintf("CROSS (%d,%d,%d) COMMIT", e.X, e.Y, e.Amt)
		}
	}
	return e.String()
}

// WALPreimage captures balances before a tentative cross-shard operation so an
// abort can restore prior state.
type WALPreimage struct {
	Balances map[int]int64 `json:"balances"` // itemID -> balance before apply
}

// NewWALPreimage builds a preimage from item/balance pairs.
func NewWALPreimage(items map[int]int64) WALPreimage {
	cp := make(map[int]int64, len(items))
	for k, v := range items {
		cp[k] = v
	}
	return WALPreimage{Balances: cp}
}
