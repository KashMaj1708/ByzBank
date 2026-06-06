package twopc

import (
	"encoding/json"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
)

// CoordinatorPrepareMsg is sent from coordinator cluster to every participant replica.
type CoordinatorPrepareMsg struct {
	Req        pbft.Request         `json:"req"`
	CommitCert pbft.CertificateMsg  `json:"commit_cert"`
	CoordSeq   int64                `json:"coord_seq"`
}

// ParticipantReplyMsg is sent from participant primary to every coordinator replica.
type ParticipantReplyMsg struct {
	Req        pbft.Request        `json:"req"`
	Outcome    store.Outcome       `json:"outcome"`
	CommitCert pbft.CertificateMsg `json:"commit_cert"`
	PartSeq    int64               `json:"part_seq"`
}

// CoordinatorCommitMsg is sent from coordinator primary to every participant replica.
type CoordinatorCommitMsg struct {
	Req        pbft.Request        `json:"req"`
	Outcome    store.Outcome       `json:"outcome"`
	CommitCert pbft.CertificateMsg `json:"commit_cert"`
	CoordSeq   int64               `json:"coord_seq"`
}

// ParticipantAckMsg is sent from participant primary to every coordinator replica.
type ParticipantAckMsg struct {
	Req     pbft.Request `json:"req"`
	PartSeq int64        `json:"part_seq"`
}

func marshal(v any) ([]byte, error) { return json.Marshal(v) }

func unmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }
