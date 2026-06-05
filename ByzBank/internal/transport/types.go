package transport

import (
	"fmt"
	"strconv"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pb"
)

// Message type constants for replica and client protocol messages.
const (
	TypePing = "PING"
	TypePong = "PONG"

	TypeClientRequest = "CLIENT_REQUEST"
	TypeClientReply   = "CLIENT_REPLY"
	TypePrePrepare    = "PRE_PREPARE"
	TypePrepare       = "PREPARE"
	TypePrepareCert   = "PREPARE_CERT"
	TypeCommit        = "COMMIT"
	TypeCommitCert    = "COMMIT_CERT"
)

// IsPBFT returns true for consensus protocol messages.
func IsPBFT(typ string) bool {
	switch typ {
	case TypeClientRequest, TypeClientReply,
		TypePrePrepare, TypePrepare, TypePrepareCert,
		TypeCommit, TypeCommitCert:
		return true
	default:
		return false
	}
}

// SigningBytes returns the canonical bytes signed for an envelope (everything
// except the signature field).
func SigningBytes(senderID int32, typ string, payload []byte) []byte {
	// Stable, simple canonical form: sender|type|payload
	out := make([]byte, 0, 32+len(typ)+len(payload))
	out = append(out, []byte(strconv.Itoa(int(senderID)))...)
	out = append(out, '|')
	out = append(out, []byte(typ)...)
	out = append(out, '|')
	out = append(out, payload...)
	return out
}

// SigningBytesFromEnvelope extracts signing bytes from a protobuf envelope.
func SigningBytesFromEnvelope(env *pb.Envelope) []byte {
	if env == nil {
		return nil
	}
	return SigningBytes(env.SenderId, env.Type, env.Payload)
}

// NewEnvelope builds an unsigned protobuf envelope.
func NewEnvelope(sender config.ServerID, typ string, payload []byte) *pb.Envelope {
	return &pb.Envelope{
		SenderId: int32(sender),
		Type:     typ,
		Payload:  append([]byte(nil), payload...),
	}
}

// SenderID parses the envelope sender as a config.ServerID.
func SenderID(env *pb.Envelope) (config.ServerID, error) {
	if env == nil || env.SenderId <= 0 {
		return 0, fmt.Errorf("invalid envelope sender")
	}
	return config.ServerID(env.SenderId), nil
}
