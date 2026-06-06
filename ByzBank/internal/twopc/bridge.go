package twopc

import (
	"context"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
)

// Bridge combines coordinator and participant hook implementations for one replica.
type Bridge struct {
	Coord *Coordinator
	Part  *Participant
}

// HandleClientRequest delegates to the coordinator when applicable.
func (b *Bridge) HandleClientRequest(ctx context.Context, req pbft.Request) bool {
	if b.Coord != nil {
		return b.Coord.HandleClientRequest(ctx, req)
	}
	return false
}

// OnCrossPrepareDropped releases coordinator cross-shard serialization.
func (b *Bridge) OnCrossPrepareDropped(ctx context.Context, req pbft.Request) {
	if b.Coord != nil {
		b.Coord.ReleaseCrossSlot()
	}
}

// OnCoordPrepareExecuted forwards to the coordinator helper.
func (b *Bridge) OnCoordPrepareExecuted(ctx context.Context, req pbft.Request, seq int64, cert pbft.CertificateMsg) {
	if b.Coord != nil {
		b.Coord.OnCoordPrepareExecuted(ctx, req, seq, cert)
	}
}

// OnPartPrepareExecuted forwards to the participant helper.
func (b *Bridge) OnPartPrepareExecuted(ctx context.Context, req pbft.Request, seq int64, cert pbft.CertificateMsg, outcome store.Outcome) {
	if b.Part != nil {
		b.Part.OnPartPrepareExecuted(ctx, req, seq, cert, outcome)
	}
}

// OnCoordCommitExecuted forwards to the coordinator helper.
func (b *Bridge) OnCoordCommitExecuted(ctx context.Context, req pbft.Request, seq int64, cert pbft.CertificateMsg, outcome store.Outcome) {
	if b.Coord != nil {
		b.Coord.OnCoordCommitExecuted(ctx, req, seq, cert, outcome)
	}
}

// OnPartCommitExecuted forwards to the participant helper.
func (b *Bridge) OnPartCommitExecuted(ctx context.Context, req pbft.Request, seq int64, cert pbft.CertificateMsg, outcome store.Outcome) {
	if b.Part != nil {
		b.Part.OnPartCommitExecuted(ctx, req, seq, cert, outcome)
	}
}

// HandleParticipantAck records a participant ack at the coordinator.
func (b *Bridge) HandleParticipantAck(ctx context.Context, from config.ServerID, payload []byte) {
	if b.Coord != nil {
		b.Coord.HandleParticipantAck(ctx, from, payload)
	}
}
