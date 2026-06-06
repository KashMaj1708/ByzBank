package pbft

import (
	"context"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
)

// CrossShardHooks receives post-consensus callbacks for cross-shard PBFT instances.
type CrossShardHooks interface {
	HandleClientRequest(ctx context.Context, req Request) bool
	OnCrossPrepareDropped(ctx context.Context, req Request)
	OnCoordPrepareExecuted(ctx context.Context, req Request, seq int64, cert CertificateMsg)
	OnPartPrepareExecuted(ctx context.Context, req Request, seq int64, cert CertificateMsg, outcome store.Outcome)
	OnCoordCommitExecuted(ctx context.Context, req Request, seq int64, cert CertificateMsg, outcome store.Outcome)
	OnPartCommitExecuted(ctx context.Context, req Request, seq int64, cert CertificateMsg, outcome store.Outcome)
}
