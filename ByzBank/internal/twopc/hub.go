package twopc

import (
	"context"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pb"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/transport"
)

// HubMessenger adapts transport.Hub to the twopc Messenger interface.
type HubMessenger struct {
	Hub    *transport.Hub
	SelfID config.ServerID
}

// NewHubMessenger constructs a messenger adapter.
func NewHubMessenger(hub *transport.Hub, self config.ServerID) *HubMessenger {
	return &HubMessenger{Hub: hub, SelfID: self}
}

func (m *HubMessenger) Self() config.ServerID { return m.SelfID }

func (m *HubMessenger) Sign(env *pb.Envelope) { m.Hub.Sign(env) }

func (m *HubMessenger) Send(ctx context.Context, to config.ServerID, env *pb.Envelope) error {
	return m.Hub.Send(ctx, to, env)
}
