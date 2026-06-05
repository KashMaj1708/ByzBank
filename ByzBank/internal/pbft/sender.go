package pbft

import (
	"context"
	"sync"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pb"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/transport"
)

// HubSender adapts transport.Hub to the pbft.Sender interface.
type HubSender struct {
	Hub    *transport.Hub
	Topo   *config.Topology
	selfID config.ServerID
	View   int
}

// NewHubSender constructs a sender adapter for one replica.
func NewHubSender(hub *transport.Hub, topo *config.Topology, self config.ServerID, view int) *HubSender {
	return &HubSender{Hub: hub, Topo: topo, selfID: self, View: view}
}

func (s *HubSender) Self() config.ServerID { return s.selfID }

func (s *HubSender) Sign(env *pb.Envelope) { s.Hub.Sign(env) }

func (s *HubSender) Send(ctx context.Context, to config.ServerID, env *pb.Envelope) error {
	return s.Hub.Send(ctx, to, env)
}

func (s *HubSender) BroadcastCluster(ctx context.Context, env *pb.Envelope) error {
	srv, ok := s.Topo.ServerByID(s.selfID)
	if !ok {
		return nil
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var firstErr error
	recordErr := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
	}
	for _, peer := range s.Topo.ServersInCluster(srv.Cluster) {
		if peer.ID == s.selfID {
			continue
		}
		wg.Add(1)
		go func(id config.ServerID) {
			defer wg.Done()
			recordErr(s.Hub.Send(ctx, id, env))
		}(peer.ID)
	}
	wg.Wait()
	return firstErr
}

func (s *HubSender) Primary() config.ServerID {
	srv, ok := s.Topo.ServerByID(s.selfID)
	if !ok {
		return s.selfID
	}
	return s.Topo.PrimaryOf(srv.Cluster, s.View)
}
