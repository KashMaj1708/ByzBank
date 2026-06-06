package twopc

import (
	"context"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pb"
)

// Messenger sends signed envelopes to arbitrary servers.
type Messenger interface {
	Self() config.ServerID
	Sign(env *pb.Envelope)
	Send(ctx context.Context, to config.ServerID, env *pb.Envelope) error
}

// SendToCluster delivers env to every server in cluster.
func SendToCluster(ctx context.Context, topo *config.Topology, msg Messenger, cluster config.ClusterID, env *pb.Envelope) error {
	msg.Sign(env)
	for _, srv := range topo.ServersInCluster(cluster) {
		if srv.ID == msg.Self() {
			continue
		}
		if err := msg.Send(ctx, srv.ID, env); err != nil {
			return err
		}
	}
	return nil
}
