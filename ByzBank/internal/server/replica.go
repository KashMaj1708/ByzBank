package server

import (
	"context"
	"fmt"
	"log"
	"path/filepath"
	"sync"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/crypto"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pb"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/transport"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/twopc"
)

// Replica wires transport, storage, PBFT, and the inbound dispatch loop.
type Replica struct {
	Self   config.ServerID
	Topo   *config.Topology
	Ring   *crypto.KeyRing
	Hub    *transport.Hub
	Store  *store.Store
	PBFT   *pbft.Engine
	TwoPC  *twopc.Bridge
	Logger *log.Logger

	mu      sync.Mutex
	pongCh  chan *pb.Envelope
	started bool
}

// ReplicaConfig configures a replica instance.
type ReplicaConfig struct {
	Self      config.ServerID
	Topo      *config.Topology
	Ring      *crypto.KeyRing
	Store     *store.Store
	Logger    *log.Logger
	ReplySink         pbft.ReplySink
	PreparedCollector *twopc.PreparedCollector
	AckCollector         *twopc.AckCollector
	Disable2PCCommitPhase bool
	Enable2PC            bool
	Fault             pbft.FaultConfig
	DataDir           string // used when Store is nil
	Addr              string // empty = topology port; "host:0" for tests
}

// NewReplica constructs a replica with storage and PBFT on the topology port.
func NewReplica(self config.ServerID, topo *config.Topology, ring *crypto.KeyRing, logger *log.Logger) (*Replica, error) {
	return NewReplicaWithConfig(ReplicaConfig{
		Self:              self,
		Topo:              topo,
		Ring:              ring,
		Logger:            logger,
		DataDir:           "data",
		Enable2PC:         true,
		PreparedCollector: twopc.NewPreparedCollector(),
		AckCollector:      twopc.NewAckCollector(),
	})
}

// NewReplicaWithConfig builds a fully wired replica.
func NewReplicaWithConfig(cfg ReplicaConfig) (*Replica, error) {
	st := cfg.Store
	if st == nil {
		path := filepath.Join(cfg.DataDir, fmt.Sprintf("%s.db", cfg.Self))
		var err error
		st, err = store.Open(store.DefaultOptions(path))
		if err != nil {
			return nil, err
		}
	}

	logger := cfg.Logger
	r := &Replica{
		Self:   cfg.Self,
		Topo:   cfg.Topo,
		Ring:   cfg.Ring,
		Store:  st,
		Logger: logger,
		pongCh: make(chan *pb.Envelope, 64),
	}
	r.Hub = transport.NewHub(cfg.Self, cfg.Topo, cfg.Ring, logger, 256)

	addr := cfg.Addr
	if addr == "" {
		srv, ok := cfg.Topo.ServerByID(cfg.Self)
		if !ok {
			return nil, fmt.Errorf("server %s not in topology", cfg.Self)
		}
		addr = srv.Addr()
	}
	if err := r.Hub.Start(addr); err != nil {
		return nil, err
	}

	sender := pbft.NewHubSender(r.Hub, cfg.Topo, cfg.Self, 0)
	engine, err := pbft.NewEngine(cfg.Self, cfg.Topo, cfg.Ring, st, sender, cfg.ReplySink, logger)
	if err != nil {
		return nil, err
	}
	engine.SetFault(cfg.Fault)

	messenger := twopc.NewHubMessenger(r.Hub, cfg.Self)
	coord := twopc.NewCoordinator(twopc.CoordinatorConfig{
		Self:         cfg.Self,
		Topo:         cfg.Topo,
		Ring:         cfg.Ring,
		Store:        st,
		Engine:       engine,
		Messenger:    messenger,
		Collector:    cfg.PreparedCollector,
		AckCollector:       cfg.AckCollector,
		DisableCommitPhase: cfg.Disable2PCCommitPhase,
		Logger:             logger,
	})
	part := twopc.NewParticipant(twopc.ParticipantConfig{
		Self:      cfg.Self,
		Topo:      cfg.Topo,
		Ring:      cfg.Ring,
		Store:     st,
		Engine:    engine,
		Messenger: messenger,
		Logger:    logger,
	})
	bridge := &twopc.Bridge{Coord: coord, Part: part}
	if cfg.Enable2PC {
		engine.SetCrossShardHooks(bridge)
		r.TwoPC = bridge
	}

	r.PBFT = engine
	return r, nil
}

// SetFault reconfigures Byzantine/crash behaviour at runtime (tests).
func (r *Replica) SetFault(fc pbft.FaultConfig) {
	if r.PBFT != nil {
		r.PBFT.SetFault(fc)
	}
}

// ResetConsensus clears volatile PBFT and 2PC state for a fresh test set.
func (r *Replica) ResetConsensus() error {
	if r.PBFT != nil {
		r.PBFT.ResetConsensus()
	}
	if r.Store != nil {
		if err := r.Store.ClearLocksAndWAL(); err != nil {
			return err
		}
	}
	if r.TwoPC != nil {
		r.TwoPC.Reset()
	}
	return nil
}

// NewReplicaOnAddr is a convenience wrapper for tests.
func NewReplicaOnAddr(self config.ServerID, topo *config.Topology, ring *crypto.KeyRing, addr string, logger *log.Logger) (*Replica, error) {
	return NewReplicaWithConfig(ReplicaConfig{
		Self:   self,
		Topo:   topo,
		Ring:   ring,
		Logger: logger,
		Addr:   addr,
	})
}

// Addr returns the bound gRPC listen address.
func (r *Replica) Addr() string { return r.Hub.Addr() }

// Run starts the dispatch loop until ctx is cancelled.
func (r *Replica) Run(ctx context.Context) {
	r.mu.Lock()
	if r.started {
		r.mu.Unlock()
		return
	}
	r.started = true
	r.mu.Unlock()

	for {
		select {
		case <-ctx.Done():
			return
		case env, ok := <-r.Hub.Inbound():
			if !ok {
				return
			}
			r.dispatch(ctx, env)
		}
	}
}

// PongWait returns a channel that receives PONG envelopes handled by this replica.
func (r *Replica) PongWait() <-chan *pb.Envelope { return r.pongCh }

// BroadcastCluster sends a signed envelope to every other server in the sender's cluster.
func (r *Replica) BroadcastCluster(ctx context.Context, env *pb.Envelope) error {
	self, err := transport.SenderID(env)
	if err != nil {
		return err
	}
	srv, ok := r.Topo.ServerByID(self)
	if !ok {
		return fmt.Errorf("sender %s not in topology", self)
	}
	for _, peer := range r.Topo.ServersInCluster(srv.Cluster) {
		if peer.ID == self {
			continue
		}
		if err := r.Hub.Send(ctx, peer.ID, env); err != nil {
			return fmt.Errorf("send to %s: %w", peer.ID, err)
		}
	}
	return nil
}

func (r *Replica) dispatch(ctx context.Context, env *pb.Envelope) {
	if transport.IsPBFT(env.Type) {
		if r.PBFT != nil {
			// Run PBFT handling asynchronously so inbound delivery is not blocked
			// while the state machine performs cluster broadcasts.
			go r.PBFT.Handle(ctx, env)
		}
		return
	}
	switch env.Type {
	case transport.TypePing:
		r.handlePing(ctx, env)
	case transport.TypePong:
		select {
		case r.pongCh <- env:
		default:
			if r.Logger != nil {
				r.Logger.Printf("pong channel full, dropping from %d", env.SenderId)
			}
		}
	default:
		if transport.Is2PC(env.Type) {
			r.handle2PC(ctx, env)
			return
		}
		if r.Logger != nil {
			r.Logger.Printf("unknown message type %q from %d", env.Type, env.SenderId)
		}
	}
}

func (r *Replica) handle2PC(ctx context.Context, env *pb.Envelope) {
	if r.TwoPC == nil {
		return
	}
	switch env.Type {
	case transport.Type2PCPrepare:
		if r.TwoPC.Part != nil {
			go r.TwoPC.Part.HandlePrepare(ctx, env.Payload)
		}
	case transport.Type2PCPrepared, transport.Type2PCAbort:
		if r.TwoPC.Coord != nil {
			go r.TwoPC.Coord.HandleParticipantReply(ctx, env.Type, env.Payload)
		}
	case transport.Type2PCCommit:
		if r.TwoPC.Part != nil {
			go r.TwoPC.Part.HandleCommit(ctx, env.Payload)
		}
	case transport.Type2PCAck:
		if r.TwoPC.Coord != nil {
			from, err := transport.SenderID(env)
			if err != nil {
				return
			}
			go r.TwoPC.HandleParticipantAck(ctx, from, env.Payload)
		}
	}
}

func (r *Replica) handlePing(ctx context.Context, ping *pb.Envelope) {
	sender, err := transport.SenderID(ping)
	if err != nil {
		return
	}
	pong := transport.NewEnvelope(r.Self, transport.TypePong, ping.Payload)
	r.Hub.Sign(pong)
	if err := r.Hub.Send(ctx, sender, pong); err != nil && r.Logger != nil {
		r.Logger.Printf("pong to %s failed: %v", sender, err)
	}
}

// Stop shuts down the transport hub and store.
func (r *Replica) Stop() {
	r.Hub.Stop()
	if r.Store != nil {
		_ = r.Store.Close()
	}
}
