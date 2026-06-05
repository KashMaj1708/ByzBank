package transport

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/crypto"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Hub is the gRPC listener plus lazy peer connections for one replica.
type Hub struct {
	self   config.ServerID
	topo   *config.Topology
	ring   *crypto.KeyRing
	logger *log.Logger

	inbound chan *pb.Envelope

	grpcServer *grpc.Server
	listener   net.Listener
	stopOnce   sync.Once

	mu    sync.RWMutex
	peers map[config.ServerID]pb.ReplicaTransportClient
	conns map[config.ServerID]*grpc.ClientConn
}

// NewHub creates a transport hub for one replica.
func NewHub(self config.ServerID, topo *config.Topology, ring *crypto.KeyRing, logger *log.Logger, inboundBuf int) *Hub {
	if inboundBuf <= 0 {
		inboundBuf = 256
	}
	return &Hub{
		self:    self,
		topo:    topo,
		ring:    ring,
		logger:  logger,
		inbound: make(chan *pb.Envelope, inboundBuf),
		peers:   make(map[config.ServerID]pb.ReplicaTransportClient),
		conns:   make(map[config.ServerID]*grpc.ClientConn),
	}
}

// Inbound exposes verified envelopes delivered to this replica.
func (h *Hub) Inbound() <-chan *pb.Envelope { return h.inbound }

// Addr returns the bound listen address (useful when port=0 in tests).
func (h *Hub) Addr() string {
	if h.listener == nil {
		return ""
	}
	return h.listener.Addr().String()
}

// Start binds addr and serves gRPC. Pass "host:0" in tests for an ephemeral port.
func (h *Hub) Start(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	h.listener = ln
	h.grpcServer = grpc.NewServer()
	pb.RegisterReplicaTransportServer(h.grpcServer, &grpcService{hub: h})
	go func() {
		if err := h.grpcServer.Serve(ln); err != nil {
			if h.logger != nil {
				h.logger.Printf("grpc serve ended: %v", err)
			}
		}
	}()
	return nil
}

// Stop drains the gRPC server and closes peer connections.
func (h *Hub) Stop() {
	h.stopOnce.Do(func() {
		if h.grpcServer != nil {
			h.grpcServer.GracefulStop()
		}
		h.mu.Lock()
		for id, conn := range h.conns {
			_ = conn.Close()
			delete(h.conns, id)
			delete(h.peers, id)
		}
		h.mu.Unlock()
		close(h.inbound)
	})
}

// Sign attaches an ed25519 signature to env using this replica's private key.
func (h *Hub) Sign(env *pb.Envelope) {
	env.Signature = h.ring.Sign(SigningBytesFromEnvelope(env))
}

// Verify checks the envelope signature against the claimed sender.
func (h *Hub) Verify(env *pb.Envelope) bool {
	sender, err := SenderID(env)
	if err != nil {
		return false
	}
	return h.ring.Verify(sender, SigningBytesFromEnvelope(env), env.Signature)
}

// Inject enqueues an envelope without signature verification (client requests).
func (h *Hub) Inject(env *pb.Envelope) error {
	select {
	case h.inbound <- env:
		return nil
	default:
		return fmt.Errorf("inbound channel full")
	}
}

// Deliver verifies env and enqueues it for the replica dispatch loop.
func (h *Hub) Deliver(env *pb.Envelope) error {
	if env.GetType() == TypeClientRequest {
		return h.Inject(env)
	}
	// PREPARE/COMMIT sign phase digests (linear PBFT); the engine verifies them.
	if env.GetType() == TypePrepare || env.GetType() == TypeCommit {
		select {
		case h.inbound <- env:
			return nil
		default:
			return fmt.Errorf("inbound channel full")
		}
	}
	if !h.Verify(env) {
		return fmt.Errorf("invalid signature from sender %d", env.GetSenderId())
	}
	select {
	case h.inbound <- env:
		return nil
	default:
		return fmt.Errorf("inbound channel full")
	}
}

// Send delivers a signed envelope to peer to via gRPC (lazy dial + reconnect).
func (h *Hub) Send(ctx context.Context, to config.ServerID, env *pb.Envelope) error {
	if env.Signature == nil {
		return fmt.Errorf("refusing to send unsigned envelope")
	}
	client, err := h.peer(ctx, to)
	if err != nil {
		return err
	}
	ack, err := client.Send(ctx, env)
	if err != nil {
		h.dropPeer(to)
		return err
	}
	if ack == nil || !ack.Ok {
		msg := ""
		if ack != nil {
			msg = ack.Error
		}
		return fmt.Errorf("peer %s rejected envelope: %s", to, msg)
	}
	return nil
}

func (h *Hub) peer(ctx context.Context, to config.ServerID) (pb.ReplicaTransportClient, error) {
	h.mu.RLock()
	if c, ok := h.peers[to]; ok {
		h.mu.RUnlock()
		return c, nil
	}
	h.mu.RUnlock()

	h.mu.Lock()
	defer h.mu.Unlock()
	if c, ok := h.peers[to]; ok {
		return c, nil
	}

	srv, ok := h.topo.ServerByID(to)
	if !ok {
		return nil, fmt.Errorf("unknown server %s", to)
	}
	dialCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	conn, err := grpc.DialContext(dialCtx, srv.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, fmt.Errorf("dial %s at %s: %w", to, srv.Addr(), err)
	}
	client := pb.NewReplicaTransportClient(conn)
	h.conns[to] = conn
	h.peers[to] = client
	return client, nil
}

func (h *Hub) dropPeer(to config.ServerID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if conn, ok := h.conns[to]; ok {
		_ = conn.Close()
		delete(h.conns, to)
		delete(h.peers, to)
	}
}

type grpcService struct {
	pb.UnimplementedReplicaTransportServer
	hub *Hub
}

func (s *grpcService) Send(ctx context.Context, env *pb.Envelope) (*pb.SendAck, error) {
	if err := s.hub.Deliver(env); err != nil {
		return &pb.SendAck{Ok: false, Error: err.Error()}, nil
	}
	return &pb.SendAck{Ok: true}, nil
}
