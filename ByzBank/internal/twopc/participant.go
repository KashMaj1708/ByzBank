package twopc

import (
	"context"
	"log"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/crypto"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/transport"
)

// Participant drives the prepare phase on the receiver cluster.
type Participant struct {
	self    config.ServerID
	cluster config.ClusterID
	topo    *config.Topology
	ring    *crypto.KeyRing
	store   *store.Store
	engine  *pbft.Engine
	msg     Messenger
	logger  *log.Logger
}

// ParticipantConfig wires a participant instance.
type ParticipantConfig struct {
	Self      config.ServerID
	Topo      *config.Topology
	Ring      *crypto.KeyRing
	Store     *store.Store
	Engine    *pbft.Engine
	Messenger Messenger
	Logger    *log.Logger
}

// NewParticipant constructs a participant helper.
func NewParticipant(cfg ParticipantConfig) *Participant {
	srv, _ := cfg.Topo.ServerByID(cfg.Self)
	return &Participant{
		self:    cfg.Self,
		cluster: srv.Cluster,
		topo:    cfg.Topo,
		ring:    cfg.Ring,
		store:   cfg.Store,
		engine:  cfg.Engine,
		msg:     cfg.Messenger,
		logger:  cfg.Logger,
	}
}

func (p *Participant) primary() config.ServerID {
	return p.topo.PrimaryOf(p.cluster, p.engine.View())
}

// HandlePrepare processes an incoming coordinator prepare message.
func (p *Participant) HandlePrepare(ctx context.Context, payload []byte) {
	if p.self != p.primary() {
		return
	}
	var msg CoordinatorPrepareMsg
	if err := unmarshal(payload, &msg); err != nil {
		return
	}
	coordCluster := p.topo.ClusterOf(msg.Req.X)
	if !VerifyClusterCert(p.ring, p.topo, coordCluster, msg.CommitCert, "COMMIT", p.topo.Quorum()) {
		return
	}
	req := msg.Req
	timers := pbft.DefaultTunables(*p.topo)
	deadline := time.Now().Add(timers.LockWaitTimeout + timers.ViewChangeTimeout)
	for p.engine.HasPendingClientPBFT() && time.Now().Before(deadline) {
		time.Sleep(timers.LockPollInterval)
	}
	if waitItemUnlocked(p.store, req.Y, timers.LockWaitTimeout, timers.LockPollInterval) {
		req.Op = pbft.OpPartPrepareCommit
	} else {
		req.Op = pbft.OpPartPrepareAbort
	}
	p.engine.StartConsensus(ctx, req)
}

// HandleCommit processes the coordinator's final commit/abort decision.
func (p *Participant) HandleCommit(ctx context.Context, payload []byte) {
	if p.self != p.primary() {
		return
	}
	var msg CoordinatorCommitMsg
	if err := unmarshal(payload, &msg); err != nil {
		return
	}
	coordCluster := p.topo.ClusterOf(msg.Req.X)
	if !VerifyClusterCert(p.ring, p.topo, coordCluster, msg.CommitCert, "COMMIT", p.topo.Quorum()) {
		return
	}
	client := msg.Req
	if msg.Outcome == store.OutcomeAbort {
		if p.prepareWasAbort(client) {
			p.sendAck(ctx, client, 0)
			return
		}
		client.Op = pbft.OpPartAbort
	} else {
		client.Op = pbft.OpPartCommit
	}
	p.engine.StartConsensus(ctx, client)
}

func (p *Participant) prepareWasAbort(req pbft.Request) bool {
	entries, err := p.store.Datastore()
	if err != nil {
		return false
	}
	want := pbft.TxnID(req)
	for _, e := range entries {
		if e.Phase != store.PhasePrepare || e.Outcome != store.OutcomeAbort {
			continue
		}
		entryReq := pbft.Request{X: e.X, Y: e.Y, Amt: e.Amt, ClientID: req.ClientID, TS: req.TS}
		if pbft.TxnID(entryReq) == want {
			return true
		}
	}
	return false
}

// OnPartPrepareExecuted sends the participant certificate back to the coordinator cluster.
func (p *Participant) OnPartPrepareExecuted(ctx context.Context, req pbft.Request, seq int64, cert pbft.CertificateMsg, outcome store.Outcome) {
	if p.self != p.primary() {
		return
	}
	client := req
	client.Op = ""
	reply := ParticipantReplyMsg{
		Req:        client,
		Outcome:    outcome,
		CommitCert: cert,
		PartSeq:    seq,
	}
	payload, err := marshal(reply)
	if err != nil {
		return
	}
	typ := transport.Type2PCPrepared
	if outcome == store.OutcomeAbort {
		typ = transport.Type2PCAbort
	}
	env := transport.NewEnvelope(p.self, typ, payload)
	coordCluster := p.topo.ClusterOf(req.X)
	_ = SendToCluster(ctx, p.topo, p.msg, coordCluster, env)
}

// OnPartCommitExecuted sends an ack to the coordinator cluster.
func (p *Participant) OnPartCommitExecuted(ctx context.Context, req pbft.Request, seq int64, _ pbft.CertificateMsg, _ store.Outcome) {
	p.sendAck(ctx, req, seq)
}

func (p *Participant) sendAck(ctx context.Context, req pbft.Request, partSeq int64) {
	client := req
	client.Op = ""
	ack := ParticipantAckMsg{Req: client, PartSeq: partSeq}
	payload, err := marshal(ack)
	if err != nil {
		return
	}
	env := transport.NewEnvelope(p.self, transport.Type2PCAck, payload)
	coordCluster := p.topo.ClusterOf(req.X)
	_ = SendToCluster(ctx, p.topo, p.msg, coordCluster, env)
}

// HandleClientRequest is unused on the participant.
func (p *Participant) HandleClientRequest(context.Context, pbft.Request) bool { return false }

// OnCrossPrepareDropped is unused on the participant.
func (p *Participant) OnCrossPrepareDropped(context.Context, pbft.Request) {}

// OnCoordPrepareExecuted is unused on the participant.
func (p *Participant) OnCoordPrepareExecuted(context.Context, pbft.Request, int64, pbft.CertificateMsg) {
}

// OnCoordCommitExecuted is unused on the participant.
func (p *Participant) OnCoordCommitExecuted(context.Context, pbft.Request, int64, pbft.CertificateMsg, store.Outcome) {
}
