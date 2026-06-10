package twopc

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/crypto"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/transport"
)

// Coordinator drives the prepare phase for cross-shard transactions.
type Coordinator struct {
	self      config.ServerID
	cluster   config.ClusterID
	topo      *config.Topology
	ring      *crypto.KeyRing
	store     *store.Store
	engine    *pbft.Engine
	msg       Messenger
	collector *PreparedCollector
	ackColl   *AckCollector
	logger    *log.Logger

	commitMu            sync.Mutex
	commitStarted       map[string]bool
	commitPhaseDisabled bool
	crossMu             sync.Mutex
	crossBusy           bool
}

// CoordinatorConfig wires a coordinator instance.
type CoordinatorConfig struct {
	Self          config.ServerID
	Topo          *config.Topology
	Ring          *crypto.KeyRing
	Store         *store.Store
	Engine        *pbft.Engine
	Messenger     Messenger
	Collector     *PreparedCollector
	AckCollector        *AckCollector
	DisableCommitPhase  bool
	Logger              *log.Logger
}

// NewCoordinator constructs a coordinator helper.
func NewCoordinator(cfg CoordinatorConfig) *Coordinator {
	srv, _ := cfg.Topo.ServerByID(cfg.Self)
	return &Coordinator{
		self:          cfg.Self,
		cluster:       srv.Cluster,
		topo:          cfg.Topo,
		ring:          cfg.Ring,
		store:         cfg.Store,
		engine:        cfg.Engine,
		msg:           cfg.Messenger,
		collector:     cfg.Collector,
		ackColl:       cfg.AckCollector,
		logger:        cfg.Logger,
		commitStarted:       make(map[string]bool),
		commitPhaseDisabled: cfg.DisableCommitPhase,
	}
}

func (c *Coordinator) primary() config.ServerID {
	return c.topo.PrimaryOf(c.cluster, c.engine.View())
}

func (c *Coordinator) acquireCrossSlot(timers pbft.Tunables) bool {
	deadline := time.Now().Add(timers.LockWaitTimeout + timers.ViewChangeTimeout)
	for time.Now().Before(deadline) {
		c.crossMu.Lock()
		if !c.crossBusy {
			c.crossBusy = true
			c.crossMu.Unlock()
			return true
		}
		c.crossMu.Unlock()
		time.Sleep(timers.LockPollInterval)
	}
	return false
}

// ReleaseCrossSlot allows the next cross-shard transaction on this coordinator.
func (c *Coordinator) ReleaseCrossSlot() {
	c.crossMu.Lock()
	c.crossBusy = false
	c.crossMu.Unlock()
}

// Reset clears volatile 2PC coordinator state for a fresh test set.
func (c *Coordinator) Reset() {
	c.commitMu.Lock()
	c.commitStarted = make(map[string]bool)
	c.commitMu.Unlock()
	c.crossMu.Lock()
	c.crossBusy = false
	c.crossMu.Unlock()
	if c.collector != nil {
		c.collector.Reset()
	}
	if c.ackColl != nil {
		c.ackColl.Reset()
	}
}

// HandleClientRequest implements pbft.CrossShardHooks.
func (c *Coordinator) HandleClientRequest(ctx context.Context, req pbft.Request) bool {
	if c.topo.SameCluster(req.X, req.Y) {
		return false
	}
	if c.topo.ClusterOf(req.X) != c.cluster || c.self != c.primary() {
		return false
	}
	if c.store.GetClientTS(req.ClientID) >= req.TS {
		return false
	}
	key := pbft.TxnID(req)
	c.commitMu.Lock()
	if c.commitStarted[key] {
		c.commitMu.Unlock()
		return false
	}
	c.commitMu.Unlock()
	if c.engine.ClientTxnInFlight(req) {
		return false
	}
	timers := pbft.DefaultTunables(*c.topo)
	if !c.acquireCrossSlot(timers) {
		return false
	}
	if !waitItemUnlocked(ctx, c.store, req.X, timers.LockWaitTimeout, timers.LockPollInterval) {
		c.ReleaseCrossSlot()
		return false
	}
	c.commitMu.Lock()
	if c.commitStarted[key] {
		c.commitMu.Unlock()
		c.ReleaseCrossSlot()
		return false
	}
	c.commitMu.Unlock()
	if c.store.GetBalance(req.X) < req.Amt {
		c.ReleaseCrossSlot()
		c.engine.RejectInsufficient(ctx, req)
		return true
	}
	prep := req
	prep.Op = pbft.OpCoordPrepare
	c.engine.StartClientConsensus(ctx, prep)
	return true
}

// OnCrossPrepareDropped implements pbft.CrossShardHooks.
func (c *Coordinator) OnCrossPrepareDropped(context.Context, pbft.Request) {
	c.ReleaseCrossSlot()
}

// OnCoordPrepareExecuted ships the coordinator prepare certificate to the participant cluster.
func (c *Coordinator) OnCoordPrepareExecuted(ctx context.Context, req pbft.Request, seq int64, cert pbft.CertificateMsg) {
	c.ReleaseCrossSlot()
	if c.self != c.primary() {
		return
	}
	client := req
	client.Op = ""
	msg := CoordinatorPrepareMsg{
		Req:        client,
		CommitCert: cert,
		CoordSeq:   seq,
	}
	payload, err := marshal(msg)
	if err != nil {
		return
	}
	env := transport.NewEnvelope(c.self, transport.Type2PCPrepare, payload)
	partCluster := c.topo.ClusterOf(req.Y)
	// Detach from the execute ctx — it is cancelled when PBFT handling returns;
	// prepare retry/timeout must survive until a participant reply or safe abort.
	phaseCtx := context.Background()
	sendPrepare := func() {
		_ = SendToCluster(phaseCtx, c.topo, c.msg, partCluster, env)
	}
	sendPrepare()

	if !c.commitPhaseDisabled {
		go c.watchPreparePhase(phaseCtx, req, sendPrepare)
	}
}

func (c *Coordinator) watchPreparePhase(ctx context.Context, req pbft.Request, resend func()) {
	timers := pbft.DefaultTunables(*c.topo)
	timeout := timers.CoordPrepareAbortTimeout
	if timeout <= 0 {
		timeout = 45 * time.Second
	}
	key := pbft.TxnID(req)
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(timers.AckRetryInterval)
	defer ticker.Stop()
	for time.Now().Before(deadline) {
		if c.collector != nil && c.collector.Has(req) {
			return
		}
		c.commitMu.Lock()
		if c.commitStarted[key] {
			c.commitMu.Unlock()
			return
		}
		c.commitMu.Unlock()
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			resend()
		}
	}
	c.commitMu.Lock()
	if c.commitStarted[key] || (c.collector != nil && c.collector.Has(req)) {
		c.commitMu.Unlock()
		return
	}
	c.commitStarted[key] = true
	c.commitMu.Unlock()
	abort := req
	abort.Op = pbft.OpCoordAbort
	abortDeadline := time.Now().Add(timers.AckRetryDeadline)
	for time.Now().Before(abortDeadline) {
		if !c.store.WALExists(key) {
			return
		}
		c.engine.StartConsensus(ctx, abort)
		time.Sleep(timers.AckRetryInterval)
	}
}

// OnPartPrepareExecuted is unused on the coordinator.
func (c *Coordinator) OnPartPrepareExecuted(context.Context, pbft.Request, int64, pbft.CertificateMsg, store.Outcome) {
}

// HandleParticipantReply records a participant prepared/abort certificate.
func (c *Coordinator) HandleParticipantReply(ctx context.Context, typ string, payload []byte) {
	var reply ParticipantReplyMsg
	if err := unmarshal(payload, &reply); err != nil {
		return
	}
	partCluster := c.topo.ClusterOf(reply.Req.Y)
	if !VerifyClusterCert(c.ring, c.topo, partCluster, reply.CommitCert, "COMMIT", c.topo.Quorum()) {
		return
	}
	if c.collector != nil {
		c.collector.Record(reply)
	}
	if c.self != c.primary() {
		return
	}
	c.maybeStartFinalCommit(ctx, reply)
}

func (c *Coordinator) maybeStartFinalCommit(ctx context.Context, reply ParticipantReplyMsg) {
	if c.commitPhaseDisabled {
		return
	}
	key := pbft.TxnID(reply.Req)
	c.commitMu.Lock()
	already := c.commitStarted[key]
	if !already {
		c.commitStarted[key] = true
	}
	c.commitMu.Unlock()
	if already {
		// Late prepare reply after a timeout abort was started: if the participant
		// credited y, re-drive coordinator abort so ack-retried delivery can undo it.
		if reply.Outcome == store.OutcomeCommit {
			abort := reply.Req
			abort.Op = pbft.OpCoordAbort
			c.engine.StartConsensus(ctx, abort)
		}
		return
	}

	req := reply.Req
	if reply.Outcome == store.OutcomeCommit {
		req.Op = pbft.OpCoordCommit
	} else {
		req.Op = pbft.OpCoordAbort
	}
	c.engine.StartConsensus(ctx, req)
}

// OnCoordCommitExecuted ships the final outcome to every participant replica.
func (c *Coordinator) OnCoordCommitExecuted(ctx context.Context, req pbft.Request, seq int64, cert pbft.CertificateMsg, outcome store.Outcome) {
	if c.self != c.primary() {
		return
	}
	client := req
	client.Op = ""
	msg := CoordinatorCommitMsg{
		Req:        client,
		Outcome:    outcome,
		CommitCert: cert,
		CoordSeq:   seq,
	}
	c.broadcastCommit(ctx, msg)
	// Retry commit and abort until f+1 participant acks — abort was previously fire-once.
	c.startAckRetry(ctx, client, msg)
}

func (c *Coordinator) broadcastCommit(ctx context.Context, msg CoordinatorCommitMsg) {
	payload, err := marshal(msg)
	if err != nil {
		return
	}
	env := transport.NewEnvelope(c.self, transport.Type2PCCommit, payload)
	partCluster := c.topo.ClusterOf(msg.Req.Y)
	_ = SendToCluster(ctx, c.topo, c.msg, partCluster, env)
}

func (c *Coordinator) startAckRetry(ctx context.Context, req pbft.Request, msg CoordinatorCommitMsg) {
	go func() {
		timers := pbft.DefaultTunables(*c.topo)
		quorum := c.topo.ClientQuorum()
		ticker := time.NewTicker(timers.AckRetryInterval)
		defer ticker.Stop()
		deadline := time.Now().Add(timers.AckRetryDeadline)
		for {
			if c.ackColl != nil && c.ackColl.Count(req) >= quorum {
				return
			}
			if time.Now().After(deadline) {
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.broadcastCommit(ctx, msg)
			}
		}
	}()
}

// HandleParticipantAck records an ack from a participant server.
func (c *Coordinator) HandleParticipantAck(_ context.Context, from config.ServerID, payload []byte) {
	var ack ParticipantAckMsg
	if err := unmarshal(payload, &ack); err != nil {
		return
	}
	if c.ackColl != nil {
		c.ackColl.Record(ack.Req, from)
	}
}

// OnPartCommitExecuted is unused on the coordinator.
func (c *Coordinator) OnPartCommitExecuted(context.Context, pbft.Request, int64, pbft.CertificateMsg, store.Outcome) {
}
