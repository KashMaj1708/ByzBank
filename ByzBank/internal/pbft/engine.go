package pbft

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/crypto"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pb"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/store"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/transport"
)

const (
	resultCommitted = "committed"
	resultAbort     = "abort"
)

// Sender sends signed envelopes to peers.
type Sender interface {
	Self() config.ServerID
	Sign(env *pb.Envelope)
	Send(ctx context.Context, to config.ServerID, env *pb.Envelope) error
	BroadcastCluster(ctx context.Context, env *pb.Envelope) error
	Primary() config.ServerID
}

// ReplySink collects client replies (tests and future client driver).
type ReplySink interface {
	Record(Reply)
}

// Engine runs linear PBFT for intra-shard transactions within one cluster.
type Engine struct {
	self    config.ServerID
	cluster config.ClusterID
	topo    *config.Topology
	ring    *crypto.KeyRing
	store   *store.Store
	sender     Sender
	sink       ReplySink
	crossShard CrossShardHooks
	logger     *log.Logger

	f               int
	prepareCollect  int // 2f+1 (same as commit quorum)
	commitQuorum    int // 2f+1
	clientQuorum    int // f+1

	fault    FaultConfig
	tunables Tunables

	mu         sync.Mutex
	proposalMu sync.Mutex // serialises new PBFT instances on the primary
	ingressOnce     sync.Once
	clientIngressCh chan ingressItem
	internalIngress chan ingressItem
	reclaimWG       sync.WaitGroup
	view       int
	nextSeq int64
	execSeq int64
	log     map[int64]*seqState

	viewChangeTimeout time.Duration
	viewChangeTimer   *time.Timer
	inViewChange      bool
	viewChangeTarget  int
	viewChangeSent    map[int]bool
	viewChangeLog     map[int]map[config.ServerID]ViewChangeMsg
	viewChangeReceived []ViewChangeMsg
	newViewIssued     map[int]bool
	newViewLog        []NewViewMsg
	pendingClients    map[string]Request
	recentReplies     map[string]Reply
}

type ingressItem struct {
	ctx context.Context
	req Request
}

type seqState struct {
	digest      []byte
	req         Request
	prePrepare  bool
	discarded   bool
	prepares    map[config.ServerID][]byte
	prepareCert *CertificateMsg
	commits     map[config.ServerID][]byte
	commitCert  *CertificateMsg
	executed    bool
}

// NewEngine constructs a PBFT engine for one replica.
func NewEngine(self config.ServerID, topo *config.Topology, ring *crypto.KeyRing, st *store.Store, sender Sender, sink ReplySink, logger *log.Logger) (*Engine, error) {
	srv, ok := topo.ServerByID(self)
	if !ok {
		return nil, fmt.Errorf("server %s not in topology", self)
	}
	t := DefaultTunables(*topo)
	return &Engine{
		self:           self,
		cluster:        srv.Cluster,
		topo:           topo,
		ring:           ring,
		store:          st,
		sender:         sender,
		sink:           sink,
		logger:         logger,
		f:              topo.F(),
		prepareCollect: topo.Quorum(),
		commitQuorum:   topo.Quorum(),
		clientQuorum:   topo.ClientQuorum(),
		fault:             DefaultFaultConfig(),
		tunables:          t,
		view:              0,
		nextSeq:           1,
		execSeq:           1,
		log:               make(map[int64]*seqState),
		viewChangeTimeout: t.ViewChangeTimeout,
		viewChangeSent:    make(map[int]bool),
		viewChangeLog:     make(map[int]map[config.ServerID]ViewChangeMsg),
		newViewIssued:     make(map[int]bool),
		pendingClients:    make(map[string]Request),
		recentReplies:     make(map[string]Reply),
	}, nil
}

// LookupReply returns a recorded client reply for a transaction, if any.
func (e *Engine) LookupReply(req Request) (Reply, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	r, ok := e.recentReplies[TxnID(req)]
	return r, ok
}

// Handle dispatches an inbound envelope to the PBFT state machine.
func (e *Engine) Handle(ctx context.Context, env *pb.Envelope) {
	if !e.fault.Alive {
		return
	}
	switch env.Type {
	case transport.TypeClientRequest:
		var req Request
		if err := unmarshal(env.Payload, &req); err != nil {
			return
		}
		e.onClientRequest(ctx, req)
	case transport.TypePrePrepare:
		var msg PrePrepareMsg
		if err := unmarshal(env.Payload, &msg); err != nil {
			return
		}
		e.onPrePrepare(ctx, msg)
	case transport.TypePrepare:
		var msg PrepareMsg
		if err := unmarshal(env.Payload, &msg); err != nil {
			return
		}
		e.onPrepare(ctx, config.ServerID(env.SenderId), msg, env.Signature)
	case transport.TypePrepareCert:
		var cert CertificateMsg
		if err := unmarshal(env.Payload, &cert); err != nil {
			return
		}
		e.onPrepareCert(ctx, cert)
	case transport.TypeCommit:
		var msg CommitMsg
		if err := unmarshal(env.Payload, &msg); err != nil {
			return
		}
		e.onCommit(ctx, config.ServerID(env.SenderId), msg, env.Signature)
	case transport.TypeCommitCert:
		var cert CertificateMsg
		if err := unmarshal(env.Payload, &cert); err != nil {
			return
		}
		e.onCommitCert(ctx, cert)
	case transport.TypeViewChange:
		var msg ViewChangeMsg
		if err := unmarshal(env.Payload, &msg); err != nil {
			return
		}
		e.onViewChange(ctx, config.ServerID(env.SenderId), msg)
	case transport.TypeNewView:
		var msg NewViewMsg
		if err := unmarshal(env.Payload, &msg); err != nil {
			return
		}
		e.onNewView(ctx, msg)
	case transport.TypeDiscardSeq:
		var msg DiscardSeqMsg
		if err := unmarshal(env.Payload, &msg); err != nil {
			return
		}
		e.onDiscardSeq(ctx, msg)
	}
}

// SetCrossShardHooks registers the 2PC coordinator/participant callbacks.
func (e *Engine) SetCrossShardHooks(h CrossShardHooks) { e.crossShard = h }

func (e *Engine) onClientRequest(ctx context.Context, req Request) {
	if e.sender.Primary() != e.self {
		e.onBackupClientRequest(ctx, req)
		return
	}
	if e.fault.ByzantineLeader {
		e.onBackupClientRequest(ctx, req)
		return
	}
	if req.Op == "" || req.Op == OpIntra {
		if !e.topo.SameCluster(req.X, req.Y) {
			if e.crossShard != nil && e.crossShard.HandleClientRequest(ctx, req) {
				return
			}
			return
		}
	}
	e.StartClientConsensus(ctx, req)
}

// StartClientConsensus begins PBFT for a client-originated request on the primary.
func (e *Engine) StartClientConsensus(ctx context.Context, req Request) {
	if e.isPrimaryIngress() {
		e.enqueuePrimary(ctx, req, true)
		return
	}
	e.startConsensus(ctx, req)
}

// StartConsensus begins a PBFT instance for an already-validated request.
func (e *Engine) StartConsensus(ctx context.Context, req Request) {
	if e.isPrimaryIngress() {
		e.enqueuePrimary(ctx, req, false)
		return
	}
	e.startConsensus(ctx, req)
}

func (e *Engine) isPrimaryIngress() bool {
	if e.sender.Primary() != e.self {
		return false
	}
	e.mu.Lock()
	alive := e.fault.Alive
	leader := e.fault.ByzantineLeader
	e.mu.Unlock()
	return alive && !leader
}

func (e *Engine) enqueuePrimary(ctx context.Context, req Request, fromClient bool) {
	e.ingressOnce.Do(func() {
		e.clientIngressCh = make(chan ingressItem, 512)
		e.internalIngress = make(chan ingressItem, 512)
		go e.primaryIngressLoop()
	})
	item := ingressItem{ctx: ctx, req: req}
	ch := e.internalIngress
	if fromClient {
		ch = e.clientIngressCh
	}
	select {
	case ch <- item:
	case <-ctx.Done():
	}
}

func (e *Engine) primaryIngressLoop() {
	for {
		select {
		case item := <-e.clientIngressCh:
			e.startConsensus(item.ctx, item.req)
			continue
		default:
		}
		select {
		case item := <-e.clientIngressCh:
			e.startConsensus(item.ctx, item.req)
		case item := <-e.internalIngress:
			e.startConsensus(item.ctx, item.req)
		}
	}
}

// ClientTxnInFlight reports whether this client transaction already has a live PBFT instance.
func (e *Engine) ClientTxnInFlight(req Request) bool {
	return e.clientTxnInFlight(req)
}

func (e *Engine) startConsensus(ctx context.Context, req Request) {
	if req.Op == "" {
		req.Op = OpIntra
	}
	isPrimary := e.sender.Primary() == e.self
	if isPrimary && !isCommitPhaseOp(req.Op) && e.clusterHonestLive() < e.prepareCollect {
		e.dropCrossPrepare(ctx, req)
		return
	}
	if isPrimary && !isCommitPhaseOp(req.Op) {
		if !e.awaitStartable(req) {
			e.dropCrossPrepare(ctx, req)
			return
		}
		e.proposalMu.Lock()
		defer e.proposalMu.Unlock()
		if !e.validateForStartQuick(req) {
			e.dropCrossPrepare(ctx, req)
			return
		}
	} else if isPrimary {
		e.proposalMu.Lock()
		defer e.proposalMu.Unlock()
		if !e.validateForStartQuick(req) {
			e.dropCrossPrepare(ctx, req)
			return
		}
	} else if !e.validateForStartQuick(req) {
		return
	}
	seq := e.assignSeq()
	digest := Digest(req)
	if isCommitPhaseOp(req.Op) {
		if !e.locksHeldForCommitOp(req) {
			return
		}
	} else if !e.acquireLocksForOp(seq, req) {
		e.releaseLocksForOp(req)
		e.dropCrossPrepare(ctx, req)
		return
	}

	msg := PrePrepareMsg{Seq: seq, View: e.view, Digest: digest, Req: req}
	e.mu.Lock()
	st := e.ensureSeqLocked(seq)
	st.digest = digest
	st.req = req
	st.prePrepare = true
	e.cancelViewChangeTimerLocked()
	e.mu.Unlock()

	if err := e.broadcastPrePrepare(ctx, msg); err != nil && e.logger != nil {
		e.logger.Printf("broadcast pre-prepare seq=%d: %v", seq, err)
	}
	e.processPrePrepare(ctx, msg)
	if isPrimary && !isCommitPhaseOp(req.Op) {
		e.watchPrepareQuorum(ctx, seq, req)
	}
}

func (e *Engine) onPrePrepare(ctx context.Context, msg PrePrepareMsg) {
	if msg.View != e.view {
		return
	}
	if !e.opMatchesCluster(msg.Req) {
		return
	}
	e.mu.Lock()
	if !e.gapClearLocked(msg.Seq) {
		e.mu.Unlock()
		return
	}
	if st, ok := e.log[msg.Seq]; ok && st.prePrepare && !st.discarded {
		e.mu.Unlock()
		return
	}
	st := e.ensureSeqLocked(msg.Seq)
	if st.discarded {
		st.discarded = false
		st.prepares = make(map[config.ServerID][]byte)
		st.commits = make(map[config.ServerID][]byte)
		st.prepareCert = nil
		st.commitCert = nil
		st.executed = false
	}
	st.digest = msg.Digest
	st.req = msg.Req
	st.prePrepare = true
	e.cancelViewChangeTimerLocked()
	e.mu.Unlock()

	e.processPrePrepare(ctx, msg)
}

func (e *Engine) processPrePrepare(ctx context.Context, msg PrePrepareMsg) {
	if e.fault.ByzantineBackup && e.sender.Primary() != e.self {
		return
	}
	if !e.validateRequest(msg.Req, msg.Seq) {
		return
	}
	if isCommitPhaseOp(msg.Req.Op) {
		if !e.locksHeldForCommitOp(msg.Req) {
			return
		}
	} else if !e.ensureLocksForOp(msg.Seq, msg.Req) {
		return
	}
	e.sendPrepare(ctx, msg.Seq, msg.View, msg.Digest)
}

func (e *Engine) onPrepare(ctx context.Context, from config.ServerID, msg PrepareMsg, sig []byte) {
	if e.sender.Primary() != e.self {
		return // collector only
	}
	if msg.View != e.view {
		return
	}
	e.mu.Lock()
	st, ok := e.log[msg.Seq]
	if !ok || !st.prePrepare || !bytesEqual(st.digest, msg.Digest) {
		e.mu.Unlock()
		return
	}
	if !e.ring.Verify(from, phaseSigningBytes("PREPARE", msg.Seq, msg.View, msg.Digest), sig) {
		e.mu.Unlock()
		return
	}
	if _, dup := st.prepares[from]; dup {
		e.mu.Unlock()
		return
	}
	st.prepares[from] = append([]byte(nil), sig...)
	ready := len(st.prepares) >= e.prepareCollect
	var cert *CertificateMsg
	if ready && st.prepareCert == nil {
		cert = e.buildCertLocked(msg.Seq, st, "PREPARE")
		st.prepareCert = cert
	}
	e.mu.Unlock()

	if cert != nil {
		e.broadcastPrepareCert(ctx, *cert)
		e.onPrepareCert(ctx, *cert)
	}
}

func (e *Engine) onPrepareCert(ctx context.Context, cert CertificateMsg) {
	if cert.View != e.view {
		return
	}
	if !e.verifyCert(cert, "PREPARE", e.prepareCollect) {
		return
	}
	e.mu.Lock()
	st, ok := e.log[cert.Seq]
	if !ok || !st.prePrepare || !bytesEqual(st.digest, cert.Digest) {
		e.mu.Unlock()
		return
	}
	st.prepareCert = &cert
	e.mu.Unlock()

	e.sendCommit(ctx, cert.Seq, cert.View, cert.Digest)
}

func (e *Engine) onCommit(ctx context.Context, from config.ServerID, msg CommitMsg, sig []byte) {
	if e.sender.Primary() != e.self {
		return
	}
	if msg.View != e.view {
		return
	}
	e.mu.Lock()
	st, ok := e.log[msg.Seq]
	if !ok || st.prepareCert == nil || !bytesEqual(st.digest, msg.Digest) {
		e.mu.Unlock()
		return
	}
	if !e.ring.Verify(from, phaseSigningBytes("COMMIT", msg.Seq, msg.View, msg.Digest), sig) {
		e.mu.Unlock()
		return
	}
	if _, dup := st.commits[from]; dup {
		e.mu.Unlock()
		return
	}
	st.commits[from] = append([]byte(nil), sig...)
	ready := len(st.commits) >= e.commitQuorum
	var cert *CertificateMsg
	if ready && st.commitCert == nil {
		cert = e.buildCertLocked(msg.Seq, st, "COMMIT")
		st.commitCert = cert
	}
	e.mu.Unlock()

	if cert != nil {
		e.broadcastCommitCert(ctx, *cert)
		e.onCommitCert(ctx, *cert)
	}
}

func (e *Engine) onCommitCert(ctx context.Context, cert CertificateMsg) {
	if cert.View != e.view {
		return
	}
	if !e.verifyCert(cert, "COMMIT", e.commitQuorum) {
		return
	}
	e.mu.Lock()
	st, ok := e.log[cert.Seq]
	if !ok || !st.prePrepare || !bytesEqual(st.digest, cert.Digest) {
		e.mu.Unlock()
		return
	}
	st.commitCert = &cert
	e.mu.Unlock()

	e.tryExecute(ctx)
}

func (e *Engine) tryExecute(ctx context.Context) {
	for {
		e.mu.Lock()
		st, ok := e.log[e.execSeq]
		if !ok {
			e.mu.Unlock()
			return
		}
		if st.discarded {
			e.execSeq++
			e.mu.Unlock()
			continue
		}
		if st.commitCert == nil || st.executed {
			e.mu.Unlock()
			return
		}
		req := st.req
		seq := e.execSeq
		cert := *st.commitCert
		st.executed = true
		e.execSeq++
		e.mu.Unlock()
		op := req.Op
		if op == "" {
			op = OpIntra
		}
		switch op {
		case OpIntra:
			e.executeIntra(ctx, req, seq)
		case OpCoordPrepare:
			e.executeCoordPrepare(ctx, req, seq, cert)
		case OpPartPrepareCommit:
			e.executePartPrepare(ctx, req, seq, cert, store.OutcomeCommit)
		case OpPartPrepareAbort:
			e.executePartPrepare(ctx, req, seq, cert, store.OutcomeAbort)
		case OpCoordCommit:
			e.executeCoordCommit(ctx, req, seq, cert)
		case OpCoordAbort:
			e.executeCoordAbort(ctx, req, seq, cert)
		case OpPartCommit:
			e.executePartCommit(ctx, req, seq, cert)
		case OpPartAbort:
			e.executePartAbort(ctx, req, seq, cert)
		default:
			e.releaseLocksForOp(req)
		}
	}
}

func (e *Engine) executeIntra(ctx context.Context, req Request, seq int64) {
	if err := e.store.ApplyTransfer(req.X, req.Y, req.Amt); err != nil {
		if e.logger != nil {
			e.logger.Printf("execute seq=%d failed: %v", seq, err)
		}
		e.releaseLocksForOp(req)
		return
	}
	_ = e.store.AppendDatastore(store.DatastoreEntry{
		Type:            store.TxnIntra,
		Phase:           store.PhaseCommit,
		X:               req.X,
		Y:               req.Y,
		Amt:             req.Amt,
		BallotOrViewSeq: int64(e.view),
		Outcome:         store.OutcomeCommit,
	})
	_ = e.store.SetClientTS(req.ClientID, req.TS)
	e.releaseLocksForOp(req)

	reply := Reply{
		ClientID: req.ClientID,
		TS:       req.TS,
		Seq:      seq,
		X:        req.X,
		Y:        req.Y,
		Amt:      req.Amt,
		Result:   resultCommitted,
	}
	e.sendClientReply(ctx, reply)
}

func (e *Engine) executeCoordPrepare(ctx context.Context, req Request, seq int64, cert CertificateMsg) {
	oldBal := e.store.GetBalance(req.X)
	if err := e.store.ApplyDebitOnly(req.X, req.Amt); err != nil {
		if e.logger != nil {
			e.logger.Printf("coord prepare seq=%d failed: %v", seq, err)
		}
		e.store.ReleaseLock(req.X)
		return
	}
	txnID := TxnID(req)
	_ = e.store.WALWrite(txnID, store.NewWALPreimage(map[int]int64{req.X: oldBal}))
	_ = e.store.AppendDatastore(store.DatastoreEntry{
		Type:            store.TxnCross,
		Phase:           store.PhasePrepare,
		X:               req.X,
		Y:               req.Y,
		Amt:             req.Amt,
		BallotOrViewSeq: int64(e.view),
		Outcome:         store.OutcomeCommit,
	})
	if e.crossShard != nil {
		e.crossShard.OnCoordPrepareExecuted(ctx, req, seq, cert)
	}
}

func (e *Engine) executePartPrepare(ctx context.Context, req Request, seq int64, cert CertificateMsg, outcome store.Outcome) {
	if outcome == store.OutcomeCommit {
		oldBal := e.store.GetBalance(req.Y)
		if err := e.store.ApplyCreditOnly(req.Y, req.Amt); err != nil {
			if e.logger != nil {
				e.logger.Printf("part prepare seq=%d failed: %v", seq, err)
			}
			e.store.ReleaseLock(req.Y)
			return
		}
		txnID := TxnID(req)
		_ = e.store.WALWrite(txnID, store.NewWALPreimage(map[int]int64{req.Y: oldBal}))
	}
	_ = e.store.AppendDatastore(store.DatastoreEntry{
		Type:            store.TxnCross,
		Phase:           store.PhasePrepare,
		X:               req.X,
		Y:               req.Y,
		Amt:             req.Amt,
		BallotOrViewSeq: int64(e.view),
		Outcome:         outcome,
	})
	if e.crossShard != nil {
		e.crossShard.OnPartPrepareExecuted(ctx, req, seq, cert, outcome)
	}
}

func (e *Engine) executeCoordCommit(ctx context.Context, req Request, seq int64, cert CertificateMsg) {
	txnID := TxnID(req)
	_ = e.store.AppendDatastore(store.DatastoreEntry{
		Type:            store.TxnCross,
		Phase:           store.PhaseCommit,
		X:               req.X,
		Y:               req.Y,
		Amt:             req.Amt,
		BallotOrViewSeq: int64(e.view),
		Outcome:         store.OutcomeCommit,
	})
	_ = e.store.WALDelete(txnID)
	e.store.ReleaseLock(req.X)
	_ = e.store.SetClientTS(req.ClientID, req.TS)

	reply := Reply{
		ClientID: req.ClientID,
		TS:       req.TS,
		Seq:      seq,
		X:        req.X,
		Y:        req.Y,
		Amt:      req.Amt,
		Result:   resultCommitted,
	}
	e.sendClientReply(ctx, reply)
	if e.crossShard != nil {
		e.crossShard.OnCoordCommitExecuted(ctx, req, seq, cert, store.OutcomeCommit)
	}
}

func (e *Engine) executeCoordAbort(ctx context.Context, req Request, seq int64, cert CertificateMsg) {
	txnID := TxnID(req)
	if e.store.WALExists(txnID) {
		_ = e.store.WALUndo(txnID)
		_ = e.store.WALDelete(txnID)
	}
	_ = e.store.AppendDatastore(store.DatastoreEntry{
		Type:            store.TxnCross,
		Phase:           store.PhaseCommit,
		X:               req.X,
		Y:               req.Y,
		Amt:             req.Amt,
		BallotOrViewSeq: int64(e.view),
		Outcome:         store.OutcomeAbort,
	})
	if e.store.IsLocked(req.X) {
		e.store.ReleaseLock(req.X)
	}
	_ = e.store.SetClientTS(req.ClientID, req.TS)

	reply := Reply{
		ClientID: req.ClientID,
		TS:       req.TS,
		Seq:      seq,
		X:        req.X,
		Y:        req.Y,
		Amt:      req.Amt,
		Result:   resultAbort,
	}
	e.sendClientReply(ctx, reply)
	if e.crossShard != nil {
		e.crossShard.OnCoordCommitExecuted(ctx, req, seq, cert, store.OutcomeAbort)
	}
}

func (e *Engine) executePartCommit(ctx context.Context, req Request, seq int64, cert CertificateMsg) {
	txnID := TxnID(req)
	_ = e.store.AppendDatastore(store.DatastoreEntry{
		Type:            store.TxnCross,
		Phase:           store.PhaseCommit,
		X:               req.X,
		Y:               req.Y,
		Amt:             req.Amt,
		BallotOrViewSeq: int64(e.view),
		Outcome:         store.OutcomeCommit,
	})
	_ = e.store.WALDelete(txnID)
	if e.store.IsLocked(req.Y) {
		e.store.ReleaseLock(req.Y)
	}
	if e.crossShard != nil {
		e.crossShard.OnPartCommitExecuted(ctx, req, seq, cert, store.OutcomeCommit)
	}
}

func (e *Engine) executePartAbort(ctx context.Context, req Request, seq int64, cert CertificateMsg) {
	txnID := TxnID(req)
	if e.store.WALExists(txnID) {
		_ = e.store.WALUndo(txnID)
		_ = e.store.WALDelete(txnID)
	}
	_ = e.store.AppendDatastore(store.DatastoreEntry{
		Type:            store.TxnCross,
		Phase:           store.PhaseCommit,
		X:               req.X,
		Y:               req.Y,
		Amt:             req.Amt,
		BallotOrViewSeq: int64(e.view),
		Outcome:         store.OutcomeAbort,
	})
	if e.store.IsLocked(req.Y) {
		e.store.ReleaseLock(req.Y)
	}
	if e.crossShard != nil {
		e.crossShard.OnPartCommitExecuted(ctx, req, seq, cert, store.OutcomeAbort)
	}
}

// --- helpers ---

func isCommitPhaseOp(op string) bool {
	switch op {
	case OpCoordCommit, OpCoordAbort, OpPartCommit, OpPartAbort:
		return true
	default:
		return false
	}
}

func (e *Engine) locksHeldForCommitOp(req Request) bool {
	switch req.Op {
	case OpCoordCommit, OpCoordAbort:
		return e.store.IsLocked(req.X)
	case OpPartCommit, OpPartAbort:
		return e.store.IsLocked(req.Y)
	default:
		return true
	}
}

func (e *Engine) assignSeq() int64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	seq := e.nextSeq
	e.nextSeq++
	return seq
}

func (e *Engine) ensureSeq(seq int64) *seqState {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.ensureSeqLocked(seq)
}

func (e *Engine) ensureSeqLocked(seq int64) *seqState {
	st, ok := e.log[seq]
	if !ok {
		st = &seqState{
			prepares: make(map[config.ServerID][]byte),
			commits:  make(map[config.ServerID][]byte),
		}
		e.log[seq] = st
	}
	return st
}

func (e *Engine) gapClearLocked(seq int64) bool {
	for i := int64(1); i < seq; i++ {
		if i < e.execSeq {
			continue
		}
		st, ok := e.log[i]
		if !ok {
			return false
		}
		if st.discarded {
			continue
		}
		if !st.prePrepare {
			return false
		}
	}
	return true
}

func (e *Engine) opMatchesCluster(req Request) bool {
	op := req.Op
	if op == "" {
		op = OpIntra
	}
	switch op {
	case OpIntra:
		return e.topo.SameCluster(req.X, req.Y)
	case OpCoordPrepare, OpCoordCommit, OpCoordAbort:
		return e.topo.ClusterOf(req.X) == e.cluster
	case OpPartPrepareCommit, OpPartPrepareAbort, OpPartCommit, OpPartAbort:
		return e.topo.ClusterOf(req.Y) == e.cluster
	default:
		return false
	}
}

func (e *Engine) lockItemsForOp(req Request) []int {
	switch req.Op {
	case OpIntra, "":
		return []int{req.X, req.Y}
	case OpCoordPrepare:
		return []int{req.X}
	case OpPartPrepareCommit:
		return []int{req.Y}
	case OpPartPrepareAbort:
		return nil
	case OpCoordCommit, OpCoordAbort:
		return nil
	case OpPartCommit, OpPartAbort:
		return nil
	default:
		return nil
	}
}

func (e *Engine) canAcquireForOp(req Request) bool {
	for _, item := range e.lockItemsForOp(req) {
		if e.store.IsLocked(item) {
			return false
		}
	}
	return true
}

func (e *Engine) waitAcquirableForOp(req Request) bool {
	deadline := time.Now().Add(e.tunables.LockWaitTimeout)
	for {
		if e.canAcquireForOp(req) {
			return true
		}
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(e.tunables.LockPollInterval)
	}
}

// awaitStartable polls until contended locks clear, then checks balances/TS.
// Runs before proposalMu so unrelated traffic is not serialized during the wait.
func (e *Engine) awaitStartable(req Request) bool {
	op := req.Op
	if op == OpIntra && e.store.GetClientTS(req.ClientID) >= req.TS {
		return false
	}
	switch op {
	case OpIntra, OpCoordPrepare:
		if !e.waitAcquirableForOp(req) {
			return false
		}
		return e.store.GetBalance(req.X) >= req.Amt
	case OpPartPrepareCommit:
		return e.waitAcquirableForOp(req)
	default:
		return true
	}
}

func (e *Engine) acquireLocksForOp(seq int64, req Request) bool {
	for _, item := range e.lockItemsForOp(req) {
		if !e.store.AcquireLock(item, seq) {
			return false
		}
	}
	return true
}

func (e *Engine) releaseLocksForOp(req Request) {
	for _, item := range e.lockItemsForOp(req) {
		e.store.ReleaseLock(item)
	}
}

func (e *Engine) ensureLocksForOp(seq int64, req Request) bool {
	for _, item := range e.lockItemsForOp(req) {
		if !e.store.AcquireLockForSeq(item, seq) {
			return false
		}
	}
	return true
}

func (e *Engine) clientTxnInFlight(req Request) bool {
	key := TxnID(clientReq(req))
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, st := range e.log {
		if st.discarded || st.executed || !st.prePrepare {
			continue
		}
		if TxnID(clientReq(st.req)) == key {
			return true
		}
	}
	return false
}

func (e *Engine) validateForStartQuick(req Request) bool {
	op := req.Op
	if op == "" {
		op = OpIntra
	}
	if op == OpIntra && e.store.GetClientTS(req.ClientID) >= req.TS {
		return false
	}
	switch op {
	case OpIntra:
		if e.clientTxnInFlight(req) {
			return false
		}
		if !e.canAcquireForOp(req) {
			return false
		}
		return e.store.GetBalance(req.X) >= req.Amt
	case OpCoordPrepare:
		if e.clientTxnInFlight(req) {
			return false
		}
		if !e.canAcquireForOp(req) {
			return false
		}
		return e.store.GetBalance(req.X) >= req.Amt
	case OpPartPrepareCommit:
		return e.canAcquireForOp(req)
	case OpPartPrepareAbort:
		return true
	case OpCoordCommit:
		return e.store.IsLocked(req.X) && e.store.WALExists(TxnID(req))
	case OpCoordAbort:
		return e.store.IsLocked(req.X)
	case OpPartCommit:
		return e.store.IsLocked(req.Y) && e.store.WALExists(TxnID(req))
	case OpPartAbort:
		return true
	default:
		return false
	}
}

func (e *Engine) validateRequest(req Request, seq int64) bool {
	op := req.Op
	if op == "" {
		op = OpIntra
	}
	if op == OpIntra && e.store.GetClientTS(req.ClientID) >= req.TS {
		return false
	}
	if op == OpIntra || op == OpCoordPrepare {
		if e.store.GetBalance(req.X) < req.Amt {
			return false
		}
	}
	if op == OpPartPrepareAbort || isCommitPhaseOp(op) {
		return e.locksHeldForCommitOp(req)
	}
	deadline := time.Now().Add(e.tunables.LockWaitTimeout)
	for time.Now().Before(deadline) {
		if e.locksReadyForSeq(seq, req) {
			return true
		}
		time.Sleep(e.tunables.LockPollInterval)
	}
	return e.locksReadyForSeq(seq, req)
}

func (e *Engine) locksReadyForSeq(seq int64, req Request) bool {
	for _, item := range e.lockItemsForOp(req) {
		if e.store.IsLocked(item) && e.store.LockSeq(item) != seq {
			return false
		}
	}
	return true
}

func (e *Engine) buildCertLocked(seq int64, st *seqState, phase string) *CertificateMsg {
	entries := make([]SigEntry, 0)
	var src map[config.ServerID][]byte
	quorum := e.commitQuorum
	if phase == "PREPARE" {
		src = st.prepares
		quorum = e.prepareCollect
	} else {
		src = st.commits
	}
	for id, sig := range src {
		entries = append(entries, SigEntry{ServerID: int32(id), Sig: sig})
	}
	for i := 0; i < len(entries); i++ {
		for j := i + 1; j < len(entries); j++ {
			if entries[j].ServerID < entries[i].ServerID {
				entries[i], entries[j] = entries[j], entries[i]
			}
		}
	}
	if len(entries) < quorum {
		return nil
	}
	return &CertificateMsg{
		Seq:    seq,
		View:   e.view,
		Digest: append([]byte(nil), st.digest...),
		Sigs:   entries,
	}
}

func (e *Engine) verifyCert(cert CertificateMsg, phase string, quorum int) bool {
	if len(cert.Sigs) < quorum {
		return false
	}
	seen := make(map[int32]struct{}, len(cert.Sigs))
	for _, ent := range cert.Sigs {
		if _, dup := seen[ent.ServerID]; dup {
			return false
		}
		seen[ent.ServerID] = struct{}{}
		pub, ok := e.ring.PublicKey(config.ServerID(ent.ServerID))
		if !ok {
			return false
		}
		if !crypto.Verify(pub, phaseSigningBytes(phase, cert.Seq, cert.View, cert.Digest), ent.Sig) {
			return false
		}
	}
	return true
}

func (e *Engine) sendPrepare(ctx context.Context, seq int64, view int, digest []byte) {
	if e.fault.ByzantineBackup && e.sender.Primary() != e.self {
		return
	}
	msg := PrepareMsg{Seq: seq, View: view, Digest: digest}
	payload, _ := marshal(msg)
	env := transport.NewEnvelope(e.self, transport.TypePrepare, payload)
	signBytes := phaseSigningBytes("PREPARE", seq, view, digest)
	env.Signature = e.ring.Sign(signBytes)
	primary := e.sender.Primary()
	if primary == e.self {
		e.onPrepare(ctx, e.self, msg, env.Signature)
		return
	}
	_ = e.sender.Send(ctx, primary, env)
}

func (e *Engine) sendCommit(ctx context.Context, seq int64, view int, digest []byte) {
	msg := CommitMsg{Seq: seq, View: view, Digest: digest}
	payload, _ := marshal(msg)
	env := transport.NewEnvelope(e.self, transport.TypeCommit, payload)
	signBytes := phaseSigningBytes("COMMIT", seq, view, digest)
	env.Signature = e.ring.Sign(signBytes)
	primary := e.sender.Primary()
	if primary == e.self {
		e.onCommit(ctx, e.self, msg, env.Signature)
		return
	}
	_ = e.sender.Send(ctx, primary, env)
}

func (e *Engine) broadcastPrePrepare(ctx context.Context, msg PrePrepareMsg) error {
	if e.fault.ByzantineLeader {
		return nil
	}
	payload, err := marshal(msg)
	if err != nil {
		return err
	}
	env := transport.NewEnvelope(e.self, transport.TypePrePrepare, payload)
	e.sender.Sign(env)
	return e.sender.BroadcastCluster(ctx, env)
}

func (e *Engine) broadcastPrepareCert(ctx context.Context, cert CertificateMsg) error {
	payload, err := marshal(cert)
	if err != nil {
		return err
	}
	env := transport.NewEnvelope(e.self, transport.TypePrepareCert, payload)
	e.sender.Sign(env)
	return e.sender.BroadcastCluster(ctx, env)
}

func (e *Engine) broadcastCommitCert(ctx context.Context, cert CertificateMsg) error {
	payload, err := marshal(cert)
	if err != nil {
		return err
	}
	env := transport.NewEnvelope(e.self, transport.TypeCommitCert, payload)
	e.sender.Sign(env)
	return e.sender.BroadcastCluster(ctx, env)
}

func (e *Engine) watchPrepareQuorum(ctx context.Context, seq int64, req Request) {
	e.reclaimWG.Add(1)
	go func() {
		defer e.reclaimWG.Done()
		deadline := time.Now().Add(e.prepareWatchDeadline())
		for time.Now().Before(deadline) {
			e.mu.Lock()
			st, ok := e.log[seq]
			ready := ok && (st.prepareCert != nil || st.commitCert != nil || st.executed)
			stale := ok && st.discarded
			certPending := ok && len(st.prepares) >= e.prepareCollect && st.prepareCert == nil
			e.mu.Unlock()
			if ready || stale {
				return
			}
			if certPending {
				select {
				case <-ctx.Done():
					return
				case <-time.After(e.tunables.LockPollInterval):
				}
				continue
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(e.tunables.LockPollInterval):
			}
		}
		e.mu.Lock()
		st, ok := e.log[seq]
		if !ok || st.executed || st.commitCert != nil || st.prepareCert != nil || st.discarded {
			e.mu.Unlock()
			return
		}
		e.mu.Unlock()
		e.reclaimSeq(ctx, seq, req)
	}()
}

func (e *Engine) reclaimSeq(ctx context.Context, seq int64, req Request) {
	if e.sender.Primary() != e.self {
		return
	}
	e.mu.Lock()
	st, ok := e.log[seq]
	if !ok || st.executed || st.commitCert != nil || st.prepareCert != nil {
		e.mu.Unlock()
		return
	}
	if len(st.prepares) >= e.prepareCollect {
		e.mu.Unlock()
		return
	}
	delete(e.log, seq)
	e.mu.Unlock()

	e.releaseLocksForOp(req)
	e.skipExecSeqIfStuck(seq)
	e.dropCrossPrepare(ctx, req)

	e.mu.Lock()
	if e.nextSeq == seq+1 {
		e.nextSeq = seq
	}
	e.mu.Unlock()

	msg := DiscardSeqMsg{Seq: seq, View: e.view}
	_ = e.broadcastDiscardSeq(ctx, msg)
	e.onDiscardSeq(ctx, msg)
}

func (e *Engine) onDiscardSeq(ctx context.Context, msg DiscardSeqMsg) {
	if msg.View != e.view {
		return
	}
	e.mu.Lock()
	st, ok := e.log[msg.Seq]
	if !ok || st.executed || st.commitCert != nil {
		e.mu.Unlock()
		return
	}
	req := st.req
	delete(e.log, msg.Seq)
	e.mu.Unlock()

	e.releaseLocksForOp(req)
	e.skipExecSeqIfStuck(msg.Seq)
	if ctx == nil {
		ctx = context.Background()
	}
	e.tryExecute(ctx)
}

func (e *Engine) skipExecSeqIfStuck(seq int64) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.execSeq == seq {
		e.execSeq++
	}
}

func (e *Engine) dropCrossPrepare(ctx context.Context, req Request) {
	if req.Op != OpCoordPrepare || e.crossShard == nil {
		return
	}
	e.crossShard.OnCrossPrepareDropped(ctx, req)
}

func (e *Engine) prepareWatchDeadline() time.Duration {
	mult := time.Duration(4)
	if e.clusterHonestLive() == e.prepareCollect {
		mult = 12
	}
	return e.tunables.ViewChangeTimeout * mult
}

func (e *Engine) clusterHonestLive() int {
	e.mu.Lock()
	n := e.fault.ClusterHonestLive
	e.mu.Unlock()
	if n <= 0 {
		return e.topo.ClusterSize
	}
	return n
}

// HasPendingClientPBFT reports client intra work still queued or in the PBFT log.
func (e *Engine) HasPendingClientPBFT() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, st := range e.log {
		if st.discarded || st.executed || !st.prePrepare {
			continue
		}
		op := st.req.Op
		if op == "" || op == OpIntra {
			return true
		}
	}
	return e.clientIngressCh != nil && len(e.clientIngressCh) > 0
}

// CoordPrepareInFlight counts live coordinator-prepare PBFT instances.
func (e *Engine) CoordPrepareInFlight() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	n := 0
	for _, st := range e.log {
		if st.discarded || st.executed || !st.prePrepare {
			continue
		}
		if st.req.Op == OpCoordPrepare {
			n++
		}
	}
	return n
}

// EngineState captures PBFT sequence metadata for catch-up.
type EngineState struct {
	View    int   `json:"view"`
	ExecSeq int64 `json:"exec_seq"`
	NextSeq int64 `json:"next_seq"`
}

// ExportState returns view and sequence pointers for state transfer.
func (e *Engine) ExportState() EngineState {
	e.mu.Lock()
	defer e.mu.Unlock()
	return EngineState{View: e.view, ExecSeq: e.execSeq, NextSeq: e.nextSeq}
}

// ResetForCatchUp clears volatile PBFT state and adopts primary sequence metadata.
func (e *Engine) ResetForCatchUp(st EngineState) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.cancelViewChangeTimerLocked()
	e.view = st.View
	e.execSeq = st.ExecSeq
	if st.NextSeq > st.ExecSeq {
		e.nextSeq = st.NextSeq
	} else {
		e.nextSeq = st.ExecSeq
	}
	e.log = make(map[int64]*seqState)
	e.inViewChange = false
	e.viewChangeTarget = 0
	e.viewChangeSent = make(map[int]bool)
	e.viewChangeLog = make(map[int]map[config.ServerID]ViewChangeMsg)
	e.pendingClients = make(map[string]Request)
}

// WaitReclaims blocks until async seq-reclaim watchers finish or timeout expires.
func (e *Engine) WaitReclaims(ctx context.Context, timeout time.Duration) {
	done := make(chan struct{})
	go func() {
		e.reclaimWG.Wait()
		close(done)
	}()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-ctx.Done():
	case <-timer.C:
	}
}

// DrainExecute runs the execution loop until no further committed slots apply.
func (e *Engine) DrainExecute(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		e.mu.Lock()
		before := e.execSeq
		e.mu.Unlock()
		e.tryExecute(ctx)
		e.mu.Lock()
		after := e.execSeq
		e.mu.Unlock()
		if after == before {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(e.tunables.LockPollInterval):
		}
	}
}

func (e *Engine) broadcastDiscardSeq(ctx context.Context, msg DiscardSeqMsg) error {
	payload, err := marshal(msg)
	if err != nil {
		return err
	}
	env := transport.NewEnvelope(e.self, transport.TypeDiscardSeq, payload)
	e.sender.Sign(env)
	return e.sender.BroadcastCluster(ctx, env)
}

func (e *Engine) sendClientReply(ctx context.Context, reply Reply) {
	e.mu.Lock()
	e.recentReplies[TxnID(Request{
		ClientID: reply.ClientID,
		TS:       reply.TS,
		X:        reply.X,
		Y:        reply.Y,
		Amt:      reply.Amt,
	})] = reply
	e.mu.Unlock()

	if e.sink != nil {
		e.sink.Record(reply)
	}
	_ = ctx
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}