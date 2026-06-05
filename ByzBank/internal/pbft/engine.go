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

const resultCommitted = "committed"

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
	sender  Sender
	sink    ReplySink
	logger  *log.Logger

	f               int
	prepareCollect  int // n-f
	commitQuorum    int // 2f+1
	clientQuorum    int // f+1

	mu      sync.Mutex
	view    int
	nextSeq int64
	execSeq int64
	log     map[int64]*seqState
}

type seqState struct {
	digest      []byte
	req         Request
	prePrepare  bool
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
		prepareCollect: topo.CollectorQuorum(),
		commitQuorum:   topo.Quorum(),
		clientQuorum:   topo.ClientQuorum(),
		view:           0,
		nextSeq:        1,
		execSeq:        1,
		log:            make(map[int64]*seqState),
	}, nil
}

// Handle dispatches an inbound envelope to the PBFT state machine.
func (e *Engine) Handle(ctx context.Context, env *pb.Envelope) {
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
	}
}

func (e *Engine) onClientRequest(ctx context.Context, req Request) {
	if e.sender.Primary() != e.self {
		return // only primary accepts client requests
	}
	if !e.topo.SameCluster(req.X, req.Y) {
		return
	}
	if e.store.GetClientTS(req.ClientID) >= req.TS {
		return
	}
	if !e.canAcquire(req.X, req.Y) {
		return
	}
	if e.store.GetBalance(req.X) < req.Amt {
		return // silently ignore insufficient balance
	}

	seq := e.assignSeq()
	digest := Digest(req)
	if !e.store.AcquireLock(req.X, seq) || !e.store.AcquireLock(req.Y, seq) {
		e.store.ReleaseLock(req.X)
		e.store.ReleaseLock(req.Y)
		return
	}

	msg := PrePrepareMsg{Seq: seq, View: e.view, Digest: digest, Req: req}
	e.mu.Lock()
	st := e.ensureSeqLocked(seq)
	st.digest = digest
	st.req = req
	st.prePrepare = true
	e.mu.Unlock()

	if err := e.broadcastPrePrepare(ctx, msg); err != nil && e.logger != nil {
		e.logger.Printf("broadcast pre-prepare seq=%d: %v", seq, err)
	}
	e.processPrePrepare(ctx, msg)
}

func (e *Engine) onPrePrepare(ctx context.Context, msg PrePrepareMsg) {
	if msg.View != e.view {
		return
	}
	if !e.topo.SameCluster(msg.Req.X, msg.Req.Y) {
		return
	}
	e.mu.Lock()
	if !e.gapClearLocked(msg.Seq) {
		e.mu.Unlock()
		return
	}
	if st, ok := e.log[msg.Seq]; ok && st.prePrepare {
		e.mu.Unlock()
		return
	}
	st := e.ensureSeqLocked(msg.Seq)
	st.digest = msg.Digest
	st.req = msg.Req
	st.prePrepare = true
	e.mu.Unlock()

	e.processPrePrepare(ctx, msg)
}

func (e *Engine) processPrePrepare(ctx context.Context, msg PrePrepareMsg) {
	if !e.validateRequest(msg.Req, msg.Seq) {
		return
	}
	if !e.ensureLocks(msg.Seq, msg.Req.X, msg.Req.Y) {
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
		if !ok || st.commitCert == nil || st.executed {
			e.mu.Unlock()
			return
		}
		req := st.req
		seq := e.execSeq
		st.executed = true
		e.execSeq++
		e.mu.Unlock()

		if err := e.store.ApplyTransfer(req.X, req.Y, req.Amt); err != nil {
			if e.logger != nil {
				e.logger.Printf("execute seq=%d failed: %v", seq, err)
			}
			e.store.ReleaseLock(req.X)
			e.store.ReleaseLock(req.Y)
			continue
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
		e.store.ReleaseLock(req.X)
		e.store.ReleaseLock(req.Y)

		reply := Reply{
			ClientID: req.ClientID,
			TS:       req.TS,
			Seq:      seq,
			X:        req.X,
			Y:        req.Y,
			Amt:      req.Amt,
			Result:   resultCommitted,
		}
		if e.sink != nil {
			e.sink.Record(reply)
		}
		e.sendClientReply(ctx, reply)
	}
}

// --- helpers ---

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
		st, ok := e.log[i]
		if !ok || !st.prePrepare {
			return false
		}
	}
	return true
}

func (e *Engine) canAcquire(x, y int) bool {
	return !e.store.IsLocked(x) && !e.store.IsLocked(y)
}

func (e *Engine) ensureLocks(seq int64, x, y int) bool {
	return e.store.AcquireLockForSeq(x, seq) && e.store.AcquireLockForSeq(y, seq)
}

func (e *Engine) validateRequest(req Request, seq int64) bool {
	if e.store.GetClientTS(req.ClientID) >= req.TS {
		return false
	}
	if e.store.GetBalance(req.X) < req.Amt {
		return false
	}
	// Brief lock wait (spec-recommended) before giving up.
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		if e.store.IsLocked(req.X) && e.store.LockSeq(req.X) != seq {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		if e.store.IsLocked(req.Y) && e.store.LockSeq(req.Y) != seq {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		return true
	}
	if e.store.IsLocked(req.X) && e.store.LockSeq(req.X) != seq {
		return false
	}
	if e.store.IsLocked(req.Y) && e.store.LockSeq(req.Y) != seq {
		return false
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

func (e *Engine) sendClientReply(ctx context.Context, reply Reply) {
	payload, _ := marshal(reply)
	env := transport.NewEnvelope(e.self, transport.TypeClientReply, payload)
	e.sender.Sign(env)
	// Client reply routing is via ReplySink in Phase 3; wire to client later.
	_ = ctx
	_ = env
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