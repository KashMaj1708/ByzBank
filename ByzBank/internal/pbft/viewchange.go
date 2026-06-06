package pbft

import (
	"context"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/transport"
)

const defaultViewChangeTimeout = 800 * time.Millisecond

func (e *Engine) onBackupClientRequest(ctx context.Context, req Request) {
	if !e.fault.Alive {
		return
	}
	if req.Op != "" && req.Op != OpIntra {
		return
	}
	if !e.topo.SameCluster(req.X, req.Y) {
		return
	}
	if e.store.GetClientTS(req.ClientID) >= req.TS {
		return
	}
	if e.store.GetBalance(req.X) < req.Amt {
		return
	}
	key := reqKey(req)
	e.mu.Lock()
	e.pendingClients[key] = req
	e.scheduleViewChangeTimerLocked()
	e.mu.Unlock()
	_ = ctx
}

func (e *Engine) scheduleViewChangeTimerLocked() {
	if e.viewChangeTimer != nil {
		return
	}
	e.viewChangeTimer = time.AfterFunc(e.viewChangeTimeout, func() {
		e.triggerViewChange(context.Background())
	})
}

func (e *Engine) cancelViewChangeTimerLocked() {
	if e.viewChangeTimer != nil {
		e.viewChangeTimer.Stop()
		e.viewChangeTimer = nil
	}
}

func (e *Engine) triggerViewChange(ctx context.Context) {
	e.mu.Lock()
	newView := e.view + 1
	if e.viewChangeSent[newView] {
		e.mu.Unlock()
		return
	}
	msg := e.buildViewChangeLocked()
	e.viewChangeSent[newView] = true
	e.inViewChange = true
	e.viewChangeTarget = newView
	e.mu.Unlock()

	e.onViewChange(ctx, e.self, msg)
	_ = e.broadcastViewChange(ctx, msg)
}

func (e *Engine) buildViewChangeLocked() ViewChangeMsg {
	newView := e.view + 1
	prepared := make([]PreparedEntry, 0)
	for seq, st := range e.log {
		if st.prePrepare && !st.executed && st.digest != nil {
			prepared = append(prepared, PreparedEntry{
				Seq:    seq,
				View:   e.view,
				Digest: append([]byte(nil), st.digest...),
				Req:    st.req,
			})
		}
	}
	pending := make([]Request, 0, len(e.pendingClients))
	for _, req := range e.pendingClients {
		pending = append(pending, req)
	}
	latest := e.execSeq - 1
	if latest < 0 {
		latest = 0
	}
	return ViewChangeMsg{
		NewView:      newView,
		LatestStable: latest,
		Prepared:     prepared,
		PendingReqs:  pending,
	}
}

func (e *Engine) onViewChange(ctx context.Context, from config.ServerID, msg ViewChangeMsg) {
	if !e.fault.Alive {
		return
	}
	if msg.NewView <= e.view {
		return
	}

	e.mu.Lock()
	e.viewChangeReceived = append(e.viewChangeReceived, msg)
	if e.viewChangeLog[msg.NewView] == nil {
		e.viewChangeLog[msg.NewView] = make(map[config.ServerID]ViewChangeMsg)
	}
	if _, dup := e.viewChangeLog[msg.NewView][from]; dup {
		e.mu.Unlock()
		return
	}
	e.viewChangeLog[msg.NewView][from] = msg
	count := len(e.viewChangeLog[msg.NewView])
	amplify := count == e.f+1 && !e.viewChangeSent[msg.NewView]
	issueNewView := count >= e.commitQuorum
	e.mu.Unlock()

	if amplify {
		e.triggerViewChange(ctx)
	}
	if issueNewView {
		e.tryIssueNewView(ctx, msg.NewView)
	}
}

func (e *Engine) tryIssueNewView(ctx context.Context, newView int) {
	if e.topo.PrimaryOf(e.cluster, newView) != e.self {
		return
	}
	if e.fault.ByzantineLeader {
		return
	}

	e.mu.Lock()
	if e.newViewIssued[newView] {
		e.mu.Unlock()
		return
	}
	nv := e.buildNewViewLocked(newView)
	e.newViewIssued[newView] = true
	e.mu.Unlock()

	// Apply locally before broadcasting so the collector has pre-prepare state
	// before backups forward PREPARE messages.
	e.onNewView(ctx, nv)
	_ = e.broadcastNewView(ctx, nv)
}

func (e *Engine) buildNewViewLocked(newView int) NewViewMsg {
	vcs := e.viewChangeLog[newView]
	allVC := make([]ViewChangeMsg, 0, len(vcs))
	for _, vc := range vcs {
		allVC = append(allVC, vc)
	}

	prepared := make(map[int64]PreparedEntry)
	pending := make(map[string]Request)
	maxStable := int64(0)

	for _, vc := range allVC {
		if vc.LatestStable > maxStable {
			maxStable = vc.LatestStable
		}
		for _, pe := range vc.Prepared {
			if pe.Seq <= maxStable {
				continue
			}
			if cur, ok := prepared[pe.Seq]; !ok || pe.View >= cur.View {
				prepared[pe.Seq] = pe
			}
		}
		for _, req := range vc.PendingReqs {
			pending[string(Digest(req))] = req
		}
	}
	for _, req := range e.pendingClients {
		pending[string(Digest(req))] = req
	}

	maxSeq := maxStable
	for seq := range prepared {
		if seq > maxSeq {
			maxSeq = seq
		}
	}

	prePrepares := make([]PrePrepareMsg, 0)
	for seq, pe := range prepared {
		prePrepares = append(prePrepares, PrePrepareMsg{
			Seq:    seq,
			View:   newView,
			Digest: append([]byte(nil), pe.Digest...),
			Req:    pe.Req,
		})
		delete(pending, string(pe.Digest))
	}

	nextSeq := maxSeq + 1
	if nextSeq < e.nextSeq {
		nextSeq = e.nextSeq
	}
	for _, req := range pending {
		if e.store.GetClientTS(req.ClientID) >= req.TS {
			continue
		}
		if e.store.GetBalance(req.X) < req.Amt {
			continue
		}
		digest := Digest(req)
		prePrepares = append(prePrepares, PrePrepareMsg{
			Seq:    nextSeq,
			View:   newView,
			Digest: digest,
			Req:    req,
		})
		nextSeq++
	}

	return NewViewMsg{
		NewView:     newView,
		PrePrepares: prePrepares,
		ViewChanges: allVC,
	}
}

func (e *Engine) onNewView(ctx context.Context, msg NewViewMsg) {
	if !e.fault.Alive {
		return
	}
	if msg.NewView <= e.view {
		return
	}

	e.mu.Lock()
	e.newViewLog = append(e.newViewLog, msg)
	e.setViewLocked(msg.NewView)
	e.inViewChange = false
	e.viewChangeTarget = 0
	e.cancelViewChangeTimerLocked()
	e.pendingClients = make(map[string]Request)
	if maxSeq := e.maxPrePrepareSeq(msg.PrePrepares); maxSeq >= e.nextSeq {
		e.nextSeq = maxSeq + 1
	}
	e.mu.Unlock()

	for _, pp := range msg.PrePrepares {
		e.acceptNewViewPrePrepare(ctx, pp)
	}
}

func (e *Engine) acceptNewViewPrePrepare(ctx context.Context, msg PrePrepareMsg) {
	if !e.topo.SameCluster(msg.Req.X, msg.Req.Y) {
		return
	}
	e.mu.Lock()
	if !e.gapClearLocked(msg.Seq) {
		e.mu.Unlock()
		return
	}
	st := e.ensureSeqLocked(msg.Seq)
	if st.prePrepare && bytesEqual(st.digest, msg.Digest) {
		e.mu.Unlock()
		return
	}
	st.digest = append([]byte(nil), msg.Digest...)
	st.req = msg.Req
	st.prePrepare = true
	e.cancelViewChangeTimerLocked()
	e.mu.Unlock()

	if e.sender.Primary() == e.self {
		if !e.canAcquireForOp(msg.Req) {
			return
		}
		if !e.acquireLocksForOp(msg.Seq, msg.Req) {
			e.releaseLocksForOp(msg.Req)
			return
		}
	}
	e.processPrePrepare(ctx, msg)
}

func (e *Engine) maxPrePrepareSeq(pps []PrePrepareMsg) int64 {
	var max int64
	for _, pp := range pps {
		if pp.Seq > max {
			max = pp.Seq
		}
	}
	return max
}

func (e *Engine) setViewLocked(v int) {
	e.view = v
	if hs, ok := e.sender.(*HubSender); ok {
		hs.SetView(v)
	}
}

func (e *Engine) broadcastViewChange(ctx context.Context, msg ViewChangeMsg) error {
	payload, err := marshal(msg)
	if err != nil {
		return err
	}
	env := transport.NewEnvelope(e.self, transport.TypeViewChange, payload)
	e.sender.Sign(env)
	return e.sender.BroadcastCluster(ctx, env)
}

func (e *Engine) broadcastNewView(ctx context.Context, msg NewViewMsg) error {
	payload, err := marshal(msg)
	if err != nil {
		return err
	}
	env := transport.NewEnvelope(e.self, transport.TypeNewView, payload)
	e.sender.Sign(env)
	return e.sender.BroadcastCluster(ctx, env)
}

func reqKey(req Request) string {
	return string(Digest(req))
}
