package testcase

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/client"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
)

// Runner drives one CSV file against live server processes.
type Runner struct {
	Topo     *config.Topology
	Remote   *client.Remote
	Metrics  *Metrics
	ClientID string
	nextTS   int64
}

// NewRunner constructs a runner for the given topology.
func NewRunner(topo *config.Topology, remote *client.Remote) *Runner {
	return &Runner{
		Topo:     topo,
		Remote:   remote,
		Metrics:  NewMetrics(),
		ClientID: "lab4-client",
	}
}

// ApplySetConfig pushes live/byzantine fault settings to every server.
func (r *Runner) ApplySetConfig(ctx context.Context, set Set) error {
	live := toIDSet(set.Live)
	byz := toIDSet(set.Byzantine)
	honestLive := make(map[config.ClusterID]int)
	for _, srv := range r.Topo.Servers {
		_, isLive := live[srv.ID]
		_, isByz := byz[srv.ID]
		if isLive && !isByz {
			honestLive[srv.Cluster]++
		}
	}
	for _, srv := range r.Topo.Servers {
		var fc pbft.FaultConfig
		_, isLive := live[srv.ID]
		_, isByz := byz[srv.ID]
		switch {
		case !isLive:
			fc = pbft.FaultConfig{Alive: false}
		case isByz:
			fc = pbft.FaultConfig{Alive: true, ByzantineBackup: true}
		default:
			fc = pbft.DefaultFaultConfig()
		}
		fc.ClusterHonestLive = honestLive[srv.Cluster]
		if err := r.Remote.SetFault(ctx, srv.ID, fc); err != nil {
			return fmt.Errorf("set fault %s: %w", srv.ID, err)
		}
		if _, isLive := live[srv.ID]; isLive {
			if err := r.Remote.ResetConsensus(ctx, srv.ID); err != nil {
				return fmt.Errorf("reset %s: %w", srv.ID, err)
			}
		}
	}
	return nil
}

// RunSet fires every transaction open-loop, then waits for replies to settle.
func (r *Runner) RunSet(ctx context.Context, set Set) ([]pbft.Request, error) {
	if err := r.ApplySetConfig(ctx, set); err != nil {
		return nil, err
	}
	pending := make([]pbft.Request, 0, len(set.Txns))
	sentAt := make(map[int64]time.Time, len(set.Txns))
	timers := pbft.DefaultTunables(*r.Topo)
	type stagedTxn struct {
		req     pbft.Request
		isCross bool
	}
	staged := make([]stagedTxn, 0, len(set.Txns))
	for _, txn := range set.Txns {
		r.nextTS++
		req := pbft.Request{
			ClientID: r.ClientID,
			TS:       r.nextTS,
			X:        txn.X,
			Y:        txn.Y,
			Amt:      txn.Amt,
		}
		staged = append(staged, stagedTxn{
			req:     req,
			isCross: !r.Topo.SameCluster(txn.X, txn.Y),
		})
		pending = append(pending, req)
	}
	for _, item := range staged {
		if item.isCross {
			continue
		}
		if err := r.sendOne(ctx, set, item.req, sentAt); err != nil {
			return nil, err
		}
	}
	for _, item := range staged {
		if !item.isCross {
			continue
		}
		if err := r.sendOne(ctx, set, item.req, sentAt); err != nil {
			return nil, err
		}
		r.waitOneTxn(ctx, set, item.req, sentAt[item.req.TS], timers)
	}
	r.waitSettle(ctx, set, pending, sentAt)
	r.waitCrossSettle(ctx, set, pending, sentAt, timers)
	unresolved := 0
	for _, req := range pending {
		if !r.collectReply(ctx, set, req) {
			unresolved++
		}
	}
	if unresolved > 0 {
		// Allow primary seq-reclaim timers to roll back no-quorum slots and release locks.
		drain := timers.ViewChangeTimeout * 6
		select {
		case <-ctx.Done():
		case <-time.After(drain):
		}
	} else {
		select {
		case <-ctx.Done():
		case <-time.After(timers.ViewChangeTimeout):
		}
	}
	r.drainReplicas(ctx, set, timers)
	return pending, nil
}

func (r *Runner) drainReplicas(ctx context.Context, set Set, timers pbft.Tunables) {
	live := toIDSet(set.Live)
	fast := timers.LockPollInterval * 20
	var wg sync.WaitGroup
	for _, srv := range r.Topo.Servers {
		if _, ok := live[srv.ID]; !ok {
			continue
		}
		wg.Add(1)
		srv := srv
		go func() {
			defer wg.Done()
			isPrimary := r.Topo.PrimaryOf(srv.Cluster, 0) == srv.ID
			if isPrimary {
				_ = r.Remote.DrainReplica(ctx, srv.ID, timers.ReclaimDrainWait, false)
				return
			}
			_ = r.Remote.DrainReplica(ctx, srv.ID, fast, true)
		}()
	}
	wg.Wait()
}

type settleTrack struct {
	sentAt      time.Time
	resendRound int
}

func (r *Runner) sendOne(ctx context.Context, set Set, req pbft.Request, sentAt map[int64]time.Time) error {
	contact, err := ContactFor(set, r.Topo.ClusterOf(req.X))
	if err != nil {
		return err
	}
	at := time.Now()
	if err := r.Remote.SendRequest(ctx, contact, req); err != nil {
		return fmt.Errorf("send (%d,%d,%d) to %s: %w", req.X, req.Y, req.Amt, contact, err)
	}
	r.Metrics.RecordSend(req, at)
	sentAt[req.TS] = at
	return nil
}

func (r *Runner) waitOneTxn(ctx context.Context, set Set, req pbft.Request, sent time.Time, timers pbft.Tunables) {
	if sent.IsZero() {
		return
	}
	deadline := time.Now().Add(timers.SettlePerCrossTxn)
	track := &settleTrack{sentAt: sent}
	for time.Now().Before(deadline) {
		if r.collectReply(ctx, set, req) {
			return
		}
		r.maybeResend(ctx, set, req, track, timers)
		select {
		case <-ctx.Done():
			return
		case <-time.After(timers.SettlePollInterval):
		}
	}
}

func (r *Runner) waitCrossSettle(ctx context.Context, set Set, pending []pbft.Request, sentAt map[int64]time.Time, timers pbft.Tunables) {
	cross := make([]pbft.Request, 0)
	for _, req := range pending {
		if r.Topo.SameCluster(req.X, req.Y) {
			continue
		}
		cluster := r.Topo.ClusterOf(req.X)
		contact, err := ContactFor(set, cluster)
		if err != nil {
			continue
		}
		// Skip extended cross wait when the coordinator will reject the txn.
		if line, err := r.Remote.PrintBalance(ctx, contact, req.X); err == nil {
			if bal := parseBalanceLine(line); bal >= 0 && bal < req.Amt {
				continue
			}
		}
		cross = append(cross, req)
	}
	if len(cross) == 0 {
		return
	}
	deadline := time.Now().Add(time.Duration(len(cross)) * timers.SettlePerCrossTxn)
	track := make(map[int64]*settleTrack, len(cross))
	for _, req := range cross {
		track[req.TS] = &settleTrack{sentAt: sentAt[req.TS]}
	}
	for time.Now().Before(deadline) {
		allDone := true
		for _, req := range cross {
			if r.collectReply(ctx, set, req) {
				continue
			}
			allDone = false
			r.maybeResend(ctx, set, req, track[req.TS], timers)
		}
		if allDone {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(timers.SettlePollInterval):
		}
	}
}

func (r *Runner) waitSettle(ctx context.Context, set Set, pending []pbft.Request, sentAt map[int64]time.Time) {
	timers := pbft.DefaultTunables(*r.Topo)
	cross := 0
	for _, req := range pending {
		if !r.Topo.SameCluster(req.X, req.Y) {
			cross++
		}
	}
	deadline := time.Now().Add(timers.SettleDeadline(len(pending)) + time.Duration(cross)*timers.SettlePerCrossTxn)
	track := make(map[int64]*settleTrack, len(pending))
	for _, req := range pending {
		track[req.TS] = &settleTrack{sentAt: sentAt[req.TS]}
	}
	for time.Now().Before(deadline) {
		allDone := true
		for _, req := range pending {
			if r.collectReply(ctx, set, req) {
				continue
			}
			allDone = false
			r.maybeResend(ctx, set, req, track[req.TS], timers)
		}
		if allDone {
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(timers.SettlePollInterval):
		}
	}
}

func (r *Runner) maybeResend(ctx context.Context, set Set, req pbft.Request, st *settleTrack, timers pbft.Tunables) {
	if st == nil {
		return
	}
	cluster := r.Topo.ClusterOf(req.X)
	elapsed := time.Since(st.sentAt)
	if elapsed < timers.ClientPrimaryWait {
		return
	}
	round := int((elapsed - timers.ClientPrimaryWait) / timers.ViewChangeTimeout)
	if round <= st.resendRound {
		return
	}
	st.resendRound = round
	if contact, err := ContactFor(set, cluster); err == nil {
		_ = r.Remote.SendRequest(ctx, contact, req)
	}
	for _, srv := range r.Topo.ServersInCluster(cluster) {
		_ = r.Remote.SendRequest(ctx, srv.ID, req)
	}
}

func (r *Runner) collectReply(ctx context.Context, set Set, req pbft.Request) bool {
	cluster := r.Topo.ClusterOf(req.X)
	matches := make(map[string]int)
	for _, srv := range r.Topo.ServersInCluster(cluster) {
		reply, ok, err := r.Remote.LookupReply(ctx, srv.ID, req)
		if err != nil || !ok {
			continue
		}
		key := reply.Result
		matches[key]++
		if matches[key] >= r.Topo.ClientQuorum() {
			r.Metrics.RecordReply(req, reply.Result, time.Now())
			return true
		}
	}
	return false
}

// PrintBalance shows an item balance on every server in its cluster.
func (r *Runner) PrintBalance(ctx context.Context, item int) error {
	cluster := r.Topo.ClusterOf(item)
	fmt.Printf("PrintBalance item=%d (cluster %s)\n", item, cluster)
	for _, srv := range r.Topo.ServersInCluster(cluster) {
		line, err := r.Remote.PrintBalance(ctx, srv.ID, item)
		if err != nil {
			fmt.Printf("  %s: ERROR %v\n", srv.ID, err)
			continue
		}
		fmt.Printf("  %s: %s\n", srv.ID, line)
	}
	return nil
}

// PrintDatastore shows the committed log on one server.
func (r *Runner) PrintDatastore(ctx context.Context, id config.ServerID) error {
	out, err := r.Remote.PrintDatastore(ctx, id)
	if err != nil {
		return err
	}
	fmt.Printf("PrintDatastore %s\n%s", id, out)
	return nil
}

var runnerBalanceRe = regexp.MustCompile(`bal\[\d+\]\s*=\s*(-?\d+)`)

func parseBalanceLine(line string) int64 {
	m := runnerBalanceRe.FindStringSubmatch(line)
	if m == nil {
		return -1
	}
	v, _ := strconv.ParseInt(m[1], 10, 64)
	return v
}

func toIDSet(ids []config.ServerID) map[config.ServerID]struct{} {
	out := make(map[config.ServerID]struct{}, len(ids))
	for _, id := range ids {
		out[id] = struct{}{}
	}
	return out
}
