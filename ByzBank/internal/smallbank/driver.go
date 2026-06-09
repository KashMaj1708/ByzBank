package smallbank

import (
	"context"
	"fmt"
	"time"

	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/client"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/config"
	"github.com/KashMaj1708/2pcbyz-kashmaj1708/internal/pbft"
)

// Config controls a SmallBank benchmark run.
type Config struct {
	Txns              int
	Amt               int64
	Skew              float64
	HotAccessFraction float64
	Seed              int64
	SettleTimeout     time.Duration
	MultiStepTimeout  time.Duration // inter-step wait for multi-request ops (e.g. Amg)
	Pace              time.Duration // delay between write submissions (open-loop pacing)
	ShowProgress      bool          // stderr progress bar during benchmark
}

// DefaultConfig returns standard benchmark parameters.
func DefaultConfig() Config {
	return Config{
		Txns:              1000,
		Amt:               2,
		Skew:              0.9,
		HotAccessFraction: 0.9,
		Seed:              42,
		SettleTimeout:     300 * time.Second,
		MultiStepTimeout:  10 * time.Second,
		Pace:              25 * time.Millisecond,
		ShowProgress:      true,
	}
}

// Driver runs the SmallBank workload against live servers.
type Driver struct {
	Topo   *config.Topology
	Remote *client.Remote
	Schema Schema
	Gen    *Generator
	Metrics *Metrics
}

// NewDriver constructs a driver for the benchmark.
func NewDriver(topo *config.Topology, remote *client.Remote) *Driver {
	schema := NewSchema(*topo)
	return &Driver{
		Topo:    topo,
		Remote:  remote,
		Schema:  schema,
		Gen:     NewGenerator(schema, "smallbank-client"),
		Metrics: NewMetrics(),
	}
}

// PrepareCluster sets all servers live and resets consensus state.
func (d *Driver) PrepareCluster(ctx context.Context) error {
	honest := make(map[config.ClusterID]int)
	for _, srv := range d.Topo.Servers {
		honest[srv.Cluster]++
	}
	for _, srv := range d.Topo.Servers {
		fc := pbft.DefaultFaultConfig()
		fc.ClusterHonestLive = honest[srv.Cluster]
		if err := d.Remote.SetFault(ctx, srv.ID, fc); err != nil {
			return fmt.Errorf("set fault %s: %w", srv.ID, err)
		}
		if err := d.Remote.ResetConsensus(ctx, srv.ID); err != nil {
			return fmt.Errorf("reset %s: %w", srv.ID, err)
		}
	}
	return nil
}

func (d *Driver) contact(cluster config.ClusterID) config.ServerID {
	return d.Topo.PrimaryOf(cluster, 0)
}

type opMeta struct {
	op      Op
	sentAt  time.Time
	lastReq pbft.Request
	done    bool
}

func countDone(meta []opMeta) int {
	n := 0
	for _, m := range meta {
		if m.done {
			n++
		}
	}
	return n
}

// Run executes the workload and waits for replies to settle.
func (d *Driver) Run(ctx context.Context, cfg Config) error {
	if err := d.PrepareCluster(ctx); err != nil {
		return err
	}
	initial, err := SumBalances(ctx, d.Remote, d.Topo, d.Schema)
	if err != nil {
		return fmt.Errorf("initial sum: %w", err)
	}

	hotAccess := cfg.Skew
	if hotAccess <= 0 {
		hotAccess = cfg.HotAccessFraction
	}
	picker := NewPicker(
		d.Schema.TotalCustomers(),
		d.Topo.NumClusters,
		d.Schema.CustomersPerCluster,
		0.1,
		hotAccess,
		cfg.Seed,
	)
	multiStep := cfg.MultiStepTimeout
	if multiStep <= 0 {
		multiStep = 10 * time.Second
	}
	kinds := UniformKinds()
	meta := make([]opMeta, 0, cfg.Txns)

	var submitBar, settleBar *progressBar
	if cfg.ShowProgress {
		submitBar = newProgressBar("submit", cfg.Txns)
	}
	if err := d.fundTreasury(ctx, cfg.ShowProgress); err != nil {
		return fmt.Errorf("fund treasury: %w", err)
	}

	for i := 0; i < cfg.Txns; i++ {
		kind := kinds[i%len(kinds)]
		op, err := d.Gen.RandomOp(kind, picker, cfg.Amt)
		if err != nil {
			return err
		}
		if op.ReadOnly {
			sent := time.Now()
			sav := d.Schema.SavingsItem(op.Cust)
			chk := d.Schema.CheckingItem(op.Cust)
			contact := d.contact(d.Schema.CustCluster(op.Cust))
			_, _ = d.Remote.PrintBalance(ctx, contact, sav)
			_, _ = d.Remote.PrintBalance(ctx, contact, chk)
			d.Metrics.Record(Record{
				Kind:      op.Kind,
				Committed: true,
				SentAt:    sent,
				RepliedAt: time.Now(),
				Latency:   time.Since(sent),
			})
			if submitBar != nil {
				submitBar.update(i+1, op.Kind.String())
			}
			continue
		}
		sent := time.Now()
		var last pbft.Request
		for _, req := range op.Requests {
			contact := d.contact(d.Topo.ClusterOf(req.X))
			if err := d.Remote.SendRequest(ctx, contact, req); err != nil {
				return fmt.Errorf("send %s: %w", op.Kind, err)
			}
			if len(op.Requests) > 1 {
				result, ok := d.waitResult(ctx, req, multiStep)
				last = req
				if !ok || result != pbft.ResultCommitted {
					break
				}
				continue
			}
			last = req
		}
		meta = append(meta, opMeta{op: op, sentAt: sent, lastReq: last})
		if op.Penalty {
			d.Metrics.AddPenalty(d.Schema.PenaltyAmount)
		}
		if cfg.Pace > 0 {
			time.Sleep(cfg.Pace)
		}
		if submitBar != nil {
			submitBar.update(i+1, op.Kind.String())
		}
	}
	if submitBar != nil {
		submitBar.finish(cfg.Txns, "done")
	}

	if cfg.ShowProgress {
		settleBar = newProgressBar("writes", len(meta))
	}
	deadline := time.Now().Add(cfg.SettleTimeout)
	for time.Now().Before(deadline) {
		allDone := true
		resolved := 0
		for i := range meta {
			if meta[i].done {
				resolved++
				continue
			}
			if d.tryResolve(ctx, &meta[i]) {
				resolved++
				continue
			}
			allDone = false
		}
		if settleBar != nil {
			settleBar.update(resolved, fmt.Sprintf("%d pending", len(meta)-resolved))
		}
		if allDone {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if settleBar != nil {
		settleBar.finish(countDone(meta), "done")
	}

	for _, m := range meta {
		if m.done {
			continue
		}
		d.Metrics.Record(Record{
			Kind:       m.op.Kind,
			CrossShard: m.op.CrossShard,
			Committed:  false,
			SentAt:     m.sentAt,
			Latency:    time.Since(m.sentAt),
		})
	}

	// Drain stragglers, then wait for a stable global sum before conservation check.
	var drainBar *progressBar
	if cfg.ShowProgress {
		drainBar = newProgressBar("drain", len(meta))
	}
	drainUntil := time.Now().Add(90 * time.Second)
	for time.Now().Before(drainUntil) {
		pending := false
		resolved := 0
		for i := range meta {
			if meta[i].done {
				resolved++
				continue
			}
			if d.tryResolve(ctx, &meta[i]) {
				resolved++
				continue
			}
			pending = true
		}
		if drainBar != nil {
			drainBar.update(resolved, fmt.Sprintf("%d pending", len(meta)-resolved))
		}
		if !pending {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if drainBar != nil {
		drainBar.finish(countDone(meta), "done")
	}
	if cfg.ShowProgress {
		newProgressBar("quiesce", 1).message("waiting for stable ledger sum...")
	}
	if err := WaitLedgerStable(ctx, d.Remote, d.Topo, d.Schema, 5*time.Second, 120*time.Second); err != nil {
		return fmt.Errorf("quiescence: %w", err)
	}

	final, err := SumBalances(ctx, d.Remote, d.Topo, d.Schema)
	if err != nil {
		return fmt.Errorf("final sum: %w", err)
	}
	if err := CheckConservation(initial, final, d.Metrics.Penalties()); err != nil {
		perCluster, _, derr := SumBalancesByCluster(ctx, d.Remote, d.Topo, d.Schema)
		if derr == nil {
			return fmt.Errorf("%w; per-cluster=%v", err, perCluster)
		}
		return err
	}
	fmt.Printf("Conservation: OK (initial=%d final=%d penalties=%d)\n", initial, final, d.Metrics.Penalties())
	fmt.Println(d.Metrics.Report())
	return nil
}

func (d *Driver) fundTreasury(ctx context.Context, showProgress bool) error {
	const fundCustomers = 10
	const fundAmt int64 = 10
	total := d.Topo.NumClusters * fundCustomers
	var bar *progressBar
	if showProgress {
		bar = newProgressBar("fund", total)
	}
	done := 0
	for cluster := 1; cluster <= d.Topo.NumClusters; cluster++ {
		cid := config.ClusterID(cluster)
		treasury := d.Schema.TreasuryItem(cid)
		for local := 1; local <= fundCustomers; local++ {
			cust := (cluster-1)*d.Schema.CustomersPerCluster + local
			chk := d.Schema.CheckingItem(cust)
			req := d.Gen.nextReq(chk, treasury, fundAmt)
			contact := d.contact(cid)
			if err := d.Remote.SendRequest(ctx, contact, req); err != nil {
				return err
			}
			_, _ = d.waitResult(ctx, req, 30*time.Second)
			done++
			if bar != nil {
				bar.update(done, fmt.Sprintf("cluster %d", cluster))
			}
		}
	}
	if bar != nil {
		bar.finish(done, "done")
	}
	return nil
}

func (d *Driver) tryResolve(ctx context.Context, m *opMeta) bool {
	result, ok := d.collectResult(ctx, m.lastReq)
	if !ok {
		return false
	}
	d.recordResult(m, result)
	m.done = true
	return true
}

func (d *Driver) recordResult(m *opMeta, result string) {
	d.Metrics.Record(Record{
		Kind:       m.op.Kind,
		CrossShard: m.op.CrossShard,
		Committed:  result == pbft.ResultCommitted,
		SentAt:     m.sentAt,
		RepliedAt:  time.Now(),
		Latency:    time.Since(m.sentAt),
	})
}

func (d *Driver) collectResult(ctx context.Context, req pbft.Request) (result string, ok bool) {
	cluster := d.Topo.ClusterOf(req.X)
	matches := make(map[string]int)
	for _, srv := range d.Topo.ServersInCluster(cluster) {
		reply, found, err := d.Remote.LookupReply(ctx, srv.ID, req)
		if err != nil || !found {
			continue
		}
		matches[reply.Result]++
		if matches[reply.Result] >= d.Topo.ClientQuorum() {
			return reply.Result, true
		}
	}
	return "", false
}

func (d *Driver) waitResult(ctx context.Context, req pbft.Request, timeout time.Duration) (string, bool) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if result, ok := d.collectResult(ctx, req); ok {
			return result, true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return "", false
}
