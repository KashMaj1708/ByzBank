# SmallBank Phase 10 — Implementation & Benchmark Report (v1)

**Date:** June 7, 2026  
**Module:** `github.com/KashMaj1708/2pcbyz-kashmaj1708`  
**Plan reference:** `Project4_Go_Implementation_Plan.md`, Phase 10  
**Prerequisite:** Fix Update 5 — TS1 **6/6**, TS2 **6/6** (per-set `ResetConsensus`)  
**Status:** Implementation **complete**; **1000-txn demo not green** (conservation violation at scale)

---

## 1. Executive Summary

Phase 10 adds the mandatory **SmallBank OLTP benchmark** on top of the existing PBFT + sharded 2PC system. The workload exercises six banking transaction types over a skewed customer population, reports throughput/latency split by intra- vs cross-shard, and asserts a global **conservation invariant** (total money constant modulo WriteCheck penalties).

| Deliverable | Status |
|-------------|--------|
| `internal/smallbank` package | **Done** |
| `go test ./internal/smallbank` | **4/4 PASS** |
| CLI `--benchmark smallbank` | **Done** |
| Conservation at 100 txns | **PASS** |
| Conservation at 500/1000 txns | **FAIL** (+7 leak) |
| `PHASE10_REPORT.md` | Not written (this doc is interim) |
| TS1/TS2 re-verify after Phase 10 | Not run |

**Bottom line:** The benchmark harness works and small runs are correct. Sustained runs (~500+ txns) produce a **deterministic +7 global balance leak** that must be root-caused before Phase 10 can sign off.

---

## 2. Goal (from plan)

Implement SmallBank on the sharded KV engine and produce a performance characterization:

1. Map customers → savings + checking item ids (intra-shard per customer).
2. Generate all six txn types as `(x, y, amt)` PBFT requests.
3. Skewed key selection (90% of accesses to 10% of accounts).
4. Open-loop driver with throughput + p50/p95/p99 latency.
5. Conservation oracle: `sum(all savings + checking + treasury) = constant − penalties`.

**Demo command (target):**

```powershell
go run ./cmd/client --benchmark smallbank --txns 1000 --skew 0.9
go test ./internal/smallbank
```

---

## 3. Schema Mapping

The engine stores one balance per **item id**. SmallBank maps each logical customer to **two items** in the **same cluster** so single-customer ops stay intra-shard; only N1↔N2 ops can cross shards.

| Concept | Mapping |
|---------|---------|
| Customers per cluster | 400 |
| Total customers (3 clusters) | 1200 |
| Savings item | `clusterBase + (local−1)×2` |
| Checking item | savings + 1 |
| Treasury item | last item in cluster (`clusterBase + ItemsPerCluster − 1`) |
| Penalty amount | 1 (WriteCheck insufficient path) |

**Example (cluster 1, customer 1):**

```
savings  = item 1
checking = item 2
treasury = item 1000   (last slot in cluster)
```

**Initial global sum:** `24030` (= 2403 tracked items × default balance 10).  
Tracked items = 1200 customers × 2 accounts + 3 treasury pools = **2403**.

**Files:** `internal/smallbank/schema.go`, `schema_test.go`

---

## 4. Six Transaction Types

| Kind | Code | PBFT shape | Shard |
|------|------|------------|-------|
| Balance | Bal | read-only (`PrintBalance`) | intra |
| DepositChecking | DC | treasury → checking | intra |
| TransactSavings | TS | treasury ↔ savings | intra |
| Amalgamate | Amg | savings→checking, checking→checking (up to 2 steps) | intra or **cross** |
| WriteCheck | WC | checking → treasury | intra |
| SendPayment | SP | checking → checking | intra or **cross** |

**Workload mix:** uniform round-robin over the six kinds (`UniformKinds()`).

**Skew:** `NewPicker(total, hotsetFraction=0.1, hotAccessFraction=skew, seed=42)` — with `--skew 0.9`, 90% of customer picks target the first 10% of accounts (120 hot customers).

**RandomOp defaults:**

- TS: 50% deposit `amt`, 50% withdraw `amt/2`
- Amg: move `amt` from savings + `amt/2` from checking (two sequential PBFT requests)
- WC: always `sufficient=true` (no penalty path in benchmark mix)
- SP/Amg: pick two distinct customers via skewed picker

**Files:** `internal/smallbank/txns.go`, `txns_test.go`, `skew.go`

---

## 5. Package Layout

```
internal/smallbank/
  schema.go       — customer → item id mapping, treasury, validation
  txns.go         — six txn generators, RandomOp, UniformKinds
  skew.go         — hotspot customer picker
  metrics.go      — throughput, mean/p50/p95/p99; intra vs cross buckets
  invariant.go    — SumBalances, CheckConservation
  driver.go       — PrepareCluster, fundTreasury, open-loop Run, settle/drain
  schema_test.go  — item mapping + conservation unit test
  txns_test.go    — cross-shard SendPayment shape test
```

**CLI wiring:** `cmd/client/main.go` — flags `--benchmark`, `--txns`, `--skew`, `--amt`.

---

## 6. Driver Lifecycle

Each benchmark run executes in this order:

```
1. PrepareCluster
   - SetFault (all 12 live per cluster) on every server
   - ResetConsensus on every server (volatile PBFT/2PC state cleared; BoltDB balances persist)

2. initial ← SumBalances()          # snapshot before workload

3. fundTreasury
   - Per cluster: transfer 5 from checking of customers 1..5 → treasury (15 intra transfers)
   - Seeds liquidity for DepositChecking ops

4. Open-loop workload (N txns)
   - Round-robin txn kind; skewed customer pick
   - Bal: immediate PrintBalance (no PBFT write)
   - Writes: SendRequest to coordinator primary of req.X
   - Amalgamate (2 steps): wait for step-1 commit before sending step-2
   - Pace: 25ms sleep between write submissions

5. Settle loop (up to 300s)
   - Poll lastReq per op for f+1 matching "committed" replies

6. Drain loop (up to 90s)
   - Continue polling uncommitted ops before final balance read

7. final ← SumBalances()
8. CheckConservation(initial, final, penalties)
9. Print metrics report
```

**Reply collection:** quorum of `f+1=4` matching replies within the cluster of `req.X` (same pattern as `testcase.Runner.collectReply`).

**Files:** `internal/smallbank/driver.go`, `cmd/client/main.go`

---

## 7. Metrics & Conservation Oracle

### Metrics (`metrics.go`)

Reports three buckets — **overall**, **intra**, **cross**:

- committed / total count
- throughput (committed / wall-clock from first send to last committed reply)
- mean, p50, p95, p99 latency (committed ops only)

Read-only Bal ops are recorded as committed immediately (not split into intra/cross).

### Conservation (`invariant.go`)

```
want = initial − penalties
PASS iff final == want
```

`SumBalances` queries one primary per cluster for every savings, checking, and treasury item. This is the strongest end-to-end correctness check — it catches lost/duplicated funds from broken 2PC undo or double-execution.

---

## 8. Demo Commands

```powershell
# Build and start fresh 36-node cluster
.\scripts\run.ps1 build
.\scripts\run.ps1 down; Remove-Item -Recurse -Force data; .\scripts\run.ps1 up

# Unit tests
go test ./internal/smallbank -v

# SmallBank benchmark (plan demo)
go run ./cmd/client --benchmark smallbank --txns 1000 --skew 0.9

# Smaller diagnostic runs
go run ./cmd/client --benchmark smallbank --txns 100 --skew 0.9
go run ./cmd/client --benchmark smallbank --txns 500 --skew 0.9
```

**Flags:**

| Flag | Default | Meaning |
|------|---------|---------|
| `--txns` | 1000 | Number of logical SmallBank operations |
| `--skew` | 0.9 | Hot-access fraction (0..1) |
| `--amt` | 1 | Transfer amount per write txn |

**Operational notes:**

- Always `.\scripts\run.ps1 build` before `up` — stale `bin/server.exe` causes false failures.
- Wipe `data/` between benchmark runs for a clean ledger.
- Lab4 TS1/TS2 should be re-verified after any further Phase 10 changes.

---

## 9. Benchmark Results

All runs: 36 servers (3×12), f=3, quorum=7, seed=42, pace=25ms, settle=300s, drain=90s.  
Fresh cluster (`down` + wipe `data/` + `up`) unless noted.

| Run | Conservation | Committed | Cross committed | Notes |
|-----|-------------|-----------|-----------------|-------|
| 1000 txns, burst (no pace) | **OK** | 520/1000 | 0/40 | Overwhelmed cluster; cross全部失败 |
| 200 txns, sequential Amg fix | **OK** | 135/200 | 2/13 | Low commit rate without pacing |
| **100 txns, paced (latest code)** | **OK** `24030→24030` | **100/100** | **8/8** | cross mean 1.34s vs intra 1.18s |
| 1000 txns, paced + drain | **FAIL** `+3` | — | — | `got=24033` |
| 1000 txns, paced + drain + Amg gate | **FAIL** `+7` | — | — | `got=24037` |
| **500 txns, paced + drain + Amg gate** | **FAIL** `+7` | — | — | Same +7 as 1000 |
| **100 txns, paced (re-run, latest)** | **OK** `24030→24030` | **100/100** | **8/8** | throughput 36 txns/s |

### 100-txn metrics (passing run)

```
Conservation: OK (initial=24030 final=24030 penalties=0)
SmallBank metrics:
  overall:  committed=100/100 throughput=36.17 txns/s mean=1.192s p50=1.244s p95=2.251s p99=2.334s
  intra:    committed=92/92  throughput=33.30 txns/s mean=1.179s p50=1.217s p95=2.251s p99=2.334s
  cross:    committed=8/8    throughput=3.10 txns/s  mean=1.342s p50=1.334s p95=1.978s p99=1.978s
  penalties applied: 0
```

### Failure signature (500/1000 txns)

```
smallbank: conservation violated: initial=24030 penalties=0 want=24030 got=24037
```

The leak is **exactly +7** at both 500 and 1000 txns — not proportional to workload size. This suggests a **deterministic set of errant commits** triggered once enough skewed cross-shard / multi-step traffic accumulates, rather than a simple "stragglers counted too early" timing artifact.

---

## 10. Fixes Attempted During Benchmark Bring-Up

| Issue | Symptom | Fix applied | Result |
|-------|---------|-------------|--------|
| Burst submission | 520/1000 committed, 0/40 cross | 25ms `Pace` between writes | 100/100 at 100 txns |
| Amalgamate races | Multi-step fired without waiting | `waitCommitted` between Amg steps; abort chain on failure | Improved cross commits at 100 txns |
| Settle too short | Many ops marked uncommitted | `SettleTimeout` 120s → 300s | Partial improvement |
| Stragglers at final sum | +3 leak at 1000 | 90s post-settle drain loop | Leak grew to +7 |
| fundTreasury blocking | Slow startup | `waitCommitted` with 30s cap per transfer | Treasury seeding completes |

**Not yet tried:**

- Per-cluster balance diff to locate which cluster gains +7
- Trace specific cross-shard 2PC paths (coord prepare / part prepare / abort undo)
- Balance-stability wait (poll until global sum unchanged for N seconds)
- Longer drain proportional to `cfg.Txns`
- Re-snapshot `initial` after `fundTreasury` completes

---

## 11. Open Problem — +7 Conservation Leak

### What we know

1. **100 txns passes** reliably with conservation OK and 8/8 cross commits.
2. **500 and 1000 txns fail** with the **same +7** every time (`24037` vs `24030`).
3. Penalties = 0 (WriteCheck always uses `sufficient=true` in `RandomOp`).
4. `fundTreasury` runs **after** `initial` snapshot — intra-cluster transfers, net-zero if all commit.
5. Lab4 oracle (TS1/TS2 6/6) passes with per-set reset — the leak appears only under **sustained open-loop SmallBank load**, not in CSV test sets.

### Leading hypotheses

| # | Hypothesis | Why plausible |
|---|------------|---------------|
| H1 | **Partial 2PC credit without matching debit** | +7 = exact count suggests specific cross-shard txn outcomes; participant `ApplyCreditOnly` without coord abort undo |
| H2 | **In-flight commits after drain** | Less likely — same +7 at 500 and 1000 implies fixed count not growing with scale |
| H3 | **Hotspot lock contention + skew 0.9** | 90% traffic to 120 accounts amplifies cross-shard races on popular items |
| H4 | **Amalgamate step-1 commits, step-2 fails, partial state** | Should conserve money (funds moved, not created) unless paired with H1 |
| H5 | **SumBalances reads primary while stragglers commit on backups** | Primary should reflect executed state; worth verifying per-cluster sums |

### Recommended debug sequence

```powershell
# 1. Reproduce at threshold (find N where +7 first appears: 200? 300? 400?)
go run ./cmd/client --benchmark smallbank --txns 300 --skew 0.9

# 2. Add per-cluster sum logging before/after run (diagnostic patch)
#    Compare C1/C2/C3 deltas — which cluster gains +7?

# 3. Run with skew=0.0 (uniform) at 500 txns
#    If leak disappears → hotspot interaction; if persists → protocol bug

# 4. Run with only intra kinds (patch UniformKinds temporarily)
#    If leak disappears → cross-shard 2PC path is culprit
```

---

## 12. Relationship to Prior Fixes

SmallBank `PrepareCluster` calls `ResetConsensus` on every server before each benchmark run — the same mechanism that fixed TS2 Set 6 (see `FIX_UPDATE_5.md` §10). This ensures each run starts with clean PBFT seq state while BoltDB balances persist.

SmallBank does **not** use per-set CSV fault injection; it runs all clusters fully live (12/12) for the entire benchmark.

---

## 13. Phase 10 Checklist vs Plan

| Plan requirement | Status |
|------------------|--------|
| Schema mapping (savings + checking per customer) | ✅ |
| Six txn generators | ✅ |
| Skewed key selection | ✅ |
| Uniform six-type mix | ✅ |
| Open-loop driver + metrics (p50/p95/p99, intra/cross) | ✅ |
| Conservation invariant oracle | ✅ (fails at 500+ txns) |
| Demo: 1000 txns + green invariant | ❌ (+7 leak) |
| `go test ./internal/smallbank` | ✅ |
| Cross-shard latency > intra-shard | ✅ at 100 txns (1.34s vs 1.18s mean) |
| Non-degenerate throughput | ✅ at 100 txns (36 txns/s) |

---

## 14. Next Steps

1. **Root-cause the +7 leak** — per-cluster balance diff, skew-off control run, cross-only isolation.
2. **Fix protocol or driver** based on findings.
3. **Green 1000-txn demo** — conservation OK + metrics table.
4. **Re-verify TS1/TS2** after any code changes.
5. Write `PHASE10_REPORT.md` (formal phase sign-off).
6. **Phase 11** — strip debug logging, final `submit lab4` commit.

---

## 15. Key File Index

| File | Role |
|------|------|
| `internal/smallbank/schema.go` | Customer → item mapping |
| `internal/smallbank/txns.go` | Txn generators + RandomOp |
| `internal/smallbank/skew.go` | Hotspot picker |
| `internal/smallbank/driver.go` | Benchmark driver, settle/drain |
| `internal/smallbank/metrics.go` | Throughput + latency report |
| `internal/smallbank/invariant.go` | SumBalances + CheckConservation |
| `cmd/client/main.go` | `--benchmark smallbank` CLI |
| `FIX_UPDATE_5.md` | Per-set ResetConsensus (TS2 6/6) |
| `Project4_Go_Implementation_Plan.md` | Phase 10 spec |

---

*End of SmallBank_1.md*
