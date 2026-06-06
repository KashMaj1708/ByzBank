# Phase 7 Report — Concurrency, Locking, and Figure 4 Interleaving

**Project:** CSE 535 Project 4 — Byzantine Fault-Tolerant Sharded 2PC (Go)  
**Plan reference:** `Project4_Go_Implementation_Plan.md`, Phase 7  
**Date:** 2026-06-05  
**Topology:** 3×12 = 36 servers, f=3, client quorum f+1=4

---

## 1. Goal

1. Open-loop client driver (fire requests without waiting for prior commits)
2. Concurrent cross-shard correctness under the locking discipline
3. Figure 4 interleaving regression: intra → cross prepare → interleaved intra → cross commit

---

## 2. Implementation summary

### 2.1 Open-loop client (`internal/client/driver.go`)

| API | Purpose |
|-----|---------|
| `Fire` | Send one request to `PrimaryOf(ClusterOf(x))` |
| `FireOpenLoop` | Submit a batch without waiting between requests |
| `FireConcurrent` | Submit requests in parallel goroutines (contention stress) |
| `WaitFor` | Block until f+1 matching replies arrive |

Servers still enforce per-client last-timestamp replay protection; the client does not implement closed-loop retry (that remains in `pbft.SubmitWithRetry` for Lab-2-style tests).

### 2.2 Server test harness (`internal/server/harness_test.go`)

Multi-cluster harness with shared `ReplyCollector`, `PreparedCollector`, `AckCollector`, and `client.Driver`. Helpers:

- `seedBalance` — pre-credit items for Figure 4 amounts
- `waitCrossShardDone` — wait for client quorum **and** all cluster replicas to release locks
- `assertLockedAll` / `assertWALAll` — per-step Figure 4 checks

### 2.3 Concurrency fix (`internal/pbft/engine.go`)

Added `proposalMu` to serialise `startConsensus` on the primary. Without this, overlapping cross-shard PBFT instances on the same cluster (e.g. coordinator + participant roles, or two open-loop requests) could race during sequence assignment and leave one transaction stuck.

### 2.4 Tests (`internal/server`)

| Test | Verifies |
|------|----------|
| `TestConcurrentContention/DisjointCrossShardBothCommit` | Open-loop C1→C2 and C3→C1 concurrently; both commit, balances correct, locks released |
| `TestConcurrentContention/SameSenderItemOneCommits` | Two cross-shard txns on same `x`; exactly one commits, no double debit |
| `TestConcurrentContention/CrossShardLockBlocksIntraOnSameItem` | Cross prepare holds lock on `x`; overlapping intra on `x` is silently ignored |
| `TestFigure4Interleaving` | Step-by-step Figure 4 sequence with per-step lock/WAL/datastore/balance assertions |

**Figure 4 item mapping** (plan symbols → topology items):

| Symbol | Item | Cluster |
|--------|------|---------|
| A | 10 | C1 |
| B | 11 | C1 |
| C | 12 | C1 |
| D | 13 | C1 |
| E | 1500 | C2 |

Items A–D are pre-seeded to balance 30 so `(A,B,20)` is valid with `InitialBalance=10`.

---

## 3. Figure 4 sequence (asserted per step)

1. **Intra (A,B,20)** — commits; A=10, B=50; no locks
2. **Cross (A,E,10) prepare** — A=0; lock A; WAL present; one prepare datastore entry; no client reply
3. **Intra (C,D,5)** — commits while A still locked; C=25, D=35
4. **Cross commit** — manual `coord_commit` PBFT (harness uses `Disable2PCCommitPhase` for steps 1–3); lock A released; WAL deleted; two cross datastore entries; E=20; client `"committed"`

---

## 4. Demo commands

```powershell
go test ./internal/server -v -run TestConcurrentContention
go test ./internal/server -v -run TestFigure4Interleaving
go test ./... -count=1 -timeout 180s
```

---

## 5. Next phase

**Phase 8** — CSV test-case runner, interactive control, and required query functions (`PrintBalance`, `PrintDatastore`, `Performance`).
