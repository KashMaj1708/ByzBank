# Phase 9 Report — Performance Tuning and Failure Hardening

**Project:** CSE 535 Project 4 — Byzantine Fault-Tolerant Sharded 2PC (Go)  
**Plan reference:** `Project4_Go_Implementation_Plan.md`, Phase 9  
**Date:** 2026-06-05  

---

## 1. Goal

Tune protocol timers for n=12/f=3 throughput, harden failure paths, fix client-side `Performance` measurement, and sign off section-6.3 scenarios via the existing test matrix.

---

## 2. Timer tuning (`internal/pbft/timers.go`)

Central `Tunables` struct scaled from `Topology.ClusterSize`:

| Timer | Default (n=12) | Used by |
|-------|----------------|---------|
| `ViewChangeTimeout` | ~1.1s | Backup suspicion (avoids spurious view-changes) |
| `LockWaitTimeout` | ~800ms | PBFT lock gap-check in `validateRequest` |
| `ClientPrimaryWait` | ~1.6s | Client retry before broadcast |
| `ClientTotalWait` | 30s | Client retry total budget |
| `AckRetryInterval` | 300ms | Coordinator 2PC ack re-forward |
| `AckRetryDeadline` | 45s | Coordinator ack retry cap |
| `SettleDeadline(n)` | 15s + 2s×n (max 120s) | CSV runner per-set settle |

Wired into: `Engine`, `Coordinator.startAckRetry`, `SubmitWithRetryForTopo`, `testcase.Runner`.

---

## 3. Performance metrics fix (`internal/testcase/metrics.go`)

Corrected client-side measurement per spec:

- **Throughput** = committed transactions / wall-clock from first send to last committed reply
- **Latency** = mean(reply_time − send_time) over committed transactions only

Added `Snapshot()` for test assertions.

---

## 4. Failure hardening

| Test | Verifies |
|------|----------|
| `TestNoQuorumSixLiveNoClientReply` | C1 with 6 live < 7 quorum → no commit, balances unchanged |
| `TestNoConsensusFourFaulty` (Phase 4) | 4 Byzantine backups → no consensus |
| CSV sets 5–6 | No-quorum / carried-state scenarios (manual + `--auto` demo) |

**Abort-on-no-consensus:** clusters below quorum never reach commit cert → no client reply, no balance mutation.

---

## 5. Production logging

`cmd/server` logs to stdout only when `BYZ_DEBUG` is set; default runs quiet (spec: strip debug logging).

---

## 6. Demo commands

```powershell
go test ./internal/pbft -v -run TestNoQuorumSixLive
go test ./internal/server -v -run TestMixedWorkloadPerformance
go test ./internal/testcase -v -run TestPerformanceMetrics
go test ./... -count=1 -timeout 180s

# Full grader sign-off (36 servers running):
go run ./cmd/client --testfile test/Lab4_Testset_1_36node.csv --auto
go run ./cmd/client --testfile test/Lab4_Testset_2_36node.csv --auto
```

---

## 7. Next phase

**Phase 10** — SmallBank OLTP benchmark implementation and performance characterization.
