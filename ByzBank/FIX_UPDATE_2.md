# Fix Update 2 — Lab4 Remaining Failures

**Date:** June 5, 2026  
**Baseline:** Fix Update 1 → TS1 **3/6**, TS2 **5/6**  
**After Fix Update 2:** TS1 **3/6**, TS2 **5/6** (no regression on passing sets)

---

## 1. Summary

Fix Update 2 implements six targeted changes from the post–Fix Update 1 plan. Each was followed by `go test ./...` and Lab4 oracle runs. **Sets 1–3 (TS1) and 1–5 (TS2) remain stable.** Sets **4–6 (TS1)** and **6 (TS2)** still fail.

**Key diagnostic:** Running **Set 4 in isolation** on a fresh `data/` directory reports `committed=2` (both intra txns succeed). The same Set 4 **after Sets 1–3** fails on `(2770,2799,1)`. The regression is **cumulative cross-set state**, not Set 4 logic alone.

---

## 2. Fixes Implemented (in order)

### Fix 2.1 — Hopeless-quorum skip

**Problem:** Primaries assign seq + locks when the cluster cannot reach `n-f` prepares (e.g. Set 5 C1 with only 6 live honest nodes, need 9).

**Change:**
- `FaultConfig.ClusterHonestLive` — runner counts live non-Byzantine replicas per cluster in `ApplySetConfig`
- Primary `startConsensus` returns early when `clusterHonestLive() < prepareCollect`

**Files:** `fault.go`, `engine.go`, `runner.go`

**Test after fix:** `go test ./...` PASS. TS1 Sets 1–3 still PASS; Set 5 C1 no-quorum txns no longer wedge the pipeline (no new failures introduced).

---

### Fix 2.2 — Synchronous reclaim + inter-set drain

**Problem:** Async `watchPrepareQuorum` reclaim could complete after the runner snapshots oracle state; locks/seq slots leak into the next set.

**Change:**
- `reclaimWG` on `watchPrepareQuorum`
- `Engine.WaitReclaims`, `Engine.DrainExecute`
- HTTP `POST /drain` on each replica
- `Runner.drainReplicas` after every set (parallel): full drain on primaries, `execute_only` fast drain on backups

**Files:** `engine.go`, `httpapi.go`, `remote.go`, `runner.go`, `timers.go`

**Test after fix:** Unit tests PASS. No TS1/TS2 regression. Drain runtime bounded (~seconds per set with parallel backup drain).

---

### Fix 2.3 — Per-primary ingress queue (client priority)

**Problem:** Open-loop traffic on one primary (e.g. S25 intra + 2PC participant prepare) races on `proposalMu` / `awaitStartable`.

**Change:**
- `clientIngressCh` + `internalIngress` with priority loop (client first)
- `StartClientConsensus` for client paths; `StartConsensus` for 2PC commit/prepare-followups
- `Participant.HandlePrepare` waits while `HasPendingClientPBFT()`

**Files:** `engine.go`, `coordinator.go`, `participant.go`

**Test after fix:** `TestConcurrentContention/SameSenderItemSerializes` PASS. TS1 Set 4 still fails (cross-participant race ruled out — Set 4 cross txn is `SKIP_INSUFFICIENT` per oracle).

---

### Fix 2.4 — Cross-shard serialization + settle scaling

**Problem:** Multiple cross-shard txns on one coordinator cluster interfere; Set 6 needs longer settle for 2PC completion.

**Change:**
- Coordinator `crossMu` / `acquireCrossSlot` / `ReleaseCrossSlot` (one outstanding coord-prepare per cluster)
- `OnCrossPrepareDropped` hook when PBFT bails or reclaims coord-prepare
- `SettlePerCrossTxn = 20s`; `waitCrossSettle` with periodic resend
- Skip `waitCrossSettle` when coordinator balance `< amt` (insufficient cross)

**Files:** `hooks.go`, `bridge.go`, `coordinator.go`, `engine.go`, `timers.go`, `runner.go`

**Test after fix:** TS2 Sets 1–5 PASS. TS2 Set 6 still fails (3 parallel cross-shard txns on **different** coordinators — per-cluster serialization does not serialize Set 6).

---

### Fix 2.5 — Reclaim guard + periodic client resend + borderline prepare deadline

**Problem:** Reclaim could fire while `len(prepares) == prepareCollect`; client resend fired only once so post-reclaim retries never reached the server.

**Change:**
- `reclaimSeq` skips when `len(prepares) >= prepareCollect`
- `maybeResend` repeats every `ViewChangeTimeout` after initial `ClientPrimaryWait`
- `prepareWatchDeadline` uses **12×** `ViewChangeTimeout` when `clusterHonestLive == prepareCollect` (zero prepare slack)

**Files:** `engine.go`, `runner.go`

**Test after fix:** Unit tests PASS. TS1 Set 4 still fails after Sets 1–3.

---

## 3. Final Oracle Scorecard

| Test File | Set 1 | Set 2 | Set 3 | Set 4 | Set 5 | Set 6 |
|-----------|-------|-------|-------|-------|-------|-------|
| **TS1** | PASS | PASS | PASS | **FAIL** | **FAIL** | **FAIL** |
| **TS2** | PASS | PASS | PASS | PASS | PASS | **FAIL** |

### TS1 remaining failures (cascade from Set 4)

| Set | Representative txns | Notes |
|-----|-------------------|-------|
| 4 | `(2770,2799,1)` | C3 intra; oracle expects cross `(1295,1990,17)` as **SKIP_INSUFFICIENT** |
| 5 | `(1495,1490,3)`, `(1690,1695,6)`, `(2975,2970,9)` | C2/C3 intra; partly cascade from Set 4 |
| 6 | `(888,777,5)`, `(1415,1189,5)`, `(2222,2333,6)` | Cascade |

### TS2 Set 6

| Txns | Type |
|------|------|
| `(298,1789,3)`, `(1061,2476,6)`, `(2850,1234,9)` | 3 cross-shard on **3 coordinators** (open-loop) |

---

## 4. Root Cause Analysis (Set 4)

1. **Isolated Set 4 works** — fresh DB + Set 4 fault matrix → client metrics `committed=2`.
2. **After Sets 1–3 fails** — same code path, same fault matrix.
3. **C3 has zero prepare slack** in Set 4: 9 honest, 3 Byzantine backups, `prepareCollect = 9`.
4. **Oracle reads first live replica** (S25) — failure means primary did not commit, not merely backup lag (backup lag was initial hypothesis; parallel `execute_only` drain did not fix).

**Leading hypothesis for Fix Update 3:** After Sets 1–3, some **honest C3 backups** reject prepares for Set 4 intra (balance/lock/seq validation divergence) so the primary never collects 9 prepares. Needs per-replica balance/lock inspection after Set 3 and/or **checkpoint + state transfer** (assignment-faithful recovery).

---

## 5. Files Touched

| File | Changes |
|------|---------|
| `internal/pbft/fault.go` | `ClusterHonestLive` |
| `internal/pbft/timers.go` | `SettlePerCrossTxn`, `ReclaimDrainWait` |
| `internal/pbft/engine.go` | Hopeless skip, ingress queue, reclaim WG, drain, borderline deadline |
| `internal/pbft/hooks.go` | `OnCrossPrepareDropped` |
| `internal/twopc/coordinator.go` | Cross slot, `StartClientConsensus` |
| `internal/twopc/participant.go` | Defer prepare while client PBFT pending |
| `internal/twopc/bridge.go` | Drop hook |
| `internal/server/httpapi.go` | `/drain`, `execute_only` |
| `internal/client/remote.go` | `DrainReplica`, longer HTTP timeout |
| `internal/testcase/runner.go` | Honest-live fault, periodic resend, cross settle, parallel drain |

---

## 6. Reproduce

```powershell
cd C:\Users\kashy\Desktop\byzbank\ByzBank\ByzBank

go test ./... -count=1 -timeout 200s

.\scripts\run.ps1 down
Remove-Item -Recurse -Force data -ErrorAction SilentlyContinue
.\scripts\run.ps1 build; .\scripts\run.ps1 up
Start-Sleep -Seconds 12

go run ./cmd/client --testfile Lab4_Testset_1_36node.csv --auto --verify Lab4_Testset_1_36node_expected.json
go run ./cmd/client --testfile Lab4_Testset_2_36node.csv --auto --verify Lab4_Testset_2_36node_expected.json
```

**Expected:** TS1 Sets 1–3 `ALL PASS` lines; TS2 Sets 1–5 `ALL PASS`; Set 6 both files `MISMATCHES FOUND`.

---

## 7. Recommended Fix Update 3

1. **Replica divergence debug** — After Set 3, compare `bal[2770]` on S25–S36; dump lock table / `execSeq` / nextSeq via debug HTTP.
2. **Checkpoint + state transfer** — Lagging honest backups catch up before participating in prepare (assignment Phase 10–11 scope).
3. **TS2 Set 6** — Global cross-shard throttle (runner-side stagger) or longer `waitCrossSettle` with N×`SettlePerCrossTxn` for N concurrent cross txns across coordinators.
4. **Backup prepare validation** — Log when `validateRequest` fails on backup for borderline clusters to confirm hypothesis.

---

*See also: `FIX_UPDATE_1.md`, `TESTING_REPORT.md`.*
