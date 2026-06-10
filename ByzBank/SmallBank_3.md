# SmallBank Phase 10 — Harness Fixes & 100-Txn Sanity (v3)

**Date:** June 9, 2026  
**Module:** `github.com/KashMaj1708/2pcbyz-kashmaj1708`  
**Prior docs:** `SmallBank_1.md` (root cause), `SmallBank_2.md` (protocol fix + 500-txn)  
**Plan reference:** `Project4_Go_Implementation_Plan.md`, Phase 10  
**Status:** **100-txn PASS** (pre-fix); **1000-txn FAIL** (−64 stranded); **liveness fixes applied** (§6) — re-verify pending

---

## 1. Executive Summary

After `SmallBank_2.md` identified that conservation passed but commit rate collapsed (84/500, 0 cross) due to silent balance rejection and harness issues, we applied a second round of fixes: **insufficient-balance replies**, **MultiStepTimeout**, **`--amt 2`**, **striped hot set**, and **progress-bar corrections**. The **100-txn sanity check passes** with strong commit rate and cross-shard activity.

| Deliverable | Status |
|-------------|--------|
| Insufficient-balance client replies | **Done** |
| Driver resolves on any quorum result | **Done** |
| `MultiStepTimeout` (10s) for Amg steps | **Done** |
| Default `--amt 2` | **Done** |
| Hot set striped across 3 clusters | **Done** |
| Treasury seed 30×10 per cluster | **Done** |
| Progress `finish(done, note)` fix | **Done** |
| **100-txn sanity** (`--skew 0.9`) | **PASS** |
| **1000-txn demo** | **FAIL** (conservation −64) |
| TS1/TS2 re-verify | **Pending** |

---

## 2. Problems Addressed (post–SmallBank_2)

### Problem 1 — Silent balance rejection (`--amt 100` vs balance 10)

**Symptom:** 84/500 commits; only ~83 `Bal` read-only ops counted as success; 416 writes polled 300s+90s with no reply.

**Cause:** `validateForStartQuick`, `validateRequest`, and `coordinator.HandleClientRequest` rejected `amt > balance` with **no client reply**. Driver `collectReply` only matched `"committed"`.

**Fix:**
- Added `pbft.ResultInsufficient = "insufficient"`
- `Engine.RejectInsufficient` / `maybeRejectInsufficient` on primary when `GetBalance(x) < amt`
- Coordinator sends insufficient reply and returns `true` (handled)
- Driver `tryResolve` uses `collectResult` — any quorum result resolves the op; only `"committed"` counts toward throughput

**Files:** `internal/pbft/messages.go`, `engine.go`, `internal/twopc/coordinator.go`, `internal/smallbank/driver.go`

### Problem 2 — 7-hour submit (Amg × SettleTimeout)

**Symptom:** 83 Amg × 300s ≈ 7h submit on failed steps.

**Fix:** `Config.MultiStepTimeout` default **10s**; Amg skips step-2 unless step-1 returns `ResultCommitted`.

### Problem 3 — 0/25 cross commits

**Symptom:** All cross writes rejected at ingress with `--amt 100`.

**Fix:** Resolved by Problem 1 + `--amt 2`. Latent `crossBusy` serialization remains (documented, acceptable for demo).

### Problem 4 — Throughput 0.00 txns/s

**Fix:** Self-corrected once Problems 1–2 fixed. Metrics now report `%.3f` throughput, wall clock, and a **writes** bucket excluding `Bal`.

### Problem 5 — `--amt 1` degenerate paths

**Fix:** Default `--amt` → **2** (`amt/2 = 1` for Amg/TS withdraw paths).

### Problem 6 — Hot set entirely in cluster 1

**Symptom:** `NewPicker(1200, 0.1, …)` → customers 1–120 all in C1.

**Fix:** Picker stripes hot set: **40 hot customers per cluster** (`numClusters`, `customersPerCluster` args). Treasury seed increased to **10 customers × 10 amt × 3 clusters** (30 fund transfers).

**Files:** `internal/smallbank/skew.go`, `skew_test.go`, `driver.go`

### Problem 7 — Progress bar `finish()` lied

**Fix:** `finish(done int, note string)` reports actual count; settle label → `writes`.

**File:** `internal/smallbank/progress.go`

---

## 3. Code Changes Summary

| File | Change |
|------|--------|
| `internal/pbft/messages.go` | `ResultCommitted`, `ResultAbort`, `ResultInsufficient` |
| `internal/pbft/engine.go` | `RejectInsufficient`, `insufficientBalance`, `sendClientReject` |
| `internal/twopc/coordinator.go` | Insufficient reply on balance check |
| `internal/smallbank/driver.go` | `tryResolve`, `MultiStepTimeout`, striped picker, fund 30×10 |
| `internal/smallbank/skew.go` | Cross-cluster hot stripe |
| `internal/smallbank/progress.go` | `finish(done, note)` |
| `internal/smallbank/metrics.go` | Writes bucket, wall clock, `%.3f` throughput |
| `cmd/client/main.go` | `--amt 2`, `MultiStepTimeout` |

**Tests:** `go test ./internal/pbft`, `./internal/twopc`, `./internal/smallbank` — **PASS** (includes `TestPickerHotStripedAcrossClusters`).

---

## 4. 100-Txn Sanity Check (June 9, 2026)

### Command

```powershell
.\scripts\run.ps1 build
.\scripts\run.ps1 down; Remove-Item -Recurse -Force data; .\scripts\run.ps1 up
go run ./cmd/client --benchmark smallbank --txns 100 --skew 0.9
```

| Parameter | Value |
|-----------|-------|
| `--txns` | 100 |
| `--skew` | 0.9 |
| `--amt` | 2 (default) |
| Pace | 25ms |
| MultiStepTimeout | 10s |
| SettleTimeout | 300s |

### Result — **PASS** (`exit_code: 0`, elapsed **~7.7 min**)

```
Conservation: OK (initial=24030 final=24030 penalties=0)
SmallBank metrics:
  overall:  committed=94/100 throughput=4.894 txns/s wall=19.208s mean=6.261s p50=4.351s p95=18.782s p99=19.048s
  writes:   committed=77 (excl. Bal) throughput=4.009 txns/s mean=7.642s
  intra:    committed=72/74 throughput=3.749 txns/s mean=6.164s p50=3.766s p95=18.814s p99=19.048s
  cross:    committed=22/26 throughput=1.167 txns/s mean=6.58s p50=4.351s p95=17.879s p99=17.963s
  penalties applied: 0
```

### Interpretation

| Metric | Value | Notes |
|--------|-------|-------|
| Conservation | **OK** | +7 leak remains fixed |
| Write commit rate | **77/83** (93%) | 6 stragglers pending after drain |
| Cross commits | **22/26** (85%) | Was 0/25 in Jun 8 run |
| Submit duration | ~minutes | No 7h Amg blocking |
| Cross vs intra p50 | 4.35s vs 3.77s | Cross slightly higher (expected) |

Progress bars: `fund` 30/30 → `submit` 100/100 → `writes` 77/83 → `drain` 77/83 → quiescence → metrics.

---

## 5. Comparison Across Runs

| Run | `--amt` | Conservation | Committed | Cross | Wall / elapsed |
|-----|---------|--------------|-----------|-------|----------------|
| 100 txns (Jun 9, pre-harness) | 1 | OK | 100/100 | 8/8 | ~30s |
| 500 txns (Jun 8, protocol fix only) | 100 | OK | 84/500 | 0/25 | 7h 2m |
| **100 txns (Jun 9, harness fix)** | **2** | **OK** | **94/100** | **22/26** | **~7.7 min** |

---

## 6. 1000-Txn Check (June 9, 2026)

### Command

```powershell
.\scripts\run.ps1 build
.\scripts\run.ps1 down; Remove-Item -Recurse -Force data; .\scripts\run.ps1 up
go run ./cmd/client --benchmark smallbank --txns 1000 --skew 0.9
```

| Parameter | Value |
|-----------|-------|
| `--txns` | 1000 |
| `--skew` | 0.9 |
| `--amt` | 2 (default) |
| Elapsed | **~20.7 min** (`exit_code: 1`) |

### Result — **FAIL** (conservation)

```
smallbank: conservation violated: initial=24030 penalties=0 want=24030 got=23966
  per-cluster: C1=8026  C2=7949  C3=7991  (sum=23966, Δ=−64)
```

| Metric | Value | Notes |
|--------|-------|-------|
| Conservation | **FAIL** | **−64** global (money lost, not the old +7 leak) |
| Writes resolved (drain) | **554/833** (67%) | 279 pending at drain timeout |
| Metrics table | Not printed | Run exits before metrics on conservation failure |

### Analysis

- **100-txn passed; 1000-txn lost 64** — scale-dependent correctness issue remains under sustained load.
- Negative Δ suggests **debits without matching credits** (or snapshot during incomplete 2PC), distinct from the prior **+amt** abort-delivery bug.
- Per-cluster loss is spread (C2 lowest at 7949) — not a single-cluster treasury artifact.
- **Next debug:** per-cluster sum diff before/after; trace cross-shard abort/insufficient paths at scale; extend quiescence or wait for zero pending writes before `SumBalances`.

---

## 6. Liveness Fixes (E → A → D → C → B)

**Root cause (−64 at 1000 txns):** Fix 2 removed presumed-abort without replacing termination. Coordinator debited `x`, participant reply lost → WAL stranded forever (−32 × `amt(2)` = −64).

| Fix | What | Files |
|-----|------|-------|
| **A** | `watchPreparePhase`: retry prepare every `AckRetryInterval`; on `CoordPrepareAbortTimeout` (45s) set `commitStarted`, `OpCoordAbort` with WAL-undo retry loop | `coordinator.go`, `timers.go` |
| **B** | Coordinator prepare retry (in A); participant idempotent re-reply when WAL/prepare record exists | `participant.go` |
| **C** | Fast participant NO: ≤1s pending wait, 500ms lock wait → `OpPartPrepareAbort` instead of 20s+ block | `participant.go` |
| **D** | `Wait2PCQuiescence`: drain + poll `/outstanding` until **WAL=0** (3s stable); stranded-txn error includes txn IDs | `invariant.go`, `driver.go`, `store.go`, `httpapi.go`, `remote.go` |
| **E** | `/outstanding` HTTP debug endpoint (`wal_count`, `lock_count`, `wal_txn_ids`) | `store.go`, `httpapi.go` |

**Additional hardening (post-diagnosis):**
- Prepare watcher uses `context.Background()` — execute ctx was cancelled when PBFT returned, killing timeout abort.
- `enqueuePrimary` retries instead of blocking forever on full ingress (512).
- Late participant commit reply after timeout: re-drive `OpCoordAbort` for ack-retried participant undo.
- Quiescence: WAL-only (locks without WAL wedge throughput but do not leak money).

**Expected post-fix:** conservation exact at 24030; write commit rate 85–95%; zero WAL at quiescence; runtime in minutes not hours.

---

## 7. Phase 10 Checklist (updated)

| Requirement | Status |
|-------------|--------|
| Schema + six txn types + skew | ✅ |
| Open-loop driver + metrics | ✅ |
| Conservation oracle | ✅ (100 + 500 post protocol-fix) |
| Insufficient-balance observability | ✅ |
| Progress bars | ✅ |
| Demo: 1000 txns + green invariant | ❌ −64 at 1000 txns |
| Cross-shard latency > intra | ✅ at 100 txns |
| Non-degenerate throughput | ✅ at 100 txns (~4.9 txns/s) |
| `go test ./internal/smallbank` | ✅ |
| TS1/TS2 regression | See `SmallBank_4.md` — TS1 ✅ TS2 Set 6 ❌ |

---

## 8. Next Steps

1. **Root-cause 1000-txn −64 leak** (scale-dependent; 100-txn OK).
2. **TS1/TS2** re-verify after harness changes.
3. Write **`PHASE10_REPORT.md`**.
4. Commit all SmallBank + protocol changes.
5. **Phase 11** — debug log strip, `submit lab4`.

---

## 9. Key File Index

| File | Role |
|------|------|
| `SmallBank_1.md` | +7 root cause, protocol fixes 1–4 |
| `SmallBank_2.md` | 500-txn protocol verification, progress bars |
| `SmallBank_3.md` | This doc — harness fixes, 100-txn pass, 1000-txn |
| `internal/smallbank/driver.go` | Benchmark driver |
| `internal/pbft/engine.go` | Insufficient replies |
| `internal/twopc/coordinator.go` | Cross-shard insufficient |

---

*End of SmallBank_3.md*
