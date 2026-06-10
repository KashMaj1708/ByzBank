# SmallBank Phase 10 — Liveness Fixes, Benchmark Regression & Lab4 Sanity (v4)

**Date:** June 10, 2026  
**Module:** `github.com/KashMaj1708/2pcbyz-kashmaj1708`  
**Prior docs:** `SmallBank_1.md` (root cause), `SmallBank_2.md` (protocol fix + 500-txn), `SmallBank_3.md` (harness + 1000-txn −64)  
**Baseline:** Fix Update 5 — TS1 **6/6**, TS2 **6/6** (per-set `ResetConsensus`)

---

## 1. Executive Summary

After the −64 stranded-WAL diagnosis in `SmallBank_3.md`, we implemented liveness fixes **E → A → D → C → B** on the coordinator/participant path. SmallBank benchmarks improved materially (32 stranded WALs → 4; drain 67% → 80%) but are **not green**. Lab4 regression:

| Test | Result | Notes |
|------|--------|-------|
| **TS1** (`Lab4_Testset_1_36node.csv`) | **6/6 ALL PASS** | Oracle verify OK (~2.5 min) |
| **TS2** (`Lab4_Testset_2_36node.csv`) | **5/6 — Set 6 FAIL** | 3 cross-shard txns missing; `committed=10` vs expected 12 |
| SmallBank 100-txn (post-fix) | **FAIL** | WAL quiescence OK on some runs; **+2** conservation leak; 77/83 writes |
| SmallBank 1000-txn (post-fix, clean) | **FAIL** | **4 WAL** stranded (not −64); 665/833 drain |

**Conclusion:** Liveness fixes help open-loop SmallBank but **regress TS2 Set 6** — likely Fix **A** (prepare timeout abort) and/or Fix **C** (fast participant NO) aborting cross-shard txns that Lab4 expects to commit under Set 6 contention.

---

## 2. Liveness Fixes Applied (recap from SmallBank_3 §6)

| Fix | Mechanism | Key files |
|-----|-----------|-----------|
| **A** | `watchPreparePhase`: prepare retry; 45s `CoordPrepareAbortTimeout` → `OpCoordAbort` + WAL-undo retry | `coordinator.go`, `timers.go` |
| **B** | Idempotent participant re-reply on duplicate prepare | `participant.go` |
| **C** | Fast participant NO: ≤1s pending wait, 500ms lock wait | `participant.go` |
| **D** | `Wait2PCQuiescence` via `GET /outstanding`; WAL=0 stable 3s | `invariant.go`, `driver.go`, `store.go`, `httpapi.go`, `remote.go` |
| **E** | `/outstanding` debug endpoint | `store.go`, `httpapi.go` |

**Post-implementation hardening:**

1. **Detached prepare watcher ctx** — `OnCoordPrepareExecuted` used the PBFT execute ctx; it was cancelled when handling returned, so timeout abort never ran.
2. **Non-blocking primary ingress** — `enqueuePrimary` retried instead of blocking forever on a full 512-slot channel (abort could never enqueue under load).
3. **Timeout race guard** — re-check `collector.Has(req)` under `commitMu` before timeout abort; late commit reply re-drives `OpCoordAbort` for ack-retried participant undo.
4. **Quiescence criterion** — WAL-only (locks without WAL wedge throughput but do not leak money).

---

## 3. SmallBank Benchmark Regression (post-fix)

**Command:**

```powershell
.\scripts\run.ps1 build
.\scripts\run.ps1 down; Remove-Item -Recurse -Force data; .\scripts\run.ps1 up
go run ./cmd/client --benchmark smallbank --txns N --skew 0.9
```

### 3.1 Pre-fix baseline (SmallBank_3)

| Run | Conservation | Drain | WAL at quiescence |
|-----|--------------|-------|-------------------|
| 100-txn | **PASS** (24030) | 94/100 | — |
| 1000-txn | **FAIL −64** (23966) | 554/833 (67%) | 32 equivalent (−64 = 32 × amt 2) |

### 3.2 Post-fix runs (June 10, 2026)

| Run | Conservation | Drain | Quiescence | Notes |
|-----|--------------|-------|------------|-------|
| 100-txn v3 | — | 77/83 | wal=1, locks=1 | Stranded txn `312fbf5f...` |
| 100-txn v4 | **FAIL +2** (24032) | 77/83, 6 pending | wal=0 | Per-cluster C1=8020 C2=8000 C3=8012 |
| 100-txn v5 | **FAIL +2** (24032) | 77/83, 6 pending | wal=0 | Same seed-42 pattern |
| 1000-txn clean | — | 665/833 (80%) | **wal=4**, locks=19 | ~27 min; 4 txn IDs stranded |

**Stranded txn IDs (1000-txn, seed 42):**

```
312fbf5f8f198f4cab8455562cbd159ed1871802cad054b4113cb45c29c2ab14
8b6c734b6d498c2c5729fa1419edd2bacf39b6c8550284400ea21190ca280a35
abd2ab5c5a2c57693b4147a10dfe5aec5157adc72e2a6ff7b129a04b706924f9
bce8b18de7dbfdb4276cc0bfc9bcb7706a913ba34119064af3c3823cc18dc659
```

`312fbf5f...` recurs across multiple 100-txn runs — deterministic workload artifact, good Fix E probe target.

**Progress vs −64:** Stranded WAL count dropped **32 → 4**; drain **67% → 80%**. Conservation not exact; occasional **+2** leak (participant credit without coordinator undo) on 100-txn runs with wal=0.

---

## 4. Lab4 TS1 / TS2 Sanity Checks

**Procedure** (per `FIX_UPDATE_5.md` — separate clean cluster per test file):

```powershell
.\scripts\run.ps1 build
.\scripts\run.ps1 down
Remove-Item -Recurse -Force data -ErrorAction SilentlyContinue
.\scripts\run.ps1 up
Start-Sleep -Seconds 12

# TS1
go run ./cmd/client --testfile Lab4_Testset_1_36node.csv --auto --verify Lab4_Testset_1_36node_expected.json

# TS2 (fresh cluster — do NOT chain after TS1)
.\scripts\run.ps1 down
Remove-Item -Recurse -Force data -ErrorAction SilentlyContinue
.\scripts\run.ps1 up
Start-Sleep -Seconds 12
go run ./cmd/client --testfile Lab4_Testset_2_36node.csv --auto --verify Lab4_Testset_2_36node_expected.json
```

**Logs:** `run/ts1_sanity.log`, `run/ts2_sanity.log`

### 4.1 TS1 — **PASS** (6/6)

| Set | committed | throughput | mean_latency |
|-----|-----------|------------|--------------|
| 1 | 3 | 25.08 txns/s | 116ms |
| 2 | 7 | 3.34 txns/s | 123ms |
| 3 | 10 | 2.51 txns/s | 115ms |
| 4 | 12 | 0.32 txns/s | 109ms |
| 5 | 15 | 0.22 txns/s | 107ms |
| 6 | 18 | 0.17 txns/s | 103ms |

```
RESULT: ALL PASS
```

Elapsed ~2.5 min · exit 0

### 4.2 TS2 — **FAIL** (5/6 oracle; Set 6 mismatch)

| Set | committed | throughput | mean_latency | Oracle |
|-----|-----------|------------|--------------|--------|
| 1 | 3 | 9.61 txns/s | 104ms | OK |
| 2 | 6 | 2.58 txns/s | 118ms | OK |
| 3 | 7 | 1.68 txns/s | 110ms | OK |
| 4 | 8 | 0.06 txns/s | **5.111s** | OK |
| 5 | 9 | 0.03 txns/s | **6.767s** | OK |
| 6 | **10** | 0.03 txns/s | **6.131s** | **FAIL** |

```
RESULT: MISMATCHES FOUND
```

Elapsed ~8.4 min · exit 1

#### Set 6 oracle failures

| Item | actual | expected | Txn |
|------|--------|----------|-----|
| bal[1234] | 10 | 19 | `(2850,1234,9)` participant credit missing |
| bal[1061] | 10 | 4 | `(1061,2476,6)` |
| bal[2476] | 10 | 16 | `(1061,2476,6)` |
| bal[2850] | 10 | 1 | `(2850,1234,9)` coordinator debit missing |

**Datastore:**

- **C2 missing:** `CROSS (2850,1234,9)` PREPARE+COMMIT, `(1061,2476,6)` PREPARE+COMMIT, `(298,1789,3)` COMMIT
- **C3 unexpected:** `CROSS (2850,1234,9) COMMIT(ABORT)` — coordinator aborted; oracle expects COMMIT

**Shape:** C3 coordinator → C2 participant for `(2850,1234,9)` — same topology as Fix Update 5 Set 6 analysis (not a 3-cycle deadlock). Under Sets 4–6 rising latency (5–7s), cross-shard prepare/abort paths interact with the new **45s timeout abort** and **500ms fast NO** lock wait.

---

## 5. Analysis — Why TS2 Set 6 Regressed

| Hypothesis | Evidence |
|------------|----------|
| **Fast NO (Fix C)** aborts when `y` hot-locked for <500ms | Set 6 has serialized cross-shard load; participant may vote `OpPartPrepareAbort` while Lab4 oracle expects commit |
| **Timeout abort (Fix A)** fires before participant reply | C3 shows `COMMIT(ABORT)` for `(2850,1234,9)` — coordinator aborted; mean latency 6s ≪ 45s timeout, so more likely NO-vote or lost-reply + abort than pure timeout |
| **Ingress saturation** | Sets 4–6 throughput collapses to 0.03 txns/s; prepare/abort delivery still lossy despite retry |
| **Not seq desync** | Sets 1–5 pass; per-set `ResetConsensus` intact; failure isolated to Set 6 cross-shard outcomes |

**Distinction from pre-SmallBank TS2 fix:** FU5 Set 6 failure was **seq-space desync** (`committed=9`). This failure is **`committed=10`** with **wrong cross-shard outcomes** — a **2PC protocol interaction**, not PBFT log holes.

---

## 6. Phase 10 Checklist (updated)

| Requirement | Status |
|-------------|--------|
| Schema + six txn types + skew | ✅ |
| Open-loop driver + metrics | ✅ |
| Conservation oracle | ⚠️ 100-txn +2 leak; 1000-txn wal=4 |
| Liveness fixes E→A→D→C→B | ✅ implemented |
| Demo: 1000 txns + green invariant | ❌ |
| **TS1 regression** | ✅ **6/6** |
| **TS2 regression** | ❌ **5/6** (Set 6) |
| `go test ./internal/pbft ./internal/twopc ./internal/smallbank` | ✅ |

---

## 7. Recommended Next Steps

1. **Gate Fix C under load** — Lab4 sets: restore longer `waitItemUnlocked` on participant, or only apply fast-NO when `BYZ_SMALLBANK=1` / benchmark mode.
2. **Gate Fix A timeout** — disable `watchPreparePhase` abort for Lab4 driver (commit phase always completes via participant reply in test sets); keep for SmallBank open-loop only.
3. **Trace `312fbf5f...`** — map seed-42 workload index to `(x,y,amt)` and replay under debug logging.
4. **Re-run TS2 Set 6 isolation** (`run/set6_only.csv`) after gating to confirm `committed=3`.
5. **`PHASE10_REPORT.md`** once TS2 green and 1000-txn conservation exact.

---

## 8. Key File Index

| File | Role |
|------|------|
| `SmallBank_3.md` | −64 diagnosis, liveness fix spec |
| `SmallBank_4.md` | This doc — regression + TS1/TS2 sanity |
| `internal/twopc/coordinator.go` | `watchPreparePhase`, timeout abort, prepare retry |
| `internal/twopc/participant.go` | Fast NO, idempotent re-reply |
| `internal/pbft/engine.go` | Non-blocking `enqueuePrimary` |
| `FIX_UPDATE_5.md` | Per-set `ResetConsensus`, TS2 Set 6 prior fix |
| `run/ts1_sanity.log` | TS1 run output |
| `run/ts2_sanity.log` | TS2 run output |
| `run/smallbank_1000_clean.log` | 1000-txn post-fix run |

---

*End of SmallBank_4.md*
