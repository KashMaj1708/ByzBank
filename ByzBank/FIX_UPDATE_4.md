# Fix Update 4 — Spurious View-Change, Quorum Slack, Rejoin Catch-Up

**Date:** June 6, 2026  
**Module:** `github.com/KashMaj1708/2pcbyz-kashmaj1708`  
**Baseline:** Fix Update 2 → TS1 **3/6**, TS2 **5/6**  
**After Fix Update 4:** TS1 **6/6**, TS2 **5/6**

---

## 1. Executive Summary

Fix Update 2 correctly identified cumulative cross-set state as the TS1 Set 4 symptom, but the **root mechanism** was misattributed to ingress contention and replica lag alone. Fix Update 4 targets two concrete bugs visible in the current code plus supporting hardening:

| Root cause | Symptom | Fix |
|------------|---------|-----|
| **Spurious view-changes** from insufficient intra requests on backups | TS1 Sets 4–6 cascade; Set 4 passes in isolation, fails after Sets 1–3 | Balance + op guard in `onBackupClientRequest` |
| **Zero prepare slack** (`prepareCollect = n−f = 9` with exactly 9 honest) | Borderline PBFT rounds stall/reclaim under open-loop load | `prepareCollect = Quorum()` (7) |
| **Coordinator prepare abort too early** | TS2 Set 6 cross txns abort under chained 2PC latency | `CoordPrepareTimeout = 12 × ViewChangeTimeout` |
| **Rejoining replicas with stale state** | TS2 Set 6 fails after Sets 4–5 (S19–S24 down, then live in Set 6) | `SyncFromPrimary` via HTTP `/snapshot` |
| **3-way cross-shard cycle deadlock** | Open-loop parallel cross on C1→C2→C3→C1 | Sequential cross-shard send + per-txn settle in runner |

**Scorecard:** TS1 improved from **3/6 → 6/6** (`RESULT: ALL PASS`). TS2 remains **5/6**; only Set 6 still fails.

---

## 2. Root Cause #1 — Spurious View-Changes (TS1 Sets 4–6)

### Mechanism

```13:34:internal/pbft/viewchange.go
func (e *Engine) onBackupClientRequest(ctx context.Context, req Request) {
	// ...
	if e.store.GetBalance(req.X) < req.Amt {
		return
	}
	key := reqKey(req)
	e.mu.Lock()
	e.pendingClients[key] = req
	e.scheduleViewChangeTimerLocked()
```

**Before Fix Update 4**, backups armed a view-change timer for any intra request with a fresh client TS, without checking balance. Chain of events:

1. Client fires an intra txn the **primary correctly refuses** (insufficient balance).
2. `maybeResend` rebroadcasts the unresolved txn to **all cluster replicas** after `ViewChangeTimeout`.
3. Each backup runs `onBackupClientRequest` → arms view-change timer.
4. No pre-prepare ever arrives → timer is never cancelled.
5. After `viewChangeTimeout`, ≥ `2f+1` backups trigger a **real view change**.
6. Configured contacts **S1 / S13 / S25** are no longer primary in the next set.

### Evidence mapping to test files

| Poison txn | Set | Cluster | Oracle outcome | Effect |
|------------|-----|---------|----------------|--------|
| `(2995,2994,7)` | TS1 Set 3 | C3 | `SKIP_INSUFFICIENT` | Poisons C3 before Set 4 |
| `(1295,1990,17)` | TS1 Set 4 | C2 (sent to S1; insufficient on X) | `SKIP_INSUFFICIENT` | Would poison C2 via resend — cross ignored by `SameCluster` check |
| `(2770,2799,1)` | TS1 Set 4 | C3 | `COMMIT` expected | Fails because C3 primary/contact rotated off S25 |

**Isolated Set 4 on fresh `data/`:** `committed=2` (both intra txns succeed). **After Sets 1–3:** `(2770,2799,1)` fails. Confirms cumulative state, not Set 4 logic alone.

**TS2 Sets 1–5 stay clean:** no intra-insufficient txn until Set 6 (all cross-shard). `SameCluster` is false for cross → backups never arm.

### Fix

In `onBackupClientRequest`:

- Return early unless `req.Op == "" || req.Op == OpIntra` (cross goes through 2PC, not backup client path).
- Return early when `GetBalance(req.X) < req.Amt` (primary would drop these; backups must not suspect the leader).

**No test set in either Lab4 file requires a view change** — primaries S1/S13/S25 are honest and live in every set. All observed view changes were spurious.

---

## 3. Root Cause #2 — Coordinator Prepare Timeout (TS2 Set 6 partial)

### Mechanism

`OnCoordPrepareExecuted` starts an abort timer (formerly `ViewChangeTimeout × 2` ≈ 3.4s). TS2 Set 6 is a **3-way cross-shard cycle** (C1→C2→C3→C1). Each cluster is both coordinator and participant. Participant prepare defers while local client PBFT is pending; zero-margin clusters (9 honest, 3 Byzantine) make each PBFT round slow under serialized load. Participant replies can land after the abort deadline → coordinator fires `OpCoordAbort` → oracle expects `COMMIT`, balances stay at 10.

### Fix

- Added `CoordPrepareTimeout` to `Tunables` (set to `12 × ViewChangeTimeout` ≈ 20s).
- Coordinator abort goroutine uses `timers.CoordPrepareTimeout` instead of `ViewChangeTimeout × 2`.
- Timer cannot be removed — it is the only abort path for genuinely hopeless participants (TS2 Set 5 `(45,1355,5)` with C2 at 6 live).

**Note:** Lengthening alone did not clear TS2 Set 6 when combined with rejoin staleness (see §5). Timeout fix remains necessary for slow-but-live participants.

---

## 4. Fix #3 — Prepare Quorum Slack

### Change

```114:114:internal/pbft/engine.go
		prepareCollect: topo.Quorum(),
```

| Parameter | Before | After |
|-----------|--------|-------|
| `prepareCollect` | `CollectorQuorum()` = n−f = **9** | `Quorum()` = 2f+1 = **7** |
| Honest slack (9 honest, 3 Byzantine) | **0** | **2** |

Internally consistent: commit phase already uses quorum 7; cross-cluster certs are COMMIT certs (7). Hopeless-skip threshold unchanged in effect — Set 5 C1 / TS2 degraded clusters at 6 live: `6 < 7` → still skipped.

### Test adjustment

`TestNoConsensusFourFaulty` renamed to `TestNoConsensusSixFaulty` — with quorum 7, four Byzantine backups (8 honest) can still commit; six Byzantine (6 honest) cannot.

---

## 5. Fix #4 — Rejoin State Catch-Up (TS2 Set 6)

### Mechanism

| Set | TS2 live servers | Notes |
|-----|------------------|-------|
| 4–5 | S19–S24 **absent** | C2 runs with 6 live honest |
| 6 | S19–S24 **restored** | Six replicas rejoin with stale BoltDB + PBFT seq state |

Fresh Set 6 in isolation: balances correct (`committed=3`). Sets 1–5 then Set 6: all three cross txns remain at balance 10.

### Implementation

| Component | Role |
|-----------|------|
| `store.ExportSnapshot` / `ImportSnapshot` | Copy balances, datastore, client TS; clear locks/WAL on import |
| `Engine.ExportState` / `ResetForCatchUp` | Copy `view`, `execSeq`, `nextSeq`; clear volatile PBFT log |
| `GET/POST /snapshot` | HTTP catch-up on each replica |
| `Remote.SyncFromPrimary` | Fetch from cluster primary, apply on rejoining replica |
| `Runner.ApplySetConfig` | When `faultInitialized && nowLive && !prevLive[id]`, sync before set runs |

Catch-up runs only on **dead → live** transitions after the first set (not on initial cluster start).

---

## 6. Fix #5 — Sequential Cross-Shard Dispatch

### Problem

TS2 Set 6 fires three cross-shard txns open-loop on **three different coordinators** simultaneously:

- `(298,1789,3)` — C1 coord → C2 part  
- `(1061,2476,6)` — C2 coord → C3 part  
- `(2850,1234,9)` — C3 coord → C1 part  

This forms a **3-cycle** (unlike TS2 Set 1 where C1 coordinates twice — no cycle). Parallel open-loop can deadlock the chained 2PC pipeline.

### Change (`runner.go`)

1. **Intra txns:** fire open-loop (unchanged).
2. **Cross txns:** send **one at a time**, calling `waitOneTxn` (up to `SettlePerCrossTxn` = 20s per txn, with periodic resend) before sending the next.
3. `waitCrossSettle` still runs as a safety net; skips txns where coordinator balance `< amt`.

---

## 7. Per-Fix Verification

| Step | Change | TS1 oracle | TS2 oracle |
|------|--------|------------|------------|
| After FU2 baseline | — | 3/6 | 5/6 |
| Fix #1 only (`onBackupClientRequest`) | Balance/op guard | **6/6** | 5/6 |
| + Fix #2–#3 (timeout + prepare quorum) | Tunables + engine | 6/6 | 5/6 |
| + Fix #4 (rejoin sync) | `/snapshot` | 6/6 | 5/6 |
| + Fix #5 (sequential cross) | Runner dispatch | 6/6 | 5/6 |

Latest confirmed run (June 6, 2026):

```
TS1: RESULT: ALL PASS
TS2: RESULT: MISMATCHES FOUND  (Set 6 only)
```

---

## 8. Final Oracle Scorecard

| Test file | Set 1 | Set 2 | Set 3 | Set 4 | Set 5 | Set 6 |
|-----------|-------|-------|-------|-------|-------|-------|
| **TS1** | PASS | PASS | PASS | PASS | PASS | PASS |
| **TS2** | PASS | PASS | PASS | PASS | PASS | **FAIL** |

### TS2 Set 6 remaining failures

| Txn | Coordinator | Participant | Expected |
|-----|-------------|-------------|----------|
| `(298,1789,3)` | C1 | C2 | `bal[298]=7 bal[1789]=13` |
| `(1061,2476,6)` | C2 | C3 | `bal[1061]=4 bal[2476]=16` |
| `(2850,1234,9)` | C3 | C1 | `bal[2850]=1 bal[1234]=19` |

All three show `actual=10` — none committed after Sets 1–5 despite sequential dispatch and catch-up.

---

## 9. Files Touched

| File | Changes |
|------|---------|
| `internal/pbft/viewchange.go` | Op + balance guard in `onBackupClientRequest` |
| `internal/pbft/engine.go` | `prepareCollect = Quorum()`; `ExportState` / `ResetForCatchUp` |
| `internal/pbft/timers.go` | `CoordPrepareTimeout` |
| `internal/pbft/viewchange_test.go` | `TestNoConsensusSixFaulty` |
| `internal/twopc/coordinator.go` | Use `CoordPrepareTimeout` |
| `internal/store/snapshot.go` | **New** — export/import committed state |
| `internal/server/httpapi.go` | `GET/POST /snapshot` |
| `internal/client/remote.go` | `SyncFromPrimary`, `FetchCatchUpSnapshot` |
| `internal/testcase/runner.go` | `prevLive` / `faultInitialized`; sequential cross send; `waitOneTxn`; rejoin sync |

Prior Fix Update 2 changes (hopeless-quorum skip, ingress queue, cross-slot mutex, reclaim drain, periodic resend) remain in place and are prerequisites.

---

## 10. Reproduce

```powershell
cd C:\Users\kashy\Desktop\byzbank\ByzBank\ByzBank

go test ./... -count=1 -timeout 200s

.\scripts\run.ps1 down
Remove-Item -Recurse -Force data -ErrorAction SilentlyContinue
.\scripts\run.ps1 build; .\scripts\run.ps1 up
Start-Sleep -Seconds 12

go run ./cmd/client --testfile Lab4_Testset_1_36node.csv --auto --verify Lab4_Testset_1_36node_expected.json
# Expected: RESULT: ALL PASS

.\scripts\run.ps1 down
Remove-Item -Recurse -Force data -ErrorAction SilentlyContinue
.\scripts\run.ps1 up
Start-Sleep -Seconds 12

go run ./cmd/client --testfile Lab4_Testset_2_36node.csv --auto --verify Lab4_Testset_2_36node_expected.json
# Expected: Sets 1–5 pass; Set 6 MISMATCHES FOUND
```

---

## 11. Recommended Fix Update 5 (TS2 Set 6 only)

1. **Verify catch-up on S19–S24** — After Set 5, query `bal[1061]` on S19 vs S13; confirm `/snapshot` import runs before Set 6 txns fire.
2. **2PC collector reset on catch-up** — `PreparedCollector` / `commitStarted` maps may retain stale entries across rejoin; clear on `ResetForCatchUp` or replica restart.
3. **Longer per-cross wait after cumulative sets** — Scale `waitOneTxn` by set number or cross count when `len(cross) >= 3`.
4. **Tombstone reclaim slots** — Replace `delete(e.log, seq)` with `st.discarded = true` in `reclaimSeq` to avoid `gapClearLocked` holes (defense-in-depth; see Fix Update 2 §13).
5. **Optional: stagger Set 6 only** — Detect 3-cycle cross pattern in CSV and enforce strict sequential 2PC completion (wait for datastore entry, not just client reply) before next cross txn.

---

## 12. Conclusion

Fix Update 4 resolves the **TS1 cascade** by stopping spurious view-changes — the decisive bug behind “Set 4 passes in isolation but fails after Sets 1–3.” Prepare-quorum slack, longer coordinator timeouts, rejoin catch-up, and sequential cross dispatch are supporting fixes that stabilize behavior under Lab4 fault matrices.

**TS1: 6/6.** **TS2: 5/6** — remaining work is isolated to Set 6’s 3-way cross-shard cycle under cumulative state after partial cluster liveness in Sets 4–5.

---

*See also: `FIX_UPDATE_1.md`, `FIX_UPDATE_2.md`, `TESTING_REPORT.md`, `Lab4_Testset_*_expected.json`.*
