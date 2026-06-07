# Fix Update 5 — TS2 Set 6 Corrected Diagnosis & Log-Hole Repair

**Date:** June 6, 2026  
**Module:** `github.com/KashMaj1708/2pcbyz-kashmaj1708`  
**Baseline:** Fix Update 4 → TS1 **6/6**, TS2 **5/6**  
**After repair-stack attempts (§4):** TS1 **6/6**, TS2 **5/6**  
**After per-set `ResetConsensus` (§12):** TS1 **6/6**, TS2 **6/6**

---

## 1. Executive Summary

Fix Update 4 correctly fixed TS1 (spurious view-changes) but **misdiagnosed TS2 Set 6** in two ways. Fix Update 5 applies the corrected diagnosis, runs a decisive probe, and initially tried log-hole / quorum-recovery hardening (still 5/6). **§10** implements the actual fix: **per-set `ResetConsensus`** at each set boundary. **TS2 reaches 6/6.**

| FU4 error | Corrected fact |
|-----------|----------------|
| Set 6 is a **3-cycle deadlock** `(2850,1234,9)` listed as C3→C1 | `1234 ∈ C2` (1001–2000) → **C3 coord → C2 part**. Shape matches TS2 Set 1 (C2 coordinator once, participant twice), which **passes** |
| Rejoining S22–S24 hold **stale committed state** after Sets 4–5 | C2 commits **nothing** in Sets 4–5 (`clusterHonestLive=6 < prepareCollect=7` → hopeless skip at `engine.go:299`). S22–S24 (live through Set 3) already match S13 at `execSeq=nextSeq=13` on committed store |
| **`SyncFromPrimary` / `ResetForCatchUp` cures Set 6** | Catch-up runs **only** in the failing scenario (`faultInitialized && nowLive && !prevLive`). Isolation passes; after Sets 1–5 fails. Full reset **wipes PBFT log** and risks silencing rejoiners in the set where they must form quorum |

**Actual root cause (probe-confirmed):** Reclaim `delete(e.log, seq)` leaves **PBFT log holes** on backups. Backups fail `gapClearLocked` → do not send PREPARE → C2 stalls below prepare quorum 7 → all three Set-6 txns fail (each touches C2).

**Scorecard:** TS1 **6/6** unchanged. TS2 **5/6** unchanged. Set 6 **in isolation** passes (`committed=3`).

---

## 2. Corrected TS2 Set 6 Topology

### Transaction routing (all three touch C2)

| Txn | Coordinator | Participant | C2 role |
|-----|-------------|-------------|---------|
| `(298,1789,3)` | C1 | C2 | **Participant** (1789) |
| `(1061,2476,6)` | C2 | C3 | **Coordinator** (1061) |
| `(2850,1234,9)` | C3 | C2 | **Participant** (1234) |

Same structural pattern as TS2 Set 1 — not a 3-cycle deadlock.

### Sets 4–5: C2 is idle on committed state

Every C2 coordinator txn in Sets 4–5 is hopeless-skipped before seq assignment:

- Set 4: `(1877,2855,5)`, `(1333,2333,3)` — C2 has 6 honest live (S13–S18 only)
- Set 5: `(45,1355,5)` participant path on C2 also blocked at `6 < 7`

C2 advances **no durable commits** while degraded. Rejoining replicas did not miss committed cross-shard work.

### Single fault that kills all three Set-6 txns

C2 cannot form a prepare quorum of 7. With S13–S18 (6 always-live) + S22–S24 (3 rejoined) = 9 honest, quorum should clear unless rejoined / backup nodes **do not contribute prepares**.

Evidence consistent with `(298,1789,3)` abort path: C1 debits 298→7, C2 never replies, coord-abort + WAL undo restores `bal[298]=10`.

---

## 3. Probe Results (`BYZ_DEBUG=1`)

Temporary `SET6_PROBE` logs in `engine.go` (since removed) keyed on S22–S24 and S13 for participant prepare of item 1789.

### Before log-hole fixes

```
S13 onPrepare: seq=14 prepares=1 from=S13 ready=false
(S22, S23, S24 — no processPrePrepare logs)
```

**Interpretation:** S13 stalled at 1 prepare; rejoined nodes not participating.

### After `RepairSeqGaps` on rejoin only

```
S22/S23/S24 processPrePrepare: sending prepare seq=14 op=part_prepare_commit
S13 onPrepare: seq=14 prepares=4 from=S22,S23,S24,S13 ready=false
S14–S18 gapClear: missing slot i=13 seq=14 execSeq=13
```

**Interpretation:**

1. Rejoin repair unblocked S22–S24 for seq 14.
2. **Always-live S14–S18** also blocked — not a catch-up-only problem.
3. Root issue is **missing log slot 13** on backups (`gapClearLocked` returns false at `onPrePrepare`).
4. Still only 4/7 prepares — quorum not reached.

### Smoking gun: catch-up only in failing path

`ApplySetConfig` gates `SyncFromPrimary` on `faultInitialized && nowLive && !prevLive` (`runner.go`). Set 6 alone: `faultInitialized == false` → catch-up never runs → passes. Set 6 after Sets 1–5: S19–S24 dead→live → catch-up runs on C2 nodes that must form Set-6 quorum.

Removing full catch-up was correct; **log-hole repair** is the needed follow-on.

---

## 4. Fixes Implemented (Fix Update 5)

### Fix 5.1 — Remove full rejoin catch-up

**Change:** Drop `SyncFromPrimary` call from `Runner.ApplySetConfig`. Keep `prevLive` tracking.

**Rationale:** Degraded clusters commit nothing while down. `ResetForCatchUp` wipes volatile PBFT state and was silencing S22–S24 in the exact set where they must prepare.

**Result:** TS2 still **5/6** — removal alone does not fix Set 6 (same as FU4 with catch-up enabled).

---

### Fix 5.2 — Tombstone reclaim (no `delete` from log)

**Problem:** `reclaimSeq` / `onDiscardSeq` used `delete(e.log, seq)`, leaving holes. `gapClearLocked` requires every slot in `[execSeq, seq)` to exist (or be `discarded`).

**Change:**

- `reclaimSeq`: set `st.discarded = true`, clear prepares, keep slot in map
- `onDiscardSeq`: same tombstone semantics
- `startConsensus`: reset discarded slot when reusing seq (fresh prepares/commits maps)
- Fixed panic: `st.prepares = nil` caused `assignment to entry in nil map` in `onPrepare` — use empty maps

**Files:** `internal/pbft/engine.go`

---

### Fix 5.3 — `gapClear` self-heal for missing slots

**Change:** In `gapClearLocked`, when slot `i` is missing (`!ok`) and `i >= execSeq`, auto-insert a `discarded` tombstone instead of returning false.

**Rationale:** Backups that missed `DiscardSeq` while dead (Sets 4–5) can self-heal on first `onPrePrepare` for a later seq.

**Files:** `internal/pbft/engine.go`

---

### Fix 5.4 — Lightweight seq repair on rejoin (`RepairSeqGaps`)

**Problem:** Full `ResetForCatchUp` is too aggressive. Backups still need primary `view` / `execSeq` / `nextSeq` alignment and tombstones for holes in `[execSeq, nextSeq)`.

**Change:**

| Component | Role |
|-----------|------|
| `Engine.RepairSeqGaps` | Adopt primary metadata; tombstone missing slots in `[execSeq, nextSeq)` — **no log wipe, no store copy** |
| `POST /repair_seq` | Apply `EngineState` on replica |
| `Remote.RepairSeqFromPrimary` | GET `/snapshot` engine metadata from primary, POST `/repair_seq` on replica |
| `Runner.ApplySetConfig` | On cluster rejoin (any dead→live in cluster), repair **all live non-primary backups** in that cluster |

**Files:** `engine.go`, `httpapi.go`, `remote.go`, `runner.go`

**Note:** `/snapshot` GET/POST and `ResetForCatchUp` remain in tree but runner no longer calls full sync.

---

### Fix 5.5 — Coordinator secondary hardening

**Problem:** Set 5 `(45,1355,5)` resends during ~20s no-quorum settle can spawn duplicate coord-prepare; `CoordPrepareTimeout` can bleed into Set 6; cross-slot can leak on view change.

**Change (`coordinator.go`):**

1. `commitStarted[TxnID(req)]` guard at `HandleClientRequest` entry and after `acquireCrossSlot` / `waitItemUnlocked`
2. `ReleaseCrossSlot()` moved **before** `self != primary()` early-return in `OnCoordPrepareExecuted`

**Files:** `internal/twopc/coordinator.go`

---

### Fix 5.6 — Quiesce between degraded sets

**Change (`runner.go`):**

- Track `degradedClusters` when a set contains a cross txn whose coordinator or participant cluster has `honestLive < quorum`
- After such a set, set `pendingQuiesce = CoordPrepareTimeout` (~20s)
- `ApplySetConfig` waits `pendingQuiesce` before pushing fault config / sending txns

**Rationale:** Let Set 5 abort/resend activity finish before Set 6 setup.

---

### Fix 5.7 — Purge inflight on quorum recovery

**Problem:** Stale non-executed PBFT instances on a primary after sub-quorum sets may block ingress for Set 6 cross txns.

**Change:**

| Component | Role |
|-----------|------|
| `Engine.PurgeInflight` | Primary reclaims all non-executed `prePrepare` slots without commit cert; wait reclaims + drain execute |
| `POST /purge_inflight` | HTTP trigger |
| `Remote.PurgeInflightPrimary` | Client wrapper |
| `Runner.ApplySetConfig` | When cluster was `degradedClusters` and now `honestLive >= quorum`, purge primary before set runs |

**Files:** `engine.go`, `httpapi.go`, `remote.go`, `runner.go`

**Result:** Latest clean run still **5/6** — purge may not be sufficient or errors are swallowed (`_ =` on repair/purge calls).

---

## 5. Per-Fix Verification

| Step | Change | TS1 | TS2 | Notes |
|------|--------|-----|-----|-------|
| FU4 baseline | — | 6/6 | 5/6 | Fix #1 (view-change guard) confirmed correct |
| Remove `SyncFromPrimary` | runner | 6/6 | 5/6 | Catch-up removal alone ≠ fix |
| Tombstone reclaim + coord hardening + quiesce | engine + coordinator + runner | 6/6 | 5/6 | S13 panic fixed (nil prepares map) |
| `RepairSeqGaps` + `gapClear` self-heal | engine + HTTP | 6/6 | 5/6 | Probe: 4/7 prepares on seq 14 |
| `PurgeInflight` on recovery | engine + runner | 6/6 | 5/6 | Latest run June 6, 2026 |

### Latest confirmed full run (June 6, 2026)

```text
TS1: RESULT: ALL PASS
TS2: RESULT: MISMATCHES FOUND  (Set 6 only)

Set 6 Performance: committed=9  (no new commits vs Set 5's 9)
```

### Set 6 isolation (fresh `data/`, `run/set6_only.csv`)

```text
Performance: committed=3 throughput=5.14 txns/sec
```

Confirms Set 6 logic is sound; failure is **cumulative state from Sets 1–5**.

---

## 6. Final Oracle Scorecard

| Test file | Set 1 | Set 2 | Set 3 | Set 4 | Set 5 | Set 6 |
|-----------|-------|-------|-------|-------|-------|-------|
| **TS1** | PASS | PASS | PASS | PASS | PASS | PASS |
| **TS2** | PASS | PASS | PASS | PASS | PASS | **FAIL** |

### TS2 Set 6 failures (latest run)

| Balance | Actual | Expected |
|---------|--------|----------|
| `bal[298]` | 10 | 7 |
| `bal[1789]` | 10 | 13 |
| `bal[1061]` | 10 | 4 |
| `bal[2476]` | 10 | 16 |
| `bal[2850]` | 10 | 1 |
| `bal[1234]` | 10 | 19 |

### Datastore mismatches (Set 6)

| Cluster | Issue |
|---------|-------|
| **C1** | `unexpected: CROSS (298,1789,3) COMMIT(ABORT)`; `missing: COMMIT` |
| **C2** | `missing: PREPARE/COMMIT` for all three cross txns |
| **C3** | `unexpected: CROSS (2850,1234,9) COMMIT(ABORT)`; `missing: (1061,2476,6)` entries |

---

## 7. Operational Notes

### Always rebuild `server.exe` before oracle runs

`.\scripts\run.ps1 up` launches `bin/server.exe`. `go build ./...` alone does **not** refresh it. Stale binaries produced false regressions (Set 1 failing on TS2).

```powershell
.\scripts\run.ps1 build   # required before up
.\scripts\run.ps1 down
Remove-Item -Recurse -Force data -ErrorAction SilentlyContinue
.\scripts\run.ps1 up
```

### Do not chain TS1 then TS2 without restart

TS1 and TS2 are independent test files. Running both on the same 36-server session without `down` + wipe `data/` poisons TS2.

### Debug logging

Set `$env:BYZ_DEBUG="1"` before `.\scripts\run.ps1 up` to write probe logs to `run/logs/S*.out.log`.

---

## 8. Files Touched

| File | Changes |
|------|---------|
| `internal/pbft/engine.go` | Tombstone reclaim; `RepairSeqGaps`; `gapClear` self-heal; `PurgeInflight`; discarded-slot reset in `startConsensus` |
| `internal/pbft/viewchange.go` | *(unchanged in FU5; FU4 Fix #1 remains)* |
| `internal/pbft/timers.go` | *(unchanged; `CoordPrepareTimeout` from FU4)* |
| `internal/twopc/coordinator.go` | `commitStarted` resend guard; `ReleaseCrossSlot` ordering |
| `internal/server/httpapi.go` | `POST /repair_seq`, `POST /purge_inflight`; `/snapshot` retained |
| `internal/client/remote.go` | `RepairSeqFromPrimary`, `PurgeInflightPrimary`; `SyncFromPrimary` retained unused by runner |
| `internal/testcase/runner.go` | Removed full catch-up; cluster-wide seq repair; `degradedClusters` + quiesce; purge on recovery |
| `internal/store/snapshot.go` | *(unchanged from FU4)* |
| `run/set6_only.csv` | **New** — single-set isolation probe file |

Prior Fix Update 2 changes (hopeless-quorum skip, ingress queue, cross-slot mutex, reclaim drain, periodic resend) and Fix Update 4 Fix #1 (view-change guard) remain prerequisites.

---

## 9. Reproduce

```powershell
cd C:\Users\kashy\Desktop\byzbank\ByzBank\ByzBank

.\scripts\run.ps1 build
.\scripts\run.ps1 down
Remove-Item -Recurse -Force data -ErrorAction SilentlyContinue
.\scripts\run.ps1 up
Start-Sleep -Seconds 12

# TS2 full oracle
go run ./cmd/client --testfile Lab4_Testset_2_36node.csv --auto --verify Lab4_Testset_2_36node_expected.json
# Expected: RESULT: ALL PASS; Set 6 committed=12

# Set 6 isolation (should pass)
.\scripts\run.ps1 down
Remove-Item -Recurse -Force data -ErrorAction SilentlyContinue
.\scripts\run.ps1 up
Start-Sleep -Seconds 12
go run ./cmd/client --testfile run\set6_only.csv --auto
# Expected: committed=3

# TS1 sanity (separate clean run)
.\scripts\run.ps1 down
Remove-Item -Recurse -Force data -ErrorAction SilentlyContinue
.\scripts\run.ps1 up
Start-Sleep -Seconds 12
go run ./cmd/client --testfile Lab4_Testset_1_36node.csv --auto --verify Lab4_Testset_1_36node_expected.json
# Expected: RESULT: ALL PASS
```

---

## 10. The Actual Fix — Per-Set Consensus Reset

The probe proved the primary (S13) assigns seq 14 while backups only have slots through 12 — a **seq-space desync at the set boundary**, created by carrying PBFT volatile state across sets. More repair layers cannot fix a primary that is one slot ahead of its backups.

**Invariant:** Each set starts from a consistent consensus baseline. Durable truth (balances, datastore, client TS) stays in BoltDB; seq numbers only order consensus within the current set. The runner already settles + drains each set before the next.

### Implementation

| Layer | API | What it clears |
|-------|-----|----------------|
| PBFT | `Engine.ResetConsensus()` | `view=0`, `execSeq=nextSeq=1`, empty `log`, view-change state, `recentReplies`; `HubSender.SetView(0)` |
| Store | `Store.ClearLocksAndWAL()` | Lock and WAL buckets only (balances/datastore untouched) |
| 2PC | `Coordinator.Reset()` | `commitStarted`, `crossBusy`, `PreparedCollector`, `AckCollector` |
| HTTP | `POST /reset_consensus` | All of the above on one replica |
| Runner | `ApplySetConfig` | After `SetFault`, call `ResetConsensus` on every **live** node before any txn fires |

### Removed (dead weight)

- `SyncFromPrimary` / `ResetForCatchUp` / `/snapshot`
- `RepairSeqGaps` / `RepairSeqFromPrimary` / `/repair_seq`
- `PurgeInflight` / `/purge_inflight`
- `degradedClusters` / `pendingQuiesce` / `prevLive` / `faultInitialized`
- `gapClear` self-heal (FU5)

**Kept:** FU2 hopeless-skip, FU4 view-change guard, tombstone-on-reclaim, coordinator `commitStarted` guard, sequential cross dispatch.

### Verification (June 6, 2026)

```text
TS2: RESULT: ALL PASS
  Set 6 Performance: committed=12  (was 9 before fix)

TS1: RESULT: ALL PASS
```

After `ApplySetConfig` for Set 6, every live C2 node starts at `execSeq=nextSeq=1` with an empty log and identical BoltDB balances. First C2 instance is seq 1 on all replicas; all 9 honest nodes prepare; quorum 7 clears.

---

## 11. Conclusion

The FU5 probe identified the real bug: **PBFT seq state carried across test sets** left primary S13 at seq 14 while backups had a hole at slot 13. Per-set `ResetConsensus` replaces five layers of cross-set repair machinery with one clean invariant.

**TS1: 6/6. TS2: 6/6.**

---

*See also: `FIX_UPDATE_1.md`, `FIX_UPDATE_2.md`, `FIX_UPDATE_4.md`, `TESTING_REPORT.md`, `Lab4_Testset_*_expected.json`.*
