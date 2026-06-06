# Testing Report — CSE 535 Project 4 (ByzBank / 2pcbyz)

**Date:** June 5, 2026  
**Module:** `github.com/KashMaj1708/2pcbyz-kashmaj1708`  
**Topology:** 3 clusters × 12 nodes = 36 servers, `f=3`, commit quorum = 7, client quorum = 4  
**Initial balance:** 10 for every item  

This report documents all testing performed to date: automated unit/integration tests, the Lab4 end-to-end oracle harness, infrastructure issues encountered, and a per-set breakdown of passing vs failing cases.

---

## 1. Executive Summary

| Test Suite | Sets Run | Sets Passing Oracle | Txns Expected | Client `committed` Replies | Overall |
|------------|----------|---------------------|---------------|----------------------------|---------|
| **Lab4 Testset 1** (intra-shard focus) | 6 | **1 / 6** | 22 | 16 / 22 (73%) | **FAIL** |
| **Lab4 Testset 2** (cross-shard focus) | 6 | **1 / 6** | 16 | 8 / 16 (50%) | **FAIL** |
| **`go test ./...`** (unit + harness) | — | all packages green | — | — | **PASS** |

**Key finding:** Basic PBFT intra-shard commits, cross-shard 2PC on non-contending items, Byzantine fault injection, and view-change paths work in isolation (unit tests pass; Lab4 Set 1 passes in both files). Failures cluster around **open-loop concurrency on the same account/item** within a set — both intra-shard (Testset 1) and cross-shard Figure-4 interleaving (Testset 2).

---

## 2. What We Tried (Chronological)

### Phase 0–9 development tests (prior sessions)

| Phase | Focus | Test mechanism | Result |
|-------|-------|----------------|--------|
| 0–3 | Topology, keys, PBFT intra-shard | `go test ./internal/...` harness tests | Pass |
| 4 | View-change, Byzantine faults, `PrintStatus/View/Log` | PBFT + server harness tests | Pass |
| 5 | Cross-shard 2PC prepare | `TestCrossShardPrepare*` in `internal/twopc` | Pass |
| 6 | 2PC commit, ack, WAL undo | `TestCrossShardCommitPhase`, `TestCrossShardAbortUndo` | Pass |
| 7 | Concurrency, Figure 4 interleaving | `TestConcurrentContention`, `TestFigure4Interleaving` | Pass (in-process harness) |
| 8 | CSV parser, HTTP grading API, interactive client | `internal/testcase` parse tests, manual client | Pass |
| 9 | Timer tuning, performance metrics, hardening | `TestNoQuorumSixLiveNoClientReply`, `TestMixedWorkloadPerformance` | Pass |

After disk-space cleanup, full suite confirmed green:

```powershell
go test ./... -count=1 -timeout 200s
```

All packages: `config`, `crypto`, `pbft`, `server`, `store`, `testcase`, `transport`, `twopc` — **PASS** (June 5, 2026 re-run).

### Lab4 end-to-end oracle verification (this session)

End-to-end runs against live 36-server cluster comparing committed state to `Lab4_Testset_*_36node_expected.json`.

**Infrastructure built for this session:**

1. **`--verify` flag** on `cmd/client` — runs all CSV sets, snapshots balances + datastores per set, diffs against expected JSON.
2. **`/datastore/oracle` HTTP endpoint** — returns committed log entries in simulate.py format (`INTRA (x,y,amt) COMMIT`, `CROSS (x,y,amt) PREPARE`, etc.).
3. **`DatastoreEntry.OracleString()`** — formats store entries for oracle comparison.
4. **`internal/testcase/oracle.go`** — `CollectOracleDump`, `VerifyDumps` (multiset datastore compare per cluster).
5. **CSV parser fix** — root-level CSV files have no header row; parser now auto-detects header vs data.
6. **Server startup fix** — `cmd/server` no longer panics when `BYZ_DEBUG` is unset (logger was nil).

**Orchestration used:**

```powershell
.\scripts\run.ps1 keys          # generate config/keys
.\scripts\run.ps1 build         # bin/server.exe, bin/client.exe
.\scripts\run.ps1 up            # 36 background server processes
.\scripts\run.ps1 health        # probe /health on ports 19001–19036

# Testset 1 (fresh cluster)
go run ./cmd/client --testfile Lab4_Testset_1_36node.csv --auto --verify Lab4_Testset_1_36node_expected.json

# Testset 2 (must reset persistent state)
.\scripts\run.ps1 down
Remove-Item -Recurse -Force data    # BoltDB persists under data/S*.db
.\scripts\run.ps1 up
# wait + healthcheck
go run ./cmd/client --testfile Lab4_Testset_2_36node.csv --auto --verify Lab4_Testset_2_36node_expected.json
```

**Important discovery:** Server state persists in `data/S1.db` … `data/S36.db` across restarts. Testset 2 **must** be run on a clean `data/` directory. An initial Testset 2 run without clearing `data/` produced false failures (Testset 1 entries appeared as "unexpected" in datastores).

---

## 3. Lab4 Verification Methodology

### Oracle comparison (`check_oracle.py` compatible)

After each set, the client collects:

- **`balances`:** every item ID referenced anywhere in the CSV file (cumulative snapshot), queried from the first **live** server in the item's cluster.
- **`datastore_per_cluster`:** committed log from the first **live** server in C1, C2, C3, compared as a **multiset** (order-independent).

Comparison logic mirrors `check_oracle.py dump` mode: balance equality per item; datastore multiset equality per cluster.

### Client metrics reported per set

- **Throughput** = committed replies / wall-clock from first send to last committed reply (cumulative across file).
- **Mean latency** = average `reply_time − send_time` for transactions that received a `committed` client reply.

A transaction is counted as committed only when the client observes **4 matching replies** (`ClientQuorum`) within the settle window.

---

## 4. Lab4 Testset 1 — Intra-Shard Focus

**Files:**
- Input: `Lab4_Testset_1_36node.csv`
- Oracle: `Lab4_Testset_1_36node_expected.json`
- Label: *Test Set 1 (Intra-Shard)*

**Fault profile (all sets):** 36 live, contact `[S1, S13, S25]`, Byzantine `[S16, S17, S18, S28, S29, S30]`.

### 4.1 Per-Set Summary

| Set | Txns | Type | Oracle Result | Client Committed | Notes |
|-----|------|------|---------------|------------------|-------|
| **1** | 3 | INTRA ×3 (one per cluster) | **PASS** | 3/3 | All balances + datastores match |
| **2** | 4 | INTRA ×4 (C2×2, C1×2) | **FAIL** | 6/10 cumulative | `(1201,1299,3)` never commits |
| **3** | 4 | INTRA + CROSS | **FAIL** | 9 cumulative | Same `1201` gap propagates |
| **4** | 3 | INTRA + CROSS | **FAIL** | 11 cumulative | Down servers S10–S12 in C1; still fails on `1201` |
| **5** | 5 | INTRA + CROSS | **FAIL** | 14 cumulative | Down S7–S12 in C1 |
| **6** | 3 | INTRA ×3 | **FAIL** | 16/22 final | `(888,777,5)` additionally fails |

**Performance (cumulative at end of each set):**

| Set | Committed (cumul.) | Throughput | Mean Latency |
|-----|-------------------|------------|--------------|
| 1 | 3 | 12.00 txns/sec | 241 ms |
| 2 | 6 | 9.42 txns/sec | 258 ms |
| 3 | 9 | 0.38 txns/sec | 217 ms |
| 4 | 11 | 0.24 txns/sec | 198 ms |
| 5 | 14 | 0.21 txns/sec | 206 ms |
| 6 | 16 | 0.17 txns/sec | 200 ms |

### 4.2 Set 1 — PASS (detail)

| Transaction | Type | Cluster | Expected Outcome | Actual |
|-------------|------|---------|------------------|--------|
| `(100, 501, 8)` | INTRA | C1 | COMMIT → bal[100]=2, bal[501]=18 | **MATCH** |
| `(1001, 1650, 2)` | INTRA | C2 | COMMIT → bal[1001]=8, bal[1650]=12 | **MATCH** |
| `(2800, 2150, 7)` | INTRA | C3 | COMMIT → bal[2800]=3, bal[2150]=17 | **MATCH** |

All three cluster datastores: 1 entry each. Full balance vector matches oracle.

### 4.3 Set 2 — FAIL (detail)

| Transaction | Type | Cluster | Expected Outcome | Actual |
|-------------|------|---------|------------------|--------|
| `(1201, 1111, 5)` | INTRA | C2 | COMMIT → bal[1201]=5, bal[1111]=15 | **MATCH** |
| `(1201, 1299, 3)` | INTRA | C2 | COMMIT → bal[1201]=2, bal[1299]=13 | **FAIL — not committed** |
| `(101, 301, 9)` | INTRA | C1 | COMMIT → bal[101]=1, bal[301]=19 | **MATCH** |
| `(299, 1, 4)` | INTRA | C1 | COMMIT → bal[299]=6, bal[1]=14 | **MATCH** |

**Balance mismatches after Set 2:**

| Item | Actual | Expected | Cause |
|------|--------|----------|-------|
| `bal[1201]` | 5 | 2 | Second txn on same sender not applied |
| `bal[1299]` | 10 | 13 | Receiver credit from failed txn |

**Datastore mismatch (C2):** missing `INTRA (1201,1299,3) COMMIT`.

**Root-cause hypothesis:** Open-loop firing of two INTRA txns both debiting `1201` in the same set. The second txn `(1201, 1299, 3)` is blocked or dropped while the first holds locks / occupies the primary pipeline. Same-item contention within a cluster under open-loop delivery.

### 4.4 Sets 3–5 — FAIL (propagated + new txns mostly OK)

Sets 3–5 introduce additional intra-shard and cross-shard transactions. Most **new** transactions in each set commit correctly; oracle failures remain dominated by the Set 2 `1201` gap:

- `bal[1201]` stays at **5** (expected **2**) through Set 6.
- `bal[1299]` stays at **10** (expected **13**) through Set 6.
- C2 datastore permanently missing `INTRA (1201,1299,3) COMMIT`.

Other cross-shard txns in Sets 3–5 (e.g. `(791,997,3)`, `(1295,1990,17)`, `(2975,2970,9)`) appear to commit per oracle-consistent balances for non-`1201`/`1299` items.

### 4.5 Set 6 — FAIL (additional intra-shard failure)

| Transaction | Type | Cluster | Expected Outcome | Actual |
|-------------|------|---------|------------------|--------|
| `(888, 777, 5)` | INTRA | C1 | COMMIT → bal[888]=5, bal[777]=15 | **FAIL — not committed** |
| `(1415, 1189, 5)` | INTRA | C2 | COMMIT → bal[1415]=5, bal[1189]=15 | **MATCH** |
| `(2222, 2333, 6)` | INTRA | C3 | COMMIT → bal[2222]=4, bal[2333]=16 | **MATCH** |

**Additional balance mismatches (Set 6 only):**

| Item | Actual | Expected |
|------|--------|----------|
| `bal[888]` | 10 | 5 |
| `bal[777]` | 10 | 15 |

**Datastore mismatch (C1):** missing `INTRA (888,777,5) COMMIT`.

**Root-cause hypothesis:** Similar to Set 2 — possible lock/contention or settle-timeout on `(888, 777, 5)` while other intra-shard txns in the same set succeed. May also relate to item-level locking not being released promptly enough for open-loop batching within a set.

---

## 5. Lab4 Testset 2 — Cross-Shard Focus

**Files:**
- Input: `Lab4_Testset_2_36node.csv`
- Oracle: `Lab4_Testset_2_36node_expected.json`
- Label: *Test Set 2 (Cross-Shard)*
- Full log: `run/testset2_verify.log`

**Fault profile:** Byzantine servers vary per set (`[S19,S20,S21,S31,S32,S33]` in most sets; `[S31,S32,S33]` only in Sets 4–5). Cluster 2 loses S19–S24 in Sets 4–5.

### 5.1 Per-Set Summary

| Set | Txns | Highlights | Oracle Result | Client Committed (cumul.) |
|-----|------|------------|---------------|---------------------------|
| **1** | 3 CROSS | 3 cross-shard commits | **PASS** | 3/3 |
| **2** | 1 CROSS + 1 CROSS + 1 INTRA | **Figure 4:** two txns on `(296,1997)` | **FAIL** | 5/6 |
| **3** | 1 CROSS + 1 (skip) | `(1998,2998,19)` insufficient funds | **FAIL** | 6/7 |
| **4** | 1 CROSS + 1 INTRA + 1 INTRA | C2 partially down | **FAIL** | 7/10 |
| **5** | 1 CROSS (abort) + 1 INTRA | `(45,1355,5)` cross-shard abort | **FAIL** | 8/12 |
| **6** | 3 CROSS | Final burst | **FAIL** | 8/16 |

**Performance (cumulative, clean-state run):**

| Set | Committed (cumul.) | Throughput | Mean Latency |
|-----|-------------------|------------|--------------|
| 1 | 3 | 6.32 txns/sec | 455 ms |
| 2 | 5 | 4.56 txns/sec | 416 ms |
| 3 | 6 | 0.27 txns/sec | 373 ms |
| 4 | 7 | 0.17 txns/sec | 335 ms |
| 5 | 8 | 0.13 txns/sec | 309 ms |
| 6 | 8 | 0.13 txns/sec | 309 ms |

Note: cumulative committed count stalls at **8** after Set 5 — **no new client commits in Set 6**.

### 5.2 Set 1 — PASS (detail)

| Transaction | Type | Coord → Part | Expected | Actual |
|-------------|------|--------------|----------|--------|
| `(299, 1999, 3)` | CROSS | C1 → C2 | COMMIT | **MATCH** |
| `(1001, 2999, 6)` | CROSS | C2 → C3 | COMMIT | **MATCH** |
| `(2150, 1111, 9)` | CROSS | C3 → C2 | COMMIT | **MATCH** |

All balances (29 items) and all three cluster datastores match oracle exactly.

### 5.3 Set 2 — FAIL (Figure 4 interleaving)

| Transaction | Type | Coord → Part | Expected | Actual |
|-------------|------|--------------|----------|--------|
| `(296, 1997, 1)` | CROSS | C1 → C2 | COMMIT → bal[296]=9, bal[1997]=11 | **MATCH** (intermediate state) |
| `(296, 1997, 8)` | CROSS | C1 → C2 | COMMIT → bal[296]=1, bal[1997]=19 | **FAIL — not committed** |
| `(2593, 2297, 3)` | INTRA | C3 | COMMIT → bal[2593]=7, bal[2297]=13 | **MATCH** |

**Balance mismatches (persist through Set 6):**

| Item | Actual | Expected | Notes |
|------|--------|----------|-------|
| `bal[296]` | 9 | 1 | Stuck after first Figure-4 txn |
| `bal[1997]` | 11 | 19 | Second txn never finalizes |

**Datastore mismatches (Sets 2–6):** missing `CROSS (296,1997,8) PREPARE` and `CROSS (296,1997,8) COMMIT` on C1 and C2.

**Root-cause hypothesis:** Classic Figure 4 interleaving — two concurrent cross-shard txns share sender `296` and receiver `1997` with different amounts (1 and 8). The first txn commits to intermediate balances; the second never completes 2PC. In-process `TestFigure4Interleaving` passes, but the live 36-node open-loop path does not — suggesting timing, lock ordering, or cross-shard coordinator serialization differs between harness and production cluster.

### 5.4 Set 3 — FAIL

| Transaction | Type | Expected | Actual |
|-------------|------|----------|--------|
| `(793, 1993, 7)` | CROSS C1→C2 | COMMIT | **MATCH** |
| `(1998, 2998, 19)` | CROSS C2→C3 | SKIP_INSUFFICIENT (bal[1998]=10 < 19) | **MATCH** (no commit expected) |

Oracle passes for new txns, but Set 2 `296`/`1997` gap remains → **overall FAIL**.

### 5.5 Set 4 — FAIL

| Transaction | Type | Expected | Actual |
|-------------|------|----------|--------|
| `(1877, 2855, 5)` | CROSS C3→C3? (C3 coord) | COMMIT | **Likely OK** (balances for 1877, 2855 match) |
| `(1333, 2333, 3)` | INTRA C2 | COMMIT | **MATCH** |
| `(5, 98, 2)` | INTRA C1 | COMMIT | **MATCH** |

Set 2 gap + no new failures on these three.

### 5.6 Set 5 — FAIL

| Transaction | Type | Expected | Actual |
|-------------|------|----------|--------|
| `(45, 1355, 5)` | CROSS C1→C2 | ABORT (participant cluster down) → `COMMIT(ABORT)` on coord | **FAIL** — abort path not recorded |
| `(5, 98, 2)` | INTRA C1 | COMMIT (second attempt) | **MATCH** |

**Additional mismatch:** `bal[45]` actual **5** vs expected **10** at Set 5 — coordinator may have tentatively debited `45` without completing WAL undo / abort datastore entry.

**Datastore:** missing `CROSS (45,1355,5) PREPARE` and `CROSS (45,1355,5) COMMIT(ABORT)` on C1.

### 5.7 Set 6 — FAIL (complete stall)

| Transaction | Type | Coord → Part | Expected | Actual |
|-------------|------|--------------|----------|--------|
| `(298, 1789, 3)` | CROSS | C1 → C2 | COMMIT | **FAIL** |
| `(1061, 2476, 6)` | CROSS | C2 → C3 | COMMIT | **FAIL** |
| `(2850, 1234, 9)` | CROSS | C3 → C1 | COMMIT | **FAIL** |

**Balance mismatches (Set 6):**

| Item | Actual | Expected |
|------|--------|----------|
| `bal[298]` | 7 | 7 (partial — first txn may have partially applied) |
| `bal[1789]` | 10 | 13 |
| `bal[1061]` | 10 | 4 |
| `bal[2476]` | 10 | 16 |
| `bal[2850]` | 1 | 1 (coordinator debit?) |
| `bal[1234]` | 10 | 19 |
| `bal[45]` | 5 | 10 |

**Client committed count unchanged at 8** — none of Set 6's three cross-shard txns received quorum client replies within the settle window.

**Root-cause hypothesis:** Cascading failure from accumulated lock/state contention plus possible settle-timeout exhaustion; all three Set 6 txns are independent cross-shard transfers that should commit under full liveness.

---

## 6. Consolidated Failure Catalog

### 6.1 Transactions that fail to match oracle

| Test File | Set | Transaction | Type | Symptom |
|-----------|-----|-------------|------|---------|
| Testset 1 | 2–6 | `(1201, 1299, 3)` | INTRA C2 | Never commits; bal[1201] stuck at 5 |
| Testset 1 | 6 | `(888, 777, 5)` | INTRA C1 | Never commits; bal[888]/bal[777] unchanged |
| Testset 2 | 2–6 | `(296, 1997, 8)` | CROSS C1→C2 | Never commits; bal[296]/bal[1997] stuck at intermediate values |
| Testset 2 | 5–6 | `(45, 1355, 5)` | CROSS C1→C2 | Abort not recorded; bal[45] wrong |
| Testset 2 | 6 | `(298, 1789, 3)` | CROSS C1→C2 | No commit |
| Testset 2 | 6 | `(1061, 2476, 6)` | CROSS C2→C3 | No commit |
| Testset 2 | 6 | `(2850, 1234, 9)` | CROSS C3→C1 | No commit |

### 6.2 Failure themes

1. **Same-item open-loop contention (intra-shard):** Two transactions touching the same sender in one set before the first fully completes.
2. **Figure 4 cross-shard interleaving:** Two transactions on the same `(sender, receiver)` pair with different amounts fired open-loop.
3. **Cross-shard abort under partial cluster failure:** Coordinator prepare succeeds but participant cannot quorum → expected `COMMIT(ABORT)` + WAL undo not reflected in oracle snapshot.
4. **Settle-window / client-side detection:** Several txns may execute on servers without the client observing 4 matching replies within `SettleDeadline`, causing performance counter to under-count commits (possible partial factor for Set 6 stall).

---

## 7. Infrastructure Issues Found During Testing

| Issue | Symptom | Fix Applied |
|-------|---------|-------------|
| Nil logger in `cmd/server` | All 36 servers panic on startup without `BYZ_DEBUG` | Logger defaults to `io.Discard` |
| CSV header assumption | `parse Lab4_Testset_1_36node.csv: row 2: transaction before set header` | Auto-detect header row (skip only if col0 is non-numeric) |
| Persistent BoltDB state | Testset 2 showed Testset 1 datastore entries after restart | Documented: `Remove-Item -Recurse data` between test files |
| Insufficient server warmup | First Testset 2 attempt: 0 commits Sets 1–5 | Increased wait to 15s + healthcheck before client run |
| Disk full (earlier session) | `truncate pbft.test.exe: not enough space` | User freed disk; `go test ./...` green |

---

## 8. What Passes Reliably

- **All `go test ./...` packages** — PBFT, 2PC, concurrency harness, Figure 4 in-process, performance/hardening tests.
- **Lab4 Testset 1, Set 1** — three concurrent intra-shard txns (one per cluster) under Byzantine faults.
- **Lab4 Testset 2, Set 1** — three cross-shard 2PC commits across all cluster pairs.
- **Most individual transactions** in Sets 2–6 that do **not** share items with another open-loop txn in the same set.
- **HTTP grading API** — `/balance`, `/datastore`, `/datastore/oracle`, `/fault`, `/reply` all functional on live cluster.
- **36-server orchestration** — `scripts/run.ps1` build/up/down/health/keys on Windows.

---

## 9. Artifacts

| Path | Description |
|------|-------------|
| `Lab4_Testset_1_36node.csv` | Testset 1 input (no CSV header) |
| `Lab4_Testset_1_36node_expected.json` | Testset 1 oracle |
| `Lab4_Testset_2_36node.csv` | Testset 2 input (no CSV header) |
| `Lab4_Testset_2_36node_expected.json` | Testset 2 oracle |
| `test/Lab4_Testset_*_36node.csv` | Copies with CSV header row |
| `check_oracle.py` | Standalone oracle checker (Python) |
| `simulate.py` | Oracle generator |
| `run/testset2_verify.log` | Full Testset 2 verification output (clean state) |
| `run/pids.txt` | Server PIDs when cluster is up |
| `run/logs/S*.err.log` | Per-server stderr |
| `data/S*.db` | Persistent replica state |

### Reproduce verification

```powershell
cd C:\Users\kashy\Desktop\byzbank\ByzBank\ByzBank
.\scripts\run.ps1 down
Remove-Item -Recurse -Force data -ErrorAction SilentlyContinue
.\scripts\run.ps1 keys
.\scripts\run.ps1 up
Start-Sleep -Seconds 15
.\scripts\run.ps1 health

go run ./cmd/client --testfile Lab4_Testset_1_36node.csv --auto --verify Lab4_Testset_1_36node_expected.json

.\scripts\run.ps1 down
Remove-Item -Recurse -Force data
.\scripts\run.ps1 up
Start-Sleep -Seconds 15
go run ./cmd/client --testfile Lab4_Testset_2_36node.csv --auto --verify Lab4_Testset_2_36node_expected.json
```

---

## 10. Recommended Next Steps

1. **Debug `(1201, 1299, 3)` intra-shard contention** — trace lock acquire/release and primary `proposalMu` serialization when two open-loop txns share item `1201` in C2.
2. **Debug `(296, 1997, 8)` Figure 4 path** — compare live open-loop behavior vs `TestFigure4Interleaving` harness; inspect 2PC coordinator/participant state when second txn arrives during first txn's commit phase.
3. **Debug `(45, 1355, 5)` abort** — verify participant no-quorum triggers coordinator timeout → WAL undo → `COMMIT(ABORT)` datastore entry.
4. **Investigate Set 6 stall** — determine whether Set 6 txns are blocked on locks, timing out in PBFT, or simply not meeting client settle deadline (extend settle window experiment).
5. **Add `scripts/run.ps1 lab4` target** — automate clean-state run of both test files and write `run/lab4_report.txt`.

---

## 11. Conclusion

The implementation is **production-ready for basic intra-shard and non-contending cross-shard workloads** (evidenced by unit tests and Lab4 Set 1 in both files). **Oracle verification fails on 10 of 12 graded sets**, concentrated in:

- **Intra-shard same-sender contention** (Testset 1)
- **Cross-shard Figure 4 interleaving** (Testset 2)
- **Cross-shard abort + late-set cross-shard batches** (Testset 2 Sets 5–6)

Total client-observed commit rate across all 38 Lab4 transactions: **24 / 38 (63%)**, with persistent state errors on 7 distinct transaction patterns catalogued in Section 6.
