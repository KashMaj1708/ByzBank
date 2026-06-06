# Phase 5 Report — Cross-Shard 2PC Prepare Phase

**Project:** CSE 535 Project 4 — Byzantine Fault-Tolerant Sharded 2PC (Go)  
**Plan reference:** `Project4_Go_Implementation_Plan.md`, Phase 5  
**Date:** 2026-06-05  
**Topology:** C1 + C2 (12 nodes each), f=3, quorums unchanged

---

## 1. Goal

Implement the **prepare half** of cross-shard 2PC:

1. Coordinator cluster locks x, runs PBFT, debits x, writes WAL, appends prepare datastore entry
2. Coordinator primary ships a signed prepare certificate to every participant replica
3. Participant cluster runs PBFT (commit or abort), credits y on commit, writes WAL, locks y
4. Participant primary returns a prepared/abort certificate to every coordinator replica
5. **No client reply yet** (Phase 6)

---

## 2. Implementation summary

### 2.1 PBFT operation tags (`internal/pbft`)

Extended `Request` with an `Op` field:

| Op | Role |
|----|------|
| `intra` | Intra-shard transfer (default) |
| `coord_prepare` | Coordinator cross-shard prepare consensus |
| `part_prepare_commit` | Participant agrees to prepare-commit |
| `part_prepare_abort` | Participant agrees to abort (e.g. y locked) |

`tryExecute` branches on `Op`:

- **coord_prepare** — `ApplyDebitOnly(x)`, WAL, prepare datastore entry, keep lock on x, callback to 2PC
- **part_prepare_commit** — `ApplyCreditOnly(y)`, WAL, prepare datastore, keep lock on y
- **part_prepare_abort** — prepare datastore with abort outcome only

`CrossShardHooks` interface wires coordinator/participant callbacks into the engine.

### 2.2 2PC package (`internal/twopc`)

| File | Purpose |
|------|---------|
| `messages.go` | `CoordinatorPrepareMsg`, `ParticipantReplyMsg` |
| `coordinator.go` | Client cross-shard intake, ship prepare cert to participant cluster |
| `participant.go` | Verify coordinator cert, run participant PBFT, return prepared/abort cert |
| `bridge.go` | Combined `CrossShardHooks` implementation per replica |
| `cert.go` | `VerifyClusterCert` — validate foreign-cluster PBFT certificates |
| `collector.go` | `PreparedCollector` for tests / Phase 6 |
| `sender.go` / `hub.go` | Cross-cluster envelope delivery |

### 2.3 Transport

New inter-cluster message types:

- `2PC_PREPARE` — coordinator → all participant replicas
- `2PC_PREPARED` — participant → all coordinator replicas
- `2PC_ABORT` — participant abort certificate → coordinators

### 2.4 Server wiring (`internal/server/replica.go`)

- `Enable2PC` flag — hooks 2PC only when true (keeps single-cluster PBFT tests unchanged)
- `PreparedCollector` — optional test sink for participant certificates
- `handle2PC` dispatch for inter-cluster messages

---

## 3. Tests

| Test | Scenario | Result |
|------|----------|--------|
| `TestCrossShardPreparePhase` | C1→C2 `(5,1500,4)` full prepare | PASS |
| `TestCrossShardInsufficientBalanceIgnored` | `amt=100` on bal=10 | PASS |
| `TestCrossShardParticipantAbort` | y pre-locked on C2 → abort cert | PASS |
| All Phase 3–4 PBFT tests | Regression | PASS |

**Balances** (initial 10 per item): after `(5,1500,4)` prepare → `bal[5]=6`, `bal[1500]=14`.

**Run:**

```powershell
cd ByzBank\ByzBank
go test ./internal/twopc -v -run TestCrossShardPreparePhase
go test ./... -count=1 -timeout 180s
```

---

## 4. Bugs found and fixed

1. **Fault config zero value** — twopc harness omitted `Fault: DefaultFaultConfig()`, so `Alive=false` and PBFT ignored all messages.
2. **`tryExecute` op switch** — empty `Op` (legacy intra requests) fell through to `default` after view-change; normalized `""` → `intra`.
3. **Single-cluster cross-shard** — gated behind `Enable2PC` so `TestCrossShardRequestIgnored` still passes on C1-only clusters.
4. **Participant timing** — test waits for both coordinator and participant prepare completion before assertions.

---

## 5. File map (Phase 5 additions)

```
internal/pbft/
  hooks.go          # CrossShardHooks
  messages.go       # Op constants, TxnID, PhaseSigningBytes export
  engine.go         # startConsensus, op-specific execute/lock paths

internal/twopc/
  bridge.go, coordinator.go, participant.go
  messages.go, cert.go, collector.go
  sender.go, hub.go
  prepare_test.go

internal/transport/types.go  # 2PC message types
internal/server/replica.go   # Enable2PC, handle2PC
```

---

## 6. Next phase

**Phase 6** — Commit phase: coordinator final PBFT round, WAL undo on abort, client reply, participant ack collection (`f+1` acks).
