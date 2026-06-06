# Phase 6 Report — Cross-Shard 2PC Commit Phase, Ack, and Undo

**Project:** CSE 535 Project 4 — Byzantine Fault-Tolerant Sharded 2PC (Go)  
**Plan reference:** `Project4_Go_Implementation_Plan.md`, Phase 6  
**Date:** 2026-06-05  
**Topology:** C1 + C2 (12 nodes each), f=3, client quorum f+1=4

---

## 1. Goal

Finish cross-shard 2PC:

1. Coordinator runs a **final PBFT round** after receiving the participant prepared/abort certificate
2. On commit: finalize datastore, release lock, delete WAL, reply `"committed"` to client
3. On abort: WAL-undo debit, finalize abort datastore, release lock, reply `"abort"`
4. Coordinator primary ships the outcome to every participant replica
5. Participant runs PBFT on commit (skip consensus on abort if already aborted at prepare)
6. Participant replicas ack back; coordinator retries commit delivery until **f+1=4** distinct acks

---

## 2. Implementation summary

### 2.1 PBFT operation tags (`internal/pbft`)

| Op | Role |
|----|------|
| `coord_commit` | Coordinator final commit consensus |
| `coord_abort` | Coordinator final abort consensus |
| `part_commit` | Participant final commit consensus |
| `part_abort` | Participant final abort consensus |

Commit-phase ops reuse locks acquired during prepare (no re-acquire). `executeCoordCommit` / `executeCoordAbort` send client replies; participant ops do not.

Extended `CrossShardHooks` with `OnCoordCommitExecuted` and `OnPartCommitExecuted`.

### 2.2 2PC package (`internal/twopc`)

| File | Purpose |
|------|---------|
| `messages.go` | `CoordinatorCommitMsg`, `ParticipantAckMsg` |
| `ack_collector.go` | Shared coordinator-side ack quorum tracking |
| `coordinator.go` | Start final commit/abort PBFT; broadcast outcome; ack retry timer |
| `participant.go` | Handle commit message; skip PBFT on prepare-abort optimization; send acks |
| `bridge.go` | Forward new hooks; route ack messages |

**Coordinator flow after participant reply:**

1. Primary verifies cert, records in `PreparedCollector`
2. Primary starts `coord_commit` or `coord_abort` PBFT (deduped per txn)
3. On execution: `OnCoordCommitExecuted` broadcasts `2PC_COMMIT` to participant cluster
4. On commit outcome: timer-driven re-broadcast until `AckCollector` reaches f+1

**Participant flow on `2PC_COMMIT`:**

1. Primary verifies coordinator commit certificate
2. If outcome is abort and prepare already aborted → send ack immediately (no PBFT)
3. Else run `part_commit` or `part_abort` PBFT
4. Every replica that executes sends `2PC_ACK` to the coordinator cluster

### 2.3 Transport (`internal/transport`)

| Type | Direction |
|------|-----------|
| `2PC_COMMIT` | Coordinator → all participant replicas |
| `2PC_ACK` | Participant → all coordinator replicas |

### 2.4 Server wiring (`internal/server/replica.go`)

- `AckCollector` shared across coordinator replicas (like `PreparedCollector`)
- `Disable2PCCommitPhase` test flag keeps Phase 5 prepare tests isolated
- `handle2PC` dispatches `Type2PCCommit` and `Type2PCAck`

---

## 3. Tests

| Test | Verifies |
|------|----------|
| `TestCrossShardCommitPhase` | End-to-end `(5,1500,4)` commit: bal[5]=6, bal[1500]=14; 2 datastore entries; locks released; WALs empty; client `"committed"` quorum; ≥4 acks |
| `TestCrossShardAbortUndo` | Participant pre-lock → abort path: WAL undo restores bal[5]=10; client `"abort"` quorum |
| Phase 5 prepare tests | Unchanged behavior via `Disable2PCCommitPhase` |

```powershell
go test ./internal/twopc -v -run TestCrossShardCommitPhase
go test ./internal/twopc -v -run TestCrossShardAbortUndo
go test ./... -count=1 -timeout 180s
```

---

## 4. Balance note

The plan shorthand “5→3 / 1500→1504” uses item IDs as balance labels. With `InitialBalance=10`, a transfer of 4 yields **bal[5]=6** and **bal[1500]=14** after prepare; commit finalizes without further balance change.

---

## 5. Next phase

**Phase 7** — Concurrency, locking correctness, and Figure 4 interleaving regression test.
