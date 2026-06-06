# Phase 4 Report — PBFT View-Change and Byzantine Behavior

**Project:** CSE 535 Project 4 — Byzantine Fault-Tolerant Sharded 2PC (Go)  
**Plan reference:** `Project4_Go_Implementation_Plan.md`, Phase 4  
**Date:** 2026-06-05  
**Topology:** 1 cluster (C1) of 12 nodes, f=3, collector quorum n−f=9, commit quorum 2f+1=7, client quorum f+1=4

---

## 1. Goal

Make intra-shard PBFT robust to a faulty primary and Byzantine replicas (Lab 2 semantics):

1. **View-change** when the primary stalls (client timeout → broadcast → backup suspicion → VIEW-CHANGE → NEW-VIEW)
2. **Byzantine leader** — no pre-prepare progress, no NEW-VIEW (still logs VIEW-CHANGE)
3. **Byzantine backup** — never sends PREPARE
4. **Grading helpers** — `PrintStatus`, `PrintView`, `PrintLog`
5. **Tests** — view-change recovery, 3 Byzantine backups still commit, 4 faulty backups abort

---

## 2. Implementation summary

### 2.1 New message types

| Type | Payload | Purpose |
|------|---------|---------|
| `VIEW_CHANGE` | `ViewChangeMsg` | Backup suspicion; carries stable seq + prepared + pending client requests |
| `NEW_VIEW` | `NewViewMsg` | New primary re-installs in-flight work as pre-prepares |

Added to `internal/transport/types.go` and `internal/pbft/messages.go`.

### 2.2 Fault configuration (`internal/pbft/fault.go`)

```go
type FaultConfig struct {
    Alive           bool
    ByzantineLeader bool // no broadcast pre-prepare, no NEW-VIEW
    ByzantineBackup bool // no PREPARE
}
```

Wired through `server.ReplicaConfig.Fault` and `Replica.SetFault`.

### 2.3 View-change logic (`internal/pbft/viewchange.go`)

**Backup client path:** non-primary replicas accept `CLIENT_REQUEST`, store pending work, and start a suspicion timer (default 800 ms).

**VIEW-CHANGE trigger:** on timer expiry, replica broadcasts a signed `VIEW_CHANGE` for view `v+1` with:

- `LatestStable` = last executed sequence
- `Prepared` = pre-prepared but not executed entries
- `PendingReqs` = client requests awaiting a pre-prepare

**Amplification:** on `f+1` matching VIEW-CHANGE messages, lagging replicas send their own.

**NEW-VIEW:** when `2f+1` VIEW-CHANGE messages are collected, the designated new primary (`PrimaryOf(cluster, v+1)`) builds and issues `NEW_VIEW` with merged pre-prepares. Byzantine leaders skip NEW-VIEW issuance but still record incoming VIEW-CHANGE.

**Critical ordering fix:** the new primary applies `NEW_VIEW` locally *before* broadcasting so the collector has pre-prepare state before backups forward PREPARE messages.

### 2.4 Client retry (`internal/pbft/client.go`)

`SubmitWithRetry` implements Lab 2 client behavior:

1. Send to primary
2. Wait for `f+1` matching replies
3. On timeout, broadcast `CLIENT_REQUEST` to all replicas
4. Wait again for client quorum

### 2.5 Grading helpers (`internal/pbft/debug.go`)

| Function | Output |
|----------|--------|
| `PrintStatus(seq)` | Per-sequence status: `PP` / `P` / `C` / `E` / `X` |
| `PrintView()` | Accepted `NEW_VIEW` messages |
| `PrintLog()` | Received `VIEW_CHANGE` messages |

Also exposed: `Engine.View()`, `SetViewChangeTimeout()` (tests).

### 2.6 Hub sender view tracking

`HubSender.SetView(v)` updates the primary lookup after view-change so collector routing follows the new leader.

---

## 3. Tests

| Test | Scenario | Result |
|------|----------|--------|
| `TestViewChange` | S1 Byzantine leader; client retry triggers view-change; S2 drives commit | PASS |
| `TestByzantineBackup` | S3,S4,S5 Byzantine backup (f=3); honest quorum still commits | PASS |
| `TestNoConsensusFourFaulty` | S3–S6 Byzantine backup (4 faulty); collector needs 9 prepares, only 8 arrive | PASS |
| `TestPrintStatusAndView` | Grading helpers after happy-path commit | PASS |
| All Phase 3 intra-shard tests | Regression | PASS |

**Run:**

```powershell
cd ByzBank\ByzBank
go test ./internal/pbft -v -count=1 -timeout 180s -run TestViewChange
go test ./internal/pbft -v -count=1 -timeout 180s -run TestByzantineBackup
go test ./... -count=1 -timeout 180s
```

---

## 4. Bugs found and fixed

1. **Byzantine primary ignored client broadcast** — now routes through `onBackupClientRequest` so suspicion timers start and VIEW-CHANGE is logged.
2. **NEW-VIEW broadcast-before-apply race** — backups sent PREPARE before the new primary had pre-prepare state; fixed by applying NEW-VIEW locally before cluster broadcast.
3. **View-change timer not cancelled on NEW-VIEW pre-prepare** — added `cancelViewChangeTimerLocked` in `acceptNewViewPrePrepare`.

---

## 5. File map (Phase 4 additions)

```
internal/pbft/
  fault.go           # FaultConfig
  viewchange.go      # VIEW-CHANGE / NEW-VIEW handlers
  debug.go           # PrintStatus, PrintView, PrintLog
  viewchange_test.go # Phase 4 tests
  messages.go        # +ViewChangeMsg, NewViewMsg, PreparedEntry
  engine.go          # Byzantine gates, dispatch new types
  client.go          # SubmitWithRetry, SubmitToAll
  sender.go          # SetView
  cluster_test.go    # startClusterWithFaults, submitWithRetry

internal/transport/types.go  # TypeViewChange, TypeNewView
internal/server/replica.go   # Fault in ReplicaConfig
```

---

## 6. Next phase

**Phase 5** — Cross-shard 2PC prepare phase (`internal/twopc`): coordinator debits x, participant credits y, both via PBFT instances inside their clusters.
