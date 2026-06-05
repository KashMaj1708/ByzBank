# Phase 3 Report — Linear PBFT Engine (Intra-shard, Happy Path)

**Project:** CSE 535 Project 4 — Byzantine Fault-Tolerant Sharded 2PC (Go)  
**Plan reference:** `Project4_Go_Implementation_Plan.md`, Phase 3  
**Date:** 2026-06-05  
**Topology:** 1 cluster (C1) of 12 nodes, f=3, collector quorum n−f=9, commit quorum 2f+1=7, client quorum f+1=4

---

## 1. Goal

Implement the **linear PBFT** intra-shard happy path with locking:

1. Client request → primary
2. Primary **pre-prepare** (lock + balance check) → broadcast to cluster
3. Backups send **prepare** to collector (primary)
4. Collector broadcasts **prepare certificate**
5. Replicas send **commit** to collector
6. Collector broadcasts **commit certificate**
7. Replicas **execute in sequence order**, update balances, append datastore, release locks, and reply to client

No view-change or Byzantine behavior in Phase 3.

---

## 2. Implementation summary

### 2.1 Protocol messages (`internal/pbft`)

Implemented JSON payload messages and a request digest:

- `Request` (client transfer)
- `PrePrepareMsg`
- `PrepareMsg`
- `CommitMsg`
- `CertificateMsg` (list of `SigEntry`)
- `Reply`

Digest: SHA-256 of stable string containing `{clientID, ts, x, y, amt}`.

### 2.2 Engine (`internal/pbft/engine.go`)

The `pbft.Engine` runs per-replica and tracks per-sequence state:

- `prePrepare` accepted state
- collected prepares/commits (maps keyed by `ServerID`)
- prepare/commit certificates
- strict in-order execution cursor `execSeq`

Key behaviors:

- **Primary** assigns monotonically increasing `seq`, locks `(x,y)` locally, broadcasts `PRE_PREPARE`, then processes its own pre-prepare.
- **Backups** perform a **gap check** (must have all earlier pre-prepares) and lock locally before sending `PREPARE` to the primary.
- **Collector** (primary) gathers:
  - **n−f = 9** PREPARE signatures, then broadcasts `PREPARE_CERT`
  - **2f+1 = 7** COMMIT signatures, then broadcasts `COMMIT_CERT`
- **Execution** happens only after a `COMMIT_CERT` and only in strict increasing `seq` order.
- After execution: `ApplyTransfer`, `AppendDatastore`, `ReleaseLock(x)`, `ReleaseLock(y)`, `SetClientTS`.

### 2.3 Transport integration (`internal/transport` + `internal/server`)

- Added PBFT message type constants in `internal/transport/types.go`.
- `server.Replica.dispatch()` routes PBFT envelopes to the PBFT engine.
- PREPARE/COMMIT signatures are **phase-specific** (signed over `"PHASE|seq|view|digest"`), so the transport layer bypasses envelope-signature verification for those message types; the PBFT engine verifies them instead.

### 2.4 Storage integration (`internal/store`)

PBFT uses Phase 2 storage for:

- balances (`ApplyTransfer`)
- locks (`AcquireLock`, plus `AcquireLockForSeq` helper)
- append-only datastore logging
- per-client last-executed timestamp

---

## 3. Tests (thorough)

All PBFT tests live under `internal/pbft`:

- `TestIntraShardHappy` (required demo): `(5,7,3)` commits on all 12, locks released, client quorum reached
- `TestInsufficientBalanceIgnored`: insufficient balance is **silently ignored** (no commit, no state change)
- `TestAllReplicasExecuteIdenticalState`: all replicas converge on identical balances + datastore
- `TestClientQuorumFourMatchingReplies`: validates **f+1 = 4** matching replies (at least)
- `TestLocksReleasedAfterCommit`
- `TestSequentialTransactions`: two sequential commits produce 2 datastore entries
- `TestCrossShardRequestIgnored`: cross-shard routed request is ignored in Phase 3
- `TestReplayAttackIgnored`: same `(clientID, ts)` does not execute twice
- `TestInitialBalancesUntouchedOnIgnoredTxn`
- `TestDigestStability`
- `TestTopologyQuorumsUsed`

---

## 4. Verification

```
go test ./internal/pbft -v
```

Result: **PASS**, and the full suite also remains green:

```
go test ./... -count=1
```

---

## 5. Next phase

**Phase 4 — view-change and Byzantine behavior**, matching Lab 2 behavior:

- View-change timer + resend logic
- VIEW-CHANGE / NEW-VIEW handling
- Byzantine leader/backup behavior toggles driven by testcases
- Required debug query functions: `PrintStatus`, `PrintView`, `PrintLog`

