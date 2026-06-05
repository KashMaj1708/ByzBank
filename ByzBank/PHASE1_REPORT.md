# Phase 1 Report — Topology, Transport, and Signed Echo

**Project:** CSE 535 Project 4 — Byzantine Fault-Tolerant Sharded 2PC (Go)
**Plan reference:** `Project4_Go_Implementation_Plan.md`, Phase 1
**Date:** 2026-06-05
**Topology:** 3 clusters × 12 nodes = **36 servers**, f=3, quorum=7

---

## 1. Goal

36 server processes start, know the static topology, and exchange **signed**
messages over gRPC. No consensus yet.

---

## 2. What was built

### 2.1 `internal/config` (from Phase 0, unchanged)

Already provides the 36-server shard map, `ClusterOf`, `PrimaryOf`, and quorum
math. Phase 1 consumes it via a shared `*config.Topology` pointer so test
ephemeral ports propagate to peer dialers.

### 2.2 `internal/crypto`

| Component | Purpose |
|---|---|
| `GenerateKeyPair` | fresh ed25519 keypair |
| `Sign` / `Verify` | sign and verify message digests |
| `KeyRing` | per-replica private key + all peers' public keys |
| `GenerateAllKeys` / `LoadKeyRing` | write/read `config/keys/S<n>.json` |
| `Certificate` | collect `2f+1` matching signatures over one digest |

Key files are JSON with hex-encoded public/private keys. `config/keys/` is
gitignored.

### 2.3 `api/replica.proto` + `internal/pb`

Protobuf schema and generated gRPC stubs:

```protobuf
message Envelope { int32 sender_id; string type; bytes payload; bytes signature; }
service ReplicaTransport { rpc Send(Envelope) returns (SendAck); }
```

Generated with portable `protoc 28.3` + `protoc-gen-go` + `protoc-gen-go-grpc`.

### 2.4 `internal/transport`

| Component | Purpose |
|---|---|
| `Hub` | gRPC listener + lazy peer dialing with reconnect |
| `Sign` / `Verify` / `Deliver` | signature attach, verify, enqueue to replica |
| `Send` | deliver a signed envelope to a peer via gRPC |
| Message types | `PING`, `PONG` (Phase 1 echo) |

**Signing rule:** signature covers canonical bytes of `sender_id|type|payload`
(excludes the signature field). Invalid signatures are **dropped** at delivery
with an error ack — never handed to the dispatch loop.

### 2.5 `internal/server`

`Replica` wires the transport hub with an inbound dispatch loop:

- `PING` → sign and send `PONG` back to the sender
- `PONG` → delivered to `PongWait()` channel (for tests/metrics)
- Unknown types logged and ignored

`BroadcastCluster` sends a signed envelope to every other server in the
sender's cluster (used by the echo test).

### 2.6 `cmd/server`

- Loads or auto-generates keys from `config/keys`
- Starts gRPC on the topology port (S1→9001 … S36→9036)
- Runs the `Replica` dispatch loop
- Keeps HTTP `/health` on **port+10000** so the Phase 0 orchestration harness
  still works (`client --healthcheck` updated accordingly)

### 2.7 `cmd/keygen`

`go run ./cmd/keygen --out config/keys` generates all 36 ed25519 keypairs.
Wired into `make keys` and `scripts/run.ps1 keys`.

---

## 3. Verification

| Check | Command | Result |
|---|---|---|
| Crypto unit tests | `go test ./internal/crypto` | **PASS** (sign/verify, certificate quorum) |
| Signed echo | `go test ./internal/transport -run TestSignedEcho` | **PASS** |
| Full suite | `go test ./...` | **PASS** |
| Key generation | `scripts/run.ps1 keys` | **36 keypairs** in `config/keys/` |
| Build | `go build ./...` | **exit 0** |

### TestSignedEcho behaviour

1. Starts all **12 replicas in cluster C1** (S1–S12) on ephemeral ports with
   shared topology pointer.
2. S1 broadcasts a signed `PING` to S2..S12.
3. Asserts S1 receives **11 valid `PONG`** replies with verified signatures.
4. Sends a `PING` with a **corrupted signature** → rejected at `Deliver`.

---

## 4. How to run

```powershell
# Generate keys (once)
.\scripts\run.ps1 keys

# Build and start all 36 servers
.\scripts\run.ps1 build
.\scripts\run.ps1 up

# Health via HTTP sidecar (port+10000)
.\bin\client.exe --healthcheck

# Phase 1 demo test
go test ./internal/transport -run TestSignedEcho -v

# Tear down
.\scripts\run.ps1 down
```

---

## 5. Next phase

**Phase 2 — Storage layer.** Implement `internal/store` over BoltDB: balances
(every item starts at 10), append-only datastore, lock table, WAL, and
per-client last-executed timestamps. Demo: `go test ./internal/store`.
