# Phase 0 Report — Environment, Toolchain, and Project Scaffolding

**Project:** CSE 535 Project 4 — Byzantine Fault-Tolerant Sharded 2PC (Go)
**Plan reference:** `Project4_Go_Implementation_Plan.md`, Phase 0
**Date:** 2026-06-05
**Machine:** Windows 10.0.26200, PowerShell
**Repository content root:** `C:\Users\kashy\Desktop\byzbank\ByzBank\ByzBank`
**Go module:** `github.com/KashMaj1708/2pcbyz-kashmaj1708`

This report documents every action taken to complete Phase 0: a reproducible Go
workspace where all 36 server processes and the client compile, run, and can be
brought up/down with one command. No protocol logic yet — pure plumbing.

---

## 1. Pre-flight inspection

Before touching anything, I inspected the workspace and toolchain.

| Check | Result |
|---|---|
| Git | installed (`git version 2.49.0.windows.1`) |
| Go | **not installed** (`go` not on PATH) |
| protoc | not installed (deferred to a later phase; protobuf used via Go modules) |
| Python | 3.10 present (`...\Python310\python.exe`) — used only by the oracle scripts |
| winget | available (`v1.28.240`) |

**Repository state.** The git repo root is `...\byzbank\ByzBank` (contains `.git`
and `.github/workflows/main.yml`, which I left untouched per the spec). The
actual content lives in the nested `...\byzbank\ByzBank\ByzBank`, alongside the
plan, the PDFs, the expected-state oracles
(`Lab4_Testset_*_36node_expected.{txt,json}`), `simulate.py`, and
`check_oracle.py`. A prior Python attempt had been deleted from the working
tree; only planning/grading materials remained. I scaffolded the Go project in
the content folder.

---

## 2. Installing the Go toolchain

The plan requires Go 1.22+. Installation steps and outcomes:

1. **Attempt 1 — winget (`winget install --id GoLang.Go`).** It downloaded and
   verified the Go 1.26.4 MSI, then stalled at "Starting package install..." for
   ~8 minutes. The MSI installs to `C:\Program Files\Go` and requires elevation;
   the silent install appeared to be blocked on a UAC prompt that cannot be
   answered in this automated context. I aborted it.
2. **Attempt 2 — portable zip (no admin required, chosen approach).**
   - Queried the official download API (`https://go.dev/dl/?mode=json`) for the
     latest stable: **go1.26.4**, file `go1.26.4.windows-amd64.zip`.
   - Downloaded it and **verified the SHA256** against the API value
     (`3ca8fb4630b07c419cbdd51f754e31363cfcfb83b3a5354d9e895c90be2cc345`) → **HASH OK**.
   - Extracted to `C:\Users\kashy\sdk\go`. (The first extraction via
     `Expand-Archive`/`tar` was interrupted and left the standard-library source
     incomplete — `go build` then failed with "package encoding/hex is not in
     std". I detected this, removed the directory, and **re-extracted cleanly**
     with `tar`. `go build std` then completed with exit 0, confirming a complete
     toolchain.)

3. **PATH configuration.**
   - `C:\Users\kashy\sdk\go\bin` and `C:\Users\kashy\go\bin` (GOBIN) added to the
     **persistent User PATH** (so new shells see `go`).
   - Same prepended to the session PATH for the current work.

**Verification:**

```
go version go1.26.4 windows/amd64
GOROOT = C:\Users\kashy\sdk\go
GOPATH = C:\Users\kashy\go
GOOS/GOARCH = windows/amd64
```

---

## 3. Module and project scaffolding

Initialised the module and created the full package tree from the plan's layout,
each package with a real or stub Go file so `go build ./...` is green.

```
2pcbyz-kashmaj1708/
├── go.mod / go.sum
├── Makefile                      # Unix/macOS orchestration
├── .gitignore
├── PHASE0_REPORT.md              # this file
├── cmd/
│   ├── server/main.go            # boots one server (health listener + graceful shutdown)
│   └── client/main.go            # client driver stub + --healthcheck mode
├── internal/
│   ├── config/                   # topology, shard map, quorum math (IMPLEMENTED)
│   │   ├── topology.go
│   │   ├── config.go
│   │   └── topology_test.go
│   ├── crypto/   doc.go          # ed25519 sign/verify, certs   (Phase 1)
│   ├── transport/doc.go          # gRPC signed envelopes        (Phase 1)
│   ├── pb/       doc.go          # protobuf/gob wire types      (Phase 1)
│   ├── store/    doc.go          # BoltDB balances/datastore/locks/WAL (Phase 2)
│   ├── pbft/     doc.go          # linear PBFT engine           (Phases 3-4)
│   ├── twopc/    doc.go          # 2PC coordinator/participant  (Phases 5-6)
│   ├── server/   doc.go          # Replica wiring               (Phase 1+)
│   ├── testcase/ doc.go          # CSV runner                   (Phase 8)
│   └── deps/     deps.go         # pins core third-party deps
├── scripts/
│   └── run.ps1                   # Windows orchestration (build/up/down/keys/test/health/status)
└── test/.gitkeep                 # sample CSVs + integration harness (later phases)
```

### 3.1 Config-driven topology (`internal/config`)

This is the only package with real logic in Phase 0, because everything
downstream reads its topology so there are no magic numbers anywhere.

- **Parameters, not constants.** `config.New(numClusters, clusterSize, totalItems,
  basePort, host, initialBalance)` builds the topology; `config.Default()` is the
  3×12 benchmark mode (36 servers, 3000 items, initial balance 10). `config.Load()`
  reads optional env overrides (`BYZ_NUM_CLUSTERS`, `BYZ_CLUSTER_SIZE`,
  `BYZ_TOTAL_ITEMS`, `BYZ_BASE_PORT`, `BYZ_HOST`), so flipping to the graded 3×4
  mode needs **no code changes** (satisfies the configurable-clusters requirement).
- **Derived quorum math (all from `f`, never `n`):** `F() = (clusterSize-1)/3`,
  `Quorum() = 2f+1`, `CollectorQuorum() = n-f`, `ClientQuorum() = f+1`.
  For the default: f=3, quorum=7, collector=9, client=4.
- **Helpers:** `ClusterOf(item)`, `SameCluster(x,y)` (intra- vs cross-shard),
  `PrimaryOf(cluster, view)` = `view mod clusterSize` mapped onto the cluster's
  server block (kept as `mod clusterSize`, never a literal), `ServersInCluster`,
  `ServerByID`, and `ParseServerID("S5")`.

Server numbering: S1..S36 in contiguous blocks of 12; ports = `basePort + id`
(S1→9001 … S36→9036). Shard map: C1 owns items 1–1000, C2 1001–2000, C3 2001–3000.

### 3.2 Server and client entrypoints

- **`cmd/server`** parses `--id S<n>`, looks itself up in the topology, binds its
  TCP port, serves a trivial `GET /health` endpoint, logs
  `S<n> listening on host:port (cluster …, f=…, quorum=…)`, and traps
  SIGINT/SIGTERM for a clean `http.Server.Shutdown`. (Phase 1 swaps the health
  listener for the gRPC signed-envelope transport behind the same lifecycle.)
- **`cmd/client`** ships a `--healthcheck` mode that probes every server's
  `/health` and reports up/down counts (exit 1 if any down), plus a stub banner.

### 3.3 Dependencies pinned now (so versions don't drift)

`internal/deps/deps.go` blank-imports the core libraries so `go mod tidy` keeps
them recorded from Phase 0:

| Module | Version | Purpose |
|---|---|---|
| `go.etcd.io/bbolt` | v1.4.3 | pure-Go embedded KV store (balances/datastore/locks/WAL) |
| `google.golang.org/grpc` | v1.81.1 | signed-envelope RPC transport |
| `google.golang.org/protobuf` | v1.36.11 | wire serialization |

(Indirect: `golang.org/x/net`, `golang.org/x/sys`, `golang.org/x/text`,
`google.golang.org/genproto/googleapis/rpc`.) **BoltDB was chosen over SQLite**
to avoid a CGO dependency, per the plan's lower-friction recommendation.

### 3.4 Orchestration

Because `make` is not installed on this Windows machine, the **primary** harness
is `scripts/run.ps1`; a `Makefile` mirrors it for Unix/grading environments.
Both expose the same targets:

| Target | Action |
|---|---|
| `build` | `go build ./...` + emit `bin/server(.exe)`, `bin/client(.exe)` |
| `up` | launch all 36 servers as background processes; PIDs → `run/pids.txt`; logs → `run/logs/` |
| `down` | stop every recorded PID cleanly; remove `run/pids.txt` |
| `status` | report how many tracked PIDs are alive |
| `keys` | ed25519 keypair generation (Phase 1 — currently a stub) |
| `test` | `go test ./...` |
| `health` | `client --healthcheck` |

Server count is derived from the same `BYZ_NUM_CLUSTERS`/`BYZ_CLUSTER_SIZE`
parameters, so the harness tracks the configurable topology too.

### 3.5 `.gitignore`

Excludes `bin/`, `*.exe`, `*.db`/`*.sqlite*`, `run/`, `data/`, `*.pid`, and
`config/keys/` (generated secrets), plus editor/OS noise — so binaries, DB
files, runtime PIDs/logs, and keys are never committed.

---

## 4. Verification — Phase 0 testable objective

All checks performed on this machine:

| Objective | Command | Result |
|---|---|---|
| Empty tree compiles | `go build ./...` | **exit 0** |
| Static analysis clean | `go vet ./...` | **exit 0** |
| Tests run (and pass) | `go test ./...` | **ok** `internal/config` (5 tests), rest "no test files" |
| Binaries produced | `run.ps1 build` | `bin/server.exe`, `bin/client.exe` |
| 36 servers reach listening | `run.ps1 up` → `Get-NetTCPConnection` | **36/36 ports** 9001–9036 listening |
| Health round-trip | `client --healthcheck` | **36 up, 0 down (of 36)** |
| Clean teardown | `run.ps1 down` → port re-scan | "stopped 36 servers"; **all byz ports freed** |

The config unit tests assert the topology shape and the quorum arithmetic that
the entire protocol depends on: 36 servers; f=3; quorum=7 (2f+1); collector=9
(n−f); client=4 (f+1); the shard map (1–1000→C1, 1001–2000→C2, 2001–3000→C3);
intra/cross-shard classification; and primary rotation (C1 v0→S1, C2 v0→S13,
C3 v0→S25, with wrap-around).

**Demo round-trip equivalent to `make build && make up && make down`:**
`run.ps1 build` (green) → `run.ps1 up` (36 listening) → `client --healthcheck`
(36/36) → `run.ps1 down` (ports freed). ✔

---

## 5. Notes, deviations, and follow-ups

- **Go install method.** Used the portable zip at `C:\Users\kashy\sdk\go` instead
  of the MSI, because the MSI's silent install blocked on UAC. This is fully
  functional and on PATH. If a system-wide install is later preferred, run the
  winget/MSI install interactively (accept the UAC prompt).
- **protoc not installed.** Not needed for Phase 0. Phase 1 can either install
  `protoc` system-wide or use `encoding/gob` structs (the plan allows either);
  the protobuf Go runtime is already pinned.
- **PowerShell pipe quirk.** When `run.ps1 up` is invoked inside another shell
  that *captures* its output (`2>&1`), the caller can block until the spawned
  servers exit, because the hidden child processes can inherit the captured
  stdout handle. Running `run.ps1 up` normally (interactively) returns
  immediately. This does not affect correctness of bring-up/teardown.
- **Topology default.** Set to the 3×12 / f=3 benchmark mode per the plan's
  non-standard topology note. Switch to the graded 3×4 / f=1 mode at runtime via
  `BYZ_CLUSTER_SIZE=4` (no rebuild). Confirm the 36-server choice with course
  staff before submission, as the plan warns.
- **`.github/` untouched** — the workflow files were not modified (spec voids
  waivers for tampering).

---

## 6. Next phase

**Phase 1 — Topology, transport, and a signed echo.** Build `internal/crypto`
(ed25519 keypairs + cert assembly), `internal/transport` (gRPC `Send(Envelope)`
with signature verification), wire `cmd/server` to the gRPC listener and peer
dialing, and the `internal/server.Replica` dispatch loop. Testable objective:
S1 broadcasts a signed PING to S2..S12 and receives 11 valid PONGs, with a
corrupted signature rejected (`go test ./internal/transport -run TestSignedEcho`).
The `make keys` target gets its real implementation here.
