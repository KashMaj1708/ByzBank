# Phase 8 Report — CSV Test-Case Runner and Interactive Client

**Project:** CSE 535 Project 4 — Byzantine Fault-Tolerant Sharded 2PC (Go)  
**Plan reference:** `Project4_Go_Implementation_Plan.md`, Phase 8  
**Date:** 2026-06-05  

---

## 1. Goal

Implement the grading workflow:

1. Parse 5-column Lab4 CSV test files (36-node adapted)
2. Apply live/byzantine fault config per set
3. Fire transactions open-loop to contact servers
4. Pause between sets for interactive queries
5. Expose `PrintBalance`, `PrintDatastore`, `Performance`, and `next`

---

## 2. Implementation summary

### 2.1 Test files (`test/`)

Generated from the expected-state oracles:

| File | Purpose |
|------|---------|
| `Lab4_Testset_1_36node.csv` | Intra-shard sets 1–6 |
| `Lab4_Testset_2_36node.csv` | Cross-shard sets 1–6 |

### 2.2 CSV parser (`internal/testcase/parse.go`)

Parses the grader format: set header row carries Set/Live/Contact/Byzantine; continuation rows carry only the next `(x,y,amt)` triple.

`ContactFor(set, cluster)` maps `ClusterOf(x)` to the contact primary (`[S1, S13, S25]` in healthy sets).

### 2.3 Set runner (`internal/testcase/runner.go`)

| Step | Behaviour |
|------|-----------|
| Fault config | Not in Live → `Alive=false`; in Byzantine → `ByzantineBackup=true`; else honest |
| Routing | Open-loop fire to contact server for `ClusterOf(x)` |
| Settle | Poll all coordinator-cluster replicas for f+1 matching replies (90s max) |
| Metrics | Record send time and first quorum reply per transaction |

### 2.4 Remote client (`internal/client/remote.go`)

| Transport | Use |
|-----------|-----|
| gRPC `Send` | `CLIENT_REQUEST` to contact server |
| HTTP `:port+10000` | `/balance`, `/datastore`, `/fault`, `/reply` |

### 2.5 Server HTTP API (`internal/server/httpapi.go`)

Query endpoints work even when `Alive=false` (down servers still serve grading queries). Production `NewReplica` now enables 2PC with per-process collectors.

### 2.6 Interactive menu (`internal/testcase/menu.go` + `cmd/client`)

```
go run ./cmd/client --testfile test/Lab4_Testset_1_36node.csv
```

Commands after each set: `PrintBalance <item>`, `PrintDatastore <S#>`, `Performance`, `next`, `quit`.

Scripted mode (no menu): `--auto`

### 2.7 PBFT reply cache (`internal/pbft/engine.go`)

`recentReplies` + `LookupReply` let the client poll executed replies via HTTP `/reply`.

---

## 3. Tests

```powershell
go test ./internal/testcase -v
go test ./... -count=1 -timeout 180s
```

`TestParseTestset1` / `TestParseTestset2` validate CSV structure (6 sets, set-5 live count, contact routing).

---

## 4. Demo workflow

```powershell
# Terminal 1: start all 36 servers (existing launch script or 36x go run ./cmd/server --id S#)
go run ./cmd/server --id S1

# Terminal 2: run test file interactively
go run ./cmd/client --testfile test/Lab4_Testset_1_36node.csv

# Or auto-run all sets
go run ./cmd/client --testfile test/Lab4_Testset_2_36node.csv --auto
```

Compare output against `Lab4_Testset_*_36node_expected.json` using `check_oracle.py`.

---

## 5. Next phase

**Phase 9** — Performance tuning, timer calibration, and full section-6.3 stress sign-off.
