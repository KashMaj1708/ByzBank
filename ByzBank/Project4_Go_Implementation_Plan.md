# CSE 535 Project 4 — Byzantine Fault-Tolerant Sharded 2PC, in Go

A phased implementation plan. Each phase has a clear deliverable and a **testable objective** you can demo before moving on. Phases build strictly on each other, so a failing test in phase N means do not start phase N+1.

---

## Before you start: what you are actually building (the mental model)

Read this once before touching code. Everything downstream depends on getting these invariants right.

> **Topology note (non-standard).** This plan uses **36 servers total**, in **3 clusters of 12 nodes each**. This intentionally departs from the assignment's mandatory 12-server / 3×4 topology and the fixed Table-1 shard map. The *provided* grader CSVs reference servers S1–S12 with per-cluster Byzantine lists sized for n=4, so **the original grader files will not match a 36-server build** — which is why this plan ships **adapted test files** (`Lab4_Testset_1_36node.csv`, `Lab4_Testset_2_36node.csv`) that preserve every test's intent (identical transactions, balances, and lock/insufficient-balance triggers) while rescaling the Live/Contact/Byzantine server columns to S1–S36 and f=3. Confirm with course staff before submitting a 36-server system, or keep 3×4 as a configurable default and treat 3×12 as a benchmark mode. The protocol logic is identical either way; only the cluster size, the derived quorum numbers, and the server-ID columns in the test files change.

**The system.** **36 servers total**, S1 to S36, organized as **3 clusters of 12 nodes each**. This is *not* a single 36-node cluster — there is no system-wide consensus anywhere, which is the entire point of sharding. Consensus (linear PBFT, from Lab 2) runs **independently inside each 12-node cluster**. The only thing that crosses cluster boundaries is **2PC**, which is certificate-carrying message passing, *not* consensus. Each cluster owns a contiguous 1000-key slice of the 3000-key dataset.

| Cluster | Servers (n=12) | Data items | PBFT params |
|---|---|---|---|
| C1 | S1 .. S12 | 1 .. 1000 | f=3, 3f+1=10 ≤ 12, quorum=7 |
| C2 | S13 .. S24 | 1001 .. 2000 | f=3, 3f+1=10 ≤ 12, quorum=7 |
| C3 | S25 .. S36 | 2001 .. 3000 | f=3, 3f+1=10 ≤ 12, quorum=7 |

**Fault model.** Up to **f = 3** Byzantine servers **per cluster**. BFT requires n ≥ 3f+1; with n = 12 the largest tolerable f is 3 (3·3+1 = 10 ≤ 12; f=4 would need 13). So each cluster carries **2 nodes of slack** (12 vs the minimum 10) — quorums are always derived from **f**, never from n. Every quorum (prepare, commit, view-change, 2PC cert) is **2f+1 = 7** matching messages **within that one cluster**; clients wait for **f+1 = 4** matching replies. Note this differs from Lab 2 (single 7-node cluster, f=2). Here each cluster is its own 12-node PBFT system tolerating up to 3 faults.

**Two transaction types.**
- **Intra-shard** (x, y, amt) where x and y are in the same cluster: just run linear PBFT in that cluster, with locking added.
- **Cross-shard** (x, y, amt) where x and y are in different clusters: the sender's cluster acts as **coordinator**, the receiver's cluster is the **participant**, and they run 2PC where *every phase decision is itself agreed by a PBFT consensus round inside each cluster*.

**The single most important structural fact:** unlike Project 3 (where the client was the 2PC coordinator and consensus was Paxos), here the **coordinator cluster's primary** drives 2PC, and **each 2PC message that crosses cluster boundaries carries a certificate of 2f+1 (= 7, at n=12) matching commit messages** proving the sending cluster reached consensus. So 2PC is layered *on top of* PBFT: a cross-shard transaction triggers multiple PBFT instances (prepare-in-coordinator, prepare-in-participant, commit-in-coordinator, commit-in-participant).

**Per-server data structures you must maintain** (Figure 4 is the canonical reference):
1. **Key-value DB** — balances, in a real lightweight DBMS (SQLite/BoltDB; *files are explicitly disallowed*).
2. **Datastore** — the ordered log of committed transactions. For cross-shard, **two entries** get appended per transaction: one after prepare, one after commit. This is distinct from the balance DB.
3. **Lock table** — which data items are currently locked.
4. **WAL (write-ahead log)** — lets a server undo an executed-but-not-yet-committed cross-shard operation on abort.
5. **PBFT message logs** — pre-prepare/prepare/commit per sequence number (from Lab 2).
6. **Last-executed-timestamp per client** — replay-attack prevention.

**Locking rules (two-phase locking, simplified):**
- Intra-shard touches both x and y → lock both.
- Cross-shard: the coordinator cluster locks **only x** (the sender), the participant cluster locks **only y** (the receiver).
- Before acquiring a lock for sequence n, a replica must have received all pre-prepares for sequence < n (so it knows the true lock state). This is the "gap check."
- Locks are released at commit/abort time.

**Abort triggers** (any of these → abort): insufficient balance, lock unavailable, or **consensus fails** (not enough matching messages, too many faulty servers). Critically: in the **coordinator** cluster, insufficient-balance/lock-unavailable means the primary *silently ignores* the transaction (no abort decision is run through consensus). In the **participant** cluster, even an abort decision must go through a consensus round.

---

## Project layout (Go)

```
2pcbyz-<username>/
├── go.mod
├── cmd/
│   ├── server/main.go        // boots one server process
│   └── client/main.go        // the single client driver + interactive menu
├── internal/
│   ├── config/               // static topology, shard map, server addresses
│   ├── pb/                    // protobuf-generated gRPC types (or encoding/gob structs)
│   ├── transport/             // gRPC server + client stubs, message signing
│   ├── crypto/                // ed25519 keypairs, sign/verify, cert assembly
│   ├── store/                 // SQLite/Bolt wrapper: balances, datastore, locks, WAL
│   ├── pbft/                  // linear PBFT engine (from Lab 2, generalized)
│   ├── twopc/                 // 2PC coordinator + participant state machines
│   ├── server/                // the Replica type wiring pbft+twopc+store+transport
│   └── testcase/              // CSV parser, set runner, live/byzantine filtering
└── test/                      // sample CSVs + integration harness
```

**Language note.** The spec requires you continue in the language used for Projects 1–3. This plan assumes you are switching to / using **Go**. If your earlier projects were in another language, get instructor sign-off first. Go is a strong fit: goroutines per server, channels for the message bus, gRPC for RPC (satisfies the "understand serialization/marshalling/RPC" intent the labs repeatedly express).

---

# PHASE 0 — Environment, toolchain, and project scaffolding

**Goal:** A reproducible Go workspace where all 36 server processes and the client compile, run, and can be brought up/down with one command. No protocol logic yet — this is pure plumbing so that every later phase has a working harness from minute one.

### Build
- **Go toolchain:** install Go 1.22+ (`go version` to confirm). Use Go modules: `go mod init github.com/<username>/2pcbyz-<username>`.
- **Repo bootstrap:** clone the assignment repo (`git clone git@github.com:F24-CSE535/2pcbyz-<YourGithubUsername>.git`), `cd` in, and **do not touch the `.github/` workflow files** — the spec voids waivers for tampering with them.
- **Directory skeleton:** create the full `internal/` + `cmd/` + `test/` tree from the layout section as empty packages, each with a stub `package` declaration so `go build ./...` succeeds green on an empty repo.
- **Dependencies to pin in `go.mod` now** (so versions don't drift mid-project):
  - `google.golang.org/grpc` + `google.golang.org/protobuf` (RPC transport — satisfies the labs' stated intent that you learn serialization/marshalling over RPC).
  - `go.etcd.io/bbolt` (pure-Go embedded KV store) **or** `github.com/mattn/go-sqlite3` (SQLite; needs CGO). Pick one and commit. BoltDB is the lower-friction choice for Go.
  - `google.golang.org/protobuf/cmd/protoc-gen-go` + `protoc-gen-go-grpc` as tool deps if using protobuf; install `protoc` system-wide.
- **Config-driven topology:** a single `config/topology.yaml` (or Go struct) listing the 36 servers, their `host:port`, cluster membership (12 per cluster), and the Table-1 shard map. Everything reads from here so there are no magic numbers scattered around. **Make cluster count and cluster size parameters**, not constants — this is what lets you flip between the graded 3×4 default and the 3×12 benchmark mode without code changes, and it directly satisfies the Project-3 "configurable clusters" bonus.
- **Process orchestration:** a `Makefile` (or `scripts/run.sh`) with targets:
  - `make build` → `go build ./...`
  - `make keys` → generate the 36 ed25519 keypairs into `config/keys/` (used from Phase 1 on)
  - `make up` → launch all 36 servers as background processes (each `go run ./cmd/server --id S<n>`), writing PIDs to a file. (At 36 processes, watch your file-descriptor and port limits; consider `ulimit -n` headroom.)
  - `make down` → kill them cleanly
  - `make test` → `go test ./...`
- **Graceful lifecycle:** each server traps SIGINT/SIGTERM and shuts its listener down cleanly, because the spec forbids killing servers between CSV sets — you need clean start/stop for dev iteration without orphaned ports.
- **.gitignore:** exclude `*.db`, `config/keys/`, build artifacts, and PID files so you never commit secrets or binaries.

### Testable objective
`make build` compiles the empty tree with zero errors. `make up` brings all 36 servers to a listening state (a trivial health-check RPC or even a logged "S<n> listening on :PORT" line for all 36). `make down` terminates all 36 with no leftover bound ports (`lsof -i` clean). **Demo:** `make build && make up && make down` round-trips cleanly; `go test ./...` runs (even if it's all empty/skipped tests).

---

# PHASE 1 — Topology, transport, and a signed echo

**Goal:** 36 server processes start, know the static topology, and can exchange **signed** messages over gRPC. No consensus yet.

### Build
- `internal/config`: hardcode the shard map (Table 1), the 36 server IDs, their host:port, and cluster membership. Add a helper `ClusterOf(dataItem int) ClusterID` and `PrimaryOf(cluster ClusterID, view int) ServerID` (primary = `view mod clusterSize` within the cluster, i.e. `mod 12` here — keep it `mod clusterSize`, never a literal, so the configurable mode works).
- `internal/crypto`: generate an ed25519 keypair per server at startup; load all public keys from config so everyone can verify everyone. Functions: `Sign(msg) sig`, `Verify(pubkey, msg, sig) bool`, and `Certificate` assembly (a set of 2f+1 sigs over the same digest).
- `internal/transport`: a gRPC service with one bidirectional `Send(Envelope)` RPC. Every envelope carries `{senderID, type, payload, signature}`. On receipt, verify the signature before handing the payload up; drop on failure.
- `cmd/server`: parse `--id`, boot gRPC listener, dial all peers (lazy reconnect).
- `internal/server`: a `Replica` struct with an inbound message channel and a dispatch loop (`switch on msg.Type`).

### Testable objective
Spin up all 36 servers. From a scratch test, have S1 broadcast a signed `PING` to the rest of its cluster (S2 .. S12); each replies with a signed `PONG`. Assert S1 receives 11 valid PONGs and that a deliberately corrupted signature is rejected. **Demo:** `go test ./internal/transport -run TestSignedEcho`.

---

# PHASE 2 — Storage layer (balances, datastore, locks, WAL)

**Goal:** A correct, concurrency-safe persistence layer with the four logical stores, backed by a real embedded DBMS.

### Build
`internal/store` over **BoltDB** (pure-Go, zero external deps, ideal) or **SQLite** (mattn/go-sqlite3). Buckets/tables:
- `balances`: key = itemID, value = int64. Initialized so **every item starts at 10 units**.
- `datastore`: append-only ordered list of committed entries. Each entry: `{seq, type(intra/cross), phase(prepare/commit), txn(x,y,amt), ballotOrViewSeq, outcome}`. Cross-shard appends one entry at prepare, one at commit.
- `locks`: set of locked itemIDs, each with the owning sequence number.
- `wal`: per-pending-cross-shard-txn, the pre-image needed to undo (e.g., old balance of x).
- `clientTS`: last executed timestamp per sender (replay prevention).

API (all guarded by a `sync.Mutex` or per-key locking):
```
GetBalance(item) int64
ApplyTransfer(x,y,amt) error      // x-=amt, y+=amt, atomic
ApplyDebitOnly(x,amt)             // coordinator side of cross-shard
ApplyCreditOnly(y,amt)            // participant side of cross-shard
AcquireLock(item, seq) bool       // false if already locked
ReleaseLock(item)
IsLocked(item) bool
AppendDatastore(entry)
WALWrite(txnID, preimage) / WALUndo(txnID) / WALDelete(txnID)
PrintBalance(item) / PrintDatastore()   // the required functions
```

### Testable objective
Unit tests: initialize → every key reads 10. `ApplyTransfer(5,7,3)` → 5 reads 7, 7 reads 13. Lock then re-lock same item → second returns false. WAL write → undo → balance restored. Datastore append then `PrintDatastore` shows ordered entries. **Demo:** `go test ./internal/store`.

---

# PHASE 3 — Linear PBFT engine for intra-shard, happy path only

**Goal:** A single cluster of 12 reaches consensus on an intra-shard transfer and executes it, using **linear** PBFT (collector pattern), with locking. All servers honest, no failures.

### Build
`internal/pbft`. This is your Lab 2 protocol, generalized to run per-cluster. Linear PBFT phases (Figure 3 of Lab 2):
1. **Request** → primary.
2. **Pre-prepare**: primary checks (a) x,y unlocked and (b) bal(x) ≥ amt. If ok, **acquires locks on x and y**, assigns seq, multicasts signed pre-prepare to the 11 backups. If not ok → silently ignore.
3. **Prepare (linear)**: each backup, after the **gap check** (received all pre-prepares for seq' < n) and its own lock acquisition, sends a signed prepare to the **collector** (primary). Collector gathers **n−f = 9** matching sigs → builds a **prepare certificate** → broadcasts it. (Lab 2 phrases the collector quorum as n−f; for commit/safety the binding threshold is 2f+1 = 7. With n=12, f=3 these differ — 9 vs 7 — so be deliberate about which you require where: n−f for the collector's signature-gathering, 2f+1 for the safety argument.)
4. **Commit (linear)**: each replica verifies the prepare certificate, sends signed commit to collector; collector gathers 2f+1 = 7 → builds **commit certificate** → broadcasts.
5. **Execute + reply**: once a replica holds the commit certificate **and** has executed all lower sequence numbers, it `ApplyTransfer`, appends to datastore, **releases locks on x and y**, and sends a reply to the client.

Client waits for **f+1 = 4 matching replies**.

Key implementation details:
- Out-of-order pre-prepares from the primary are allowed (Lab 2), but **execution is strictly in seq order**. Maintain an execution cursor.
- The gap check is what makes locking correct: never evaluate a lock/balance for seq n until all seq < n pre-prepares are in hand.
- Recommended: a backup may **briefly wait** for a lock to release before giving up (spec-recommended solution; not mandatory).

### Testable objective
One cluster, 12 honest servers. Client sends intra-shard `(5, 7, 3)`. Assert: all 12 commit and execute, balances become 5→7 and 7→13 **on all 12 servers**, locks released afterward, client got ≥4 matching replies. Then a second txn `(5, 8, 100)` with insufficient balance → ignored, no state change. **Demo:** `go test ./internal/pbft -run TestIntraShardHappy`.

---

# PHASE 4 — PBFT view-change and Byzantine behavior

**Goal:** The cluster tolerates a faulty primary (view-change) and a Byzantine replica, exactly as Lab 2 specifies. This phase makes intra-shard robust before 2PC is layered on.

### Build
- **Byzantine behaviors** (driven by the test case's Byzantine list):
  - Byzantine **leader**: never marks anything prepared (sends no valid pre-prepare progress) AND sends no NEW-VIEW during view-change (but still logs received VIEW-CHANGE).
  - Byzantine **backup**: never marks anything prepared.
- **View-change** (identical to PBFT, unchanged from Lab 2): client timer times out → resends to all replicas → backups suspect leader → exchange signed VIEW-CHANGE (carrying latest stable checkpoint + prepared requests) → on **2f+1 = 7**, the designated next leader (`(v+1) mod clusterSize`, i.e. mod 12 here) issues NEW-VIEW with pre-prepares for in-flight requests.
- Add the **`PrintStatus(seq)`** (PP/P/C/E/X), **`PrintView`** (all NEW-VIEW messages), and **`PrintLog`** functions from Lab 2 — you'll reuse them for grading.

### Testable objective
Cluster of 12, mark the current primary Byzantine. Client request stalls → view-change fires → new primary drives the request to commit on the honest replicas. Separately, mark **3 backups** Byzantine (the max f) and assert consensus still completes (9 honest ≥ 2f+1 = 7). Then mark **4 servers** down/faulty in one cluster (exceeding f) and assert **no consensus** → transaction aborts (only 8 honest, but a Byzantine-safe quorum needs the faulty set ≤ 3; with 4 down you can't form a sound 2f+1 of honest-and-agreeing replicas). **Demo:** `go test ./internal/pbft -run TestViewChange` and `-run TestByzantineBackup`.

> Quorum sanity for n=12, f=3: you tolerate **up to 3** faulty per cluster. At exactly 3 faulty, 9 honest remain ≥ the 7-message quorum, so progress holds. At 4 faulty, liveness/safety guarantees break — that's your "no consensus → abort" case. This is a much wider margin than the 3×4 default (which tolerates only 1), and it's the main reason 3×12 is more interesting to test.

---

# PHASE 5 — Cross-shard 2PC: prepare phase (coordinator + participant)

**Goal:** The first half of 2PC works. Coordinator cluster locks x, runs PBFT to "prepare-commit", executes a debit into WAL, and ships a prepare certificate to the participant; participant locks y, runs PBFT (even to decide abort), executes a credit into WAL, and ships back a prepared/abort certificate.

### Build
`internal/twopc`. Wire it so a cross-shard `(x,y,amt)` arriving at the coordinator primary kicks off:

**Coordinator prepare:**
1. Primary checks x unlocked **and** bal(x) ≥ amt. If not → silently ignore (no abort). Else acquire lock on x.
2. Run a **PBFT instance** inside the coordinator cluster to agree on "prepare this cross-shard txn." Backups do the gap check + lock x on valid pre-prepare.
3. On 2f+1 commit: each coordinator replica **appends a prepare entry to its datastore**, **executes the debit** (`ApplyDebitOnly(x,amt)`), **writes WAL** (old bal of x). **No client reply yet.**
4. Primary assembles the prepare message = `{client request, 2f+1 matching commit messages}` and sends it to **every** node in the participant cluster.

**Participant prepare:**
1. Participant primary verifies the incoming certificate, does a lock check on y.
2. Run a PBFT instance inside the participant cluster on the decision. **Even an abort decision runs through consensus here** (the asymmetry vs coordinator).
3. If consensus = commit: append prepare entry, `ApplyCreditOnly(y,amt)`, write WAL, lock y. No client reply.
   If consensus = abort: the primary sends an **abort** message + 2f+1 matching commit messages (proving the abort) to every coordinator node.
4. On commit decision, primary sends a **prepared** message + 2f+1 matching commits to every coordinator node.

### Testable objective
Two clusters wired. Cross-shard `(5, 1500, 4)` (C1→C2). Assert: every C1 replica has a prepare datastore entry, x debited (5→3) with WAL present, lock on x held; every C2 replica has a prepare datastore entry, y credited (1500→1504) with WAL present, lock on y held; coordinator received a valid prepared certificate. No client reply has been sent yet. Also test the abort path: cross-shard where bal(x) is insufficient → coordinator silently ignores; and a case where participant must abort → participant runs consensus and returns an abort certificate. **Demo:** `go test ./internal/twopc -run TestCrossShardPreparePhase`.

---

# PHASE 6 — Cross-shard 2PC: commit phase, ack, and undo

**Goal:** Finish 2PC. Coordinator runs a final PBFT round on the outcome, commits or undoes via WAL, replies to the client; participant commits/aborts and acks back; coordinator retries unresponsive participant servers until f+1 acks.

### Build
**Coordinator commit:**
1. On receiving the participant's prepared/abort certificate, the coordinator primary runs **another PBFT instance** on the final outcome.
2. On consensus:
   - **Commit:** each coordinator replica appends a commit datastore entry, **releases lock on x**, **deletes WAL** for the txn, sends a **PBFT reply to the client**.
   - **Abort / timeout:** each replica appends a commit(=abort) datastore entry, **WAL-undoes** the debit, releases lock on x, sends a client reply with result `"abort"`.
3. Primary sends the outcome to every participant node.

**Participant commit:**
1. Participant primary runs PBFT on the outcome.
2. **Commit:** append commit entry, release lock on y, delete WAL, send **Ack to coordinator primary** (no client reply from participant).
3. **Optimization:** if the participant already aborted in its prepare phase, it does **not** re-run consensus on abort.
4. **Coordinator reliability:** if participant committed in prepare and final outcome is commit, the coordinator primary **keeps re-forwarding commit (timer-driven) to unresponsive participant servers until it collects f+1 = 4 acks** from distinct participant servers.

### Testable objective
Full cross-shard commit: `(5,1500,4)` runs end to end. Assert final balances 5→3 on **every C1 replica**, 1500→1504 on **every C2 replica**; both clusters have **two** datastore entries (prepare + commit); all locks released; WALs empty; client received an "abort"-or-"committed" reply (committed here); coordinator collected ≥4 acks. Then force an abort outcome and assert WAL-undo restored x and y, and client got `"abort"`. **Demo:** `go test ./internal/twopc -run TestCrossShardCommitPhase` and `-run TestCrossShardAbortUndo`.

---

# PHASE 7 — Concurrency, locking correctness, and the interleaving from Figure 4

**Goal:** Concurrent intra- and cross-shard transactions behave correctly under the locking discipline. Reproduce the exact Figure-4 interleaving as a regression test.

### Build
- Make the client driver **open-loop** (spec: do *not* implement closed-loop clients; a single client process fires all requests). Servers still maintain per-client last-timestamp for replay protection.
- Stress the gap-check + lock interaction: ensure a cross-shard holding a lock on x correctly blocks/lets-skip a later intra-shard touching x, and that a slow backup waiting on a lock doesn't deadlock.
- Implement the **Figure 4 sequence** as a deterministic scenario: intra `(A,B,20)` → cross `(A,E,10)` prepare (lock A, WAL, debit) → intra `(C,D,5)` interleaved before A's commit → then A's cross-shard commit (release A, delete WAL). Assert the lock table, datastore (two entries for the cross-shard), and balances match the figure at each step.

### Testable objective
Run two concurrent independent cross-shard txns in disjoint cluster pairs → both commit. Run two concurrent txns contending on the **same** item → exactly one proceeds, the other is ignored/aborted, no balance corruption, no deadlock. Run the Figure-4 script and assert per-step state. **Demo:** `go test ./internal/server -run TestConcurrentContention` and `-run TestFigure4Interleaving`.

---

# PHASE 8 — CSV test-case runner, interactive control, and the required functions

**Goal:** The exact grading workflow: read the CSV, process **one set at a time**, pause for user input between sets, never terminate servers, and expose all query functions while idle. Drive it with the **adapted 36-node test files** (`Lab4_Testset_1_36node.csv`, `Lab4_Testset_2_36node.csv`).

### Build
`internal/testcase` + `cmd/client`:
- **CSV parser** for the grader's **5-column** format: `Set Number, Transactions (x,y,amt), Live Servers [..], Contact Servers [..], Byzantine Servers [..]`. Only the first row of each set carries the Set/Live/Contact/Byzantine columns; continuation rows carry only the next transaction (blank elsewhere). The **Contact Servers** column names the per-cluster primary the client must send to — in the 36-node files this is `[S1, S13, S25]` (primaries of C1/C2/C3). (Test Set 1 = intra-shard only; Test Set 2 = cross-shard. The two files are run independently: finish Set 1's six sets, restart, then run Set 2.)
- **Client routing**: for each `(x,y,amt)`, compute `ClusterOf(x)` and send to that cluster's Contact server. Intra- vs cross-shard is decided by whether `ClusterOf(x) == ClusterOf(y)`. The client is **open-loop** (single process, fires all requests; not closed-loop).
- **Set runner**: applies the live/byzantine config to all servers (servers **not** in Live are treated as down; Byzantine servers adopt phase-4 behavior). Processes transactions in the given order. State **persists across sets** (interdependent — unlike Lab 2's flush; this is Project-4 semantics, matching Project 3).
- **Interactive menu** (terminal, no UI): after a set finishes and servers are idle, accept commands:
  - `PrintBalance <item>` — balance of that item on **all servers of its cluster**, shown even for **disconnected or Byzantine** servers (the guide requires this).
  - `PrintDatastore <server>` — committed-txn log on that server, shown **regardless of connection/Byzantine status**.
  - `Performance` — throughput (committed txns/sec) and latency (client-initiate → client-reply), measured **at the client process**.
  - `next` — advance to the next set (and **only** then; never auto-advance).
- Strip all debug logging from the final build (spec requirement).

### What each adapted set exercises (preserved 1:1 from the guide)
These mirror the original test descriptions exactly; only the server-ID columns were rescaled from 3×4/f=1 to 3×12/f=3. Transaction triples and balance arithmetic are unchanged, so the pass/fail behavior is identical in meaning.

**Test Set 1 — intra-shard** (Byzantine `[S16,S17,S18, S28,S29,S30]` = 3 per affected cluster = f):
1. **Set 1** — concurrent intra-shard commits across C1/C2/C3; 3 Byzantine per cluster, 9 honest ≥ 7, all commit.
2. **Set 2** — lock-wait: two txns from sender `1201` (C2) serialize on the lock for item 1201.
3. **Set 3** — insufficient balance (part 1): `2995` drops to 5 after the first txn, then can't cover 7 → skipped.
4. **Set 4** — insufficient balance (part 2): `1295` (bal 10) can't cover 17 → skipped; C1 has 3 nodes down (9 live) but still commits its valid txns.
5. **Set 5** — **no quorum in a cluster**: C1 reduced to **6 live < 7** → its txns `(973,707,2)` and `(333,691,4)` can't reach quorum and abort; C2/C3 commit normally.
6. **Set 6** — no execution due to incomplete previous set (depends on Set 5's crippled state carried forward).

**Test Set 2 — cross-shard** (Byzantine `[S19,S20,S21, S31,S32,S33]` for healthy sets; `[S31,S32,S33]` for the crippled sets):
1. **Set 1** — concurrent cross-shard commits (C1→C2, C2→C3, C3→C2); all commit.
2. **Set 2** — lock-wait: repeated `(296,1997,·)` serializes on x=296 (coordinator C1) and y=1997 (participant C2).
3. **Set 3** — insufficient balance in **coordinator**: `(1998,2998,19)`, coord C2, bal 10 < 19 → coordinator silently ignores (no abort consensus).
4. **Set 4** — **no quorum in the coordinator cluster**: C2 reduced to **6 live < 7**; the C2-coordinated txns `(1877,2855,5)`/`(1333,2333,3)` can't prepare → abort; the C1-intra `(5,98,2)` commits.
5. **Set 5** — **no quorum in the participant cluster**: C2 again at 6 live; `(45,1355,5)` has coord C1 (healthy) but **participant C2** can't reach consensus → coordinator prepares/locks then **WAL-undoes and aborts**.
6. **Set 6** — no execution due to incomplete previous set (carried from Set 5).

### Testable objective
Run `Lab4_Testset_1_36node.csv` end to end: Sets 1–4 and 6 commit their valid txns, Set 5's C1 txns abort (no quorum), insufficient-balance txns are skipped, and the runner pauses for `next` between every set. Then independently run `Lab4_Testset_2_36node.csv` and assert the cross-shard commit/abort/lock-wait/no-quorum outcomes above, including a clean WAL-undo on the participant-no-quorum case. Between sets, `PrintBalance`/`PrintDatastore` return correct values **even for downed/Byzantine servers**. **Demo:** `go run ./cmd/client --testfile test/Lab4_Testset_1_36node.csv` and `--testfile test/Lab4_Testset_2_36node.csv`, walked through interactively, plus `go test ./internal/testcase`.

**Ground-truth oracle.** Each test file ships with a pre-computed expected-state oracle so you can diff your output instead of eyeballing it:
- `Lab4_Testset_1_36node_expected.txt` / `.json` and `Lab4_Testset_2_36node_expected.txt` / `.json` — per-set transaction outcomes (commit/skip/abort with reason), committed balances for every touched item, and the per-cluster datastore contents after each set. Down/Byzantine servers are flagged as lagging where relevant.
- `simulate.py` regenerates the oracle from the CSVs if you tweak a test.
- `check_oracle.py` diffs your implementation's output against the oracle. Have your client emit a small JSON dump per set (`{set, balances, datastore_per_cluster}`) and run `./check_oracle.py dump <expected.json> <your_dump.json>`; it compares balances exactly and datastores as a **multiset** (so concurrent-commit ordering differences don't cause false failures), exiting non-zero on any mismatch. A single value can be spot-checked with `./check_oracle.py balances <expected.json> <set> <item> <actual>`.
- Both oracles satisfy a **conservation-of-money invariant** (sum of all balances = 10 × items touched, no money created or destroyed), which is your strongest end-to-end sanity check — wire the same assertion into your own run.

> Quorum reminder for n=12/f=3: a cluster needs **≥ 7 honest-and-live** replicas to commit. The "no quorum" sets force this by dropping a cluster to **6 live** (S1–S6 for C1 in TS1 Set 5; S13–S18 for C2 in TS2 Sets 4–5). The crippled cluster's **contact/primary stays live** (S1, S13) so the test exercises "primary tries but can't gather quorum," not "no primary." Healthy clusters keep their full 12 with 3 Byzantine (the max f), so 9 honest still clear the 7-quorum.

---

# PHASE 9 — Performance tuning and failure hardening

**Goal:** Reasonable throughput/latency and clean failure/timeout handling end-to-end, across every section-6.3 scenario. This is the correctness-under-stress gate before benchmarking.

### Build & verify
- Tune timers (collector wait for n−f sigs, client resend, coordinator ack-retry). Spec warns: too-large timers tank throughput; too-small cause spurious view-changes.
- Run the full Project-2 test suite (it must still pass — explicitly required) plus all Project-4 scenarios in section 6.3: concurrent independent intra in different clusters, concurrent independent cross-shard, mixed, same-item contention, all 2PC failure/timeout cases, no-consensus-on-too-many-failures, no-commit-if-any-cluster-aborts. **The two adapted grader files cover this matrix directly** — `Lab4_Testset_1_36node.csv` exercises the intra-shard half (Tests 1–6) and `Lab4_Testset_2_36node.csv` the cross-shard half (Tests 1–6), so passing both end-to-end is your section-6.3 sign-off.
- Confirm the **abort-on-no-consensus** rule fires everywhere consensus can fail.
- Make sure `Performance` measures correctly: throughput = committed txns / wall-clock, latency = mean(client-reply-time − client-initiate-time), both **at the client process**.

### Testable objective
A scripted end-to-end run over a multi-set CSV that exercises every section-6.3 scenario passes; `Performance` reports non-degenerate throughput/latency under concurrent load. **Demo:** green integration suite + a `Performance` printout on a mixed workload.

---

# PHASE 10 — SmallBank benchmark (mandatory)

**Goal:** Implement the SmallBank OLTP benchmark on top of your system and produce a performance characterization. SmallBank is the standard way to evaluate a banking-style distributed datastore, and running it surfaces real throughput/latency behavior under a realistic, skewed workload rather than hand-written CSV sets. (Spec reference [1]: Cahill, Röhm, Fekete.)

### What SmallBank is
A banking workload over **three tables** and **six transaction types**, with access **skewed** so a small set of hot accounts receives most requests.

- **Tables:**
  - `Account(name, custid)` — maps a customer name to a customer id.
  - `Savings(custid, balance)` — savings balance per customer.
  - `Checking(custid, balance)` — checking balance per customer.
- **Six transactions** (each touches a small number of tuples):
  1. **Balance (Bal)** — read-only: return savings + checking for a customer.
  2. **DepositChecking (DC)** — add amount to a customer's checking.
  3. **TransactSavings (TS)** — add (or subtract) amount to a customer's savings; abort if it would go negative.
  4. **Amalgamate (Amg)** — move *all* funds from customer N1's savings+checking into N2's checking; zero out N1.
  5. **WriteCheck (WC)** — write a check against a customer: if savings+checking < amount, apply a penalty; debit checking.
  6. **SendPayment (SP)** — transfer amount from N1's checking to N2's checking; abort if insufficient.

Three of these (Amalgamate, SendPayment, and cross-customer WriteCheck variants) naturally become **cross-shard** when the two customers live in different clusters; the rest are **intra-shard** single-customer ops. That mapping is exactly what stresses your 2PC path under load.

### Build
`internal/smallbank`:
- **Schema mapping onto your engine.** You already have a single balance-per-item KV store. Extend it so each customer id maps to **two sub-balances** (savings, checking) — e.g. encode keys as `custid*2` (savings) and `custid*2+1` (checking), keeping them in the **same cluster** so single-customer txns stay intra-shard and only N1↔N2 ops can go cross-shard. Initialize per the benchmark (commonly a fixed savings + checking starting balance).
- **Transaction generators** for all six types, each producing your `(x, y, amt)` (or the bonus `(x,y,z,...)`) requests routed to the correct coordinator primary.
- **Skewed key selection.** Use a Zipfian/hotspot picker (e.g. a configurable `hotsetFraction` getting `hotAccessFraction` of requests — the classic "90% of accesses to 10% of accounts") so the workload matches SmallBank's skew.
- **Workload mix.** A configurable ratio across the six txn types (a uniform mix across the six is the standard default; make it a flag).
- **Driver + metrics.** Fire N transactions (open-loop, matching the project's non-closed-loop client requirement) at a target rate, and record committed throughput (txns/sec) and end-to-end latency distribution (mean + p50/p95/p99). Report separately for intra- vs cross-shard so you can see the 2PC overhead.
- **Correctness oracle.** SmallBank has a **conservation invariant**: total money in the system (sum of all savings + checking) is constant except for WriteCheck penalties. After a run, assert the global sum changed only by the total penalties applied. This is your strongest end-to-end correctness check — far stronger than per-txn assertions, because it catches any lost/duplicated funds from a broken 2PC undo or double-execution.

### Testable objective
Run a SmallBank workload of, say, 1000 transactions with a configured skew and the standard six-type mix across all 3 clusters. Assert: (1) the **conservation invariant** holds (global balance sum constant modulo penalties); (2) every committed cross-shard txn left exactly two datastore entries per involved server and released all locks; (3) `Performance`/the SmallBank metrics report non-degenerate throughput and a latency breakdown showing cross-shard > intra-shard. **Demo:** `go run ./cmd/client --benchmark smallbank --txns 1000 --skew 0.9` prints the metrics table and a green invariant check; `go test ./internal/smallbank`.

---

# PHASE 11 — Final integration, report, and submission

**Goal:** A correct, clean submission with the demo recording and report.

### Build & verify
- Strip all debug logging from the final build (spec requirement — extra log noise is not allowed in submission).
- Final full-suite pass: Project-2 cases + all Project-4 section-6.3 scenarios + SmallBank invariant.
- Record the demo and write the report (format announced separately by course staff).
- Commit with the exact message **`submit lab4`** on `main`, then verify via the provided link. **No waivers for submission errors** — wrong commit message or touched workflow files are not excused.
- Sanity-check late-policy math if relevant: 10%/day, max 30% off within 3 days; Dec 18 is the last day to still earn 70%.

### Testable objective
Fresh clone → `make build` → `make up` → walk a multi-set CSV and a SmallBank run → all green; the `submit lab4` commit is present on `main` and the verification link confirms it.

---

# Bonus (optional extra credit, beyond the mandatory phases)

1. **Three-shard cross transactions.** Extend the input to `(x, y, z, amt1, amt2)` — atomic transfer to two receivers across all three shards. The coordinator now runs 2PC against **two** participant clusters; commit only if *both* prepare-commit. This generalizes your twopc package to N participants and makes SmallBank's Amalgamate/SendPayment richer.

---

# Dependency graph (don't skip ahead)

```
P0 env + scaffolding
   └─> P1 transport+crypto
         └─> P2 store
               └─> P3 PBFT happy path ──> P4 view-change/Byzantine
                                               └─> P5 2PC prepare
                                                     └─> P6 2PC commit/undo
                                                           └─> P7 concurrency/locking
                                                                 └─> P8 CSV runner + functions
                                                                       └─> P9 perf + hardening
                                                                             └─> P10 SmallBank (mandatory)
                                                                                   └─> P11 report + submit
```

A red test at any phase blocks the next. P3→P4 is the one place you can demo intra-shard fully before the 2PC layer exists, which is the natural mid-project checkpoint.
