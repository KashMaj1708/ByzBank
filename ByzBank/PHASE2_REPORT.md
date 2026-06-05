# Phase 2 Report — Storage Layer (Balances, Datastore, Locks, WAL)

**Project:** CSE 535 Project 4 — Byzantine Fault-Tolerant Sharded 2PC (Go)
**Plan reference:** `Project4_Go_Implementation_Plan.md`, Phase 2
**Date:** 2026-06-05
**Topology:** 36 servers, 3000 items, initial balance **10**

---

## 1. Goal

A correct, concurrency-safe persistence layer with four logical stores backed by
a real embedded DBMS (BoltDB), plus per-client replay timestamps.

---

## 2. What was built — `internal/store`

### BoltDB buckets

| Bucket | Key | Value |
|---|---|---|
| `balances` | item ID (decimal string) | int64 balance |
| `datastore` | big-endian seq (0,1,2,…) | JSON `DatastoreEntry` |
| `locks` | item ID | owning sequence number |
| `wal` | txn ID string | JSON `WALPreimage` |
| `clientTS` | client ID string | last executed timestamp |

Balances use **lazy initialization**: untouched items return `initialBalance`
(10) without writing 3000 keys at startup. First mutation persists the value.

### API (all guarded by `sync.Mutex`)

| Method | Behaviour |
|---|---|
| `GetBalance(item)` | read balance (default 10) |
| `ApplyTransfer(x,y,amt)` | atomic debit/credit; errors if insufficient |
| `ApplyDebitOnly(x,amt)` | coordinator cross-shard prepare debit |
| `ApplyCreditOnly(y,amt)` | participant cross-shard prepare credit |
| `AcquireLock(item,seq)` | false if already locked |
| `ReleaseLock(item)` | remove lock |
| `IsLocked(item)` / `LockSeq(item)` | lock queries |
| `AppendDatastore(entry)` | append-only committed log with auto seq |
| `Datastore()` / `PrintDatastore()` | read / format committed entries |
| `PrintBalance(item)` | `bal[item] = N` format string |
| `WALWrite(txnID, preimage)` | save pre-image before tentative apply |
| `WALUndo(txnID)` | restore balances from preimage |
| `WALDelete(txnID)` | remove WAL after commit |
| `SetClientTS` / `GetClientTS` | replay-attack prevention |

### Datastore entry shape

```go
{seq, type(intra/cross), phase(prepare/commit), x, y, amt, ballotOrViewSeq, outcome}
```

Cross-shard transactions will append **two** entries per server (prepare + commit)
in later phases.

### WAL preimage

`WALPreimage` maps `itemID → balance before apply`. Supports undo of coordinator
debits and participant credits on abort.

---

## 3. Verification

| Check | Command | Result |
|---|---|---|
| Phase 2 demo | `go test ./internal/store` | **PASS** (10 tests) |
| Full suite | `go test ./...` | **PASS** |
| Build | `go build ./...` | **exit 0** |

### Tests exercised (per plan)

1. **Initialize** → items 1, 5, 1000, 1500, 2001, 3000 all read **10**
2. **`ApplyTransfer(5,7,3)`** → bal[5]=7, bal[7]=13
3. **Lock** → second `AcquireLock` on same item returns **false**
4. **WAL** → debit + `WALWrite` + `WALUndo` restores original balance
5. **Datastore** → two `AppendDatastore` calls; `PrintDatastore` shows ordered entries
6. **Bonus** → insufficient transfer rejected; cross-shard debit/credit undo;
   client timestamps; concurrent access smoke test

---

## 4. Usage

```go
s, err := store.Open(store.DefaultOptions("data/S1.db"))
defer s.Close()

s.GetBalance(5)                    // 10
s.ApplyTransfer(5, 7, 3)           // intra-shard apply
s.AcquireLock(5, seq)
s.AppendDatastore(store.DatastoreEntry{...})
s.WALWrite("txn-1", store.NewWALPreimage(map[int]int64{5: 10}))
s.WALUndo("txn-1")
fmt.Println(s.PrintBalance(5))
fmt.Println(s.PrintDatastore())
```

DB files use the `.db` extension and are gitignored.

---

## 5. Next phase

**Phase 3 — Linear PBFT engine (intra-shard happy path).** Wire `internal/pbft`
into `internal/server.Replica`: pre-prepare with lock/balance checks, linear
prepare/commit collector, execute `ApplyTransfer`, append datastore, release
locks. Demo: `go test ./internal/pbft -run TestIntraShardHappy`.
