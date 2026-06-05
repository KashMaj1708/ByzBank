#!/usr/bin/env python3
"""
Oracle checker for CSE 535 Project 4 (36-node adaptation).

Use this to validate your implementation's output against the ground-truth
expected JSON produced by simulate.py.

Two modes:

1) Balance check:
   ./check_oracle.py balances <expected.json> <set_number> <item> <actual_balance>
   -> exits 0 if actual matches the oracle's balance for that item after that set.

2) Bulk check from a dump file:
   Produce a JSON dump from your client after each set in this shape:
     {
       "set": 5,
       "balances": {"973": 10, "1495": 7, ...},
       "datastore_per_cluster": {
          "C1": ["INTRA (100,501,8) COMMIT", ...],
          "C2": [...], "C3": [...]
       }
     }
   Then:
   ./check_oracle.py dump <expected.json> <your_dump.json>
   -> prints a per-set diff of balances and datastores, exits non-zero on mismatch.

The oracle stores datastore entries as the sequence committed by LIVE servers in
each cluster. If your implementation orders concurrent commits differently, compare
as a multiset (the checker does this for datastores by default) rather than a list.
"""
import json, sys
from collections import Counter


def load(path):
    return json.load(open(path))


def find_set(oracle, setnum):
    for s in oracle['sets']:
        if s['set'] == int(setnum):
            return s
    raise SystemExit(f"set {setnum} not found in oracle")


def check_balance(expected_path, setnum, item, actual):
    oracle = load(expected_path)
    s = find_set(oracle, setnum)
    exp = s['balances'].get(str(item))
    if exp is None:
        print(f"[WARN] item {item} not touched in/through set {setnum}; oracle has no value")
        return 0
    if int(actual) == int(exp):
        print(f"[OK] set {setnum} bal[{item}] = {actual} matches oracle")
        return 0
    print(f"[FAIL] set {setnum} bal[{item}]: actual={actual} expected={exp}")
    return 1


def check_dump(expected_path, dump_path):
    oracle = load(expected_path)
    dump = load(dump_path)
    dumps = dump if isinstance(dump, list) else [dump]
    rc = 0
    for d in dumps:
        s = find_set(oracle, d['set'])
        print(f"\n=== SET {d['set']} ===")

        # balances
        if 'balances' in d:
            for item, val in d['balances'].items():
                exp = s['balances'].get(str(item))
                if exp is None:
                    print(f"  [WARN] bal[{item}] not in oracle (untouched)")
                elif int(val) != int(exp):
                    print(f"  [FAIL] bal[{item}]: actual={val} expected={exp}")
                    rc = 1
                else:
                    print(f"  [OK]   bal[{item}] = {val}")

        # datastores (compare as multiset per cluster — ignores concurrent ordering)
        if 'datastore_per_cluster' in d:
            for c in ("C1", "C2", "C3"):
                actual_entries = d['datastore_per_cluster'].get(c, [])
                exp_entries = s['datastore_per_cluster'][c]['committed_entries']
                if Counter(actual_entries) == Counter(exp_entries):
                    print(f"  [OK]   {c} datastore: {len(actual_entries)} entries match (multiset)")
                else:
                    print(f"  [FAIL] {c} datastore mismatch:")
                    extra = Counter(actual_entries) - Counter(exp_entries)
                    missing = Counter(exp_entries) - Counter(actual_entries)
                    if extra:   print(f"          unexpected: {list(extra.elements())}")
                    if missing: print(f"          missing:    {list(missing.elements())}")
                    rc = 1
    print(f"\nRESULT: {'ALL PASS' if rc == 0 else 'MISMATCHES FOUND'}")
    return rc


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print(__doc__); sys.exit(2)
    mode = sys.argv[1]
    if mode == "balances":
        sys.exit(check_balance(sys.argv[2], sys.argv[3], sys.argv[4], sys.argv[5]))
    elif mode == "dump":
        sys.exit(check_dump(sys.argv[2], sys.argv[3]))
    else:
        print(__doc__); sys.exit(2)
