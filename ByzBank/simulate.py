#!/usr/bin/env python3
"""
Ground-truth oracle for CSE 535 Project 4 (36-node / 3x12 / f=3 adaptation).

Simulates the protocol semantics described in the implementation plan:
  - per-cluster linear PBFT with quorum 2f+1 = 7
  - intra-shard: lock x,y -> check balance -> commit or skip(insufficient) or abort(no quorum)
  - cross-shard 2PC: coordinator prepare (lock x, debit, WAL) -> participant prepare
    (lock y, credit, WAL) -> commit both, or abort with WAL-undo
  - balances + datastores carried across sets within a file
  - per-server datastore tracking so down/Byzantine servers can lag

Outputs, per test file:
  *_expected.txt  : human-readable per-set state (balances touched + datastore summary)
  *_expected.json : machine-diffable structured ground truth
"""
import csv, re, json, sys, copy

F = 3
QUORUM = 2 * F + 1            # 7
CLUSTER_SIZE = 12

def cluster_of(item):
    if 1 <= item <= 1000:    return 1
    if 1001 <= item <= 2000: return 2
    if 2001 <= item <= 3000: return 3
    raise ValueError(item)

def cluster_servers(c):
    base = {1: 1, 2: 13, 3: 25}[c]
    return list(range(base, base + CLUSTER_SIZE))

PRIMARY = {1: 1, 2: 13, 3: 25}

def parse_servers(s):
    return set(int(d) for d in re.findall(r'S(\d+)', s))

def load_sets(path):
    rows = list(csv.reader(open(path, newline='')))
    sets = []
    cur = None
    for r in rows:
        if not any(cell.strip() for cell in r):
            continue
        if r[0].strip():
            if cur:
                sets.append(cur)
            cur = {
                'set': int(r[0]),
                'txns': [],
                'live': parse_servers(r[2]),
                'contact': parse_servers(r[3]),
                'byz': parse_servers(r[4]),
            }
        cur['txns'].append(tuple(int(v) for v in re.findall(r'\d+', r[1])))
    if cur:
        sets.append(cur)
    return sets


class Sim:
    def __init__(self):
        self.balance = {}                       # item -> balance (default 10)
        # per-server ordered datastore of committed entries (strings)
        self.datastore = {f"S{i}": [] for i in range(1, 37)}
        self.events = []                        # log of what happened, per txn

    def bal(self, i):
        return self.balance.get(i, 10)

    def set_bal(self, i, v):
        self.balance[i] = v

    def can_commit(self, cluster, live, byz):
        cs = set(cluster_servers(cluster))
        live_in = cs & live
        byz_in = cs & byz
        honest_live = len(live_in) - len(byz_in)
        return (len(live_in) >= QUORUM) and (honest_live >= QUORUM), len(live_in), len(byz_in), honest_live

    def append_entry(self, cluster, live, entry):
        """Append a committed datastore entry to all LIVE servers of the cluster."""
        for s in cluster_servers(cluster):
            if s in live:
                self.datastore[f"S{s}"].append(entry)

    def run_set(self, st):
        live, byz = st['live'], st['byz']
        for (x, y, amt) in st['txns']:
            cx, cy = cluster_of(x), cluster_of(y)
            if cx == cy:
                self._intra(x, y, amt, cx, live, byz)
            else:
                self._cross(x, y, amt, cx, cy, live, byz)

    def _intra(self, x, y, amt, c, live, byz):
        ok, nlive, nbyz, honest = self.can_commit(c, live, byz)
        if not ok:
            self.events.append(dict(txn=(x, y, amt), type="INTRA", cluster=f"C{c}",
                                     outcome="ABORT_NO_QUORUM",
                                     detail=f"C{c} live={nlive} honest={honest} < quorum {QUORUM}"))
            return
        if self.bal(x) < amt:
            self.events.append(dict(txn=(x, y, amt), type="INTRA", cluster=f"C{c}",
                                     outcome="SKIP_INSUFFICIENT",
                                     detail=f"bal[{x}]={self.bal(x)} < {amt}"))
            return
        # commit
        self.set_bal(x, self.bal(x) - amt)
        self.set_bal(y, self.bal(y) + amt)
        self.append_entry(c, live, f"INTRA ({x},{y},{amt}) COMMIT")
        self.events.append(dict(txn=(x, y, amt), type="INTRA", cluster=f"C{c}",
                                 outcome="COMMIT",
                                 detail=f"bal[{x}]={self.bal(x)} bal[{y}]={self.bal(y)}"))

    def _cross(self, x, y, amt, cx, cy, live, byz):
        # ---- coordinator prepare ----
        okc, nlc, nbc, hc = self.can_commit(cx, live, byz)
        if not okc:
            self.events.append(dict(txn=(x, y, amt), type="CROSS", coord=f"C{cx}", part=f"C{cy}",
                                     outcome="ABORT_NO_QUORUM_COORD",
                                     detail=f"coord C{cx} live={nlc} honest={hc} < quorum {QUORUM}"))
            return
        if self.bal(x) < amt:
            self.events.append(dict(txn=(x, y, amt), type="CROSS", coord=f"C{cx}", part=f"C{cy}",
                                     outcome="SKIP_INSUFFICIENT_COORD",
                                     detail=f"bal[{x}]={self.bal(x)} < {amt} (coordinator ignores)"))
            return
        # coordinator commits prepare: lock x, debit, WAL, append prepare entry
        pre_x = self.bal(x)
        self.set_bal(x, self.bal(x) - amt)            # tentative debit (WAL holds pre_x)
        self.append_entry(cx, live, f"CROSS ({x},{y},{amt}) PREPARE")

        # ---- participant prepare ----
        okp, nlp, nbp, hp = self.can_commit(cy, live, byz)
        if not okp:
            # participant cannot reach consensus -> coordinator times out -> ABORT + undo
            self.set_bal(x, pre_x)                     # WAL-undo
            self.append_entry(cx, live, f"CROSS ({x},{y},{amt}) COMMIT(ABORT)")
            self.events.append(dict(txn=(x, y, amt), type="CROSS", coord=f"C{cx}", part=f"C{cy}",
                                     outcome="ABORT_NO_QUORUM_PART",
                                     detail=f"part C{cy} live={nlp} honest={hp} < quorum {QUORUM}; "
                                            f"coordinator WAL-undo, bal[{x}] restored to {self.bal(x)}"))
            return
        # participant commits prepare: lock y, credit, WAL, append prepare entry
        self.set_bal(y, self.bal(y) + amt)
        self.append_entry(cy, live, f"CROSS ({x},{y},{amt}) PREPARE")

        # ---- commit phase: both append commit entry, release, reply/ack ----
        self.append_entry(cx, live, f"CROSS ({x},{y},{amt}) COMMIT")
        self.append_entry(cy, live, f"CROSS ({x},{y},{amt}) COMMIT")
        self.events.append(dict(txn=(x, y, amt), type="CROSS", coord=f"C{cx}", part=f"C{cy}",
                                 outcome="COMMIT",
                                 detail=f"bal[{x}]={self.bal(x)} bal[{y}]={self.bal(y)}"))


def items_in_file(sets):
    items = set()
    for st in sets:
        for (x, y, amt) in st['txns']:
            items.add(x); items.add(y)
    return sorted(items)


def simulate(path, label):
    sets = load_sets(path)
    sim = Sim()
    snapshots = []   # after each set
    for st in sets:
        sim.run_set(st)
        # snapshot state after this set
        touched = items_in_file([st])
        snap = {
            'set': st['set'],
            'live': sorted(st['live']),
            'contact': sorted(st['contact']),
            'byz': sorted(st['byz']),
            'events': [e for e in sim.events if e['txn'] in [(x, y, a) for (x, y, a) in st['txns']]
                       and sim.events.index(e) >= 0],  # placeholder; rebuilt below
            'balances_all_items': {i: sim.bal(i) for i in items_in_file(sets)},
            'datastore_lengths': {f"S{i}": len(sim.datastore[f"S{i}"]) for i in range(1, 37)},
        }
        snapshots.append(snap)

    # rebuild per-set events cleanly (the inline filter above is unreliable)
    sim2 = Sim()
    snapshots = []
    for st in sets:
        before_events = len(sim2.events)
        sim2.run_set(st)
        new_events = sim2.events[before_events:]
        snapshots.append({
            'set': st['set'],
            'live': sorted(st['live']),
            'contact': sorted(st['contact']),
            'byz': sorted(st['byz']),
            'events': new_events,
            'balances_all_items': {i: sim2.bal(i) for i in items_in_file(sets)},
            'final_datastore_per_cluster': cluster_datastore_view(sim2, st['live']),
            'datastore_lengths': {f"S{i}": len(sim2.datastore[f"S{i}"]) for i in range(1, 37)},
        })

    return sets, sim2, snapshots


def cluster_datastore_view(sim, live):
    """For each cluster, return the datastore as seen by its LIVE servers
    (they agree) and flag any down servers as lagging."""
    view = {}
    for c in (1, 2, 3):
        servers = cluster_servers(c)
        live_servers = [s for s in servers if s in live]
        down_servers = [s for s in servers if s not in live]
        # live servers in a cluster share identical committed history in our model
        rep = sim.datastore[f"S{live_servers[0]}"] if live_servers else []
        view[f"C{c}"] = {
            'committed_entries': list(rep),
            'live_servers': [f"S{s}" for s in live_servers],
            'down_servers_lagging': [f"S{s}" for s in down_servers],
        }
    return view


def write_reports(path, label, sets, sim, snapshots):
    base = path.rsplit('.', 1)[0]
    txt_path = base + "_expected.txt"
    json_path = base + "_expected.json"

    lines = []
    lines.append(f"EXPECTED OUTPUT ORACLE — {label}")
    lines.append(f"Topology: 3 clusters x 12 nodes, f={F}, quorum={QUORUM}")
    lines.append(f"  C1=S1..S12 (items 1-1000) primary S1")
    lines.append(f"  C2=S13..S24 (items 1001-2000) primary S13")
    lines.append(f"  C3=S25..S36 (items 2001-3000) primary S25")
    lines.append("All accounts start at 10. State carries across sets within this file.")
    lines.append("=" * 78)

    for snap in snapshots:
        lines.append("")
        lines.append(f"################  AFTER SET {snap['set']}  ################")
        lines.append(f"Live: {compact(snap['live'])}")
        lines.append(f"Contact (primaries): {['S'+str(s) for s in snap['contact']]}")
        lines.append(f"Byzantine: {['S'+str(s) for s in snap['byz']]}")
        lines.append("")
        lines.append("Transaction outcomes:")
        for e in snap['events']:
            t = e['txn']
            if e['type'] == "INTRA":
                head = f"  ({t[0]},{t[1]},{t[2]}) INTRA {e['cluster']}"
            else:
                head = f"  ({t[0]},{t[1]},{t[2]}) CROSS coord={e['coord']} part={e['part']}"
            lines.append(f"{head:48s} -> {e['outcome']}")
            lines.append(f"        {e['detail']}")
        lines.append("")
        lines.append("PrintBalance (committed balances of items touched in THIS set):")
        touched = sorted(set([t[0] for e in snap['events'] for t in [e['txn']]] +
                             [t[1] for e in snap['events'] for t in [e['txn']]]))
        for i in touched:
            lines.append(f"        bal[{i}] = {snap['balances_all_items'][i]}  "
                         f"(shown on all servers of {('C'+str(cluster_of(i)))}, incl. down/Byzantine)")
        lines.append("")
        lines.append("PrintDatastore (committed entries per cluster, as seen by LIVE servers):")
        for c in ("C1", "C2", "C3"):
            dv = snap['final_datastore_per_cluster'][c]
            lines.append(f"    {c}: {len(dv['committed_entries'])} entries")
            for entry in dv['committed_entries']:
                lines.append(f"          {entry}")
            if dv['down_servers_lagging']:
                lines.append(f"          [lagging/down this set: {dv['down_servers_lagging']}]")
        lines.append("-" * 78)

    # final full-balance snapshot of all touched items
    lines.append("")
    lines.append("================  FINAL BALANCES (all items touched in file)  ================")
    for i in items_in_file(sets):
        lines.append(f"  bal[{i}] = {sim.bal(i)}   (C{cluster_of(i)})")

    open(txt_path, "w").write("\n".join(lines) + "\n")

    # JSON: machine-diffable
    jsnap = []
    for snap in snapshots:
        jsnap.append({
            'set': snap['set'],
            'live': [f"S{s}" for s in snap['live']],
            'contact': [f"S{s}" for s in snap['contact']],
            'byzantine': [f"S{s}" for s in snap['byz']],
            'outcomes': [
                {
                    'txn': list(e['txn']),
                    'type': e['type'],
                    'cluster': e.get('cluster'),
                    'coordinator': e.get('coord'),
                    'participant': e.get('part'),
                    'outcome': e['outcome'],
                    'detail': e['detail'],
                } for e in snap['events']
            ],
            'balances': {str(k): v for k, v in snap['balances_all_items'].items()},
            'datastore_per_cluster': snap['final_datastore_per_cluster'],
            'datastore_lengths': snap['datastore_lengths'],
        })
    out = {
        'label': label,
        'topology': {'clusters': 3, 'cluster_size': 12, 'f': F, 'quorum': QUORUM},
        'sets': jsnap,
        'final_balances': {str(i): sim.bal(i) for i in items_in_file(sets)},
    }
    open(json_path, "w").write(json.dumps(out, indent=2))
    return txt_path, json_path


def compact(servers):
    """Compress a sorted server-id list into ranges for readability."""
    if not servers:
        return "[]"
    out = []
    start = prev = servers[0]
    for s in servers[1:]:
        if s == prev + 1:
            prev = s
        else:
            out.append(f"S{start}-S{prev}" if start != prev else f"S{start}")
            start = prev = s
    out.append(f"S{start}-S{prev}" if start != prev else f"S{start}")
    return "[" + ", ".join(out) + "]"


if __name__ == "__main__":
    jobs = [
        ("Lab4_Testset_1_36node.csv", "Test Set 1 (Intra-Shard)"),
        ("Lab4_Testset_2_36node.csv", "Test Set 2 (Cross-Shard)"),
    ]
    for path, label in jobs:
        sets, sim, snapshots = simulate(path, label)
        txt, js = write_reports(path, label, sets, sim, snapshots)
        print(f"Wrote {txt} and {js}")
