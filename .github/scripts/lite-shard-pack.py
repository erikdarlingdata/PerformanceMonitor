#!/usr/bin/env python3
"""Duration-aware shard assignment for the Lite test matrix, with a proof that it loses no class (#5208).

  lite-shard-pack.py pack      --classes FILE --shards N --out DIR [--timings DIR ...]
  lite-shard-pack.py reconcile --classes FILE --shards N --out DIR
  lite-shard-pack.py --self-test

`pack` reads the full discovered class list (one class per line) and writes DIR/shard-K.txt for K in
0..N-1. Weights come from xUnit v3 `-xml` result files (sum of <test time> per `type`). Classes are placed
longest-first into the shard with the least accumulated time; ties break on class name, then shard index,
so the same inputs always give the same assignment. A class with no timing history gets the MEAN weight of
the known classes. When the timings are missing, unreadable, empty, or cover less than MIN_COVERAGE of the
discovered classes, the assignment falls back to the class-name hash (SHA-256 first byte modulo N), the cut
the workflow used before this script existed.

`reconcile` is the guard: it fails (exit 1) unless the shard files hold every discovered class exactly once
and nothing else, and prints both differences. `pack` runs it before it exits, so a bad packer cannot
produce a green run, and the workflow runs it again on the files each shard actually reads.
"""
import collections
import glob
import hashlib
import os
import sys
import tempfile
import xml.etree.ElementTree as ET

MIN_COVERAGE = 0.8


def read_classes(path):
    with open(path, encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]


def load_weights(dirs):
    """Seconds per class from every *.xml under dirs. Returns {} when nothing usable was read."""
    weights = collections.defaultdict(float)
    for d in dirs:
        paths = sorted(glob.glob(os.path.join(d, "**", "*.xml"), recursive=True)) if os.path.isdir(d) else sorted(glob.glob(d))
        for p in paths:
            try:
                root = ET.parse(p).getroot()
            except (ET.ParseError, OSError):
                continue
            for t in root.iter("test"):
                cls = t.get("type")
                if not cls:
                    continue
                try:
                    secs = float(t.get("time") or 0)
                except ValueError:
                    continue
                weights[cls] += secs
    return dict(weights)


def hash_shard(name, shards):
    return hashlib.sha256(name.encode("utf-8")).digest()[0] % shards


def pack(classes, shards, weights):
    """Returns (assignment: list of lists, method). Never drops or duplicates a class."""
    unique = sorted(set(classes))
    known = {c: weights[c] for c in unique if c in weights}
    usable = bool(unique) and bool(known) and len(known) / len(unique) >= MIN_COVERAGE
    out = [[] for _ in range(shards)]
    if not usable:
        for c in unique:
            out[hash_shard(c, shards)].append(c)
        return out, "hash"
    default = sum(known.values()) / len(known)
    weight = {c: known.get(c, default) for c in unique}
    load = [0.0] * shards
    for c in sorted(unique, key=lambda c: (-weight[c], c)):
        k = min(range(shards), key=lambda i: (load[i], i))
        out[k].append(c)
        load[k] += weight[c]
    return out, "duration"


def reconcile(classes, assignment):
    """Returns a list of problems; empty means the shards are exactly the discovered set, once each."""
    full = set(classes)
    seen = collections.Counter(c for shard in assignment for c in shard)
    missing = sorted(full - set(seen))
    extra = sorted(set(seen) - full)
    dup = sorted(c for c, n in seen.items() if n > 1)
    problems = []
    if missing:
        problems.append(f"{len(missing)} discovered class(es) in NO shard (would never run): " + ", ".join(missing[:50]))
    if extra:
        problems.append(f"{len(extra)} shard class(es) not in the discovered set: " + ", ".join(extra[:50]))
    if dup:
        problems.append(f"{len(dup)} class(es) in more than one shard: " + ", ".join(dup[:50]))
    if not full:
        problems.append("the discovered class set is empty")
    return problems


def write_shards(out, assignment):
    os.makedirs(out, exist_ok=True)
    for k, shard in enumerate(assignment):
        with open(os.path.join(out, f"shard-{k}.txt"), "w", encoding="utf-8", newline="\n") as f:
            f.write("".join(c + "\n" for c in shard))


def read_shards(out, shards):
    result = []
    for k in range(shards):
        p = os.path.join(out, f"shard-{k}.txt")
        result.append(read_classes(p) if os.path.exists(p) else [])
    return result


def run_reconcile(classes, assignment):
    problems = reconcile(classes, assignment)
    for p in problems:
        print("::error title=Lite shard reconciliation failed::" + p)
    if problems:
        print("RECONCILIATION FAILED: the shard lists are not exactly the discovered class set.")
        return 1
    print(f"reconciled: {len(set(classes))} classes, each in exactly one of {len(assignment)} shards")
    return 0


def arg(args, name, default=None, many=False):
    vals = [args[i + 1] for i, a in enumerate(args) if a == name and i + 1 < len(args)]
    if many:
        return vals
    return vals[-1] if vals else default


def main(argv):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if "--self-test" in argv:
        return self_test()
    if not argv or argv[0] not in ("pack", "reconcile"):
        print(__doc__)
        return 2
    cmd, rest = argv[0], argv[1:]
    classes = read_classes(arg(rest, "--classes"))
    shards = int(arg(rest, "--shards"))
    out = arg(rest, "--out")
    if cmd == "reconcile":
        return run_reconcile(classes, read_shards(out, shards))
    weights = load_weights(arg(rest, "--timings", many=True))
    assignment, method = pack(classes, shards, weights)
    write_shards(out, assignment)
    load = [sum(weights.get(c, 0.0) for c in s) for s in assignment]
    print(f"packed {len(set(classes))} classes into {shards} shards by {method}; "
          f"{len(weights)} classes timed; shard sizes {[len(s) for s in assignment]}; "
          f"timed seconds per shard {[round(x) for x in load]}")
    return run_reconcile(classes, read_shards(out, shards))


def self_test():
    checks = 0

    def ok(cond, msg):
        nonlocal checks
        checks += 1
        if not cond:
            print("SELF-TEST FAILED: " + msg)
            raise SystemExit(1)

    names = [f"Lite.Tests.C{i:03d}" for i in range(60)]
    # Heavily skewed like the real data: a few classes carry most of the time.
    skew = {n: (600.0 if i < 3 else 30.0 if i < 12 else 0.05) for i, n in enumerate(names)}

    for shards in (1, 2, 3, 4, 7):
        for w in (skew, {}, {n: 1.0 for n in names}):
            a, _ = pack(names, shards, w)
            ok(reconcile(names, a) == [], f"totality {shards} shards")
            ok(sorted(c for s in a for c in s) == sorted(names), "union equals input")
    a1, m1 = pack(names, 4, skew)
    a2, m2 = pack(list(reversed(names)), 4, skew)
    ok(a1 == a2 and m1 == m2 == "duration", "deterministic regardless of input order")
    load = [sum(skew[c] for c in s) for s in a1]
    ok(max(load) - min(load) <= 600.0, "skewed input spreads the three heavy classes")
    ok(len({next(c for c in s if skew[c] == 600.0) for s in a1 if any(skew[c] == 600.0 for c in s)}) == 3, "heavy classes on different shards")

    # A class with no timing history is assigned, not dropped.
    known = {n: skew[n] for n in names[:-2]}
    a, m = pack(names, 4, known)
    ok(m == "duration" and reconcile(names, a) == [], "unknown classes still assigned")
    ok(names[-1] in {c for s in a for c in s}, "the new class is in a shard")

    # Missing, empty and drifted timing data fall back to the hash and drop nothing.
    for w in ({}, {"Other.Gone": 5.0}, {n: 1.0 for n in names[:10]}):
        a, m = pack(names, 4, w)
        ok(m == "hash" and reconcile(names, a) == [], "fallback is the hash and total")
        ok(all(c in a[hash_shard(c, 4)] for c in names), "fallback matches the class-name hash cut")
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "bad.xml"), "w") as f:
            f.write("<assemblies><not closed")
        ok(load_weights([d]) == {}, "corrupt xml reads as no data")
        ok(load_weights([os.path.join(d, "missing")]) == {}, "missing dir reads as no data")
        with open(os.path.join(d, "good.xml"), "w") as f:
            f.write('<assemblies><assembly><collection><test type="A" time="1.5"/><test type="A" time="0.5"/>'
                    '<test type="B" time="x"/></collection></assembly></assemblies>')
        ok(load_weights([d]) == {"A": 2.0}, "sums per class, skips unparsable time")

    # The guard must FAIL when a class is withheld, duplicated or invented.
    a, _ = pack(names, 4, skew)
    withheld = [list(s) for s in a]
    gone = withheld[2].pop()
    p = reconcile(names, withheld)
    ok(len(p) == 1 and gone in p[0] and "NO shard" in p[0], "withheld class is reported by name")
    with tempfile.TemporaryDirectory() as d, open(os.path.join(d, "c.txt"), "w") as f:
        f.write("\n".join(names) + "\n")
        f.flush()
        write_shards(d, withheld)
        print("--- expected failure below (a class was deliberately withheld) ---")
        ok(run_reconcile(read_classes(f.name), read_shards(d, 4)) == 1, "reconcile exits 1 when a class is withheld")
        print("--- end expected failure ---")
        write_shards(d, a)
        ok(run_reconcile(read_classes(f.name), read_shards(d, 4)) == 0, "reconcile passes on the real pack")
    dup = [list(s) for s in a]
    dup[0].append(dup[1][0])
    ok(any("more than one" in x for x in reconcile(names, dup)), "duplicate detected")
    inv = [list(s) for s in a]
    inv[0].append("Invented.Class")
    ok(any("not in the discovered" in x for x in reconcile(names, inv)), "invented class detected")
    ok(reconcile([], [[], []]) != [], "empty discovered set fails")

    print(f"self-test: {checks} assertions passed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
