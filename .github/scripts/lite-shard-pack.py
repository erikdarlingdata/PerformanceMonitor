#!/usr/bin/env python3
"""Duration-aware shard assignment for the Lite test matrix, with a proof that it loses no class (#5208).

  lite-shard-pack.py pack      --classes FILE --shards N --out DIR [--timings DIR ...] [--tests-dir DIR]
  lite-shard-pack.py reconcile --classes FILE --shards N --out DIR
  lite-shard-pack.py --self-test

`pack` reads the full discovered class list (one class per line) and writes DIR/shard-K.txt for K in
0..N-1. Weights come from xUnit v3 `-xml` result files (sum of <test time> per `type`; a test with no `time`,
or a NaN, infinite or negative one, has no timing). Classes are placed longest-first into the shard with the
least accumulated time; ties break on class name, then shard index, so the same inputs always give the same
assignment. A class that runs one at a time weighs its test seconds; any other class weighs its test seconds
divided by PARALLEL_CONCURRENCY, because that is how many seconds of parallel classes fit in one wall second.
Serial classes are the ones whose xUnit collection is declared `DisableParallelization = true`: the XML names each
class's collection, and the declarations are read from the test sources (`--tests-dir`, default Lite.Tests at
the repo root), so there is no hand-kept list to rot. Every class weighs at least MIN_WEIGHT, so a class whose tests were all skipped still moves a
shard's load and no shard is left empty. Each class's seconds are blended with a share
proportional to its test count (COUNT_BLEND, below), because one run's per-class seconds are noisy. A class with no
timing history gets the MEAN weight of the known classes. When the timings are missing, unreadable, empty, all zero, or cover less than MIN_COVERAGE of the
discovered classes, the assignment falls back to the class-name hash (SHA-256 first byte modulo N), the cut
the workflow used before this script existed.

`reconcile` is the guard: it fails (exit 1) unless the shard files hold every discovered class exactly once
and nothing else, and prints both differences. `pack` runs it before it exits, so a bad packer cannot
produce a green run, and the workflow runs it again on the files each shard actually reads.
"""
import collections
import glob
import hashlib
import math
import os
import re
import sys
import tempfile
import xml.etree.ElementTree as ET

MIN_COVERAGE = 0.8
MIN_WEIGHT = 0.001
# Effective concurrency of the parallel classes in a Lite shard (#5208): least squares over the 36 shard walls of nine
# full dev and train runs (each shard's own parallel and serial seconds against its assembly wall), where
# wall = parallel seconds / 3.56 + serial seconds. A serial class (DisableParallelization collection) runs alone, so
# each of its seconds costs a whole wall second; a parallel class's seconds overlap about 3.56 deep (the assembly
# runs 4 collection threads). The old 3.4 over-predicted by 16 s on average; a free serial multiplier or a fixed
# per-shard cost fits no better than this one constant, so the model keeps its shape.
PARALLEL_CONCURRENCY = 3.56
# Share of a class's weight that comes from its test count instead of its measured seconds (#5208). The fit above
# is good (3.5 % mean error on a run's own seconds, 2.0 % after the re-fit), but a class's seconds are not stable
# from run to run: they inflate under contention with whatever else shares the shard (one class took 37 s in one
# run and 421 s in another, same tests), so one run's seconds predicted a shard's wall 26 % off on average and cut
# worse than the class-name hash. Blending half the seconds with half a count-proportional share (each group scaled
# to keep its total seconds) took the replayed cut of seven dev and train runs from a mean max/min shard ratio of
# 2.25 to 1.56, and the mean slowest shard from 680 s to 600 s (modelled on each run's own seconds).
COUNT_BLEND = 0.5
DEFAULT_TESTS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "Lite.Tests")


def read_classes(path):
    with open(path, encoding="utf-8") as f:
        return [ln.strip() for ln in f if ln.strip()]


def timed_tests(dirs):
    """Yields (class, seconds) for every test under dirs that has a usable time."""
    for d in dirs:
        paths = sorted(glob.glob(os.path.join(d, "**", "*.xml"), recursive=True)) if os.path.isdir(d) else sorted(glob.glob(d))
        for p in paths:
            try:
                root = ET.parse(p).getroot()
            except (ET.ParseError, OSError):
                continue
            for t in root.iter("test"):
                cls = t.get("type")
                raw = t.get("time")
                if not cls or raw is None:
                    continue
                try:
                    secs = float(raw)
                except ValueError:
                    continue
                if not math.isfinite(secs) or secs < 0:
                    continue
                yield cls, secs


def load_weights(dirs):
    """Seconds per class from every *.xml under dirs. Returns {} when nothing usable was read."""
    weights = collections.defaultdict(float)
    for cls, secs in timed_tests(dirs):
        weights[cls] += secs
    return dict(weights)


def load_counts(dirs):
    """Timed tests per class from every *.xml under dirs: the same tests load_weights sums."""
    counts = collections.Counter(cls for cls, _ in timed_tests(dirs))
    return dict(counts)


def load_collections(dirs):
    """{class: xUnit collection name} from every *.xml under dirs (the <collection name> a test sits in)."""
    result = {}
    for d in dirs:
        paths = sorted(glob.glob(os.path.join(d, "**", "*.xml"), recursive=True)) if os.path.isdir(d) else sorted(glob.glob(d))
        for p in paths:
            try:
                root = ET.parse(p).getroot()
            except (ET.ParseError, OSError):
                continue
            for col in root.iter("collection"):
                name = col.get("name")
                if not name:
                    continue
                for t in col.iter("test"):
                    if t.get("type"):
                        result[t.get("type")] = name
    return result


_SERIAL_DEF = re.compile(r"\[\s*CollectionDefinition\(\s*([^,)]+?)\s*,\s*DisableParallelization\s*=\s*true\b")


def serial_collection_names(tests_dir):
    """Names of the collections declared `[CollectionDefinition(name, DisableParallelization = true)]` under
    tests_dir, plus the declarations it could not resolve to a name. `name` is a string literal or a
    `Holder.Member` constant, found as `const string Member = "..."` in a class Holder."""
    texts = []
    for p in sorted(glob.glob(os.path.join(tests_dir, "**", "*.cs"), recursive=True)) if os.path.isdir(tests_dir) else []:
        try:
            with open(p, encoding="utf-8-sig") as f:
                texts.append(f.read())
        except OSError:
            continue
    names, unresolved = set(), []
    for text in texts:
        for m in _SERIAL_DEF.finditer(text):
            expr = m.group(1).strip()
            if len(expr) >= 2 and expr[0] == expr[-1] == '"':
                names.add(expr[1:-1])
                continue
            holder, _, member = expr.rpartition(".")
            found = None
            if holder:
                pat = re.compile(r"\bclass\s+" + re.escape(holder.rpartition(".")[2]) + r"\b[\s\S]{0,400}?const\s+string\s+"
                                 + re.escape(member) + r'\s*=\s*"([^"]*)"')
                for t in texts:
                    mm = pat.search(t)
                    if mm:
                        found = mm.group(1)
                        break
            if found is None:
                unresolved.append(expr)
            else:
                names.add(found)
    return names, unresolved


def hash_shard(name, shards):
    return hashlib.sha256(name.encode("utf-8")).digest()[0] % shards


def class_weight(raw, is_serial):
    """Wall-clock cost of a class: a serial class's seconds, or a parallel class's seconds spread over the overlap."""
    return raw if is_serial else raw / PARALLEL_CONCURRENCY


def blend_weights(known, counts, serial, blend=COUNT_BLEND):
    """Seconds per class, each blended with a test-count share (#5208): (1 - blend) * its seconds + blend * its
    test count * (the group's seconds per test), the serial and the parallel group each scaled to keep their own
    total seconds. A class with no count, a group with no counts, or no `counts` at all keeps its measured seconds."""
    est = dict(known)
    if not counts or blend <= 0:
        return est
    for want_serial in (True, False):
        group = [c for c in sorted(known) if (c in serial) == want_serial and counts.get(c, 0) > 0]
        total_n = sum(counts[c] for c in group)
        total_s = sum(known[c] for c in group)
        if total_n <= 0 or total_s <= 0:
            continue
        per_test = total_s / total_n
        for c in group:
            est[c] = (1 - blend) * known[c] + blend * counts[c] * per_test
    return est


def predicted_walls(assignment, weights, serial, counts=None):
    """Modelled wall seconds per shard: parallel seconds / PARALLEL_CONCURRENCY + serial seconds, on the same blended
    estimates `pack` balanced. A class with no timing counts at the mean of the timed classes, as `pack` places it
    (#5208)."""
    assigned = sorted({c for shard in assignment for c in shard})
    est = blend_weights({c: weights[c] for c in assigned if c in weights}, counts, serial)
    default = sum(est.values()) / len(est) if est else 0.0
    return [sum(class_weight(est.get(c, default), c in serial) for c in shard) for shard in assignment]


def pack(classes, shards, weights, serial=frozenset(), counts=None):
    """Returns (assignment: list of lists, method). Never drops or duplicates a class. `serial` is the set of
    classes that run one at a time; with none, every weight is scaled alike and the cut is the plain sum cut.
    `counts` is the timed test count per class; with it each class's seconds are blended with a count share."""
    unique = sorted(set(classes))
    known = {c: weights[c] for c in unique if c in weights}
    usable = bool(unique) and bool(known) and len(known) / len(unique) >= MIN_COVERAGE and sum(known.values()) > 0
    out = [[] for _ in range(shards)]
    if not usable:
        for c in unique:
            out[hash_shard(c, shards)].append(c)
        return out, "hash"
    est = blend_weights(known, counts, serial)
    default = sum(est.values()) / len(est)
    # The floor makes an empty shard impossible whenever there are at least as many classes as shards: a zero-time
    # class (every test skipped) would otherwise never move a shard's load off zero.
    weight = {c: max(class_weight(est.get(c, default), c in serial), MIN_WEIGHT) for c in unique}
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
    timings = arg(rest, "--timings", many=True)
    weights = load_weights(timings)
    counts = load_counts(timings)
    tests_dir = arg(rest, "--tests-dir", DEFAULT_TESTS_DIR)
    serial_names, unresolved = serial_collection_names(tests_dir)
    for u in unresolved:
        print(f"::warning title=Lite shard packer::could not resolve the serial collection name {u}; its classes are weighed as parallel")
    if not serial_names:
        print(f"::warning title=Lite shard packer::no DisableParallelization collection found under {tests_dir}; every class is weighed as parallel")
    serial = {c for c, col in load_collections(timings).items() if col in serial_names}
    if serial_names and not serial:
        print("::warning title=Lite shard packer::serial collections are declared in the sources but no timed class sits in one of them; every class is weighed as parallel")
    assignment, method = pack(classes, shards, weights, serial, counts)
    write_shards(out, assignment)
    load = [sum(weights.get(c, 0.0) for c in s) for s in assignment]
    walls = predicted_walls(assignment, weights, serial, counts)
    print(f"packed {len(set(classes))} classes into {shards} shards by {method}; "
          f"{len(weights)} classes timed; shard sizes {[len(s) for s in assignment]}; "
          f"timed seconds per shard {[round(x) for x in load]}; "
          f"{len(serial)} serial classes, per shard {[sum(1 for c in s if c in serial) for s in assignment]}; "
          f"predicted wall seconds per shard {[round(x) for x in walls]}")
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

    # Fewer timed classes than shards, the rest at 0 s (every test skipped): the MIN_WEIGHT floor spreads the
    # zero-time classes, so no shard goes empty. Without it the lowest-index empty shard took every one of them.
    many = [f"Lite.Tests.Z{i:03d}" for i in range(100)]
    a, m = pack(many, 4, {n: (900.0 if i < 2 else 0.0) for i, n in enumerate(many)})
    ok(m == "duration" and [len(s) for s in a] == [1, 1, 49, 49], "zero-time classes spread instead of emptying a shard")

    # Missing, empty and drifted timing data fall back to the hash and drop nothing.
    for w in ({}, {"Other.Gone": 5.0}, {n: 1.0 for n in names[:10]}):
        a, m = pack(names, 4, w)
        ok(m == "hash" and reconcile(names, a) == [], "fallback is the hash and total")
        ok(all(c in a[hash_shard(c, 4)] for c in names), "fallback matches the class-name hash cut")
    # All-zero timings (no `time` on any test, or every test skipped) carry no information: hash, not a cut that
    # piles every class onto shard 0.
    a, m = pack(names, 4, {n: 0.0 for n in names})
    ok(m == "hash" and reconcile(names, a) == [], "all-zero timings fall back to the hash")
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "bad.xml"), "w") as f:
            f.write("<assemblies><not closed")
        ok(load_weights([d]) == {}, "corrupt xml reads as no data")
        ok(load_weights([os.path.join(d, "missing")]) == {}, "missing dir reads as no data")
        with open(os.path.join(d, "good.xml"), "w") as f:
            f.write('<assemblies><assembly><collection><test type="A" time="1.5"/><test type="A" time="0.5"/>'
                    '<test type="B" time="x"/></collection></assembly></assemblies>')
        ok(load_weights([d]) == {"A": 2.0}, "sums per class, skips unparsable time")
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "t.xml"), "w") as f:
            f.write('<a><test type="A"/><test type="B" time="nan"/><test type="C" time="-1"/><test type="D" time="2"/>'
                    '<test type="E" time="inf"/></a>')
        ok(load_weights([d]) == {"D": 2.0}, "no time, NaN, infinite and negative times are not timings")
        ok(load_counts([d]) == {"D": 1}, "a test with no usable time is not counted either")

    # Serial-aware weighting (#5208). Three parallel classes of 1000 s and one of 300 s fix the loads; twelve serial
    # classes of 50 s then all land on the lightest shard under a plain sum cut, where each serial second costs a
    # whole wall second but a parallel one only 1/PARALLEL_CONCURRENCY.
    par = {"Lite.Tests.P0": 1000.0, "Lite.Tests.P1": 1000.0, "Lite.Tests.P2": 1000.0, "Lite.Tests.P3": 300.0}
    ser = {f"Lite.Tests.S{i:02d}": 50.0 for i in range(12)}
    fw = {**par, **ser}
    fc = sorted(fw)
    fs = frozenset(ser)
    plain, _ = pack(fc, 4, fw)
    aware, m = pack(fc, 4, fw, fs)
    ok(m == "duration" and reconcile(fc, aware) == [] and reconcile(fc, plain) == [], "serial fixture packs and reconciles")
    ok(max(sum(1 for c in s if c in fs) for s in plain) >= 10, "fixture: a plain sum cut piles the serial classes on one shard")
    ok(max(sum(1 for c in s if c in fs) for s in aware) <= 6, "serial classes spread: no shard takes more than half of them")
    pw = predicted_walls(plain, fw, fs)
    aw = predicted_walls(aware, fw, fs)
    ok(max(pw) / min(pw) > 1.25, "fixture: the plain sum cut predicts unbalanced walls")
    ok(max(aw) / min(aw) <= 1.25, "serial-aware cut predicts walls within 1.25x")
    ok(max(aw) < max(pw), "serial-aware cut lowers the slowest predicted shard")
    ok(pack(list(reversed(fc)), 4, fw, fs)[0] == aware, "serial-aware cut is deterministic regardless of input order")
    ok(class_weight(34.0, True) == 34.0 and abs(class_weight(34.0, False) - 34.0 / PARALLEL_CONCURRENCY) < 1e-9, "serial weighs full, parallel is divided")

    # The wall model and the count blend (#5208 re-fit). A shard's modelled wall is its parallel seconds over the
    # concurrency plus its serial seconds, with no per-shard constant: the fit found none worth keeping.
    pw = predicted_walls([["Lite.Tests.P", "Lite.Tests.S"]], {"Lite.Tests.P": 356.0, "Lite.Tests.S": 10.0}, frozenset({"Lite.Tests.S"}))
    ok(abs(pw[0] - (356.0 / PARALLEL_CONCURRENCY + 10.0)) < 1e-9, "wall = parallel seconds / concurrency + serial seconds")
    ok(3.4 < PARALLEL_CONCURRENCY < 3.8, "the concurrency stays inside the range the nine fitted runs support")
    # Equal test counts, one class measured five times slower: the blend halves the gap and keeps the group's total.
    bw = blend_weights({"A": 100.0, "B": 20.0}, {"A": 10, "B": 10}, frozenset())
    ok(abs(bw["A"] - 80.0) < 1e-9 and abs(bw["B"] - 40.0) < 1e-9, "blend pulls a noisy class toward its test-count share")
    ok(abs(sum(bw.values()) - 120.0) < 1e-9, "blend keeps the total seconds")
    sb = blend_weights({"A": 100.0, "B": 20.0, "S": 50.0}, {"A": 10, "B": 10, "S": 1}, frozenset({"S"}))
    ok(sb["S"] == 50.0 and abs(sb["A"] - 80.0) < 1e-9, "serial and parallel classes are scaled in separate groups")
    ok(blend_weights({"A": 100.0, "B": 20.0}, None, frozenset()) == {"A": 100.0, "B": 20.0}, "no counts: seconds untouched")
    ok(blend_weights({"A": 100.0, "B": 20.0}, {"B": 4}, frozenset()) == {"A": 100.0, "B": 20.0}, "a class with no count keeps its seconds")
    # Eight classes with the same test count, one measured ten times slower in the single run read: the blend keeps
    # the outlier from deciding the cut alone, and the blended pack still reconciles and is order independent.
    noisy = {f"Lite.Tests.N{i}": 20.0 for i in range(8)}
    noisy["Lite.Tests.N0"] = 200.0
    nc = {c: 10 for c in noisy}
    nclasses = sorted(noisy)
    plain_cut, _ = pack(nclasses, 2, noisy)
    blend_cut, mb = pack(nclasses, 2, noisy, frozenset(), nc)
    ok(mb == "duration" and reconcile(nclasses, blend_cut) == [], "blended pack reconciles")
    ok(sorted(len(x) for x in plain_cut) == [1, 7], "fixture: seconds alone give the outlier a shard to itself")
    ok(sorted(len(x) for x in blend_cut) == [3, 5], "blended weights give the outlier two more classes instead of a shard to itself")
    ok(pack(list(reversed(nclasses)), 2, noisy, frozenset(), nc)[0] == blend_cut, "blended pack is deterministic regardless of input order")
    pwb = predicted_walls(blend_cut, noisy, frozenset(), nc)
    ok(abs(sum(pwb) * PARALLEL_CONCURRENCY - sum(noisy.values())) < 1e-6, "predicted walls use the blended estimates, which keep the total")

    # Collection names from the XML, and serial declarations read from sources (literal and Holder.Name forms).
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "t.xml"), "w") as f:
            f.write('<assemblies><assembly><collection name="Gate"><test type="A" time="1"/><test type="B" time="1"/></collection>'
                    '<collection name="Test collection for C (id: 1)"><test type="C" time="1"/></collection></assembly></assemblies>')
        ok(load_collections([d]) == {"A": "Gate", "B": "Gate", "C": "Test collection for C (id: 1)"}, "XML collection names per class")
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "x.cs"), "w", encoding="utf-8") as f:
            f.write('[CollectionDefinition("Lit", DisableParallelization = true)]\npublic class A {}\n'
                    '[CollectionDefinition(Holder.Name, DisableParallelization = true)]\npublic class B {}\n'
                    '[CollectionDefinition("Par")]\npublic class C {}\n'
                    '[CollectionDefinition("Off", DisableParallelization = false)]\npublic class D {}\n'
                    'public static class Holder\n{\n    public const string Name = "from-const";\n}\n'
                    '[CollectionDefinition(Missing.Name, DisableParallelization = true)]\npublic class E {}\n')
        cnames, unresolved = serial_collection_names(d)
        ok(cnames == {"Lit", "from-const"} and unresolved == ["Missing.Name"], "serial collections: literal and constant resolved, parallel skipped, unknown reported")
    # The real test sources: every DisableParallelization definition must resolve to a name, or the packer would
    # silently weigh that collection as parallel.
    if os.path.isdir(DEFAULT_TESTS_DIR):
        cnames, unresolved = serial_collection_names(DEFAULT_TESTS_DIR)
        ok(unresolved == [], "every serial collection in Lite.Tests resolves to a name: " + ", ".join(unresolved))
        # The scan must find at least one serial collection; no collection name is hard-coded here, so renaming a
        # collection does not fail the shard-0 job (#5208).
        ok(len(cnames) > 0, "Lite.Tests declares at least one serial collection")

    # The guard must FAIL when a class is withheld, duplicated or invented. `names` is the 60-class list from the top
    # of the self-test; the checks below must run on it, not on collection names (#5208 review F1).
    ok(len(names) == 60, "self-test class list intact")
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
