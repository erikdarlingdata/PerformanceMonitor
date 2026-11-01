#!/usr/bin/env python3
"""Duration-balanced shard assignment for the Darling PostgreSQL test matrix (#5669).

  darling-shard-pack.py pack    --classes FILE --shards N --shard K [--weights FILE]
  darling-shard-pack.py refresh --timings DIR [DIR ...] --out FILE [--tests-dir DIR]
  darling-shard-pack.py --self-test

`pack` reads the discovered class list (one class per line), assigns every class to one of N shards, and prints the
classes of shard K, one per line, after one comment line that starts with '#'. Every shard runs the same
deterministic assignment on the same list, so each class lands in exactly one shard; the script proves that before it
prints. Classes go longest-first into the least-loaded shard (ties break on class name, then shard index) by the
weights in the committed table (darling-shard-weights.json). A class the table does not know weighs the MEAN of the
known ones. When the table is missing, unreadable, empty, or knows under MIN_COVERAGE of the listed classes, the cut
falls back to the class-name hash (SHA-256, first four bytes, modulo N), the cut the workflow used before this script.
A weight for a class the test sources no longer declare is left out, with one warning line each on stderr (#5833).

A weight is the wall-clock cost of a class in one shard, in seconds, not its raw test seconds (the model is fitted on
24 shard runs of four full dev runs and is within about 18 s of a shard's wall; raw seconds were 56 s off):
  - the `live-postgres` collection runs its ~450 classes one after another, in parallel with the rest of the shard,
    so a second there costs CHAIN_WEIGHT of a wall second;
  - a class in a collection declared `DisableParallelization = true` runs alone, so a second costs a whole one;
  - any other class overlaps about PARALLEL_CONCURRENCY deep (the assembly runs 4 collection threads).
The hash cut left the chain at 95 to 375 s per shard, which is what made one shard 4 to 6 minutes slower than the mean.

`refresh` rebuilds the table from the per-test timings the Darling PG shards upload on every run
(since #6656 the `ci-test-results-N` artifacts of a PR CI run hold `darling-timing-*.xml` and `lite-timing-*.xml`, xUnit `-xml`, kept a week). Use FULL runs (a push to dev runs every class; a pull request
runs the test map's selection, so its timings cover part of the suite), the more the steadier (a class's seconds swing
with what runs beside it); the table holds the mean over the runs given:

  for id in <run id> <run id> <run id>; do gh run download $id -R erikdarlingdata/PerformanceMonitor \\
      -p 'ci-test-results-*' -D <scratch>/$id; done
  python .github/scripts/darling-shard-pack.py refresh --timings <scratch>/<id> <scratch>/<id> ... \\
      --out .github/scripts/darling-shard-weights.json
Each --timings dir must hold one kind only: copy the darling-timing-*.xml files of a run into <scratch>/darling/<id> and the
lite-timing-*.xml files into <scratch>/lite/<id>. The Lite table is the same command over the lite dirs with
--tests-dir Lite.Tests --out .github/scripts/lite-shard-weights.json. Then replay the new tables against runs left out of the
mean (pack each suite's classes into the leg count, sum the measured weights per leg) before committing them.

Refresh when the shards drift apart again (the slowest shard more than about 20 % over the mean), after a large batch
of new live classes, or about once a month. A stale table only costs balance, never a class: a renamed or new class
weighs the mean and still runs in exactly one shard.
"""
import collections
import glob
import hashlib
import importlib.util
import io
import json
import math
import os
import struct
import sys
import tempfile
import xml.etree.ElementTree as ET

MIN_COVERAGE = 0.8
MIN_WEIGHT = 0.001
# Fitted on the 24 shard runs of four full dev runs: wall = 0.8 * chain + parallel / 4 + serial - 57 s, mean absolute
# error 18 s (best single constant for the chain was 0.8, for the overlap 4.0, the number of collection threads).
CHAIN_COLLECTION = "live-postgres"
CHAIN_WEIGHT = 0.8
PARALLEL_CONCURRENCY = 4.0
HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_WEIGHTS = os.path.join(HERE, "darling-shard-weights.json")
DEFAULT_TESTS_DIR = os.path.join(HERE, "..", "..", "Darling", "Darling.Tests")


def hash_shard(name, shards):
    """The cut the PowerShell script made before this packer: first four SHA-256 bytes as a little-endian uint32."""
    return struct.unpack("<I", hashlib.sha256(name.encode("utf-8")).digest()[:4])[0] % shards


def load_table(path):
    """{class: weight} from the committed table, or {} when it is missing, unreadable or malformed."""
    try:
        with open(path, encoding="utf-8") as f:
            raw = json.load(f).get("classes", {})
    except (OSError, ValueError, AttributeError):
        return {}
    out = {}
    for name, w in raw.items() if isinstance(raw, dict) else []:
        if isinstance(w, (int, float)) and not isinstance(w, bool) and math.isfinite(w) and w >= 0:
            out[name] = float(w)
    return out


def pack(classes, shards, table):
    """Returns (assignment: list of sorted class lists, method, loads). Never drops or duplicates a class."""
    unique = sorted(set(classes))
    known = {c: table[c] for c in unique if c in table}
    usable = bool(unique) and bool(known) and len(known) / len(unique) >= MIN_COVERAGE and sum(known.values()) > 0
    out = [[] for _ in range(shards)]
    loads = [0.0] * shards
    if not usable:
        for c in unique:
            out[hash_shard(c, shards)].append(c)
        return out, "hash", loads
    default = sum(known.values()) / len(known)
    # The floor keeps a shard from staying empty while another takes several zero-time classes.
    weight = {c: max(known.get(c, default), MIN_WEIGHT) for c in unique}
    for c in sorted(unique, key=lambda c: (-weight[c], c)):
        k = min(range(shards), key=lambda i: (loads[i], i))
        out[k].append(c)
        loads[k] += weight[c]
    return [sorted(shard) for shard in out], "duration", loads


def reconcile(classes, assignment):
    """Empty string when the shards hold every class exactly once and nothing else, else the difference."""
    seen = collections.Counter(c for shard in assignment for c in shard)
    want = set(classes)
    dup = sorted(c for c, n in seen.items() if n > 1)
    missing = sorted(want - set(seen))
    extra = sorted(set(seen) - want)
    if not (dup or missing or extra):
        return ""
    return f"duplicated={dup[:5]} missing={missing[:5]} extra={extra[:5]}"


def _lite_pack():
    spec = importlib.util.spec_from_file_location("lite_shard_pack", os.path.join(HERE, "lite-shard-pack.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.dont_write_bytecode = True  # no __pycache__ beside the scripts in a checkout
    spec.loader.exec_module(mod)
    return mod


def without_missing_classes(table, err=None):
    """#5833: the table less every class the test sources no longer declare, each named on one stderr line (stdout is
    the shard's class list). The check is lite-shard-pack.py's, so the two packers judge a stale weight the same way."""
    mod = _lite_pack()
    kept, warnings = mod.drop_missing_classes(table, mod.declared_class_names(), "darling-shard-weights.json")
    for line in warnings:
        print(line, file=err or sys.stderr)
    return kept


def _serial_names(tests_dir):
    names, unresolved = _lite_pack().serial_collection_names(tests_dir)
    if unresolved:
        raise SystemExit(f"refresh: could not resolve the collection name of {unresolved}")
    return names


def class_weights(timing_dirs, serial):
    """{class: mean weight over the runs} from every *.xml under each dir; one dir is one run."""
    per_run = []
    for d in timing_dirs:
        secs = collections.defaultdict(float)
        kind = {}
        paths = sorted(glob.glob(os.path.join(d, "**", "*.xml"), recursive=True)) if os.path.isdir(d) else sorted(glob.glob(d))
        for p in paths:
            try:
                root = ET.parse(p).getroot()
            except (ET.ParseError, OSError):
                continue
            for col in root.iter("collection"):
                name = col.get("name") or ""
                for t in col.iter("test"):
                    cls, raw = t.get("type"), t.get("time")
                    if not cls or raw is None:
                        continue
                    try:
                        s = float(raw)
                    except ValueError:
                        continue
                    if not math.isfinite(s) or s < 0:
                        continue
                    secs[cls] += s
                    kind[cls] = CHAIN_WEIGHT if name == CHAIN_COLLECTION else (1.0 if name in serial else 1.0 / PARALLEL_CONCURRENCY)
        if secs:
            per_run.append({c: s * kind[c] for c, s in secs.items()})
    total = collections.defaultdict(list)
    for run in per_run:
        for c, w in run.items():
            total[c].append(w)
    return {c: sum(v) / len(v) for c, v in total.items()}, len(per_run)


def write_table(path, weights, runs):
    body = ",\n".join("    " + json.dumps(c) + ": " + format(round(weights[c], 2), "g") for c in sorted(weights))
    text = ('{\n  "note": "Per-class wall-second weights for darling-shard-pack.py (#5669); rebuild with its `refresh` subcommand. '
            'live-postgres classes x %s, DisableParallelization classes x 1, any other class / %s; mean of %d full runs.",\n'
            '  "classes": {\n%s\n  }\n}\n') % (CHAIN_WEIGHT, PARALLEL_CONCURRENCY, runs, body)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        f.write(text)
    json.loads(text)


def arg(args, name, default=None, many=False):
    if name not in args:
        return [] if many else default
    i = args.index(name) + 1
    if not many:
        return args[i] if i < len(args) else default
    out = []
    while i < len(args) and not args[i].startswith("--"):
        out.append(args[i])
        i += 1
    return out


def main(argv):
    if argv[:1] == ["--self-test"]:
        return self_test()
    cmd, args = (argv[0] if argv else ""), argv[1:]
    if cmd == "pack":
        classes_file, shards, shard = arg(args, "--classes"), int(arg(args, "--shards", "0")), int(arg(args, "--shard", "-1"))
        if not classes_file or shards < 1 or not 0 <= shard < shards:
            print("pack: --classes FILE --shards N --shard K (0 <= K < N) are required", file=sys.stderr)
            return 2
        with open(classes_file, encoding="utf-8") as f:
            classes = [ln.strip() for ln in f if ln.strip()]
        out, method, loads = pack(classes, shards, without_missing_classes(load_table(arg(args, "--weights", DEFAULT_WEIGHTS))))
        problem = reconcile(classes, out)
        if problem:
            print(f"pack: the assignment lost or repeated a class ({problem})", file=sys.stderr)
            return 1
        print(f"# method={method} classes={len(set(classes))} shard_loads={','.join(format(x, '.0f') for x in loads)}")
        for c in out[shard]:
            print(c)
        return 0
    if cmd == "refresh":
        dirs, out_path = arg(args, "--timings", many=True), arg(args, "--out")
        if not dirs or not out_path:
            print("refresh: --timings DIR [DIR ...] --out FILE are required", file=sys.stderr)
            return 2
        weights, runs = class_weights(dirs, _serial_names(arg(args, "--tests-dir", DEFAULT_TESTS_DIR)))
        if not weights:
            print("refresh: no usable timings under the given directories", file=sys.stderr)
            return 1
        write_table(out_path, weights, runs)
        print(f"wrote {len(weights)} classes from {runs} run(s) to {out_path}")
        return 0
    print(__doc__)
    return 2


def self_test():
    # Hash fallback matches the cut the PowerShell script made: first four bytes, little endian.
    assert hash_shard("Darling.Tests.Foo", 6) == struct.unpack("<I", hashlib.sha256(b"Darling.Tests.Foo").digest()[:4])[0] % 6
    names = [f"N.C{i}" for i in range(40)]
    table = {n: float(1 + (i * 7) % 13) for i, n in enumerate(names)}
    a, method, loads = pack(names, 4, table)
    assert method == "duration" and reconcile(names, a) == "", (method, reconcile(names, a))
    assert pack(list(reversed(names)), 4, table)[0] == a, "the same class set must give the same assignment in any order"
    assert max(loads) - min(loads) <= max(table.values()), loads
    # A class missing from the table weighs the mean and is still assigned exactly once.
    more = names + ["N.NewClass"]
    b, method, _ = pack(more, 4, table)
    assert method == "duration" and reconcile(more, b) == ""
    # Too little coverage, no table, or an all-zero table falls back to the hash cut, and still loses nothing.
    for bad in ({}, {n: 0.0 for n in names}, {names[0]: 5.0}):
        c, method, _ = pack(names, 4, bad)
        assert method == "hash" and reconcile(names, c) == "", method
    # More shards than classes leaves shards empty but never drops a class.
    d, _, _ = pack(names[:3], 6, {n: 1.0 for n in names[:3]})
    assert reconcile(names[:3], d) == "" and sum(1 for s in d if s) == 3
    # The longest class does not share a shard with the next longest while a shard is empty.
    e, _, _ = pack(["A", "B", "C"], 3, {"A": 9.0, "B": 8.0, "C": 1.0})
    assert sorted(len(s) for s in e) == [1, 1, 1]
    # Table loading: a malformed file is no table.
    with tempfile.TemporaryDirectory() as tmp:
        p = os.path.join(tmp, "w.json")
        with open(p, "w", encoding="utf-8") as f:
            f.write("{not json")
        assert load_table(p) == {} and load_table(os.path.join(tmp, "missing.json")) == {}
        write_table(p, {"N.A": 1.234, "N.B": 0.0}, 2)
        assert load_table(p) == {"N.A": 1.23, "N.B": 0.0}
    # class_weights: the chain, a serial collection and a parallel one weigh differently.
    with tempfile.TemporaryDirectory() as tmp:
        with open(os.path.join(tmp, "t.xml"), "w", encoding="utf-8") as f:
            f.write('<assemblies><assembly>'
                    '<collection name="live-postgres"><test type="N.Live" time="10"/></collection>'
                    '<collection name="serial-one"><test type="N.Ser" time="10"/></collection>'
                    '<collection name="other"><test type="N.Par" time="8"/><test type="N.Par" time="2"/></collection>'
                    '</assembly></assemblies>')
        w, runs = class_weights([tmp], {"serial-one"})
        assert runs == 1 and w == {"N.Live": 8.0, "N.Ser": 10.0, "N.Par": 2.5}, w
    # A weight for a class no test source declares is left out (with its warning line on stderr).
    err = io.StringIO()
    kept = without_missing_classes({"Darling.Tests.DarlingShardPackNoSuchClassTests": 1.0}, err)
    assert kept == {} and err.getvalue().count("DarlingShardPackNoSuchClassTests") == 1, (kept, err.getvalue())
    print("darling-shard-pack self-test passed")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
