#!/usr/bin/env python3
"""Print the slowest Lite.Tests classes and tests from xUnit v3 `-xml` result files (#5208).

Usage: lite-timing-top.py <file-or-directory> [...]
       lite-timing-top.py --self-test
Reads every *.xml given (or found under a directory), sums <test time="..."> seconds per class,
and prints the 25 slowest classes and the 25 slowest tests. Informational only.

When the tests carry the `cpu-ms` attachment (Lite.Tests' per-test CPU accountant, #5208), each class line also shows
the managed CPU seconds charged to it, a second list ranks the top classes by CPU, and the coverage line says how much
of the process's CPU the charged total explains: the sum of every cpu-ms against the process's own CPU time, read from
the `<xml>.cpu.json` file the run wrote beside each XML. Native DuckDB threads and work that does not flow the
execution context are not charged, so the ratio is the honest limit of any CPU weight.
"""
import collections
import contextlib
import glob
import io
import json
import os
import sys
import tempfile
import xml.etree.ElementTree as ET

TOP = 25
TOP_CPU = 10


def _expand(args, suffix):
    for a in args:
        if os.path.isdir(a):
            yield from sorted(glob.glob(os.path.join(a, "**", "*" + suffix), recursive=True))
        else:
            yield from sorted(p for p in glob.glob(a) if p.endswith(suffix))


def files(args):
    return _expand(args, ".xml")


def sidecars(args):
    return _expand(args, ".cpu.json")


def test_cpu_seconds(t):
    """Seconds from a <test>'s `cpu-ms` attachment, or None when it has none or it is not a finite non-negative number."""
    for a in t.iter("attachment"):
        if a.get("name") == "cpu-ms":
            try:
                ms = float((a.text or "").strip())
            except ValueError:
                return None
            return ms / 1000.0 if 0 <= ms < float("inf") else None
    return None


def coverage(args):
    """(charged cpu seconds, process cpu seconds, files read) summed over every sidecar."""
    charged = process = 0.0
    n = 0
    for p in sidecars(args):
        try:
            with open(p, encoding="utf-8") as f:
                d = json.load(f)
            c = float(d["cpu_ms"]) / 1000.0
            pr = float(d["process_cpu_ms"]) / 1000.0
        except (OSError, ValueError, KeyError, TypeError):
            print(f"skipped {p}: not a CPU coverage file")
            continue
        charged += c
        process += pr
        n += 1
    return charged, process, n


def main(args):
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    if "--self-test" in args:
        return self_test()
    classes = collections.defaultdict(lambda: [0.0, 0, 0.0, 0])  # wall s, tests, cpu s, tests with cpu
    tests = []
    seen = 0
    for path in files(args):
        seen += 1
        try:
            root = ET.parse(path).getroot()
        except (ET.ParseError, OSError) as e:
            print(f"skipped {path}: {e}")
            continue
        for t in root.iter("test"):
            try:
                secs = float(t.get("time") or 0)
            except ValueError:
                continue
            cls = t.get("type") or "(unknown)"
            classes[cls][0] += secs
            classes[cls][1] += 1
            cpu = test_cpu_seconds(t)
            if cpu is not None:
                classes[cls][2] += cpu
                classes[cls][3] += 1
            tests.append((secs, t.get("name") or "(unnamed)"))
    if not tests:
        print(f"no <test> elements found in {seen} file(s)")
        return 0
    total = sum(v[0] for v in classes.values())
    with_cpu = sum(v[3] for v in classes.values())
    print(f"{len(tests)} tests in {len(classes)} classes from {seen} file(s); summed test time {total:.1f}s")
    print(f"\n== {TOP} slowest classes (summed seconds; cpu = managed CPU charged to the class's tests) ==")
    for cls, (secs, n, cpu, ncpu) in sorted(classes.items(), key=lambda kv: -kv[1][0])[:TOP]:
        col = f"{cpu:8.2f}s cpu" if ncpu else "     n/a cpu"
        print(f"{secs:9.2f}s {col} {n:5d} tests  {cls}")
    if with_cpu:
        print(f"\n== {TOP_CPU} classes with the most CPU ==")
        for cls, (secs, n, cpu, ncpu) in sorted(classes.items(), key=lambda kv: -kv[1][2])[:TOP_CPU]:
            print(f"{cpu:8.2f}s cpu {secs:9.2f}s wall {n:5d} tests  {cls}")
    print(f"\n== {TOP} slowest tests ==")
    for secs, name in sorted(tests, reverse=True)[:TOP]:
        print(f"{secs:9.2f}s  {name}")
    charged, process, n = coverage(args)
    if with_cpu or n:
        summed = sum(v[2] for v in classes.values())
        line = f"\nCPU coverage: {with_cpu} of {len(tests)} tests carry cpu-ms; charged {summed:.1f}s"
        if n and process > 0:
            line += f"; process CPU {process:.1f}s over {n} run file(s); ratio {charged / process:.1%}"
        else:
            line += "; no process CPU file found"
        print(line)
    return 0


def self_test():
    def ok(cond, msg):
        if not cond:
            print("SELF-TEST FAILED: " + msg)
            raise SystemExit(1)

    def test(name, cls, secs, cpu_ms=None):
        att = f'<attachments><attachment name="cpu-ms">{cpu_ms}</attachment></attachments>' if cpu_ms is not None else ""
        return f'<test name="{name}" type="{cls}" time="{secs}">{att}</test>'

    def run(path):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            ok(main([path]) == 0, "main exits 0")
        return buf.getvalue()

    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "a.xml"), "w") as f:
            f.write("<assemblies><assembly><collection>"
                    + test("A.t1", "A", 2.0, 1500) + test("A.t2", "A", 1.0, 500)
                    + test("B.t1", "B", 5.0, 100) + test("C.t1", "C", 0.5) + test("C.t2", "C", 0.5, "junk")
                    + "</collection></assembly></assemblies>")
        with open(os.path.join(d, "a.xml.cpu.json"), "w") as f:
            f.write('{"cpu_ms":2100,"process_cpu_ms":4200,"tests":4}')
        root = ET.parse(os.path.join(d, "a.xml")).getroot()
        ok([test_cpu_seconds(t) for t in root.iter("test")] == [1.5, 0.5, 0.1, None, None], "cpu-ms parsed; absent or junk is None")
        ok(list(files([d])) == [os.path.join(d, "a.xml")], "a sidecar is not read as a result file")
        ok(list(sidecars([d])) == [os.path.join(d, "a.xml.cpu.json")], "the sidecar is found")
        ok(coverage([d]) == (2.1, 4.2, 1), "coverage sums the sidecars")
        out = run(d)
        ok("CPU coverage: 3 of 5 tests carry cpu-ms; charged 2.1s; process CPU 4.2s over 1 run file(s); ratio 50.0%" in out,
           "coverage line, got: " + out[-200:])
        top = out.split("classes with the most CPU ==")[1].splitlines()[1]
        ok(top.rstrip().endswith("A") and "2.00s cpu" in top, "A has the most CPU: " + top)
        first = out.split("slowest classes")[1].splitlines()[1]
        ok(first.rstrip().endswith("B") and "0.10s cpu" in first, "the wall list shows each class's cpu: " + first)
    with tempfile.TemporaryDirectory() as d:
        with open(os.path.join(d, "b.xml"), "w") as f:
            f.write("<assemblies><assembly>" + test("A.t1", "A", 1.0) + "</assembly></assemblies>")
        out = run(d)
        ok("CPU coverage" not in out and "n/a cpu" in out, "XML without cpu-ms prints no coverage line")
    print("self-test ok")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
