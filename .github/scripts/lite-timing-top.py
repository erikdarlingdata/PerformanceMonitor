#!/usr/bin/env python3
"""Print the slowest Lite.Tests classes and tests from xUnit v3 `-xml` result files (#5208).

Usage: lite-timing-top.py <file-or-directory> [...]
Reads every *.xml given (or found under a directory), sums <test time="..."> seconds per class,
and prints the 25 slowest classes and the 25 slowest tests. Informational only.
"""
import collections
import glob
import os
import sys
import xml.etree.ElementTree as ET

TOP = 25


def files(args):
    for a in args:
        if os.path.isdir(a):
            yield from sorted(glob.glob(os.path.join(a, "**", "*.xml"), recursive=True))
        else:
            yield from sorted(glob.glob(a))


def main(args):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    classes = collections.defaultdict(lambda: [0.0, 0])
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
            tests.append((secs, t.get("name") or "(unnamed)"))
    if not tests:
        print(f"no <test> elements found in {seen} file(s)")
        return 0
    total = sum(v[0] for v in classes.values())
    print(f"{len(tests)} tests in {len(classes)} classes from {seen} file(s); summed test time {total:.1f}s")
    print(f"\n== {TOP} slowest classes (summed seconds) ==")
    for cls, (secs, n) in sorted(classes.items(), key=lambda kv: -kv[1][0])[:TOP]:
        print(f"{secs:9.2f}s {n:5d} tests  {cls}")
    print(f"\n== {TOP} slowest tests ==")
    for secs, name in sorted(tests, reverse=True)[:TOP]:
        print(f"{secs:9.2f}s  {name}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
