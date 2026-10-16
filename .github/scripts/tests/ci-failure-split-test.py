#!/usr/bin/env python3
"""Checks for ci-failure-split.py (#5943). Run: python -I .github/scripts/tests/ci-failure-split-test.py"""
import importlib.util
import os
import sys

here = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("split", os.path.join(here, "..", "ci-failure-split.py"))
split = importlib.util.module_from_spec(spec)
spec.loader.exec_module(split)

failures = []


def check(name, actual, expected):
    if actual != expected:
        failures.append(f"{name}: expected {expected!r}, got {actual!r}")


check("marker", split.classify(["CI not run"]), "skip_marker")
check("marker wins over a gate title", split.classify(["CI gate failed", "CI not run"]), "skip_marker")
check("gate", split.classify(["CI gate failed"]), "gate_failed")
check("real, no annotations", split.classify([]), "real")
check("real, other annotation", split.classify(["Test failed"]), "real")

# 147 marker runs of 200 failed runs, as measured: the rest are real, and 100 more runs succeeded.
result = split.summarize(["skip_marker"] * 147 + ["real"] * 50 + ["gate_failed"] * 3, other_runs=100)
check("failed", result["failed"], 200)
check("marker share", round(result["share"]["skip_marker"], 3), 0.735)
check("real rate leaves the marker out", round(result["real_failure_rate"], 4), round(50 / 153, 4))
check("empty", split.summarize([])["real_failure_rate"], 0.0)

if failures:
    print("\n".join(failures))
    sys.exit(1)
print("ci-failure-split: all checks passed")
