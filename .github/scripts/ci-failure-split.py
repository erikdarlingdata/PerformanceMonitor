#!/usr/bin/env python3
"""Split recent failed Build runs into the deliberate "CI not run" marker and real failures (#5943).

A pull request whose CI is declined by the merge-train label still runs one job that fails on purpose
with the annotation title "CI not run" (build.yml, "Report when CI did not run"), so the pull request cannot merge
without an admin override. Counted as an ordinary failure it made up 147 of 200 recent failed runs, and the real
failure rate looked far worse than it was. This script counts that marker apart:

    skip_marker   a failed job carries a "CI not run" annotation: the run never started CI, by design
    gate_failed   a failed job carries a "CI gate failed" annotation: the job that decides whether CI runs failed
    real          anything else: CI ran and something in it failed

Usage (needs an authenticated gh):

    python .github/scripts/ci-failure-split.py [--repo owner/name] [--workflow Build] [--limit 200]

Prints one line per class with its count and its share of the failed runs, then the real failure rate against the
runs where CI ran (successes and cancelled runs count in the denominator, the marker runs do not).
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys

MARKER_TITLE = "CI not run"
GATE_TITLE = "CI gate failed"
CLASSES = ("skip_marker", "gate_failed", "real")


def classify(annotation_titles) -> str:
    """The class of ONE failed run, from the titles of the annotations on its failed jobs."""
    titles = set(annotation_titles)
    if MARKER_TITLE in titles:
        return "skip_marker"
    if GATE_TITLE in titles:
        return "gate_failed"
    return "real"


def summarize(failed_classes, other_runs: int = 0) -> dict:
    """Counts per class, each class's share of the failed runs, and the real failure rate.

    other_runs is every examined run that did not fail (success, cancelled, ...). The real failure rate is real /
    (real + gate_failed + other_runs): the marker runs are left out of the denominator, because CI never ran for them.
    """
    counts = {name: 0 for name in CLASSES}
    for name in failed_classes:
        counts[name] += 1
    failed = sum(counts.values())
    share = {name: (counts[name] / failed if failed else 0.0) for name in CLASSES}
    denominator = counts["real"] + counts["gate_failed"] + other_runs
    return {
        "failed": failed,
        "counts": counts,
        "share": share,
        "real_failure_rate": (counts["real"] / denominator if denominator else 0.0),
    }


def _gh(*args: str):
    out = subprocess.run(["gh", *args], check=True, capture_output=True, text=True).stdout
    return json.loads(out) if out.strip() else None


def _titles_for_run(repo: str, run_id: int) -> list:
    jobs = _gh("api", f"repos/{repo}/actions/runs/{run_id}/jobs?per_page=100", "--jq", '[.jobs[] | select(.conclusion == "failure") | .id]') or []
    titles = []
    for job_id in jobs:
        annotations = _gh("api", f"repos/{repo}/check-runs/{job_id}/annotations?per_page=100", "--jq", "[.[].title]") or []
        titles.extend(t for t in annotations if t)
        if MARKER_TITLE in titles:
            break
    return titles


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--repo", default="erikdarlingdata/PerformanceMonitor")
    parser.add_argument("--workflow", default="Build")
    parser.add_argument("--limit", type=int, default=200)
    args = parser.parse_args(argv)

    runs = _gh("run", "list", "-R", args.repo, "--workflow", args.workflow, "--limit", str(args.limit), "--json", "databaseId,conclusion") or []
    failed_ids = [r["databaseId"] for r in runs if r["conclusion"] == "failure"]
    classes = [classify(_titles_for_run(args.repo, run_id)) for run_id in failed_ids]
    result = summarize(classes, other_runs=len(runs) - len(failed_ids))

    print(f"{len(runs)} runs of {args.workflow}, {result['failed']} failed")
    for name in CLASSES:
        print(f"  {name:12} {result['counts'][name]:4}  {result['share'][name]:6.1%} of failed runs")
    print(f"real failure rate (marker runs left out): {result['real_failure_rate']:.1%}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
