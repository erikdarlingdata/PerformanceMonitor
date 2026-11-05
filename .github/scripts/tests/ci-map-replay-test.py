#!/usr/bin/env python3
"""Checks for ci-map-replay.py (#7728). Run: python -I .github/scripts/tests/ci-map-replay-test.py

The git reads run against a throwaway repository built in a temp dir (a root commit, a normal commit, an empty
commit, a side branch). The map and the class scan are replaced by a stub, so no nightly map or source tree is
needed. No workflow runs this file: run it by hand when ci-map-replay.py changes."""
import argparse
import contextlib
import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("mr", os.path.join(here, "..", "ci-map-replay.py"))
mr = importlib.util.module_from_spec(spec)
sys.modules["mr"] = mr
spec.loader.exec_module(mr)

failures = []


def check(name, actual, expected):
    if actual != expected:
        failures.append(f"{name}: expected {expected!r}, got {actual!r}")


def run_git(repo, *args):
    out = subprocess.run(["git", "-C", repo, "-c", "user.name=t", "-c", "user.email=t@example.com",
                          "-c", "commit.gpgsign=false", *args], capture_output=True, text=True, check=True)
    return out.stdout.strip()


def commit(repo, name, text):
    with open(os.path.join(repo, name), "w", encoding="utf-8") as fh:
        fh.write(text)
    run_git(repo, "add", name)
    run_git(repo, "commit", "-m", "add " + name)
    return run_git(repo, "rev-parse", "HEAD")


def raises(fn):
    """The SystemExit message of fn(), or None when it returns."""
    try:
        fn()
    except SystemExit as e:
        return str(e.code)
    return None


class StubReplay:
    """Stands in for Replay: no map, no class scan."""
    map = {"sha": "0" * 40, "built_at": "2026-01-01T00:00:00Z"}
    size = {"darling": 4}
    full_minutes = {"darling": 8.0}
    found = {"darling": {"WidgetTests": ()}}

    def __init__(self, map_path, tree):
        pass

    def select(self, files, sha, base_sha=None, with_named=False):
        return {"full": False, "reason": "", "selected": {"darling": ["WidgetTests"]}, "why": {"darling": {}}}

    def count(self, sel):
        return {"darling": 1}

    def minutes(self, sel):
        return 2.0


with tempfile.TemporaryDirectory() as tmp:
    repo = os.path.join(tmp, "repo")
    os.makedirs(repo)
    run_git(repo, "init", "-q", "-b", "main")
    root_sha = commit(repo, "a.txt", "one\n")
    normal_sha = commit(repo, "b.txt", "two\n")
    run_git(repo, "commit", "-q", "--allow-empty", "-m", "empty")
    empty_sha = run_git(repo, "rev-parse", "HEAD")
    run_git(repo, "checkout", "-q", "-b", "side", normal_sha)
    side_sha = commit(repo, "c.txt", "three\n")
    run_git(repo, "checkout", "-q", "main")
    missing_sha = "deadbeef" * 5
    real_root = mr.ROOT
    mr.ROOT = repo
    try:
        # --- files_of --------------------------------------------------------------------------------------
        check("files_of: root commit is counted against the empty tree", mr.files_of(root_sha), ["a.txt"])
        check("files_of: normal commit", mr.files_of(normal_sha), ["b.txt"])
        check("files_of: empty commit changes no files", mr.files_of(empty_sha), [])
        check("files_of: pull request head against the merge base", mr.files_of(side_sha, "main"), ["c.txt"])
        msg = raises(lambda: mr.files_of(missing_sha))
        check("files_of: a missing sha stops the run (raised)", msg is not None, True)
        check("files_of: the message names the sha", msg is not None and missing_sha in msg, True)
        check("files_of: the message names the git command", msg is not None and msg.startswith("git "), True)
        check("files_of: a missing pull request head stops the run", raises(lambda: mr.files_of(missing_sha, "main")) is not None, True)

        # --- merged: the window total adds up ---------------------------------------------------------------
        real_replay = mr.Replay
        mr.Replay = StubReplay
        try:
            out_path = os.path.join(tmp, "rows.json")
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                code = mr.cmd_merged(argparse.Namespace(map="m", tree="t", since="2000-01-01", ref="main", maps=None,
                                                        slice=None, out=out_path))
            check("merged: exit code", code, 0)
            with open(out_path, encoding="utf-8") as fh:
                rows = {r["sha"]: r for r in json.load(fh)}
            check("merged: every commit of the window has a row", sorted(rows), sorted([root_sha, normal_sha, empty_sha]))
            check("merged: the root commit is counted with its files", rows[root_sha]["files"], 1)
            check("merged: the empty commit has no files", rows[empty_sha]["files"], 0)
            check("merged: the empty commits are counted in the output",
                  "3 commits: 2 change files and are replayed below, 1 change no files" in buf.getvalue(), True)

            # --- misses: a pull request head that was never fetched -----------------------------------------
            runs_path = os.path.join(tmp, "runs.json")
            run = {"id": 4242, "event": "pull_request", "sha": missing_sha, "branch": "x", "prs": [1],
                   "fail_names": ["Darling.Tests.WidgetTests"]}
            with open(runs_path, "w", encoding="utf-8") as fh:
                json.dump([run], fh)
            args = argparse.Namespace(map="m", tree="t", runs=runs_path, base="main", out=None)
            with contextlib.redirect_stdout(io.StringIO()):
                msg = raises(lambda: mr.cmd_misses(args))
            check("misses: a head that is not fetched stops the run", msg is not None, True)
            check("misses: the message names the run id", msg is not None and "4242" in msg, True)
            check("misses: the message names the missing sha", msg is not None and missing_sha in msg, True)

            with open(runs_path, "w", encoding="utf-8") as fh:
                json.dump([{**run, "id": 4243, "sha": side_sha}], fh)
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                code = mr.cmd_misses(args)
            check("misses: a fetched pull request head is replayed", code, 0)
            check("misses: ... and is checked, not skipped", "1 failing classes checked" in buf.getvalue(), True)
        finally:
            mr.Replay = real_replay

        # --- select: the map is replayed as current ---------------------------------------------------------
        seen = {}

        def fake_map_select(test_map, changed, drift, discovered, **kw):
            seen["drift"] = drift
            seen["kw"] = kw
            return {"full": False, "reason": "", "selected": {}, "why": {}}

        rp = mr.Replay.__new__(mr.Replay)
        rp.map, rp.found, rp.index, rp.extra = {"classes": {}}, {}, {}, {}
        real_select = mr.CS.map_select
        mr.CS.map_select = fake_map_select
        try:
            rp.select(["a.txt"], normal_sha)
        finally:
            mr.CS.map_select = real_select
        check("select: no drift is passed, the map is taken as current", seen.get("drift"), [])
        check("select: no age limit is passed", seen.get("kw", {}).get("max_age_days", "absent"), None)
    finally:
        mr.ROOT = real_root

if failures:
    print("\n".join(failures))
    sys.exit(1)
print("ci-map-replay: all checks passed")
