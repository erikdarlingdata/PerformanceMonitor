#!/usr/bin/env python3
"""Replay a window of merged dev commits against a nightly test map (#7728).

What the map would have selected for each commit, as ci-select.py's map_select() answers it, and what the failed CI runs of
the window would have missed. Reports only, writes JSON rows for the PR body. It reads the repository through git and
the map from a file; it never talks to GitHub (the run and failure lists are inputs made with `gh`).

  merged  --map MAP --tree DIR --since DATE [--maps FILE] [--out FILE]
          every first-parent commit of --ref since DATE: the classes selected for its changed files (no drift, no
          age check: the map is taken as current), the FULL ones with their reason, the test minutes from the weight
          tables, and with --maps (a JSON list of {sha, built_at} of nightly maps) how many files dev had moved past
          the newest map at that commit. --slice K/N takes every Nth commit from K, so N processes can share a window.
  report  --map MAP --tree DIR ROWS.json...
          the numbers of `merged` for the --out files of several slices.
  misses --map MAP --tree DIR --runs FILE
          FILE: a JSON list of {id, event, sha, branch, prs, fail_names} (fail_names: full test class names). A miss is a
          failing class the selection for that run's changed files would NOT have run.
          A failing class listed in replay-accepted.txt (next to this script: class, run id, reason) for that run is
          counted as accepted, not as a miss: the change did not cause it.

--tree is a directory holding the source tree at the map's commit (`git archive <map sha>`), so the class scan sees the
tree the map was built from. Standard library only. Run it with `python -I`.
"""
from __future__ import annotations

import argparse
import datetime
import importlib.util
import json
import os
import re
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", ".."))


def _load(name: str, file: str):
    spec = importlib.util.spec_from_file_location(name, os.path.join(HERE, file))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


CS = _load("ci_select", "ci-select.py")
PLAN = _load("ci_plan", "ci-plan.py")


ACCEPTED = os.path.join(HERE, "replay-accepted.txt")


def load_accepted(path: str = ACCEPTED) -> "set[tuple[str, str]]":
    """{(run id, simple class name)} the replay does not count as a miss. One `<Class> <run id>  # reason` a line (#7728)."""
    out: "set[tuple[str, str]]" = set()
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                parts = line.split("#", 1)[0].split()
                if len(parts) >= 2:
                    out.add((parts[1], parts[0]))
    return out


EMPTY_TREE = "4b825dc642cb6eb9a060e54bf8d69288fbee4904"  # what a root commit is diffed against


def git(*args: str, ok: "tuple[int, ...]" = (0,)) -> str:
    """git's stdout. A failure stops the run with the command and git's stderr: an empty answer must never stand in
    for an error, or a window quietly shrinks (#7728). `ok` lists the exit codes that are an answer, not a failure."""
    out = subprocess.run(["git", "-C", ROOT, *args], capture_output=True, text=True, encoding="utf-8", errors="replace")
    if out.returncode not in ok:
        raise SystemExit(f"git {' '.join(args)} failed (exit {out.returncode}): {out.stderr.strip() or 'no error text'}")
    return out.stdout


def parent_of(sha: str) -> str:
    """`sha^`, or the empty tree for a root commit, so a root commit's files are its whole tree."""
    return sha + "^" if len(git("rev-list", "--parents", "-n", "1", sha).split()) > 1 else EMPTY_TREE


def merge_base(base: str, sha: str) -> str:
    mb = git("merge-base", base, sha, ok=(0, 1)).strip()
    if not mb:
        raise SystemExit(f"{sha} has no merge base with {base}")
    return mb


def files_of(sha: str, base: "str | None" = None) -> "list[str]":
    """The files a commit changes against its first parent (the empty tree for a root commit), or against the merge
    base with `base` when one is given. A git failure raises; an empty list means the commit changes no files."""
    since = merge_base(base, sha) if base else parent_of(sha)
    return [x for x in git("diff", "--no-renames", "--name-only", since, sha).split("\n") if x]


def _pct(values: "list[float]", q: float) -> float:
    v = sorted(values)
    return v[min(len(v) - 1, int(round(q * (len(v) - 1))))] if v else 0.0


class Replay:
    def __init__(self, map_path: str, tree: str):
        self.map = CS.load_map(map_path)
        if not isinstance(self.map, dict):
            raise SystemExit(f"cannot read the map {map_path}")
        self.tree_root = tree
        found = CS.scan_classes(tree)
        self.found = {s: v for s, v in found.items() if s in self.map["classes"]}
        self.index = CS.test_file_index(tree)
        self.extra = CS.selection_args(CS.Rules())
        self.weights = {"darling": PLAN.load_weights(PLAN.DARLING_WEIGHTS), "lite": PLAN.load_weights(PLAN.LITE_WEIGHTS)}
        runs = lambda s: [c for c, t in self.found[s].items() if CS.NO_TESTS_TRAIT not in t]
        self.size = {s: len(runs(s)) for s in self.found}
        self.full_minutes = {s: PLAN.suite_minutes(runs(s), self.weights[s], 0.0) for s in self.found}

    def select(self, files: "list[str]", sha: str, base_sha: "str | None" = None, with_named: bool = False) -> dict:
        diff = None
        if any(f.startswith(CS.SHARED_PROJECT_DIRS) for f in files):
            diff = CS.symbol_diff_text(ROOT, base_sha or sha + "^", sha)
        named = None
        if with_named:
            text = CS.symbol_diff_text(ROOT, base_sha or sha + "^", sha)
            if text is not None:
                tokens, _tests = CS.changed_symbols(text)
                named = CS.named_classes(CS.symbol_matches(tokens, set(), self.index, None))
        return CS.map_select(self.map, files, [], self.found, event="pull_request", base_ref="dev", max_age_days=None,
                             shared_diff=diff, named=named, **self.extra)

    def count(self, sel: dict) -> "dict[str, int]":
        if sel["full"]:
            return dict(self.size)
        return {s: len([c for c in sel["selected"].get(s, []) if CS.NO_TESTS_TRAIT not in self.found.get(s, {}).get(c, ())])
                for s in self.found}

    def minutes(self, sel: dict) -> float:
        if sel["full"]:
            return sum(self.full_minutes.values())
        total = 0.0
        for s in self.found:
            names = [c for c in sel["selected"].get(s, []) if CS.NO_TESTS_TRAIT not in self.found[s].get(c, ())]
            total += PLAN.suite_minutes(names, self.weights[s], 0.0)
        return total


def reason_kind(reason: str) -> str:
    if "is a keep-full file" in reason:
        return "keep-full file: " + reason.split(" is a keep-full file")[0]
    if reason.startswith("unknown file"):
        return "unknown file: " + reason.split(":")[1].strip()
    return reason[:110]


def cmd_merged(a) -> int:
    rp = Replay(a.map, a.tree)
    maps = sorted(json.load(open(a.maps)), key=lambda m: m["built_at"]) if a.maps else []
    log = git("log", a.ref, "--first-parent", f"--since={a.since}", "--format=%H%x09%cI%x09%s").split("\n")
    log = [x for x in log if x.strip()]
    if a.slice:  # "K/N": every Nth commit from K, so N processes can share a window (`report` joins their rows)
        k, n = (int(x) for x in a.slice.split("/"))
        log = log[k::n]
    rows = []
    for line in log:
        sha, when, subject = line.split("\t", 2)
        parent = parent_of(sha)
        files = files_of(sha)
        m = re.search(r"\(#(\d+)\)\s*$", subject)
        sel = rp.select(files, sha, base_sha=parent, with_named=True)
        bare = rp.select(files, sha, base_sha=parent)
        n, nb = rp.count(sel), rp.count(bare)
        row = {"sha": sha, "when": when, "pr": int(m.group(1)) if m else None, "files": len(files), "full": sel["full"],
               "reason": sel["reason"], "darling": n.get("darling", 0), "lite": n.get("lite", 0),
               "darling_bare": nb.get("darling", 0), "lite_bare": nb.get("lite", 0),
               "minutes": round(rp.minutes(sel), 1), "minutes_bare": round(rp.minutes(bare), 1)}
        if maps:
            before = [x for x in maps if x["built_at"] <= when]
            if before:
                row["map_sha"] = before[-1]["sha"]
                row["drift"] = len([x for x in git("diff", "--no-renames", "--name-only", before[-1]["sha"], parent).split("\n") if x])
        rows.append(row)
    if a.out:
        with open(a.out, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(rows, fh)
    print(f"map {rp.map['sha'][:10]} built {rp.map['built_at']}; {len(rows)} commits since {a.since}; "
          f"suite sizes {rp.size}; full-suite minutes {sum(rp.full_minutes.values()):.0f} (darling {rp.full_minutes.get('darling', 0):.0f})")
    report(rows, rp)
    return 0


def report(rows: "list[dict]", rp: Replay) -> None:
    total = sum(rp.size.values())
    prod = [r for r in rows if r["files"]]
    print(f"{len(rows)} commits: {len(prod)} change files and are replayed below, {len(rows) - len(prod)} change no files"
          f" (empty commits: counted here, left out of every figure below)")
    full = [r for r in prod if r["full"]]
    part = [r for r in prod if not r["full"]]
    print(f"FULL: {len(full)} of {len(prod)} ({100 * len(full) / max(1, len(prod)):.0f}%)")
    for label, key, mk in (("with the changed-symbol rule", "", "minutes"), ("without it", "_bare", "minutes_bare")):
        counts = [r["darling" + key] + r["lite" + key] for r in prod if not r["full"]]
        shares = [c / total for c in counts]
        mins = [r[mk] for r in prod if not r["full"]]
        print(f"not FULL, {label}: n={len(counts)} classes median {_pct(counts, .5):.0f} p90 {_pct(counts, .9):.0f} max {max(counts or [0])};"
              f" share median {100 * _pct(shares, .5):.0f}% p90 {100 * _pct(shares, .9):.0f}%;"
              f" minutes median {_pct(mins, .5):.1f} p90 {_pct(mins, .9):.1f} (full {sum(rp.full_minutes.values()):.0f})")
    allc = [(total if r["full"] else r["darling"] + r["lite"]) for r in prod]
    allm = [(sum(rp.full_minutes.values()) if r["full"] else r["minutes"]) for r in prod]
    print(f"every commit, FULL counted as the whole suite: classes median {_pct(allc, .5):.0f} p90 {_pct(allc, .9):.0f}; "
          f"share median {100 * _pct([c / total for c in allc], .5):.0f}% p90 {100 * _pct([c / total for c in allc], .9):.0f}%; "
          f"minutes median {_pct(allm, .5):.1f} p90 {_pct(allm, .9):.1f}; total {sum(allm):.0f} against {len(prod) * sum(rp.full_minutes.values()):.0f}")
    kinds: "dict[str, int]" = {}
    for r in full:
        kinds[reason_kind(r["reason"])] = kinds.get(reason_kind(r["reason"]), 0) + 1
    for k, n in sorted(kinds.items(), key=lambda x: -x[1])[:40]:
        print(f"  FULL x{n}: {k}")
    drifts = [r["drift"] for r in prod if "drift" in r]
    if drifts:
        print(f"drift past the newest nightly map at merge time: median {_pct(drifts, .5):.0f} p90 {_pct(drifts, .9):.0f} max {max(drifts)}")


def cmd_misses(a) -> int:
    rp = Replay(a.map, a.tree)
    runs = json.load(open(a.runs))
    accepted = load_accepted()
    out = []
    checked = 0
    for r in runs:
        names = r.get("fail_names") or []
        if not names:
            continue
        try:  # a head that was never fetched must stop the run, not read as a run with no files
            base_sha = None if r["event"] == "push" else merge_base(a.base, r["sha"])
            files = r.get("files") or files_of(r["sha"], None if r["event"] == "push" else a.base)
            if not files:
                out.append({**r, "status": "no changed files found"})
                continue
            base_sha = base_sha or parent_of(r["sha"])
        except SystemExit as e:
            raise SystemExit(f"run {r['id']}: cannot read commit {r['sha']} (fetch it first): {e}")
        sel = rp.select(files, r["sha"], base_sha=base_sha, with_named=True)
        for cls in names:
            prefix, simple = CS.split_failed_class(cls)
            suite = CS.SUITE_BY_PREFIX.get(prefix)
            if suite is None or simple not in rp.found.get(suite, {}):
                out.append({"id": r["id"], "event": r["event"], "class": cls, "status": "class not in the map's tree"})
                continue
            checked += 1
            picked = sel["full"] or simple in sel["selected"].get(suite, [])
            out.append({"id": r["id"], "event": r["event"], "sha": r["sha"][:10], "prs": r.get("prs"), "class": cls,
                        "files": len(files), "first_file": files[0], "full": sel["full"], "reason": sel["reason"][:100],
                        "picked": picked, "why": sel["why"].get(suite, {}).get(simple, ""), "status": "ok",
                        "accepted": (str(r["id"]), simple) in accepted})
    unpicked = [x for x in out if x.get("status") == "ok" and not x["picked"]]
    miss = [x for x in unpicked if not x["accepted"]]
    print(f"{len(runs)} failed runs read, {sum(1 for r in runs if r.get('fail_names'))} with failing classes, "
          f"{checked} failing classes checked, {sum(1 for x in out if x.get('picked') and not x['full'])} picked by the map, "
          f"{sum(1 for x in out if x.get('picked') and x['full'])} on a FULL answer, {len(unpicked) - len(miss)} accepted "
          f"(replay-accepted.txt), {len(miss)} misses")
    for x in miss:
        print("MISS", x["id"], x["event"], x["class"], "files=", x["files"], x["first_file"])
    if a.out:
        with open(a.out, "w", encoding="utf-8", newline="\n") as fh:
            json.dump(out, fh)
    return 0


def cmd_report(a) -> int:
    rp = Replay(a.map, a.tree)
    rows = []
    for f in a.rows:
        rows += json.load(open(f))
    print(f"map {rp.map['sha'][:10]} built {rp.map['built_at']}; {len(rows)} commits; suite sizes {rp.size}; "
          f"full-suite minutes {sum(rp.full_minutes.values()):.0f} (darling {rp.full_minutes.get('darling', 0):.0f})")
    report(rows, rp)
    return 0


def main(argv: "list[str]") -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("merged", "misses"):
        p = sub.add_parser(name)
        p.add_argument("--map", required=True)
        p.add_argument("--tree", required=True)
        p.add_argument("--out")
        if name == "merged":
            p.add_argument("--since", required=True)
            p.add_argument("--ref", default="origin/dev")
            p.add_argument("--maps")
            p.add_argument("--slice")
        else:
            p.add_argument("--runs", required=True)
            p.add_argument("--base", default="origin/dev")
    rep = sub.add_parser("report")
    rep.add_argument("--map", required=True)
    rep.add_argument("--tree", required=True)
    rep.add_argument("rows", nargs="+")
    args = ap.parse_args(argv)
    return {"merged": cmd_merged, "misses": cmd_misses, "report": cmd_report}[args.cmd](args)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
