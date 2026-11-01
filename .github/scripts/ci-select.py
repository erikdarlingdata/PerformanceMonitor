#!/usr/bin/env python3
"""The ONE place CI's test-selection rules live (#5459).

Input: the changed files of a run (and the event). Output: which test jobs run and in what scope.

These readers use the same code:

* build.yml's jobs call `lite-scope`, `darling-scope` and `tree-scope` with the area answers
  dorny/paths-filter already computed, and get the decision back as `key=value` lines for $GITHUB_OUTPUT.
* `map-select` answers, from a class-to-file map, which test classes a pull request needs (build.yml's
  test-map-select job writes it as an artifact), and `map-keep` reads that artifact for one suite: the Darling PG,
  Lite and no-store Darling passes of a pull request keep only those classes, and run everything when the answer
  is FULL, missing or unreadable. See map_select() for the map's schema. `--replay --map FILE` also replays the
  corpus against such a map and reports what it would miss (it does not fail on that yet); with `--symbols BASE`
  it also prints the classes each row selects before and after the changed-symbol reason (#5798).
* `local-classes` and `local-filter` are for a lane on its own machine. `local-classes` lists, most specific first,
  the test classes whose source names a symbol the branch declares or edits (#5776). `local-filter` (#5798) prints
  the filter CI's selection gives the branch: per suite, one line is the test executable's arguments
  (`darling: -class "*.A" -class "*.B"`) and the next is the class list (`darling classes: A B`), and the last line
  is the guard set (`guard: -trait Stage=Guard`). With `--map` (the nightly test map) it is map-select's own answer;
  without one, or when that answer is FULL or over 300 classes, it is the part a lane can run, at most 300: the
  classes of a changed test file, the map's text-pattern classes for the changed files (with `--map`) and the
  classes that name a changed symbol, which CI selects too. A note on stderr says which. A suite with more than 40
  classes gets one filter line per 40. Run it as
  `python .github/scripts/ci-select.py local-filter [--base origin/dev] [--map test-map.json.gz]`.
* `quarantine` (#5942) decides, once per pull request run in the test-map-select job, which entries of
  .github/ci-quarantine.txt the run honors, and `quarantine-args` turns the decision into `-method- <test>` runner
  arguments for the Guard job and the two shard runners. See quarantine_decide() for the rule.
* `--replay` reads .github/ci-history/failures.jsonl (a trimmed record of every run with a real test failure)
  and checks that each failing class would have been selected for that run's changed files. A cut to the rules
  lands only while the replay finds 0 misses. A Guard-tagged test runs it on every pull request.

The path patterns are NOT copied here. They are read from the filter blocks in .github/workflows/build.yml and
from .github/darling-paths-filter.yml and .github/lite-paths-filter.yml, the same text dorny/paths-filter
evaluates, so the replay and the
workflow cannot disagree about which area a file belongs to. What lives here is the decision logic that
used to be spread over `if:` expressions and a bash if-chain.

Standard library only (the Windows and Linux runners both have Python 3).
"""
from __future__ import annotations

import argparse
import concurrent.futures
import datetime
import gzip
import json
import os
import re
import subprocess
import sys
import tempfile
import xml.etree.ElementTree as ET

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
WORKFLOW = os.path.join(ROOT, ".github", "workflows", "build.yml")
DARLING_FILTER = os.path.join(ROOT, ".github", "darling-paths-filter.yml")
LITE_FILTER = os.path.join(ROOT, ".github", "lite-paths-filter.yml")
TREE_FILTER = os.path.join(ROOT, ".github", "darling-tree-paths-filter.yml")
CORPUS = os.path.join(ROOT, ".github", "ci-history", "failures.jsonl")
ACCEPTED = os.path.join(ROOT, ".github", "ci-history", "replay-accepted.txt")


# ---------------------------------------------------------------------------------------------------------
# Glob matching: the subset of micromatch (dot: true) the filter files use.
# ---------------------------------------------------------------------------------------------------------

def _segment_regex(seg: str) -> str:
    """One path segment (no '/') as a regex fragment that matches a whole segment name."""
    m = re.fullmatch(r"!\((.*)\)", seg)
    if m:
        inner = _segment_regex(m.group(1))
        return rf"(?!(?:{inner})$)[^/]+"
    out: list[str] = []
    i = 0
    while i < len(seg):
        c = seg[i]
        if c == "*":
            out.append("[^/]*")
        elif c == "?":
            out.append("[^/]")
        elif c == "{":
            j = seg.index("}", i)
            alts = seg[i + 1:j].split(",")
            out.append("(?:" + "|".join(_segment_regex(a) for a in alts) + ")")
            i = j
        else:
            out.append(re.escape(c))
        i += 1
    return "".join(out)


def compile_glob(pattern: str) -> re.Pattern[str]:
    parts = pattern.split("/")
    rx = ""
    for idx, seg in enumerate(parts):
        last = idx == len(parts) - 1
        if seg == "**":
            rx += ".*" if last else "(?:[^/]+/)*"
        else:
            rx += _segment_regex(seg) + ("" if last else "/")
    return re.compile(rx + r"\Z")


_GLOB_CACHE: dict[str, re.Pattern[str]] = {}


def matches(pattern: str, path: str) -> bool:
    rx = _GLOB_CACHE.get(pattern)
    if rx is None:
        rx = _GLOB_CACHE[pattern] = compile_glob(pattern)
    return rx.match(path) is not None


# ---------------------------------------------------------------------------------------------------------
# Reading the filter blocks.
# ---------------------------------------------------------------------------------------------------------

_PATTERN_LINE = re.compile(r"^\s*-\s+'([^']*)'")
_KEY_LINE = re.compile(r"^(\s*)([A-Za-z0-9_-]+):\s*(?:#.*)?$")


def _parse_filter_lines(lines: list[str]) -> dict[str, list[str]]:
    filters: dict[str, list[str]] = {}
    current: list[str] | None = None
    for line in lines:
        s = line.strip()
        if not s or s.startswith("#"):
            continue
        p = _PATTERN_LINE.match(line)
        if p:
            if current is not None:
                current.append(p.group(1))
            continue
        k = _KEY_LINE.match(line)
        if k:
            current = filters.setdefault(k.group(2), [])
    return filters


def load_workflow_filters(path: str = WORKFLOW) -> dict[str, dict[str, list[str]]]:
    """{job id: {filter name: [patterns]}} for every inline `filters: |` block in the workflow."""
    with open(path, encoding="utf-8") as fh:
        lines = fh.read().split("\n")
    out: dict[str, dict[str, list[str]]] = {}
    job = ""
    in_jobs = False
    i = 0
    while i < len(lines):
        line = lines[i]
        if line.startswith("jobs:"):
            in_jobs = True
        elif in_jobs and re.match(r"^  [A-Za-z0-9_-]+:\s*$", line):
            job = line.strip().rstrip(":")
        m = re.match(r"^(\s+)filters:\s*\|\s*$", line)
        if m:
            base = len(m.group(1))
            block: list[str] = []
            i += 1
            while i < len(lines) and (not lines[i].strip() or len(lines[i]) - len(lines[i].lstrip()) > base):
                block.append(lines[i])
                i += 1
            out[job] = _parse_filter_lines(block)
            continue
        i += 1
    return out


def load_file_filters(path: str = DARLING_FILTER) -> dict[str, list[str]]:
    with open(path, encoding="utf-8") as fh:
        return _parse_filter_lines(fh.read().split("\n"))


class Rules:
    """The pattern sets, loaded once."""

    def __init__(self, workflow: str = WORKFLOW, darling_filter: str = DARLING_FILTER, lite_filter: str = LITE_FILTER,
                 tree_filter: str = TREE_FILTER) -> None:
        wf = load_workflow_filters(workflow)
        self.build = wf["build"]
        # The Lite shard sets live in their own file (#5669), the one the shard legs and the lite-scope job give
        # dorny/paths-filter, so both engines read the same text. A missing or empty file raises: no silent none.
        self.lite_shards = load_file_filters(lite_filter)
        if not self.lite_shards:
            raise SystemExit(f"{lite_filter} holds no filters")
        self.tree = load_file_filters(tree_filter)
        self.gate = load_file_filters(darling_filter)

    @staticmethod
    def _hits(patterns: list[str], files: list[str]) -> list[str]:
        return [f for f in files if any(matches(p, f) for p in patterns)]

    def areas(self, files: list[str]) -> dict[str, bool]:
        """Every area answer dorny/paths-filter gives a job, by name, for one list of changed files."""
        out: dict[str, bool] = {}
        for name, pats in self.build.items():
            out[name] = bool(self._hits(pats, files))
        out["docs_count"] = len(self._hits(self.build["docs"], files))  # type: ignore[assignment]
        out["all_count"] = len(files)  # type: ignore[assignment]
        for name, pats in self.gate.items():
            out["gate_" + name] = bool(self._hits(pats, files))
        for name, pats in self.lite_shards.items():
            out[name] = bool(self._hits(pats, files))
        out["workflow"] = any(matches(p, f) for p in self.tree["workflow"] for f in files)
        out["lite_tree_count"] = len(self._hits(self.tree["lite_tree"], files))  # type: ignore[assignment]
        return out


# ---------------------------------------------------------------------------------------------------------
# The decisions.
# ---------------------------------------------------------------------------------------------------------

def darling_scope(event: str, runs: bool, shards_run: bool, full_paths: bool = True) -> str:
    """What the build job's no-store "Run Darling tests" step runs: full, guard (only the Stage=Guard classes),
    reads-lite (the Guard classes plus the Reads=Lite ones) or none.

    The PostgreSQL shards run every Darling.Tests class, with a live store, whenever the Darling gate
    (.github/darling-paths-filter.yml) matches. The no-store pass would repeat their non-live classes, so while the
    shards run it keeps only the Guard stage (cut 3b of #5459; the replay finds no failing class it stops covering).
    A release always runs the whole suite.

    Cut 3a of #5459: the step also fires for a Lite file alone (Darling.Tests reads Lite source in its parity guards,
    so a Lite-only change has to reach the suite). On a pull request that reaches it ONLY through those Lite paths
    (`full_paths` false: no Darling file, shared library or root build file changed) the classes that read Lite are
    the only ones the change can affect, so it runs the Guard stage and the classes tagged Reads=Lite, the mirror of
    Lite's `reads` mode for a Darling-only change. Push and merge-queue runs keep the whole suite, as Lite's do."""
    if not runs:
        return "none"
    if event != "release" and shards_run:
        return "guard"
    if event == "pull_request" and not full_paths:
        return "reads-lite"
    return "full"


def tree_scope(event: str, all_count: int, lite_count: int) -> str:
    """What darling-tree-guards runs once it has decided to run: full, or reads-lite (cut 3c of #5459).

    The job runs the Darling suite when the build job's Darling step was skipped, which for a change made entirely
    of Lite files (a Lite.Tests edit, a Lite file type the `darling` filter does not name) leaves only the classes
    that read Lite to run. Every changed file must be under Lite/ or Lite.Tests/, and only a pull request narrows."""
    if event == "pull_request" and all_count > 0 and lite_count == all_count:
        return "reads-lite"
    return "full"


def scope_filter(scope: str) -> str:
    """The Darling.Tests runner arguments for a scope ('' is the whole suite). `-trait Reads=Lite` is a token the consumers
    expand (#5961): no assembly trait of that name exists, the classes come from `reads-classes --suite darling`."""
    if scope == "guard":
        return "-trait Stage=Guard"
    if scope == "reads-lite":
        return "-trait Stage=Guard -trait Reads=Lite"
    return ""


def lite_scope(event: str, lite: bool, core: bool, root: bool, reads: bool, linked: bool) -> str:
    """How much of the Lite suite a pull request runs: full, reads (only Reads=Darling classes) or none."""
    if lite or core or root or linked:
        return "full"
    if reads and event != "pull_request":
        return "full"
    if reads:
        return "reads"
    return "none"


def decide(files: list[str], event: str, rules: Rules) -> dict:
    """Which jobs run, and with which filter, for one change. Everything the workflow decides, in one place."""
    a = rules.areas(files)
    release = event == "release"
    all_count, docs_count = a["all_count"], a["docs_count"]
    docs_only = all_count > 0 and all_count == docs_count
    area_names = ("root", "core", "lite", "darling")
    fastpath = (not release) and docs_only and not any(a[n] for n in area_names)

    build_lite = release or a["lite"] or a["core"] or a["root"]
    build_darling = release or a["darling"] or a["core"] or a["root"]

    pg_run = (not release) and a["gate_darling"]
    # The tree guards run when the build job's Darling step did not (or build.yml itself changed).
    tree_run = (not release) and not fastpath and (not build_darling or a["workflow"])

    mode = lite_scope(event, a["lite_shard"], a["core_shard"], a["root_shard"], a["darling_reads_shard"],
                      a["lite_linked_shard"]) if not release else "release"

    return {
        "guard_run": (not release) and not docs_only,
        "docs_fastpath": fastpath,
        "build_lite_tests": build_lite,
        "build_darling_tests": build_darling,
        "darling_scope": darling_scope(
            event, build_darling, pg_run, release or a["darling_full"] or a["core"] or a["root"]),
        "darling_pg": pg_run,
        "darling_linux": release or a["gate_darling"],
        "lite_mode": mode,
        "tree_guards": tree_run,
        "tree_scope": tree_scope(event, all_count, a["lite_tree_count"]) if tree_run else "none",
    }


# ---------------------------------------------------------------------------------------------------------
# The test classes of the tree (for the replay).
# ---------------------------------------------------------------------------------------------------------

_CLASS_DECL = re.compile(
    r"^[ \t]*(?:(?:public|internal|private|protected|sealed|static|abstract|partial)\s+)*class\s+(\w+)")
_TRAIT = re.compile(r'Trait\(\s*"(\w+)"\s*,\s*"(\w+)"\s*\)')

# #5872: scan_classes() marks a class that neither holds nor inherits a [Fact]/[Theory] (a fake, a logger, a harness a
# test file also declares). The map never has such a class, because it runs nothing, so map_select() must not count it
# as a "new class": 377 Darling and 143 Lite helper types were selected on every pull request that way.
NO_TESTS_TRAIT = "Tests=None"
_HOLDS_MARK = "\x00holds"
_BASE_MARK = "\x00base="
_TEST_ATTR_LINE = re.compile(r"^\[(?:[^\]]*[\s,(])?(?:Fact|Theory)\b")
COLLECTION_PREFIX = "Collection="
COLLECTION_MATE_CAP = 60
_COLLECTION_ATTR = re.compile(r'\bCollection\(\s*"([^"]+)"\s*\)')
_BASE_LIST = re.compile(r"^\s*(?:<[^>]*>\s*)?(?:\([^)]*\)\s*)?:\s*([\w.]+)")


def _holders_of(lines: list[str]) -> set[str]:
    """The classes of one file that hold a test: a [Fact] or [Theory] attribute belongs to the nearest class declared
    above it and indented less than it (the same walk as _test_holders())."""
    out: set[str] = set()
    stack: list[tuple[int, str]] = []
    for line in lines:
        indent = len(line) - len(line.lstrip(" \t"))
        m = _CLASS_DECL.match(line)
        is_test = not m and bool(_TEST_ATTR_LINE.match(line.strip()))
        if m or is_test:
            while stack and stack[-1][0] >= indent:
                stack.pop()
        if m:
            stack.append((indent, m.group(1)))
        elif is_test and stack:
            out.add(stack[-1][1])
    return out

# The test map is built on shards that set only DARLING_TEST_PG and DARLING_TEST_PGRUNTIME (nightly.yml's "Run this
# shard's classes" step; a test pins that env block to MAP_SHARD_ENV). A class that gates its tests on any OTHER
# DARLING_TEST_PG_* or DARLING_TEST_PGRUNTIME_* variable (the log-format targets, the log-rotation targets, the
# store-upgrade fixtures) skips at map build, so the map has no coverage edges for what its tests would run and a
# change to the code under them would never select it (#5459; measured on the 2026-10-09 map: 16 files, 19
# mapped classes, 12 to 44 coverage files each against 100 and more for a fully covered class). The map cannot tell
# us, so the SOURCE does: every class in a file that names such a variable in a quoted literal carries the Gate=Env
# trait and map_select always selects it. It is found from the source at selection time, so a new gated class needs
# no list edit.
#
# What the scan cannot see (a miss here is silent, so a new gate must be written to be seen): it reads each test file
# on its own, for a quoted "DARLING_TEST_PG_..." or "DARLING_TEST_PGRUNTIME_..." literal. A gate variable that a class
# reads through a constant or a helper declared in ANOTHER file, or one that is not named DARLING_TEST_PG_* /
# DARLING_TEST_PGRUNTIME_*, is invisible to it, and the class stays unmarked and unmapped. A new gate of either kind
# must also name its variable as a quoted literal in the class's own file (or take the Gate=Env trait by hand).
#
# What it costs: the mark is per CLASS, not per test, so a class where only some tests are gated runs whole on every
# narrowed Darling pull request, and the darling-pg job sets these variables, so the gated tests really run. Measured
# on a full run of this change (workflow run 37911378693, the 19 test classes that carry the mark, summed test time
# per class, the shard each lands on in brackets): DarlingStoreUpgradeTests 274 s [shard 5],
# ManagedConfUpgradePathTests 85 s [4], DarlingManagedPostgresTests 73 s [3], PgServerLogTailCsvJsonRotationLiveTests
# 44 s [1], PgLogBurstAndRotationLiveTests 38 s [2], PgRaiseShapedRecordsLiveTests 31 s [1],
# PgServerLogTailRotationLiveTests 15 s [1], and 12 more classes of 11 s or less (29 s together): about 590 s of test
# time in all. Per shard that is 274 s on shard 5, 100 s on shard 4, 90 s on shard 1, 73 s on shard 3, 43 s on shard 2
# and 9 s on shard 0. A class runs on one thread, so shard 5 cannot get below the 274 s of DarlingStoreUpgradeTests: on
# a narrowed pull request whose other shards finish in a minute or two, shard 5 becomes the slowest by about 3
# minutes (it is 174 s ahead of shard 4, the next). It never passes the full run's slowest shard (shard 0, 589 s of
# test time, against 509 s for shard 5 in that run). The shard cut is unchanged.
MAP_SHARD_ENV = frozenset({"DARLING_TEST_PG", "DARLING_TEST_PGRUNTIME"})
GATE_TRAIT = "Gate=Env"
GATE_ENV_LITERAL = re.compile(r'"DARLING_TEST_PG(?:RUNTIME)?_[A-Z0-9_]+"')

SUITE_DIRS = {
    "darling": os.path.join("Darling", "Darling.Tests"),
    "lite": "Lite.Tests",
}
SUITE_BY_PREFIX = {
    "Darling.Tests": "darling",
    "PerformanceMonitor.Darling.Tests": "darling",
    "Lite.Tests": "lite",
    "PerformanceMonitorLite.Tests": "lite",
}


def split_failed_class(cls: str) -> tuple[str, str]:
    """(namespace prefix, simple class name) of a failing test class's full name. A nested class is reported as
    `Ns.Outer+Inner` (some runners use `Ns.Outer/Inner`): the prefix is the namespace of the OUTER class, the text
    before the first '+', and the simple name is the innermost class, which is the name _walk_classes yields for a
    nested declaration. Splitting on the last '.' alone left `Outer+Inner` as the "simple name", found no such class
    in the tree, and so never counted a nested failure as a miss (#5459)."""
    head = re.split(r"[+/]", cls, maxsplit=1)[0]
    return head.rpartition(".")[0], re.split(r"[.+/]", cls)[-1]


_SOURCES: "dict[str, list]" = {}


def _open_text(path: str, errors: str) -> str:
    with open(path, encoding="utf-8-sig", errors=errors) as fh:
        return fh.read()


def read_source(path: str, errors: str = "strict") -> str:
    """The text of one test source, read at most once per process (#6346). A replay or a selection reads every test
    file for several rules (the class walk, the slow-class text, the Reads detector, the symbol index) and each used
    to open it again: about 3,300 files, three or four times over, which a slow disk turned into minutes. The entry
    is dropped when the file's size or modification time changes, so a caller that edits a tree and reads it again
    sees the new text. Decoding is the same as open(): "replace" returns the strict text when that decodes."""
    st = os.stat(path)
    stamp = (st.st_mtime_ns, st.st_size)
    ent = _SOURCES.get(path)
    if ent is None or ent[0] != stamp:
        ent = _SOURCES[path] = [stamp, None, None]
    if errors == "strict":
        if ent[1] is None:
            ent[1] = _open_text(path, "strict")
        return ent[1]
    if ent[2] is None:
        ent[2] = ent[1] if ent[1] is not None else _open_text(path, errors)
    return ent[2]


_PREFETCH_THREADS = 8


def prefetch_sources(paths: "list[str]", errors: str = "strict") -> None:
    """Fill read_source()'s cache for `paths` with a few threads: a file open is latency, not CPU, on a disk with a
    virus scanner in front of it, so the opens overlap. A file that cannot be read is left for the caller's own read
    to report, as before."""
    todo = [p for p in paths if p not in _SOURCES]
    if len(todo) < 16:
        return

    def one(path: str) -> None:
        try:
            read_source(path, errors)
        except (OSError, UnicodeDecodeError):
            pass

    with concurrent.futures.ThreadPoolExecutor(max_workers=_PREFETCH_THREADS) as pool:
        list(pool.map(one, todo))


_CLASS_ROWS: "dict[tuple[str, str], tuple[tuple[int, int], list[tuple[str, set[str]]]]]" = {}


def _classes_of_file(root: str, path: str) -> "list[tuple[str, set[str]]] | None":
    """[(simple class name, traits)] for every class declaration of one source file, parsed at most once per process;
    None when the file cannot be read."""
    try:
        text = read_source(path)
        st = os.stat(path)
    except OSError:
        return None
    stamp = (st.st_mtime_ns, st.st_size)
    hit = _CLASS_ROWS.get((root, path))
    if hit is not None and hit[0] == stamp:
        return hit[1]
    lines = text.split("\n")
    gated = any(GATE_ENV_LITERAL.search(line) for line in lines)
    holders = _holders_of(lines)
    rows: "list[tuple[str, set[str]]]" = []
    for n, line in enumerate(lines):
        m = _CLASS_DECL.match(line)
        if not m:
            continue
        traits: set[str] = {GATE_TRAIT} if gated else set()
        # scan_classes() turns these two markers into NO_TESTS_TRAIT and drops them (#5872).
        if m.group(1) in holders:
            traits.add(_HOLDS_MARK)
        tail = line[m.end():]
        if ":" not in tail and "{" not in tail and n + 1 < len(lines):
            tail += " " + lines[n + 1]
        b = _BASE_LIST.match(tail)
        if b:
            traits.add(_BASE_MARK + b.group(1).split(".")[-1])
        j = n - 1
        while j >= 0:
            s = lines[j].strip()
            if s.startswith(("[", "///", "//", "*", "/*")) or s.endswith("*/") or s == "" and False:
                traits.update(f"{k}={v}" for k, v in _TRAIT.findall(s))
                traits.update(COLLECTION_PREFIX + c for c in _COLLECTION_ATTR.findall(s))
                j -= 1
            else:
                break
        rows.append((m.group(1), traits))
    _CLASS_ROWS[(root, path)] = (stamp, rows)
    return rows


def _walk_classes(root: str):
    """Yield (suite, repo-relative file with '/', simple class name, {"Key=Value", ...}) for every class declaration."""
    found: "list[tuple[str, str]]" = []
    for suite, rel in SUITE_DIRS.items():
        base = os.path.join(root, rel)
        for dp, dn, fn in os.walk(base):
            dn[:] = [d for d in dn if d not in ("bin", "obj")]
            found += [(suite, os.path.join(dp, f)) for f in fn if f.endswith(".cs")]
    prefetch_sources([p for _s, p in found if (root, p) not in _CLASS_ROWS])
    for suite, path in found:
        rows = _classes_of_file(root, path)
        if rows is None:
            continue
        relfile = os.path.relpath(path, root).replace(os.sep, "/")
        for name, traits in rows:
            yield suite, relfile, name, set(traits)


def scan_classes(root: str = ROOT) -> dict[str, dict[str, set[str]]]:
    """{suite: {simple class name: {"Key=Value", ...}}} read from the test sources."""
    out: dict[str, dict[str, set[str]]] = {suite: {} for suite in SUITE_DIRS}
    for suite, _file, name, traits in _walk_classes(root):
        out[suite].setdefault(name, set()).update(traits)
    # #5872: a class holds a test when it (any declaration of the name) carries a [Fact] or [Theory], or its base class
    # does, followed through the suite's own classes; every other class gets NO_TESTS_TRAIT.
    for classes in out.values():
        holds = {c for c, t in classes.items() if _HOLDS_MARK in t}
        grew = True
        while grew:
            grew = False
            for c, t in classes.items():
                if c not in holds and any(x[len(_BASE_MARK):] in holds for x in t if x.startswith(_BASE_MARK)):
                    holds.add(c)
                    grew = True
        for c, t in classes.items():
            t.difference_update({x for x in t if x == _HOLDS_MARK or x.startswith(_BASE_MARK)})
            if c not in holds:
                t.add(NO_TESTS_TRAIT)
    # #5961: Reads=Lite / Reads=Darling are derived from the source, not read off a [Trait] attribute.
    with_reads_traits(out, root)
    return out


def class_selected(suite: str, traits: set[str], d: dict) -> bool:
    """Would today's rules run this class for this decision?"""
    guard = "Stage=Guard" in traits
    if suite == "darling":
        if guard and d["guard_run"]:
            return True
        # The build job's no-store pass runs the whole suite (full) or only the Guard stage; the shards run every class.
        reads_lite = "Reads=Lite" in traits
        return d["darling_scope"] == "full" or d["darling_pg"] \
            or (d["tree_guards"] and (d["tree_scope"] == "full" or reads_lite)) \
            or (d["darling_scope"] == "guard" and guard) \
            or (d["darling_scope"] == "reads-lite" and (guard or reads_lite))
    if suite == "lite":
        if guard and d["guard_run"]:
            return True
        if d["lite_mode"] in ("full", "release"):
            return True
        return d["lite_mode"] == "reads" and "Reads=Darling" in traits
    return True


# ---------------------------------------------------------------------------------------------------------
# The classes that read the OTHER product's files (#5961), derived from the source, not from a trait.
# ---------------------------------------------------------------------------------------------------------
#
# A pull request that changes only Lite files runs the Darling.Tests classes that read a Lite path (and the Guard stage);
# one that changes only Darling files named in lite-paths-filter.yml's reads filter runs the Lite.Tests classes that read a
# Darling path. Those two class sets used to be hand-written `[Trait("Reads", ...)]` attributes with a C# census beside
# them, and the runners filtered on the trait. They now run `-class` lists from reads_classes() below, so there is no
# roster to keep: a new class that reads a Lite path is in the set the moment it is written.
#
# This is a port of GuardStageScanner.ReadsALitePath and of the Darling-path detector Lite.Tests had (comments are ignored,
# a string literal is read for a path segment, a recursive enumeration from the repo root reads every file). The scan is per
# FILE, as the census was: a helper class in the same file may hold the path a test reads, so every test class (one that
# holds a [Fact] or [Theory] in its own body) in a file that reads the other tree is in the set. A path assembled from a
# constant another file declares is not visible to a source scan; the replay and the nightly are the backstop for that.

_RC_ENUM_CALL = re.compile(
    r"\b(?:EnumerateFiles|EnumerateFileSystemEntries|EnumerateDirectories|GetFiles|GetFileSystemEntries|GetDirectories)\s*\(")
# Darling.Tests' spelling of "the repo root" (GuardStageScanner.s_repoRootExpression); Lite.Tests' was narrower.
_RC_ROOT_DARLING = re.compile(r"^(?:\w+\.)*\w*(?:RepoRoot|FindRoot|ResolveRoot)\w*\(\s*\)$|^(?:RepoFile\.)?Root$")
_RC_ROOT_LITE = re.compile(r"^(?:\w+\.)*(?:RepoRoot|FindRepoRoot)\(\s*\)$")
_RC_ASSIGN = re.compile(r"\b(\w+)\s*(?:=>|=)\s*([^;=]+);")
_RC_RECURSE = re.compile(r"RecurseSubdirectories\s*=\s*true")
_RC_CLASS = re.compile(
    r"^[ \t]*(?:(?:public|internal|private|protected|sealed|static|abstract|partial)\s+)*class\s+(\w+)", re.MULTILINE)
_RC_NAMESPACE = re.compile(r"^[ \t]*namespace\s+([\w.]+)", re.MULTILINE)
_RC_TEST_ATTR = re.compile(r"\[(?:Fact|Theory)\b")
_RC_ENUM_WORD = r"Enumerate|GetFiles|GetDirectories|GetFileSystemEntries"
# The detectors need the other tree's name (the copyright header says "Darling Data", so a bare Darling is no signal).
_RC_CHEAP = {"darling": re.compile(r"Lite|" + _RC_ENUM_WORD), "lite": re.compile(r'Darling[./\\"]|' + _RC_ENUM_WORD)}


def _rc_run(text: str, i: int, c: str) -> int:
    j = i
    while j < len(text) and text[j] == c:
        j += 1
    return j - i


def _rc_char_literal_end(text: str, i: int) -> int:
    j = i + 1
    while j < len(text):
        if text[j] == "\\":
            j += 2
            continue
        if text[j] == "'":
            return j + 1
        if text[j] == "\n":
            return i + 1
        j += 1
    return j


def _rc_opener(text: str, i: int):
    """(kind, dollars, quote_run, body_start) of a string literal opening at i, or None. kind: R regular, V verbatim, W raw."""
    j, dollars, verbatim = i, 0, False
    while j < len(text) and text[j] in "$@":
        if text[j] == "$":
            dollars += 1
        elif verbatim:
            return None
        else:
            verbatim = True
        j += 1
    if j >= len(text) or text[j] != '"':
        return None
    if verbatim:
        return "V", dollars, 1, j + 1
    quotes = _rc_run(text, j, '"')
    if quotes >= 3:
        return "W", dollars, quotes, j + quotes
    if quotes == 2:
        return "R", dollars, 0, j + 2
    return "R", dollars, 1, j + 1


def _rc_scan_code(text: str, i: int, code: bytearray, hole, bodies) -> int:
    """Port of CSharpSourceWalker.ScanCode: marks code characters, collects string-literal bodies as (start, end)."""
    depth, n = 0, len(text)
    while i < n:
        c = text[i]
        if hole is not None and depth == 0:
            if c == "}" and _rc_run(text, i, "}") >= hole[1]:
                return i
            if c == ":":
                if i + 1 < n and text[i + 1] == ":":
                    code[i] = code[i + 1] = 1
                    i += 2
                    continue
                return i
        if c == "/" and i + 1 < n and text[i + 1] == "/":
            nl = text.find("\n", i)
            i = n if nl < 0 else nl
            continue
        if c == "/" and i + 1 < n and text[i + 1] == "*":
            close = text.find("*/", i + 2)
            i = n if close < 0 else close + 2
            continue
        if c == "'":
            i = _rc_char_literal_end(text, i)
            continue
        op = _rc_opener(text, i) if c in "$@\"" else None
        if op is not None:
            _kind, _dollars, run, body_start = op
            if run == 0:
                bodies.append((body_start, body_start))
                i = body_start
                continue
            i, body_end = _rc_scan_literal(text, body_start, code, op, bodies)
            bodies.append((body_start, body_end))
            continue
        if c in "([{":
            depth += 1
        elif c in ")]}":
            depth -= 1
        code[i] = 1
        i += 1
    return i


def _rc_scan_literal(text: str, i: int, code: bytearray, frame, bodies) -> "tuple[int, int]":
    """(index just past the literal, index where its body ends)."""
    kind, dollars, quote_run, _ = frame
    n = len(text)
    while i < n:
        c = text[i]
        if kind == "W" and c == '"':
            run = _rc_run(text, i, '"')
            if run >= quote_run:
                return i + run, i
            i += run
            continue
        if kind == "V" and c == '"':
            if i + 1 < n and text[i + 1] == '"':
                i += 2
                continue
            return i + 1, i
        if kind == "R" and c == "\\":
            i += 2
            continue
        if kind == "R" and c == '"':
            return i + 1, i
        if kind == "R" and c == "\n":
            return i, i
        if dollars > 0 and c == "{":
            run = _rc_run(text, i, "{")
            if dollars == 1:
                i += run // 2 * 2
                if run % 2 == 0:
                    continue
                i = _rc_scan_hole(text, i + 1, code, frame, bodies)
                continue
            if run >= dollars:
                i = _rc_scan_hole(text, i + dollars, code, frame, bodies)
                continue
            i += run
            continue
        if dollars > 0 and c == "}":
            i += _rc_run(text, i, "}")
            continue
        i += 1
    return i, i


def _rc_scan_hole(text: str, i: int, code: bytearray, frame, bodies) -> int:
    dollars = frame[1]
    i = _rc_scan_code(text, i, code, frame, bodies)
    if i < len(text) and text[i] == ":":
        while i < len(text) and not (text[i] == "}" and _rc_run(text, i, "}") >= dollars):
            i += 1
    if i < len(text) and text[i] == "}":
        i += min(_rc_run(text, i, "}"), dollars)
    return i


def cs_code_and_literals(text: str) -> "tuple[str, list[tuple[int, str]]]":
    """(source with comments and literal text blanked, newlines kept; [(start, body)] of every string literal) -
    CSharpSourceWalker.StripCommentsAndStrings and .StringLiteralBodies, from one walk."""
    code = bytearray(len(text))
    spans: list[tuple[int, int]] = []
    _rc_scan_code(text, 0, code, None, spans)
    stripped = "".join(ch if code[k] else ("\n" if ch == "\n" else " ") for k, ch in enumerate(text))
    bodies = [(s, "".join(" " if code[k] else text[k] for k in range(s, e))) for s, e in spans]
    return stripped, bodies


def _rc_brace_end(code: str, open_at: int) -> int:
    """Index just past the brace block opening at open_at (CSharpSourceWalker.BraceBalanced); len(code) if it never closes."""
    depth = 0
    for i in range(open_at, len(code)):
        if code[i] == "{":
            depth += 1
        elif code[i] == "}":
            depth -= 1
            if depth == 0:
                return i + 1
    return len(code)


def _rc_call_args(code: str, start: int) -> "list[str]":
    args, depth, frm = [], 0, start
    for i in range(start, len(code)):
        c = code[i]
        if c in "([{":
            depth += 1
        elif c in ")]}":
            if depth == 0:
                args.append(code[frm:i])
                return args
            depth -= 1
        elif c == "," and depth == 0:
            args.append(code[frm:i])
            frm = i + 1
    return []


def enumerates_repo_root_recursively(code: str, root_re: "re.Pattern[str]") -> bool:
    """Whether a call such as Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories) starts at the repo root:
    its first argument is a repo-root call or a variable the file sets from one, and it is recursive."""
    root_vars = {m.group(1) for m in _RC_ASSIGN.finditer(code) if root_re.match(m.group(2).strip())}
    recursive_options = bool(_RC_RECURSE.search(code))
    for call in _RC_ENUM_CALL.finditer(code):
        args = _rc_call_args(code, call.end())
        if not args:
            continue
        first = args[0].strip()
        from_root = bool(root_re.match(first)) or first in root_vars
        recursive = any("AllDirectories" in a for a in args) \
            or (recursive_options and len(args) >= 3 and "TopDirectoryOnly" not in args[-1])
        if from_root and recursive:
            return True
    return False


def _rc_bare_literal_is_a_path(text: str, start: int) -> bool:
    before = text[max(0, start - 150):start]
    cut = before.rfind(";")
    if cut >= 0:
        before = before[cut + 1:]
    return "Combine(" in before or "new[]" in before or "new string[]" in before


def reads_lite_path(text: str) -> bool:
    """GuardStageScanner.ReadsALitePath: a string literal (not a comment) naming a path segment Lite (alone, or beside a
    slash), Lite.Tests or a PerformanceMonitorLite* project, or a recursive enumeration from the repo root."""
    code, bodies = cs_code_and_literals(text)
    for _start, body in bodies:
        norm = body.replace("\\", "/")
        segs = [s for s in norm.split("/") if s]
        if any(s.startswith("PerformanceMonitorLite") or s == "Lite.Tests" for s in segs):
            return True
        if body == "Lite" or ("/" in norm and "Lite" in segs):
            return True
    return enumerates_repo_root_recursively(code, _RC_ROOT_DARLING)


def reads_darling_path(text: str) -> bool:
    """The Darling-path twin Lite.Tests had: a literal naming PerformanceMonitor.Darling.*, Darling.Tests, Darling beside a
    slash, or a bare "Darling" that is a path-join argument or a segment-array element; or a recursive repo-root walk."""
    code, bodies = cs_code_and_literals(text)
    for start, body in bodies:
        norm = body.replace("\\", "/")
        segs = [s for s in norm.split("/") if s]
        if any(s.startswith("PerformanceMonitor.Darling.") or s == "Darling.Tests" for s in segs):
            return True
        if "/" in norm and "Darling" in segs:
            return True
        if body == "Darling" and _rc_bare_literal_is_a_path(text, start):
            return True
    return enumerates_repo_root_recursively(code, _RC_ROOT_LITE)


def test_classes_of_text(text: str) -> "list[str]":
    """Full names (Namespace.Outer+Inner) of the classes in one source text that hold a [Fact] or [Theory] in their OWN body
    (a nested class does not take its enclosing class's tests). The namespace is the nearest `namespace` line above."""
    code, _ = cs_code_and_literals(text)
    found: list[tuple[str, int, int, int]] = []
    for m in _RC_CLASS.finditer(code):
        open_at = code.find("{", m.start())
        if open_at >= 0:
            found.append((m.group(1), m.start(), open_at, _rc_brace_end(code, open_at)))
    spaces = [(m.start(), m.group(1)) for m in _RC_NAMESPACE.finditer(code)]
    out: list[str] = []
    for name, idx, open_at, end in found:
        own = list(code[open_at:end])
        for _n, nidx, _o, nend in found:
            if nidx > open_at and nend <= end:
                for k in range(nidx - open_at, nend - open_at):
                    own[k] = " "
        if not _RC_TEST_ATTR.search("".join(own)):
            continue
        outers = sorted((f for f in found if f[2] < idx and f[3] >= end and f[1] != idx), key=lambda f: f[2])
        ns = next((n for pos, n in reversed(spaces) if pos < idx), "")
        out.append((ns + "." if ns else "") + "+".join([f[0] for f in outers] + [name]))
    return out


def _rc_project_links(project: str) -> "list[str]":
    """The files a csproj compiles in through an explicit Compile Include with a Link (wildcards skipped)."""
    try:
        with open(project, encoding="utf-8-sig") as fh:
            xml = fh.read()
    except OSError:
        return []
    base = os.path.dirname(project)
    out = []
    for m in re.finditer(r"<Compile\s+[^>]*Include=\"([^\"*]+)\"[^>]*Link=\"[^\"]+\"", xml):
        out.append(os.path.normpath(os.path.join(base, m.group(1).replace("\\", os.sep))))
    return out


def _rc_suite_files(root: str, suite: str) -> "list[str]":
    base = os.path.join(root, SUITE_DIRS[suite])
    files = []
    for dp, dn, fn in os.walk(base):
        dn[:] = [d for d in dn if d not in ("bin", "obj")]
        files += [os.path.join(dp, f) for f in fn if f.endswith(".cs")]
    project = os.path.join(base, ("Darling.Tests" if suite == "darling" else "Lite.Tests") + ".csproj")
    have = set(files)
    files += [f for f in _rc_project_links(project) if f not in have]
    return sorted(files)


def reads_classes(root: str = ROOT, suites: "tuple[str, ...]" = ("darling", "lite")) -> "dict[str, set[str]]":
    """{"darling": full names of the Darling.Tests classes that read a Lite path, "lite": full names of the Lite.Tests
    classes that read a Darling path}. Replaces the [Trait("Reads", "Lite")] and [Trait("Reads", "Darling")] attributes."""
    out: dict[str, set[str]] = {"darling": set(), "lite": set()}
    for suite, detector in (("darling", reads_lite_path), ("lite", reads_darling_path)):
        paths = _rc_suite_files(root, suite) if suite in suites else []
        prefetch_sources(paths)
        for path in paths:
            try:
                text = read_source(path).replace("\r\n", "\n")
            except OSError:
                continue
            # A file that names neither tree and calls no file enumeration cannot read a path: skip the character walk.
            if not _RC_CHEAP[suite].search(text) or not _RC_TEST_ATTR.search(text):
                continue
            if detector(text):
                out[suite].update(test_classes_of_text(text))
    return out


def with_reads_traits(classes: "dict[str, dict[str, set[str]]]", root: str = ROOT) -> None:
    """Put Reads=Lite / Reads=Darling on the scanned classes the source scan names, in place (the rules in class_selected())."""
    reads = reads_classes(root)
    for suite, trait in (("darling", "Reads=Lite"), ("lite", "Reads=Darling")):
        simple = {re.split(r"[.+]", n)[-1] for n in reads[suite]}
        for name, traits in classes[suite].items():
            if name in simple:
                traits.add(trait)


# ---------------------------------------------------------------------------------------------------------
# Slow classes: pull requests skip them unless the change reaches them (#5459, change 5).
# ---------------------------------------------------------------------------------------------------------
#
# A class tagged [Trait("Cost", "Slow")] is slow, has never failed in the corpus, and is covered by the push to dev
# one merge later. A pull_request run leaves it out unless the change reaches it. Push, merge-queue, nightly and
# release runs run every class: slow_skip answers nothing for them. This is a separate pass over the classes a leg
# already chose, not part of darling_scope or lite_scope, so a new scope composes with it.
#
# "Reaches it", from the changed file names alone (the same cheap signal the area filters use):
#   * the file is a source file the class is declared in;
#   * the file's name (without the extension) appears as a word in the class's source, which is how a test names the
#     product file, harness script or helper it exercises;
#   * a .js/.mjs/.css/.html file, and a class whose source names a .js or .mjs file (the Node-driven classes load
#     scripts through a harness that imports others);
#   * any non-.cs file inside the class's own test project (a csproj, a fixture, a harness), or a build input
#     (the workflow, this script, the shard scripts, a props/csproj/lock file, global.json).
# When the changed file list is unknown (the API call failed, or it came back empty) nothing is skipped.

SLOW_TRAIT = "Cost=Slow"
_BUILD_INPUT_NAMES = {"directory.build.props", "directory.build.targets", "directory.packages.props", "global.json",
                      "nuget.config", "packages.lock.json"}
_BUILD_INPUT_PATHS = {".github/workflows/build.yml", ".github/scripts/ci-select.py", ".github/darling-paths-filter.yml",
                      ".github/lite-paths-filter.yml", ".github/scripts/run-darling-pg-shard.ps1", ".github/scripts/run-lite-shard.ps1",
                      ".github/scripts/darling-shard-pack.py", ".github/scripts/darling-shard-weights.json",
                      ".github/scripts/lite-shard-pack.py"}
_BUILD_INPUT_SUFFIXES = (".csproj", ".props", ".targets", ".sln", ".slnx", ".slnf")
_WEB_SUFFIXES = (".js", ".mjs", ".css", ".html")


def scan_slow(root: str = ROOT) -> dict[str, dict[str, list[str]]]:
    """{suite: {class: [repo-relative source files]}} for every class tagged Cost=Slow (a Guard class never counts:
    the Guard stage runs on every pull request)."""
    files: dict[tuple[str, str], list[str]] = {}
    traits: dict[tuple[str, str], set[str]] = {}
    for suite, relfile, name, t in _walk_classes(root):
        files.setdefault((suite, name), [])
        if relfile not in files[(suite, name)]:
            files[(suite, name)].append(relfile)
        traits.setdefault((suite, name), set()).update(t)
    out: dict[str, dict[str, list[str]]] = {suite: {} for suite in SUITE_DIRS}
    for (suite, name), t in traits.items():
        if SLOW_TRAIT in t and "Stage=Guard" not in t:
            out[suite][name] = sorted(files[(suite, name)])
    return out


class SlowIndex:
    """The Cost=Slow classes of the tree and what reaches them."""

    def __init__(self, root: str = ROOT) -> None:
        self.root = root
        self.slow = scan_slow(root)
        self._text: dict[tuple[str, str], str] = {}
        self._words: dict[tuple[str, str], set[str]] = {}

    def text(self, suite: str, name: str) -> str:
        key = (suite, name)
        if key not in self._text:
            parts = []
            for rel in self.slow[suite][name]:
                try:
                    parts.append(read_source(os.path.join(self.root, rel)))
                except OSError:
                    pass
            self._text[key] = "\n".join(parts)
        return self._text[key]

    def words(self, suite: str, name: str) -> set[str]:
        """The class source's words (letters, digits, underscore, hyphen), the unit a file stem is matched against."""
        key = (suite, name)
        if key not in self._words:
            self._words[key] = set(re.findall(r"[\w-]+", self.text(suite, name)))
        return self._words[key]

    def skip(self, files: list[str], event: str, base_ref: str = "") -> dict[str, list[str]]:
        """{suite: [simple class names a leg leaves out]} for one change. Empty for every event but pull_request,
        for a pull request into main (narrows), and when the changed file list is unknown."""
        out: dict[str, list[str]] = {suite: [] for suite in SUITE_DIRS}
        if not narrows(event, base_ref) or not files:
            return out
        changed = set(files)
        # Everything about the change is computed once; a class is then a few set lookups.
        stems: set[str] = set()
        odd_stems: set[str] = set()
        exts: set[str] = set()
        build_input = False
        for f in changed:
            low = f.lower()
            base = f.rsplit("/", 1)[-1]
            stem, dot, ext = base.rpartition(".")
            ext = dot + ext.lower() if dot else ""
            if low in _BUILD_INPUT_PATHS or base.lower() in _BUILD_INPUT_NAMES or low.endswith(_BUILD_INPUT_SUFFIXES):
                build_input = True
            if stem:
                (stems if re.fullmatch(r"[\w-]+", stem) else odd_stems).add(stem)
            if ext in _WEB_SUFFIXES:
                exts.add(ext)
        if build_input:
            return out
        for suite, classes in self.slow.items():
            test_dir = SUITE_DIRS[suite].replace(os.sep, "/") + "/"
            if any(f.startswith(test_dir) and not f.endswith(".cs") for f in changed):
                continue
            for name, own in classes.items():
                text = self.text(suite, name)
                reached = bool(changed.intersection(own)) or bool(stems & self.words(suite, name)) \
                    or any(x in text and re.search(r"(?<![\w-])" + re.escape(x) + r"(?![\w-])", text)
                           for x in odd_stems) \
                    or (bool(exts) and (".mjs" in text or any(e in text for e in exts)))
                if not reached:
                    out[suite].append(name)
            out[suite].sort()
        return out


def changed_files_of_pr(repo: str, pr: str) -> list[str]:
    """The pull request's changed files (both sides of a rename) through `gh api`; [] when the call fails."""
    import subprocess
    try:
        r = subprocess.run(["gh", "api", f"repos/{repo}/pulls/{pr}/files", "--paginate", "--jq",
                            '.[] | .filename, (.previous_filename // empty)'],
                           capture_output=True, text=True, timeout=120)
    except (OSError, subprocess.SubprocessError) as e:
        print(f"slow-skip: could not list the pull request's files ({e}); skipping nothing", file=sys.stderr)
        return []
    if r.returncode != 0:
        print(f"slow-skip: gh api exited {r.returncode}; skipping nothing", file=sys.stderr)
        return []
    out = [ln.strip() for ln in r.stdout.splitlines() if ln.strip()]
    if len(out) >= 3000:  # the API lists at most 3000 files, so a list this long may be cut: do not trust it
        print("slow-skip: the file list may be truncated; skipping nothing", file=sys.stderr)
        return []
    return out


def load_accepted(path: str = ACCEPTED) -> set[tuple[str, str]]:
    """The rows today's rules miss for a reason that is not a selection gap. One `<sha prefix> <class>` per line,
    the reason in a `#` comment above the group. A miss that is not listed here fails the replay."""
    out: set[tuple[str, str]] = set()
    if os.path.exists(path):
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                line = line.split("#", 1)[0].strip()
                if line:
                    sha, cls = line.split()[:2]
                    out.add((sha, cls))
    return out


def replay(corpus: str = CORPUS, root: str = ROOT, quiet: bool = False) -> int:
    rules = Rules()
    accepted = load_accepted()
    accepted_seen = 0
    tree = scan_classes(root)
    slow = SlowIndex(root)
    rows = checked = misses = exempt = 0
    gone: set[str] = set()
    missed: list[str] = []
    with open(corpus, encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            row = json.loads(line)
            rows += 1
            d = decide(row["changed_files"], row["event"], rules)
            skipped = slow.skip(row["changed_files"], row["event"])
            for cls in row["failed_classes"]:
                prefix, simple = split_failed_class(cls)
                suite = SUITE_BY_PREFIX.get(prefix)
                if suite is None:
                    continue
                traits = tree.get(suite, {}).get(simple)
                if traits is None:
                    exempt += 1
                    gone.add(cls)
                    continue
                checked += 1
                if simple in skipped.get(suite, ()) or not class_selected(suite, traits, d):
                    if (row["head_sha"][:10], cls) in accepted:
                        accepted_seen += 1
                        continue
                    misses += 1
                    missed.append(f"{row['date']} {row['head_sha'][:10]} {cls} files={len(row['changed_files'])}"
                                  f" e.g. {row['changed_files'][0]}")
    if not quiet:
        print(f"replay: {rows} rows, {checked} failing classes checked, {exempt} exempt (class no longer in the tree: "
              f"{len(gone)} distinct), {accepted_seen} accepted misses "
              f"(replay-accepted.txt), {misses} misses")
        for m in missed[:40]:
            print("MISS", m)
    return misses


# ---------------------------------------------------------------------------------------------------------
# Selection from a class-to-file map (#5459 change 4, slice 1: nothing in build.yml calls this yet).
# ---------------------------------------------------------------------------------------------------------

MAP_SCHEMA = 1
MAP_MAX_AGE_DAYS = 7
MAP_MAX_DRIFT = 300

# Files whose change can alter any test in ways a call map cannot see: the build and restore configuration.
# selection_args() adds the root and shared-library areas read from build.yml, so the lists cannot disagree.
KEEP_FULL = (
    ".github/workflows/build.yml",
    "Directory.Build.props",
    "Directory.Build.targets",
    "Directory.Packages.props",
    "global.json",
    "nuget.config",
    "NuGet.config",
    "**/*.csproj",
    "**/*.props",
    "**/*.targets",
    "**/packages.lock.json",
    "**/xunit.runner.json",
    # The store factory (#7097): 1,218 sites build their store through it, and the map cannot see a change in the stores they get.
    "Darling/Darling.Tests/ScratchPostgres.cs",
)

# The shared projects (#5951). A change under one of them used to run everything, because both are referenced by
# every product and a compiled `const` or `enum` value is copied into the caller at build time, so the map's call
# edges cannot see the caller change. The measured price was 182 merged dev pull requests in four weeks, all FULL, 71
# of which a map selection would have narrowed to a median 1,180 classes. The rule now reads the diff: FULL only for
# a change to a constant or an enum, or to a registration file below; any other change selects by the map's edges.
SHARED_PROJECT_DIRS = ("PerformanceMonitor.Common/", "PerformanceMonitor.PlanAnalysis/")

# The files of the shared projects that register or discover types, or load a resource by name, so a census class
# sees the change without any edge to the file: a change to one of them is FULL (#5951). Each has the one-line reason.
# Found with: grep -rnE "AddSingleton|AddScoped|AddTransient|GetTypes\(\)|Assembly\." under the two projects.
REGISTRATION_FILES = {
    "PerformanceMonitor.Common/Mcp/McpSchemaCompat.cs":
        "registers the MCP tool catalog and parameter types in the service collection (AddSingleton) and discovers "
        "the tools by reflecting over a tool type's methods and attributes",
    "PerformanceMonitor.Common/Mcp/McpToolParameterTypes.cs":
        "holds the registry the reflection scan in McpSchemaCompat fills from each tool method's parameters",
    "PerformanceMonitor.Common/Services/UpdateCheckService.cs":
        "reads the running build's version from the entry assembly (Assembly.GetEntryAssembly)",
    "PerformanceMonitor.PlanAnalysis/WaitStatsConfig.cs":
        "loads the embedded Resources/WaitStats.json by manifest resource name (Assembly.GetManifestResourceNames)",
}

# A `const` declaration on a diff line (modifiers and attributes allowed before it), and an `enum` declaration.
_CONST_DECL = re.compile(r"^\s*(?:\[[^\]]*\]\s*)*(?:(?:public|private|protected|internal|static|new|unsafe)\s+)*const\s+\S+\s+\w+\s*=")
_ENUM_DECL = re.compile(r"^\s*(?:\[[^\]]*\]\s*)*(?:(?:public|private|protected|internal|new)\s+)*enum\s+\w+")
_ENUM_HEADER = re.compile(r"\benum\b")


def shared_change_reason(shared: list[str], diff_text: "str | None",
                         registrations: "dict[str, str]" = REGISTRATION_FILES) -> "str | None":
    """Why a change to these shared-project files runs everything (#5951), or None when the map may select by edges.

    A file in `registrations` is FULL. Otherwise `diff_text` (`git diff -U0` with the csharp driver's hunk headers, as
    symbol_diff_text() builds it) is read: FULL when a `+` or `-` line declares a `const` or an `enum`, or a hunk
    sits inside an enum body (its header is the enum's declaration, the nearest type or member line above it).
    Without a diff, or for a `.cs` file the diff does not hold, the answer is FULL: a change that is not read is not
    narrowed. A shared file that is not C# holds no constant or enum, so it is never FULL for this rule."""
    for f in shared:
        if f in registrations:
            return f"{f} is a shared-project registration file ({registrations[f]})"
    cs_files = [f for f in shared if f.endswith(".cs")]
    if not cs_files:
        return None
    if diff_text is None:
        return f"{cs_files[0]} is in a shared project and its diff could not be read (keep-full)"
    sections: dict[str, list[str]] = {}
    current: "list[str] | None" = None
    in_header = False
    for line in diff_text.split("\n"):
        if line.startswith("diff --git "):
            m = re.match(r"diff --git a/(.+?) b/(.+)$", line)
            current = sections.setdefault(m.group(2), []) if m else None
            in_header = True
        elif current is None:
            continue
        elif line.startswith("@@"):
            in_header = False
            current.append(line)
        elif not in_header and line[:1] in ("+", "-"):
            current.append(line)
    for f in cs_files:
        lines = sections.get(f)
        if lines is None:
            return f"{f} is in a shared project and the diff does not hold it (keep-full)"
        for ln in lines:
            if ln.startswith("@@"):
                header = ln.split("@@", 2)[2] if ln.count("@@") >= 2 else ""
                if _ENUM_HEADER.search(header):
                    return f"{f} changes an enum member (a compiled value the map's edges cannot see)"
            elif _CONST_DECL.match(ln[1:]):
                return f"{f} changes a const (a compiled value the map's edges cannot see): {ln[1:].strip()[:80]}"
            elif _ENUM_DECL.match(ln[1:]):
                return f"{f} adds or removes an enum (a compiled value the map's edges cannot see)"
    return None


# map_select()'s `reason` for a change that holds documentation files only (#5894).
DOCS_ONLY_REASON = ("documentation-only change: only a class that reads a changed document as text is selected "
                    "(CI's docs fast path runs no test)")

# The documentation allowlist (the `docs` filter in build.yml); selection_args() passes the live one.
DOC_PATTERNS = (
    "**/*.md",
    "LICENSE",
    "CITATION.cff",
    ".gitignore",
    ".gitattributes",
    "llms.txt",
    "tools/changelog/archive-census.txt",
    "docs/**/*.{md,svg,png,jpg,jpeg,gif}",
    "Screenshots/**/*.{md,svg,png,jpg,jpeg,gif}",
)


_TEST_PROJECT_FILE = re.compile(r"(^|/)[^/]*Tests/")


MAIN_BRANCH = "main"


def narrows(event: str, base_ref: str = "") -> bool:
    """Whether a run may run less than everything: only a pull_request whose base branch is not `main` (#5459).
    A pull request into main is the release gate (dev to main), so it runs every class, Cost=Slow ones included.
    An empty or absent base ref keeps the old behaviour: the replay corpus rows carry none."""
    if event != "pull_request":
        return False
    ref = (base_ref or "").strip()
    if ref.startswith("refs/heads/"):
        ref = ref[len("refs/heads/"):]
    return ref != MAIN_BRANCH


def _full(reason: str) -> dict:
    return {"full": True, "reason": reason, "selected": {}, "why": {}, "seconds": {}, "total": {}}


def _ids(value: object, universe: list) -> set[str]:
    """Paths for a list of file ids. An id is an index into the map's `files`; a string is taken as the path itself."""
    out: set[str] = set()
    if value is None:
        return out
    if isinstance(value, (int, str)):
        value = [value]
    for v in value:  # type: ignore[union-attr]
        if isinstance(v, bool):
            continue
        if isinstance(v, int):
            if 0 <= v < len(universe):
                out.add(universe[v])
        elif isinstance(v, str):
            out.add(v)
    return out


def _seconds(entry: dict) -> float:
    v = entry.get("seconds", 0)
    return float(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else 0.0


def _parse_time(text: object) -> "datetime.datetime | None":
    if not isinstance(text, str):
        return None
    try:
        t = datetime.datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return t if t.tzinfo else t.replace(tzinfo=datetime.timezone.utc)


def _pattern_hit(g: object, live: list[str], docs_changed: list[str]) -> bool:
    """Whether a class's text patterns `g` (a glob list) match a changed file: `live` are the changed files outside
    the documentation allowlist, `docs_changed` the rest."""
    if not isinstance(g, list):
        return False
    # A class that reads a documentation file by name (the migration ladder reads CHANGELOG.md) is selected by a
    # change to exactly that file; the documentation allowlist only keeps wildcard patterns from matching it.
    return (any(matches(p, f) for p in g for f in live)
            or any(p == f for p in g if "*" not in p for f in docs_changed))


def text_pattern_classes(test_map: object, changed: list[str], doc_patterns: "tuple[str, ...] | list[str]") -> dict:
    """{suite: {Class, ...}} of the map's classes whose text patterns (a glob list, not "tree") match a changed
    file: the classes map_select() gives the reason "text pattern" (#5798). Read straight from the map, so it
    answers when map_select() is FULL too; an unreadable map gives nothing."""
    if not isinstance(test_map, dict):
        return {}
    classes, patterns = test_map.get("classes"), test_map.get("text_patterns")
    if not isinstance(classes, dict) or not isinstance(patterns, dict):
        return {}
    live = [f for f in changed if not any(matches(p, f) for p in doc_patterns)]
    live_names = set(live)
    docs_changed = [f for f in changed if f not in live_names]
    out: dict[str, set[str]] = {}
    for suite, entries in classes.items():
        for cls in entries if isinstance(entries, dict) else ():
            g = patterns.get(cls)
            if isinstance(g, list) and all(isinstance(p, str) for p in g) and _pattern_hit(g, live, docs_changed):
                out.setdefault(suite, set()).add(cls)
    return out


def map_select(test_map: object, changed: list[str], drift: "list[str] | None", discovered: dict,
               *, event: str = "pull_request", keep_full: "tuple[str, ...] | list[str]" = KEEP_FULL,
               doc_patterns: "tuple[str, ...] | list[str]" = DOC_PATTERNS,
               now: "datetime.datetime | None" = None, max_age_days: "float | None" = MAP_MAX_AGE_DAYS,
               max_drift: int = MAP_MAX_DRIFT, base_ref: str = "",
               named: "dict[str, set[str]] | None" = None, guard_stage_separate: bool = False,
               shared_diff: "str | None" = None, shared_dirs: "tuple[str, ...]" = SHARED_PROJECT_DIRS) -> dict:
    """Which test classes a pull request runs, from a class-to-file map. Pure: no file, git or clock access
    (`now` is the clock; `max_age_days=None` turns the age check off, for a replay of old runs).

    Schema 1 of the map (nightly's `test-map.json.gz`):

        {"schema": 1, "sha": "<commit the map was built at>", "built_at": "<ISO 8601 UTC>", "tool": "<producer>",
         "files": ["<repo path>", ...],                       # the universe; an id below is an index into it
         "classes": {"<suite>": {"<Class>": {"own": <id or [ids]>,       # the class's own source file(s)
                                             "files": [<id>, ...],       # every file the class ran code from
                                             "seconds": <float>}}},      # its last measured run time
         "text_patterns": {"<Class>": ["<glob>", ...] or "tree"}}        # sources read as text; "tree" = any

    Arguments: `changed` is the pull request's files; `drift` the files changed between the map's commit and the
    merge base (None when they could not be computed); `discovered` is `{suite: {Class: {"Key=Value", ...}}}` as
    scan_classes() returns it, which is how a class the map has never seen is found. `named` is `{suite: {Class, ...}}`:
    the test classes whose source names a symbol the pull request declares or changes (symbol_classes(), tight tokens
    only, #5776); None or empty adds nothing.

    A class is selected when it carries Stage=Guard, its own file changed, it covers a changed file, a text pattern
    of it matches a changed file, it reads the whole tree and any non-documentation file changed, the map has
    never seen it and it holds a test, it shares an xunit collection (of at most COLLECTION_MATE_CAP classes) with a
    class whose own file changed, or its source names a changed symbol (`named`). The last reason only adds classes,
    so a selection never shrinks for it. With `guard_stage_separate` (#5894) the caller runs the whole Stage=Guard
    trait as a stage of its own (the pre-push gate does), so a Stage=Guard class is left out of `selected` and
    `seconds`: `why` keeps it, marked "run in the guard stage", and the answer says `guard_stage_separate`. A pull
    request whose every changed file is documentation (`doc_patterns`) selects only the classes that read a changed
    document as text (no Guard, new or environment-gated class, no changed symbol); the answer says `docs_only` and
    `reason` says why: CI's docs fast path runs nothing for it either (#5894). A file changed since the map counts as changed too (it may have grown new
    call edges), but only when the change holds a product source file the map knows (#5872). `via` names, per
    "covers a changed file" class, the changed file the walk came from; `unknown` names each file the map did not know
    and the rule that took it; `drift` is how many drifted files counted.
    A change under a shared project (`shared_dirs`, #5951) is not keep-full as a whole: it is FULL only when
    shared_change_reason() finds a registration file, or a const or enum in `shared_diff` (the pull request's
    `-U0` diff text; None means the diff could not be read, which is FULL). Any other shared file selects by the
    map's edges. A keep-full file forces FULL only when the pull request's own change holds it: one that changed on dev since
    the map goes through the class links like any other drifted file.

    Returns `{"full": bool, "reason": str, "selected": {suite: [Class, ...]}, "why": {suite: {Class: reason}},
    "seconds": {suite: float}, "total": {suite: int}}`. FULL (never "nothing") whenever the map cannot be trusted:
    missing, unreadable, wrong schema, too old, drift over `max_drift` files, a keep-full file, a file the map knows
    nothing about, or any event but a pull request. That includes a new product .cs file: types found by reflection,
    DI or attribute scanning change what a census class sees without any file it covers changing. The one exception
    is a new .cs file under a test project (a `*Tests/` or `*.Tests/` directory) while `discovered` holds a class the
    map has never seen; that class runs as a "new class"."""
    if event != "pull_request":
        return _full(f"event {event or '(none)'} always runs everything")
    if not narrows(event, base_ref):
        return _full("a pull request into main always runs everything")
    if test_map is None:
        return _full("no test map")
    if not isinstance(test_map, dict):
        return _full("test map unreadable")
    if test_map.get("schema") != MAP_SCHEMA:
        return _full(f"test map schema {test_map.get('schema')!r}, expected {MAP_SCHEMA}")
    universe = test_map.get("files")
    classes = test_map.get("classes")
    patterns = test_map.get("text_patterns", {})
    if not isinstance(universe, list) or not isinstance(classes, dict) or not isinstance(patterns, dict):
        return _full("test map unreadable")
    if max_age_days is not None:
        built = _parse_time(test_map.get("built_at"))
        if built is None:
            return _full("test map has no readable build time")
        clock = now or datetime.datetime.now(datetime.timezone.utc)
        if clock - built > datetime.timedelta(days=max_age_days):
            return _full(f"test map older than {max_age_days:g} days")
    if drift is None:
        return _full("drift since the map's commit unknown")
    if len(drift) > max_drift:
        return _full(f"{len(drift)} files changed since the map (over {max_drift})")

    # Keep-full is about THIS pull request's own change (merge base to head). A keep-full file that only changed on dev
    # since the map's commit is not this pull request's doing and the dev run that merged it already tested it, so it
    # goes through the class links like any other drifted file (#5459: ten pull requests were FULL for that alone).
    for f in changed:
        if any(matches(p, f) for p in keep_full):
            return _full(f"{f} is a keep-full file")
    shared = [f for f in changed if f.startswith(shared_dirs) and not f.endswith(".md")]
    if shared:
        why_shared = shared_change_reason(shared, shared_diff)
        if why_shared:
            return _full(why_shared)
    known: set[str] = {u for u in universe if isinstance(u, str)}
    parsed: dict[str, dict[str, tuple[set[str], set[str]]]] = {}
    for suite, entries in classes.items():
        if not isinstance(entries, dict):
            return _full("test map unreadable")
        parsed[suite] = {}
        for cls, e in entries.items():
            if not isinstance(e, dict):
                return _full("test map unreadable")
            own, covered = _ids(e.get("own"), universe), _ids(e.get("files"), universe)
            parsed[suite][cls] = (own, covered)
            known |= own | covered
    globs: dict[str, "list[str] | str"] = {}
    for cls, p in patterns.items():
        if p == "tree" or (isinstance(p, list) and all(isinstance(x, str) for x in p)):
            globs[cls] = p
        else:
            return _full("test map unreadable")

    # #5872: the drift (files changed on dev between the map's commit and the merge base) counts as changed only when
    # this pull request's own change holds a product source file the map knows. Drift is there to catch a drifted file
    # that grew a call edge INTO the change, which the map's links cannot show yet; nothing grows an edge into a doc,
    # a script or workflow under .github/, or a test file. A drifted test that newly calls a changed test helper
    # names one of its symbols, so `named` (the changed-symbol reason below) still selects it, and a test class added
    # since the map runs as a "new class" whatever the drift. Counting the drift for every change made a one-file test,
    # docs or script change select nearly the whole tree (52-84 drifted files: about 2,300 of 2,870 Darling classes).
    # A miss here is caught by the push to dev, which runs everything (narrows() is False for a push).
    drift_applies = any(f in known and not _TEST_PROJECT_FILE.search(f) and not f.startswith(".github/")
                        and not any(matches(p, f) for p in doc_patterns) for f in changed)
    effective = list(dict.fromkeys(list(changed) + (list(drift) if drift_applies else [])))
    live = [f for f in effective if not any(matches(p, f) for p in doc_patterns)]
    live_names = set(live)
    docs_changed = [f for f in effective if f not in live_names]

    def text_hit(cls: str) -> bool:
        g = globs.get(cls)
        if g == "tree":
            return bool(live)
        return _pattern_hit(g, live, docs_changed)

    # #5894: a change of documentation files only. CI's `docs` filter takes the docs fast path for it (decide():
    # docs_fastpath, no guard run, no shards) and runs nothing, so the selection holds only the classes that read a
    # changed document as text (#5872: a class pinned to README.md still runs for it): no Guard class, no "new class",
    # no class gated on an environment, no changed symbol.
    docs_only = bool(changed) and not live

    new_classes = any(cls not in parsed.get(suite, {}) for suite, found in discovered.items() for cls in found)
    # A file the map does not know is named with the rule that caught it (#5872), in the FULL reason or in `unknown`.
    unknown: dict[str, str] = {}
    for f in live:
        if f in known:
            continue
        if any(isinstance(g, list) and any(matches(p, f) for p in g) for g in globs.values()):
            continue
        if f.endswith(".cs"):
            if _TEST_PROJECT_FILE.search(f) and new_classes:
                unknown[f] = "selecting the new test classes the tree scan finds"
                continue  # a new test file: its classes are found through `discovered`
            # Types found by reflection, DI or attribute scanning change what a census class sees with no covered
            # file changing, so a new product .cs file still runs everything.
            return _full(f"unknown file: {f}: selecting everything (new file not in the map)")
        if f.startswith(".github/"):
            # A script or workflow under .github/ that no class covers and no text pattern reads cannot change a test
            # class's outcome (the keep-full build.yml returned FULL above), so it selects nothing of its own.
            unknown[f] = "selecting the classes that read it as text (none)"
            continue
        return _full(f"unknown file: {f}: selecting everything (in no class, no text pattern and not in the map)")

    # A class the map has never seen runs because `discovered` lists it. If the tree scan found no class at all for a
    # suite the map has classes for, the scan itself is broken and every new class would be silently left out.
    for suite, entries in parsed.items():
        if entries and not discovered.get(suite):
            return _full(f"no test class was found in the tree for suite {suite}")

    live_set = set(live)
    via: dict[str, dict[str, str]] = {}
    selected: dict[str, list[str]] = {}
    why: dict[str, dict[str, str]] = {}
    seconds: dict[str, float] = {}
    total: dict[str, int] = {}
    for suite in sorted(set(parsed) | set(discovered)):
        found = discovered.get(suite, {})
        mapped = parsed.get(suite, {})
        reasons: dict[str, str] = {}
        for cls, (own, covered) in mapped.items():
            if live_set & own:
                reasons[cls] = "own file changed"
            elif live_set & covered:
                reasons[cls] = "covers a changed file"
                # #5872: the changed file (a shared base class, fixture or product file) the dependency walk came from.
                via.setdefault(suite, {})[cls] = sorted(live_set & covered)[0]
            elif text_hit(cls):
                reasons[cls] = "reads the tree" if globs.get(cls) == "tree" else "text pattern"
        for cls, traits in ([] if docs_only else found.items()):
            if cls not in mapped:
                # #5872: a helper type (a fake, a logger, a harness) declares no test, so the map never has it and the
                # runner runs nothing for it; only a class that holds or inherits a test is new.
                if NO_TESTS_TRAIT not in traits:
                    reasons[cls] = "new class"
            elif "Stage=Guard" in traits:
                reasons.setdefault(cls, "guard stage")
            if GATE_TRAIT in traits and NO_TESTS_TRAIT not in traits:
                reasons.setdefault(cls, "gated on an environment the map shards lack")
        # #5872: a changed test class's xunit collection mates run too: they share its fixture and its serialized
        # statics, and moving a class into a collection (a changed [Collection] attribute) changes what runs beside
        # them. A collection over COLLECTION_MATE_CAP classes (live-postgres, about 650) is a serialization bucket for
        # one shared store, not a fixture its members read state from, so its mates are not added.
        members: dict[str, list[str]] = {}
        for cls, traits in found.items():
            if NO_TESTS_TRAIT not in traits:
                for t in traits:
                    if t.startswith(COLLECTION_PREFIX):
                        members.setdefault(t, []).append(cls)
        for cls in [c for c, r in reasons.items() if r == "own file changed"]:
            for t in sorted(x for x in found.get(cls, ()) if x.startswith(COLLECTION_PREFIX)):
                if len(members.get(t, ())) <= COLLECTION_MATE_CAP:
                    for mate in members.get(t, ()):
                        reasons.setdefault(mate, "shares a collection with a changed class")
        # #5776: a class whose source names a symbol the change declares or edits runs too. The map links a class to
        # the files it RAN; a class that only reads a source file as text, or reflects over a catalog, has no such
        # link, and a small pull request whose files the map knows would leave it out. Last, with setdefault, so a
        # class keeps the reason it already had and the selection only grows.
        for cls in sorted({} if docs_only else (named or {}).get(suite, ())):
            if cls in mapped or cls in found:
                reasons.setdefault(cls, "names a changed symbol")
        # #5894: the caller runs the whole Stage=Guard trait on its own, so a Guard class in the selection would run twice.
        # It keeps its `why` entry (the printed summary still counts it) but is not selected.
        in_guard_stage = {c for c in reasons if "Stage=Guard" in found.get(c, ())} if guard_stage_separate else set()
        kept = sorted(c for c in reasons if c not in in_guard_stage)
        selected[suite] = kept
        why[suite] = {c: ("run in the guard stage" if c in in_guard_stage else reasons[c]) for c in sorted(reasons)}
        seconds[suite] = round(sum(_seconds(classes[suite][c]) for c in kept if c in mapped), 3)
        total[suite] = len(set(mapped) | set(found))
    return {"full": False, "reason": DOCS_ONLY_REASON if docs_only else "", "selected": selected, "why": why,
            "seconds": seconds, "total": total, "via": via, "drift": len(drift) if drift_applies else 0,
            "unknown": unknown, "docs_only": docs_only,
            "guard_stage_separate": guard_stage_separate}


def load_map(path: str) -> "dict | None":
    """The map file (plain or gzip), or None when it is missing or not JSON."""
    try:
        opener = gzip.open if path.endswith(".gz") else open
        with opener(path, "rt", encoding="utf-8") as fh:  # type: ignore[operator]
            return json.load(fh)
    except (OSError, ValueError):
        return None


def git_drift(map_sha: str, base: str, root: str = ROOT) -> "list[str] | None":
    """`git diff --no-renames --name-only <map sha>..<base>`: both paths of a rename count. None when git cannot say."""
    try:
        out = subprocess.run(["git", "-C", root, "diff", "--no-renames", "--name-only", f"{map_sha}..{base}"],
                             capture_output=True, text=True, timeout=120, check=True).stdout
    except (OSError, subprocess.SubprocessError):
        return None
    return [ln.strip() for ln in out.split("\n") if ln.strip()]


def selection_args(rules: Rules) -> dict:
    """The keep-full and documentation patterns, read from the same filters the workflow evaluates."""
    keep = list(KEEP_FULL)
    for name in ("root", "core"):
        # #5951: the shared projects are judged by map_select()'s shared_change_reason(), not as keep-full wholesale.
        keep.extend(p for p in rules.build.get(name, []) if not p.startswith(SHARED_PROJECT_DIRS))
    return {"keep_full": tuple(dict.fromkeys(keep)), "doc_patterns": tuple(rules.build["docs"])}


def _median(values: list) -> float:
    v = sorted(values)
    n = len(v)
    return 0.0 if not n else float(v[n // 2]) if n % 2 else (v[n // 2 - 1] + v[n // 2]) / 2


def _selected_count(sel: dict, size: int) -> int:
    """How many classes a map_select() answer runs: `size` (every class of the tree) when it is FULL."""
    return size if sel["full"] else sum(len(v) for v in sel["selected"].values())


def map_replay(test_map: "dict | None", corpus: str = CORPUS, root: str = ROOT, quiet: bool = False,
               misses_out: "str | None" = None, symbols_base: "str | None" = None) -> int:
    """Like replay(), but asks the map: would each failing class of each corpus row have been selected, taking the map
    as current (no drift, no age check)? Returns the miss count. Reports only; the caller does not fail on it yet.
    The printed list is capped at 40; `misses_out` names a file that gets EVERY miss as one JSON row per line
    (date, event, head_sha, suite, class, changed_files), so none goes unclassified (#5459).

    With `symbols_base` (#5798), each row whose head commit this clone has is also asked with the classes that name
    a symbol the commit changed against its merge base with `symbols_base` (map_select()'s `named`), and the
    report prints the classes selected per row before and after that rule (median and maximum) and how many the
    rule names on its own. A miss is still counted on the selection without it, so the rule cannot hide one."""
    extra = selection_args(Rules())
    accepted = load_accepted()
    tree = scan_classes(root)
    if isinstance(test_map, dict) and isinstance(test_map.get("classes"), dict):
        tree_sel = {s: v for s, v in tree.items() if s in test_map["classes"]}
    else:
        tree_sel = tree
    rows = checked = misses = full_rows = accepted_seen = 0
    missed: list[str] = []
    rows_out: list[dict] = []
    index = test_file_index(root) if symbols_base else None
    size = sum(len(v) for v in tree_sel.values())
    before: list[int] = []
    after: list[int] = []
    named_counts: list[int] = []
    partial_before: list[int] = []
    partial_after: list[int] = []
    no_commit = 0
    with open(corpus, encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            row = json.loads(line)
            rows += 1
            sdiff = commit_diff_text(root, symbols_base, row["head_sha"]) if symbols_base else None
            sel = map_select(test_map, row["changed_files"], [], tree, event=row["event"], max_age_days=None,
                             shared_diff=sdiff, **extra)
            full_rows += bool(sel["full"])
            if symbols_base:
                named = symbol_classes(root, symbols_base, head=row["head_sha"], index=index)
                if named is None:
                    no_commit += 1
                else:
                    with_named = map_select(test_map, row["changed_files"], [], tree_sel, event=row["event"],
                                            max_age_days=None, named=named, shared_diff=sdiff, **extra)
                    without = map_select(test_map, row["changed_files"], [], tree_sel, event=row["event"],
                                         max_age_days=None, shared_diff=sdiff, **extra)
                    b, a = _selected_count(without, size), _selected_count(with_named, size)
                    before.append(b)
                    after.append(a)
                    named_counts.append(sum(len(v) for v in named.values()))
                    if not without["full"]:
                        partial_before.append(b)
                        partial_after.append(a)
            for cls in row["failed_classes"]:
                prefix, simple = split_failed_class(cls)
                suite = SUITE_BY_PREFIX.get(prefix)
                if suite is None or simple not in tree.get(suite, {}):
                    continue
                checked += 1
                if sel["full"] or simple in sel["selected"].get(suite, []):
                    continue
                if (row["head_sha"][:10], cls) in accepted:
                    accepted_seen += 1
                    continue
                misses += 1
                missed.append(f"{row['date']} {row['head_sha'][:10]} {cls} files={len(row['changed_files'])}"
                              f" e.g. {row['changed_files'][0]}")
                rows_out.append({"date": row["date"], "event": row["event"], "head_sha": row["head_sha"],
                                 "suite": suite, "class": cls, "changed_files": row["changed_files"]})
    if not quiet:
        print(f"map replay: {rows} rows ({full_rows} FULL), {checked} failing classes checked, {accepted_seen} accepted, "
              f"{misses} misses (reported, not failing yet)")
        for m in missed[:40]:
            print("MAP-MISS", m)
        if len(missed) > 40:
            print(f"... {len(missed) - 40} more misses not printed; --misses-out FILE lists every one")
        if symbols_base:
            print(f"symbol rule (#5798): {len(before)} rows measured ({no_commit} without their head commit here), "
                  f"{size} classes in the tree, cap {symbol_cap(index)} test files")
            if before:
                print(f"  selected per row: before median {_median(before):g} max {max(before)}, after median "
                      f"{_median(after):g} max {max(after)}")
                print(f"  the rule names on its own: median {_median(named_counts):g} max {max(named_counts)}")
            if partial_before:
                added = [a - b for a, b in zip(partial_after, partial_before)]
                print(f"  rows not FULL ({len(partial_before)}): before median {_median(partial_before):g} max "
                      f"{max(partial_before)}, after median {_median(partial_after):g} max {max(partial_after)}, "
                      f"added median {_median(added):g} max {max(added)}")
    if misses_out:
        with open(misses_out, "w", encoding="utf-8", newline="\n") as out:
            for r in rows_out:
                out.write(json.dumps(r, sort_keys=True) + "\n")
    return misses


def map_keep(selection: object, suite: str) -> "tuple[set[str] | None, str]":
    """The class names a shard of `suite` keeps, from the selection `map-select --out` wrote (#5459, live selection).

    Returns (names, reason). `names` is None, meaning the shard runs everything it was cut, whenever the selection
    cannot be trusted: it is missing, not a JSON object, FULL, has no list of names for the suite, a name that is not
    text, or an empty list. Empty is distrusted on purpose: the Guard classes are always selected, so a selection with
    no class in it for a suite means the selection broke, not that nothing needs to run."""
    if not isinstance(selection, dict):
        return None, "the selection is missing or unreadable"
    if selection.get("full") is not False:
        return None, f"the selection is FULL ({selection.get('reason') or 'no reason given'})"
    chosen = selection.get("selected")
    names = chosen.get(suite) if isinstance(chosen, dict) else None
    # #5894: an empty list is trusted only when the selection says why it is empty: Guard classes left out because the
    # caller runs that stage itself, or a documentation-only change. CI's selection sets neither.
    explained = selection.get("guard_stage_separate") is True or selection.get("docs_only") is True
    if not isinstance(names, list) or (not names and not explained) or not all(isinstance(n, str) and n for n in names):
        return None, f"the selection holds no usable class list for the {suite} suite"
    return set(names), ""


def shadow_summary(result: dict) -> str:
    """The step-summary text of the selection job: how many classes the map picked, and the estimated seconds."""
    lines = ["### Test map selection (#5459): the Darling PG, Lite and no-store Darling passes run only the classes below", ""]
    if result.get("full"):
        lines.append(f"Selection: FULL ({result.get('reason') or 'no reason given'}), so every shard ran everything.")
        return "\n".join(lines) + "\n"
    lines += ["Estimated seconds are the nightly's instrumented times, so they read high.", "",
              "| suite | selected classes | of | estimated seconds |", "|---|---|---|---|"]
    for suite in sorted(result.get("selected", {})):
        lines.append(f"| {suite} | {len(result['selected'][suite])} | {result['total'].get(suite, 0)} | "
                     f"{result['seconds'].get(suite, 0):.0f} |")
    for suite in sorted(result.get("why", {})):
        counts: dict[str, int] = {}
        for reason in result["why"][suite].values():
            counts[reason] = counts.get(reason, 0) + 1
        lines.append(f"- {suite} by reason: " + ", ".join(f"{k} {v}" for k, v in sorted(counts.items())))
    return "\n".join(lines) + "\n"


REPORT_ATTEMPT = re.compile(r"-a(\d+)\.xml$")


def failed_classes(reports: str) -> "tuple[dict[str, set[str]], dict[str, int], int]":
    """The classes with a failed test in the xunit reports under `reports`, one subfolder per uploaded artifact: the
    shards' `darling-tests-timing-N` and `lite-tests-timing-N`, and the Guard tests job's `guard-tests-timing-darling`
    and `guard-tests-timing-lite` (#5459). The suite is the one the folder name says.

    A report named `...-a<N>.xml` was written by run attempt N. Staleness is decided per artifact folder: within a
    folder, the reports of the highest attempt present are kept and the reports of a lower attempt are left out and
    counted as stale. A shard that failed in attempt 1 and passed in attempt 2 uploads its artifact again with
    `overwrite`, so the folder holds attempt 2 only; if an old attempt-1 file is still there, it must not list its
    attempt-1 class (#5459, run 37776652739). A shard that passed in attempt 1 is not re-run by "Re-run failed jobs",
    so its folder still holds only `-a1` files and they count in attempt 2. A report with no attempt in its name
    always counts. Returns ({suite: {Class}}, {suite: report count}, stale report count)."""
    failed: dict[str, set[str]] = {"darling": set(), "lite": set()}
    seen: dict[str, int] = {"darling": 0, "lite": 0}
    stale = 0
    if not os.path.isdir(reports):
        return failed, seen, stale
    # (artifact folder, path) of every report, with the attempt its name carries (None when it carries none).
    found: list[tuple[str, str, "int | None"]] = []
    for dirpath, _, names in os.walk(reports):
        top = os.path.relpath(dirpath, reports).replace("\\", "/").split("/")[0]
        for name in names:
            if name.endswith(".xml"):
                written_by = REPORT_ATTEMPT.search(name)
                found.append((top, os.path.join(dirpath, name), int(written_by.group(1)) if written_by else None))
    latest: dict[str, int] = {}
    for top, _, written_by in found:
        if written_by is not None:
            latest[top] = max(latest.get(top, written_by), written_by)
    for top, path, written_by in found:
        suite = "darling" if "darling" in top else "lite" if "lite" in top else ""
        if not suite:
            continue
        if written_by is not None and written_by < latest[top]:
            stale += 1
            continue
        seen[suite] += 1
        try:
            for _, el in ET.iterparse(path, events=("end",)):
                if el.tag == "test" and (el.get("result") or "").lower() == "fail":
                    failed[suite].add(re.split(r"[.+/]", el.get("type") or "")[-1])
                el.clear()
        except (ET.ParseError, OSError):
            seen[suite] -= 1  # an unreadable report counts as no report
    return failed, seen, stale


def failed_jobs(path: "str | None") -> "list[str] | None":
    """Names of the jobs that concluded `failure`, from the latest attempt's job list (`gh api .../attempts/N/jobs`: one
    `{"name", "conclusion"}` object per line, or the API's own `{"jobs": [...]}` object). None when it cannot be read.
    A job that failed without a failed test (the whole-tree guards job ending on a leaked foreground thread) shows up
    here and nowhere else."""
    if not path:
        return None
    try:
        with open(path, encoding="utf-8") as fh:
            text = fh.read()
    except OSError:
        return None
    items: list = []
    try:
        data = json.loads(text)
        if isinstance(data, dict):
            items = data["jobs"] if isinstance(data.get("jobs"), list) else [data]
        elif isinstance(data, list):
            items = data
    except ValueError:
        for line in text.splitlines():
            try:
                items.append(json.loads(line))
            except ValueError:
                continue
    return sorted({str(j.get("name")) for j in items if isinstance(j, dict) and j.get("conclusion") == "failure"})


def shadow_row(selection: "dict | None", failed: "dict[str, set[str]]", seen: "dict[str, int]", meta: dict,
               stale: int = 0, jobs: "list[str] | None" = None) -> dict:
    """One JSONL row per run: the selection's size and the classes that failed although it would not have run them.
    A FULL (or missing) selection selects every class, so it can have no miss. `stale` counts the reports left out as
    a lower attempt's in their artifact folder; `jobs` is the latest attempt's failed jobs (None when unknown)."""
    if isinstance(selection, dict):
        sel = selection
    else:
        # No selection file. When the selection job itself did not finish (a newer push cancels the run), say so: that
        # is expected, not a script fault (#5459, run 37832135291).
        job = str(meta.get("selection_job") or "")
        sel = _full(f"the selection job ended {job}, so no selection was written" if job and job != "success"
                    else "no selection was written")
    misses: list[dict] = []
    if not sel.get("full"):
        for suite in sorted(failed):
            chosen = set(sel.get("selected", {}).get(suite, []))
            misses += [{"suite": suite, "class": c} for c in sorted(failed[suite]) if c not in chosen]
    row = {"v": 1, **meta, "full": bool(sel.get("full")), "reason": sel.get("reason", ""),
           "selected": {s: len(v) for s, v in sel.get("selected", {}).items()},
           "total": sel.get("total", {}), "seconds": sel.get("seconds", {}),
           "reports": seen, "stale_reports": stale, "failed": {s: sorted(v) for s, v in failed.items()},
           "misses": misses}
    if jobs is not None:
        row["failed_jobs"] = jobs
    return row


# ---------------------------------------------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------------------------------------------

# ---------------------------------------------------------------------------------------------------------
# Changed symbols: the test classes whose source names what a change declares or edits (#5776).
# ---------------------------------------------------------------------------------------------------------
#
# Most test classes that went red on a lane's pull request after it passed the lane's own runs named a symbol the
# change declared or edited: a source scan that greps for `_fleetGateStats?.RecordSlot(`, a pin on a constant's
# value, a census over a method's callers. The class-to-file map does not link them (they read the file as text or
# reflect over it), and on a big change its coverage answer picks 80% or more of every class, which no lane runs.
# `local-classes` lists the classes that name a changed symbol, most specific first, for a lane to run before it
# pushes; map_select() takes the same answer (tight tokens only) as one more reason to select a class.
#
# Tokens. TIGHT: the names a changed .cs/.js/.xaml line declares (class, record, struct, enum, interface, const,
# method, property, field), the member a C# hunk sits in (its hunk header, with git's csharp driver), and SQL object
# names (v_* anywhere, CREATE/ALTER TABLE/VIEW/FUNCTION/PROCEDURE in .sql). WIDE adds every identifier of 6 or more
# characters on a changed line. A token taken only from test files is dropped (a test's own names select nothing
# else), and so is a stop word or a name shorter than 4 characters. A token named in more than `cap` test files says
# too little to choose by: it is dropped and printed as NOISY. A class declared in a changed test file is listed too.
#
# What it cannot see: a class that names no changed symbol at all (it reflects over the whole tool catalog or the
# collector set); those carry Stage=Guard and run in the guard stage, except the few slow ones
# on its SlowCatalogClasses list, which the shards run. Matching is per test FILE and case-sensitive on
# whole words, so every class of a file that names the token is listed.

SYMBOL_EXTENSIONS = (".cs", ".sql", ".js", ".xaml")
SYMBOL_SUITES = ("darling", "lite")
SYMBOL_CAP = 25
# #5798: the cap is a share of the test files, so it grows with the tree: 1% of the 3,023 test files of 2026-10-14
# is 30. SYMBOL_CAP is its floor. Measured on the corpus (`--replay --map FILE --symbols origin/dev`, 414 rows),
# the rule names a median of 51 classes on its own (at most 700, 18% of the 3,826) and adds a median of 1 (at most
# 202) to a map answer that is not FULL, well under the third of the tree that would call for a tighter cap.
SYMBOL_CAP_SHARE = 0.01
SYMBOL_MIN_LEN = 4
WIDE_MIN_LEN = 6
LOCAL_CLASS_LIMIT = 300
RIG_LITERAL = '"DARLING_TEST_PG"'  # the quoted variable name a live class reads to skip without a store

_CS_KEYWORDS = frozenset("""
abstract as base bool break byte case catch char checked class const continue decimal default delegate do double else
enum event explicit extern false finally fixed float for foreach goto if implicit in int interface internal is lock
long namespace new null object operator out override params private protected public readonly ref return sbyte sealed
short sizeof stackalloc static string struct switch this throw true try typeof uint ulong unchecked unsafe ushort using
virtual void volatile while add alias and ascending async await by descending dynamic equals from get global group
init into join let managed nameof nint not notnull nuint on or orderby partial record remove required scoped select set
unmanaged value var when where with yield function export import const let undefined
""".split())
_TEST_API_WORDS = frozenset("""
Assert Equal NotEqual True False Null NotNull Contains DoesNotContain Empty NotEmpty Single Throws ThrowsAsync ThrowsAny
ThrowsAnyAsync Same NotSame InRange IsType IsAssignableFrom StartsWith EndsWith Matches DoesNotMatch Fact Theory
InlineData MemberData ClassData Trait Skip SkipUnless Collection CollectionDefinition IClassFixture ICollectionFixture
IAsyncLifetime IDisposable IAsyncDisposable Dispose DisposeAsync InitializeAsync Xunit ITestOutputHelper Fixture
TestContext Task ValueTask CancellationToken ToString Equals GetHashCode Length Count Any All First Select Where
ToList ToArray String Exception Console Regex
""".split())
_COMMON_WORDS = frozenset("""
Server Servers Collector Collectors Query Queries Name Names Value Values Type Data Result Results Text Message Error
Errors Status Time Start Stop Read Write Item Items List Keys Source Target Path File Files Line Lines Store Service
Options Settings Config Connection Command Database Table Index Logger Test Tests Main Helper Model State Info Context
Default Event Events Window View Builder Reader Writer Parse Format Create Update Delete Execute Build Check Close Open
""".split())
STOP_WORDS = _CS_KEYWORDS | _TEST_API_WORDS | _COMMON_WORDS

_MODIFIERS = (r"(?:public|private|protected|internal|static|readonly|const|async|override|virtual|sealed|abstract|"
              r"partial|new|extern|unsafe|volatile|required|file|event|implicit|explicit)")
_DECL_TYPE = re.compile(r"\b(?:class|record|struct|enum|interface)\s+(?:class\s+|struct\s+)?([A-Za-z_]\w*)")
_DECL_CONST = re.compile(r"\bconst\s+[\w<>\[\]?,.\s]+?\s([A-Za-z_]\w*)\s*=")
_DECL_MEMBER = re.compile(r"^\s*(?:\[[^\]]*\]\s*)*(?:" + _MODIFIERS + r"\s+)+[\w<>\[\]?,.()\s]*?([A-Za-z_]\w*)\s*"
                          r"(?:<[^>]*>\s*)?(?:\(|\{|=>|=|;)")
# A method without a modifier (`Task<int> ReadAsync(`): the first word must be a type, not a statement keyword, and
# the line must not end a statement (`return Foo(x);` calls, it does not declare).
_DECL_BARE = re.compile(r"^\s*([\w.]+(?:<[^>]*>)?(?:\[\])?\??)\s+([A-Za-z_]\w*)\s*(?:<[^>]*>\s*)?\((?!.*;\s*$)")
_BARE_NOT_TYPE = frozenset("return await new throw else yield case goto using in is as not and or when var typeof "
                           "nameof if while for foreach switch lock catch".split())
_JS_DECL = re.compile(r"\bfunction\s*\*?\s*([A-Za-z_$][\w$]*)|\b(?:const|let|var|class)\s+([A-Za-z_$][\w$]*)"
                      r"|^\s*(?:async\s+)?([A-Za-z_$][\w$]*)\s*\([^)]*\)\s*\{|([A-Za-z_$][\w$]*)\s*:\s*(?:async\s+)?function")
_XAML_DECL = re.compile(r'x:Name="(\w+)"|x:Class="[\w.]*?(\w+)"|\{Binding\s+(?:Path=)?(\w+)')
_SQL_OBJECT = re.compile(r"\b(?:CREATE|ALTER)\s+(?:OR\s+(?:REPLACE|ALTER)\s+)?(?:MATERIALIZED\s+)?"
                         r"(?:TABLE|VIEW|FUNCTION|PROCEDURE|PROC)\s+(?:IF\s+NOT\s+EXISTS\s+)?([\w\[\]\".]+)", re.I)
_SQL_VIEW = re.compile(r"\bv_\w+")
# #5798: a store migration is `private const string V183Sql = ...` plus `new Migration(183, ...)`; the tests name it
# V183 (in a rung pin, a sentinel or a comment), so both give the token V183. `V183Sql` matches a reference too
# (`sb.Append(V183Sql)`), wanted: a line that uses a migration's SQL touches that migration. The V-number comes on
# top of what the other rules take from the line (`new Migration(` still gives `Migration`, which so many test files
# name that the cap drops it as noisy): a rule here may only add tokens, never take one away, or the selection
# could run fewer classes than before. A known limit: an edit inside an existing migration's verbatim SQL body
# (the lines between `V183Sql = @"` and its closing quote) declares nothing, so it gives no token.
_MIGRATION = re.compile(r"\bV(\d+)Sql\b|\bnew\s+Migration\s*\(\s*(\d+)\s*,")
_WORD = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
_HUNK = re.compile(r"^@@ -\d+(?:,\d+)? \+\d+(?:,\d+)? @@ ?(.*)$")


def _git(root: str, *args: str) -> "str | None":
    try:
        return subprocess.run(["git", "-C", root, *args], capture_output=True, text=True, encoding="utf-8",
                              errors="replace", timeout=120, check=True).stdout
    except (OSError, subprocess.SubprocessError):
        return None


def symbol_diff_text(root: str, *revs: str) -> "str | None":
    """`git diff -U0 --no-renames <revs>` over the symbol file types, plus the .cs part a second time for its hunk
    headers. The repository's .gitattributes sets no diff driver, so a C# hunk header is the last column-0 line
    above it: the type of a file-scoped namespace. The first pass uses git's built-in csharp driver, whose header is
    the member the hunk sits in; the second keeps the default, so both the member and its type are read. None when
    git fails."""
    fd, attrs = tempfile.mkstemp(suffix=".gitattributes")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write("*.cs diff=csharp\n")
        members = _git(root, "-c", f"core.attributesFile={attrs}", "diff", "-U0", "--no-renames", *revs, "--",
                       *[f"*{e}" for e in SYMBOL_EXTENSIONS])
    finally:
        os.remove(attrs)
    types = _git(root, "diff", "-U0", "--no-renames", *revs, "--", "*.cs")
    if members is None or types is None:
        return None
    headers = [ln for ln in types.split("\n") if ln.startswith(("diff --git ", "--- ", "+++ ", "@@ "))]
    return members + "\n" + "\n".join(headers)


def symbol_diff(root: str, base: str, untracked: bool = True) -> "tuple[str, str, list[str]] | None":
    """(merge base, symbol_diff_text() of the WORKING TREE against it, every changed path) for `base`, so it answers
    before a commit too. With `untracked`, a new file git does not track yet joins as all added lines. None when git
    fails."""
    mb = _git(root, "merge-base", base, "HEAD")
    if mb is None or not mb.strip():
        return None
    mb = mb.strip()
    text = symbol_diff_text(root, mb)
    names = _git(root, "diff", "--name-only", "--no-renames", mb)
    if text is None or names is None:
        return None
    paths = [ln.strip() for ln in names.split("\n") if ln.strip()]
    if untracked:
        extra = _git(root, "ls-files", "--others", "--exclude-standard") or ""
        for rel in (ln.strip() for ln in extra.split("\n")):
            if not rel or rel in paths:
                continue
            paths.append(rel)
            if not rel.endswith(SYMBOL_EXTENSIONS):
                continue
            try:
                with open(os.path.join(root, rel), encoding="utf-8-sig", errors="replace") as fh:
                    body = fh.read().split("\n")
            except OSError:
                continue
            text += f"diff --git a/{rel} b/{rel}\n--- /dev/null\n+++ b/{rel}\n@@ -0,0 +1,{len(body)} @@\n"
            text += "".join(f"+{ln}\n" for ln in body)
    return mb, text, paths


def _declared(path: str, line: str) -> set[str]:
    """The names a line of `path` declares (the TIGHT tokens of one line)."""
    out = set(_SQL_VIEW.findall(line))
    if path.endswith(".sql"):
        out.update(m.split(".")[-1].strip('[]"') for m in _SQL_OBJECT.findall(line))
    elif path.endswith(".js"):
        out.update(g for m in _JS_DECL.findall(line) for g in m if g)
    elif path.endswith(".xaml"):
        out.update(g for m in _XAML_DECL.findall(line) for g in m if g)
    elif path.endswith(".cs"):
        out.update(_DECL_TYPE.findall(line))
        out.update(_DECL_CONST.findall(line))
        out.update(f"V{a or b}" for a, b in _MIGRATION.findall(line))
        m = _DECL_MEMBER.match(line)
        if m:
            out.add(m.group(1))
        m = _DECL_BARE.match(line)
        if m and m.group(1) not in _BARE_NOT_TYPE:
            out.add(m.group(2))
    return out


def changed_symbols(diff_text: str, wide: bool = False) -> "tuple[dict[str, set[str]], set[str]]":
    """({token: {paths it came from}}, {changed test files}) from symbol_diff()'s text. Removed lines count as
    changed (a renamed or deleted name is what a stale test still names). Tokens from test files only are dropped."""
    found: dict[str, set[str]] = {}
    test_files: set[str] = set()
    path = ""
    for raw in diff_text.split("\n"):
        line = raw.rstrip("\r")
        if line.startswith("diff --git "):
            path = ""
            continue
        if line.startswith("--- "):
            if line[4:] != "/dev/null":
                path = line[6:] if line.startswith("--- a/") else line[4:]
            continue
        if line.startswith("+++ "):
            if line[4:] != "/dev/null":
                path = line[6:] if line.startswith("+++ b/") else line[4:]
            if path and _TEST_PROJECT_FILE.search(path):
                test_files.add(path)
            continue
        if not path:
            continue
        hunk = _HUNK.match(line)
        if hunk:
            words = _declared(path, hunk.group(1))
        elif line[:1] in ("+", "-"):
            body = line[1:]
            words = _declared(path, body)
            if wide:
                words |= {w for w in _WORD.findall(body) if len(w) >= WIDE_MIN_LEN}
        else:
            continue
        for w in words:
            found.setdefault(w, set()).add(path)
    tokens = {w: paths for w, paths in found.items()
              if len(w) >= SYMBOL_MIN_LEN and w not in STOP_WORDS
              and any(not _TEST_PROJECT_FILE.search(p) for p in paths)}
    return tokens, test_files


def test_file_index(root: str = ROOT) -> "list[tuple[str, str, list[str], bool, set[str]]]":
    """(suite, repo path, [class names], needs a PostgreSQL rig, {words}) for every test source file of SYMBOL_SUITES."""
    out = []
    found: "list[tuple[str, str]]" = []
    for suite in SYMBOL_SUITES:
        base = os.path.join(root, SUITE_DIRS[suite])
        for dp, dn, fn in os.walk(base):
            dn[:] = [d for d in dn if d not in ("bin", "obj")]
            found += [(suite, os.path.join(dp, f)) for f in fn if f.endswith(".cs")]
    prefetch_sources([p for _s, p in found], "replace")
    for suite, path in found:
        try:
            text = read_source(path, "replace")
        except OSError:
            continue
        classes = [m.group(1) for m in (_CLASS_DECL.match(ln) for ln in text.split("\n")) if m]
        if classes:
            out.append((suite, os.path.relpath(path, root).replace(os.sep, "/"), classes,
                        RIG_LITERAL in text, set(_WORD.findall(text))))
    return out


def symbol_cap(index: list) -> int:
    """The most test files a token may be named in and still select (#5798): SYMBOL_CAP_SHARE of the test files of
    `index`, never under SYMBOL_CAP."""
    return max(SYMBOL_CAP, round(SYMBOL_CAP_SHARE * len(index)))


def symbol_matches(tokens: "dict[str, set[str]]", test_files: set[str], index: list, cap: "int | None" = None) -> dict:
    """Match tokens to test classes. Returns {"hits": {(suite, Class): {"tokens": set, "rig": bool, "own": bool}},
    "counts": {token: test files naming it}, "noisy": {token: count}, "cap": the cap used} (a token over `cap`
    files is noisy and unused; None takes symbol_cap(index))."""
    if cap is None:
        cap = symbol_cap(index)
    counts = {t: 0 for t in tokens}
    for _suite, _rel, _classes, _rig, words in index:
        for t in tokens.keys() & words:
            counts[t] += 1
    noisy = {t: n for t, n in counts.items() if n > cap}
    usable = {t for t, n in counts.items() if 0 < n <= cap}
    hits: dict[tuple[str, str], dict] = {}
    for suite, rel, classes, rig, words in index:
        named = usable & words
        own = rel in test_files
        if not named and not own:
            continue
        for cls in classes:
            h = hits.setdefault((suite, cls), {"tokens": set(), "rig": False, "own": False})
            h["tokens"] |= named
            h["rig"] = h["rig"] or rig
            h["own"] = h["own"] or own
    return {"hits": hits, "counts": counts, "noisy": noisy, "cap": cap}


def ordered_hits(match: dict) -> "list[tuple[str, str, dict]]":
    """Most specific first: a changed test file's own classes, then by the fewest test files that name the class's
    rarest token, then by more tokens, then by name."""
    counts = match["counts"]

    def key(item):
        (suite, cls), h = item
        rarest = min((counts[t] for t in h["tokens"]), default=10 ** 9)
        return (0 if h["own"] else 1, rarest, -len(h["tokens"]), suite, cls)

    return [(s, c, h) for (s, c), h in sorted(match["hits"].items(), key=key)]


def named_classes(match: dict) -> "dict[str, set[str]]":
    """{suite: {Class, ...}} of the hits that name a token (a changed test file's own classes alone are not named)."""
    out: dict[str, set[str]] = {}
    for (suite, cls), h in match["hits"].items():
        if h["tokens"]:
            out.setdefault(suite, set()).add(cls)
    return out


def commit_diff_text(root: str, base: str, head: str) -> "str | None":
    """symbol_diff_text() of `head` against its merge base with `base`: the diff map_select()'s `shared_diff` reads
    for a replayed commit (#5951). None when this clone lacks the commit, so a shared-project row stays FULL."""
    mb = _git(root, "merge-base", base, head)
    return symbol_diff_text(root, mb.strip(), head) if mb and mb.strip() else None


def symbol_classes(root: str, base: str, cap: "int | None" = None, head: "str | None" = None,
                   index: "list | None" = None) -> "dict[str, set[str]] | None":
    """map_select()'s `named`: {suite: {Class, ...}} whose source names a TIGHT token of the change against `base`
    (tracked files only). With `head`, the change is `head` against its merge base with `base` (the replay's
    measure, #5798); without it, the working tree. None when git cannot say, so the selection is what it was
    without this reason."""
    if head is None:
        diff = symbol_diff(root, base, untracked=False)
        if diff is None:
            return None
        text = diff[1]
    else:
        mb = _git(root, "merge-base", base, head)
        text = symbol_diff_text(root, mb.strip(), head) if mb and mb.strip() else None
        if text is None:
            return None
    tokens, _tests = changed_symbols(text)
    return named_classes(symbol_matches(tokens, set(), test_file_index(root) if index is None else index, cap))


def _class_args(names: list[str]) -> list[str]:
    """`-class` argument lines for an MTP test executable, 40 classes a line."""
    return [" ".join(f'-class "*.{c}"' for c in names[i:i + 40]) for i in range(0, len(names), 40)]


def local_classes(root: str, base: str, map_path: "str | None" = None, wide: bool = False, cap: "int | None" = None,
                  limit: int = LOCAL_CLASS_LIMIT) -> "tuple[int, list[str]]":
    """The `local-classes` report as (exit code, lines). See the section comment above."""
    diff = symbol_diff(root, base)
    if diff is None:
        return 2, [f"local-classes: git could not diff the working tree against the merge base of {base} and HEAD"]
    mb, text, paths = diff
    tokens, test_files = changed_symbols(text, wide)
    match = symbol_matches(tokens, test_files, test_file_index(root), cap)
    cap = match["cap"]
    rows = ordered_hits(match)
    usable = sum(1 for t, n in match["counts"].items() if 0 < n <= cap)
    lines = [f"local-classes: merge base {mb[:10]} of {base}, {len(paths)} changed files, "
             f"{len(tokens)} {'wide' if wide else 'tight'} tokens ({usable} named in 1 to {cap} test files), "
             f"{len(rows)} classes"]
    for t in sorted(match["noisy"], key=lambda t: (-match["noisy"][t], t)):
        lines.append(f"NOISY {t} {match['noisy'][t]}")
    if len(rows) > limit:
        lines.append(f"{len(rows)} classes: CI will run FULL; run the first {limit} below")
        rows = rows[:limit]
    if not rows:
        lines.append("no test class names a changed symbol")
    for suite, cls, h in rows:
        why = ", ".join(sorted(h["tokens"], key=lambda t: (match["counts"][t], t)))
        if h["own"]:
            why = "own file changed" + (f"; {why}" if why else "")
        lines.append(f"{suite} {cls} {why}" + ("  needs rig" if h["rig"] else ""))
    for suite in SYMBOL_SUITES:
        for rig in (False, True):
            names = [c for s, c, h in rows if s == suite and h["rig"] == rig]
            label = f"{suite} (needs rig, DARLING_TEST_PG)" if rig else suite
            for args in _class_args(names):
                lines.append(f"RUN {label}: {args}")
    if map_path:
        test_map = load_map(map_path)
        sha = test_map.get("sha") if isinstance(test_map, dict) else None
        drift = git_drift(sha, mb, root) if isinstance(sha, str) else None
        discovered = scan_classes(root)
        if isinstance(test_map, dict) and isinstance(test_map.get("classes"), dict):
            discovered = {s: v for s, v in discovered.items() if s in test_map["classes"]}
        sel = map_select(test_map, paths, drift, discovered, named=named_classes(match), shared_diff=text,
                         **selection_args(Rules()))
        lines.append(f"MAP coverage answer ({map_path}):")
        if sel["full"]:
            lines.append(f"MAP FULL: {sel['reason']}")
        for suite in sorted(sel["selected"]):
            reasons: dict[str, int] = {}
            for r in sel["why"][suite].values():
                reasons[r] = reasons.get(r, 0) + 1
            lines.append(f"MAP {suite}: {len(sel['selected'][suite])} of {sel['total'][suite]} classes ("
                         + ", ".join(f"{k} {v}" for k, v in sorted(reasons.items())) + ")")
    return 0, lines


GUARD_FILTER = "-trait Stage=Guard"
_TEST_ATTRIBUTE = re.compile(r"\[(?:Fact|Theory)\b")


def _test_holders(root: str, index: list, files: set[str]) -> set[str]:
    """The classes of `files` that hold a test: a [Fact] or [Theory] between the class's declaration and the next
    one. A helper class a test file also declares (a fake, a fixture row) has no test, so a filter skips it."""
    out: set[str] = set()
    for _suite, rel, _classes, _rig, _words in index:
        if rel not in files:
            continue
        try:
            lines = read_source(os.path.join(root, rel), "replace").split("\n")
        except OSError:
            continue
        # (indent, class) of the classes open at this line, by indentation: a test attribute belongs to the nearest
        # class indented less than it, so a nested helper declared above more tests does not take them.
        stack: list[tuple[int, str]] = []
        for line in lines:
            indent = len(line) - len(line.lstrip(" \t"))
            m = _CLASS_DECL.match(line)
            if m or _TEST_ATTRIBUTE.match(line.strip()):
                while stack and stack[-1][0] >= indent:
                    stack.pop()
            if m:
                stack.append((indent, m.group(1)))
            elif stack and _TEST_ATTRIBUTE.match(line.strip()):
                out.add(stack[-1][1])
    return out


def local_filter(root: str, base: str, map_path: "str | None" = None, cap: "int | None" = None,
                 limit: int = LOCAL_CLASS_LIMIT) -> "tuple[int, list[str], list[str]]":
    """The `local-filter` answer (#5798) as (exit code, stdout lines, stderr notes). Per suite, the test
    executable's filter (`<suite>: -class "*.A" -class "*.B"`, a line per 40 classes), then a line that lists the
    classes (`<suite> classes: A B`); the last line is the guard set, which CI's guard job runs on every pull request.

    The classes are CI's own selection for the branch: with `map_path`, map_select() over the changed files with the
    changed-symbol reason, exactly as the test-map-select job asks it. Without a map, or when that answer is FULL or
    over `limit` classes (a lane does not run the whole suite), they are the part of it a lane can run: the
    classes of a changed test file, then (with `map_path`) the classes whose text pattern in the map matches a
    changed file, then the classes that name a changed symbol, most specific first and cut at `limit`. CI selects
    each of them too. A note on stderr says which. Stage=Guard classes are left to the guard line. A suite's filter
    takes 40 classes a line, so one line stays well under Windows' command-line limit."""
    diff = symbol_diff(root, base)
    if diff is None:
        return 2, [], [f"local-filter: git could not diff the working tree against the merge base of {base} and HEAD"]
    mb, text, paths = diff
    tokens, test_files = changed_symbols(text)
    index = test_file_index(root)
    match = symbol_matches(tokens, test_files, index, cap)
    holders = _test_holders(root, index, {rel for _s, rel, _c, _r, _w in index
                                          if rel in test_files or match["hits"].keys() & {(_s, c) for c in _c}})
    discovered = scan_classes(root)

    def runnable(suite: str, cls: str) -> bool:
        # Stage=Guard classes are left to the guard line.
        return "Stage=Guard" not in discovered.get(suite, {}).get(cls, set())

    # The part a lane can run, most specific first: a changed test file's classes, then the map's text-pattern
    # classes, then the classes that name a changed symbol (in ordered_hits' order).
    ranked = ordered_hits(match)
    own = [(s, c) for s, c, h in ranked if h["own"] and c in holders and runnable(s, c)]
    named = [(s, c) for s, c, h in ranked if not h["own"] and c in holders and runnable(s, c)]
    pattern: list[tuple[str, str]] = []
    chosen: dict[str, set[str]] = {}
    notes = [f"local-filter: merge base {mb[:10]} of {base}, {len(paths)} changed files"]
    fallback = True
    if map_path:
        test_map = load_map(map_path)
        sha = test_map.get("sha") if isinstance(test_map, dict) else None
        drift = git_drift(sha, mb, root) if isinstance(sha, str) else None
        mapped = discovered
        if isinstance(test_map, dict) and isinstance(test_map.get("classes"), dict):
            mapped = {s: v for s, v in discovered.items() if s in test_map["classes"]}
        args = selection_args(Rules())
        sel = map_select(test_map, paths, drift, mapped, named=named_classes(match), shared_diff=text, **args)
        count = sum(len(v) for v in sel["selected"].values())
        part = ("the classes that name a changed symbol, sit in a changed test file or have a text pattern in the "
                "map that matches a changed file")
        if sel["full"]:
            notes.append(f"CI's map answer is FULL ({sel['reason']}): CI runs every class; below is the part a lane "
                         f"runs ({part})")
        elif count > limit:
            notes.append(f"CI's map answer runs {count} classes, over {limit}: below is the part a lane runs ({part})")
        else:
            fallback = False
            for suite, cls in own + named:
                chosen.setdefault(suite, set()).add(cls)
            for suite, names in sel["selected"].items():
                chosen.setdefault(suite, set()).update(c for c in names if runnable(suite, c))
            notes.append(f"CI's map answer: {count} classes")
        if fallback:
            # #5798: a class that reads a source file as text (the web tests read wwwroot/js/**) names no symbol, so
            # the symbol rule misses it; CI selects it through the map's text_patterns, and so does a lane.
            hits = text_pattern_classes(test_map, list(dict.fromkeys(paths + (drift or []))), args["doc_patterns"])
            pattern = [(s, c) for s in sorted(hits) for c in sorted(hits[s])
                       if c in discovered.get(s, {}) and runnable(s, c)]
    else:
        notes.append("no --map: below are the classes that name a changed symbol or sit in a changed test file, "
                     "which CI selects too; pass the nightly test map for CI's whole answer")
    if fallback:
        order = list(dict.fromkeys(own + pattern + named))
        if len(order) > limit:
            notes.append(f"{len(order)} classes, over {limit}: below are the first {limit}, most specific first")
            order = order[:limit]
        for suite, cls in order:
            chosen.setdefault(suite, set()).add(cls)
    lines: list[str] = []
    for suite in SYMBOL_SUITES:
        names = sorted(chosen.get(suite, ()))
        # 40 classes a line, as local-classes prints them: 700 `-class "*.X"` arguments on one line come near
        # Windows' 32,767-character command line (#5100).
        for arg_line in _class_args(names):
            lines.append(f"{suite}: {arg_line}")
        if names:
            lines.append(f"{suite} classes: " + " ".join(names))
    if not lines:
        notes.append("no class outside the guard set is selected")
    lines.append(f"guard: {GUARD_FILTER}")
    return 0, lines, notes


# ---------------------------------------------------------------------------------------------------------
# The CI quarantine (#5942): a test that dev's own latest push broke is left out of an unrelated pull request.
# ---------------------------------------------------------------------------------------------------------

QUARANTINE_FILE = os.path.join(ROOT, ".github", "ci-quarantine.txt")
QUARANTINE_HOURS = 72
# map_select()'s `why` reasons that mean the pull request reaches the class through a file or a symbol of its own.
# "reads the tree", "guard stage", "new class" and "gated on an environment" are NOT reach: they select a class for
# nearly every change, and an unrelated pull request failing on one of them is the case this list exists for.
QUARANTINE_REACH = ("own file changed", "covers a changed file", "text pattern", "names a changed symbol",
                    "shares a collection with a changed class")
# A test runner's name prefix says which suite a test belongs to.
QUARANTINE_SUITES = (("Darling.Tests.", "darling"), ("Lite.Tests.", "lite"))
_FAIL_LINE = re.compile(r"^\S+Z\s+(?P<name>\S.*?)\s+\[FAIL\]\s*$")
_ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")


def parse_quarantine(text: str) -> "tuple[list[dict], list[str]]":
    """(entries, malformed lines) of the quarantine file's text. A line is `<test> | <dev run id> | <UTC time> | #<issue>`:
    the test is `Namespace.Class.Method` as the runner prints it (arguments of a theory case left off), the run is the
    dev push run that failed it, the time is ISO 8601 UTC and the issue is the bug filed for the test. Blank lines and
    lines starting with `#` are comments. A malformed line is reported and never honored."""
    entries: list[dict] = []
    bad: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        parts = [p.strip() for p in line.split("|")]
        at = _parse_time(parts[2]) if len(parts) == 4 else None
        if len(parts) != 4 or not parts[0] or not parts[1].isdigit() or at is None or not re.fullmatch(r"#\d+", parts[3]):
            bad.append(line)
            continue
        entries.append({"name": parts[0], "run": parts[1], "at": at, "issue": parts[3]})
    return entries, bad


def failed_tests_of_log(text: str) -> set[str]:
    """The tests a job log reports as failed: the runner prints `<time>Z     Namespace.Class.Method [FAIL]`, with the
    arguments of a theory case in parentheses before the marker, which are left off."""
    out: set[str] = set()
    for line in text.splitlines():
        m = _FAIL_LINE.match(_ANSI.sub("", line).lstrip("﻿"))
        if m:
            out.add(m.group("name").split("(")[0].strip())
    return out


def _gh_text(args: list[str], timeout: int = 120) -> "str | None":
    try:
        r = subprocess.run(["gh", "api", *args], capture_output=True, text=True, encoding="utf-8", errors="replace",
                           timeout=timeout)
    except (OSError, subprocess.SubprocessError) as e:
        print(f"quarantine: gh api {args[0]} failed ({e})", file=sys.stderr)
        return None
    if r.returncode != 0:
        print(f"quarantine: gh api {args[0]} exited {r.returncode}", file=sys.stderr)
        return None
    return r.stdout


def dev_failures(repo: str, max_jobs: int = 30) -> "tuple[str, set[str] | None]":
    """(run id, failed tests) of the latest COMPLETED push run of build.yml on dev that ended success or failure (a
    cancelled or skipped run says nothing). The tests come from the `[FAIL]` lines of that run's failed jobs. None for
    the tests when anything could not be read: the caller then honors nothing and every test runs (fail open)."""
    listing = _gh_text([f"repos/{repo}/actions/workflows/build.yml/runs?branch=dev&event=push&status=completed&per_page=10"])
    try:
        runs = json.loads(listing or "")["workflow_runs"]
    except (ValueError, KeyError, TypeError):
        return "", None
    latest = next((r for r in runs if isinstance(r, dict) and r.get("conclusion") in ("success", "failure")), None)
    if latest is None:
        return "", None
    run_id = str(latest.get("id"))
    if latest.get("conclusion") == "success":
        return run_id, set()
    jobs_text = _gh_text([f"repos/{repo}/actions/runs/{run_id}/jobs?filter=latest&per_page=100"])
    try:
        jobs = json.loads(jobs_text or "")["jobs"]
    except (ValueError, KeyError, TypeError):
        return run_id, None
    failed = [j for j in jobs if isinstance(j, dict) and j.get("conclusion") == "failure"]
    if len(failed) > max_jobs:
        return run_id, None
    found: set[str] = set()
    for j in failed:
        log = _gh_text([f"repos/{repo}/actions/jobs/{j.get('id')}/logs"], timeout=300)
        if log is None:
            return run_id, None
        found |= failed_tests_of_log(log)
    return run_id, found


def quarantine_decide(entries: list[dict], now: "datetime.datetime", dev_failed: "set[str] | None",
                      selection: object, hours: float = QUARANTINE_HOURS) -> dict:
    """Which quarantine entries a pull request honors. Pure. An entry is HONORED only when all of these hold; any
    other state is IGNORED (the test runs), so every doubt fails open:

    * it is younger than `hours` from its timestamp, whatever else is true (it never renews itself);
    * `dev_failed`, the tests the latest completed dev push run failed, holds this exact test (None: the Actions API
      could not be read);
    * `selection`, map_select()'s answer for this pull request, is usable (not FULL) and no class named in the test's
      name is selected for a reason in QUARANTINE_REACH: the pull request touches the test's own file or a file the
      map says reaches it, so it runs the test;
    * the name says which suite it belongs to.

    Returns {"honored": [{name, suite, issue, run}], "ignored": [{name, reason}]}."""
    honored: list[dict] = []
    ignored: list[dict] = []
    why = selection.get("why") if isinstance(selection, dict) and not selection.get("full") else None
    seen: set[str] = set()
    for e in entries:
        name = e["name"]

        def no(reason: str) -> None:
            ignored.append({"name": name, "reason": reason})

        if name in seen:
            no("listed twice; the first line counts")
            continue
        seen.add(name)
        age = now - e["at"]
        if age > datetime.timedelta(hours=hours):
            no(f"expired: {age.total_seconds() / 3600:.0f} hours old, the limit is {hours:g}")
            continue
        if age < datetime.timedelta(minutes=-5):
            no("its timestamp is in the future")
            continue
        if dev_failed is None:
            no("the latest dev run's failures could not be read, so the test runs")
            continue
        if name not in dev_failed:
            no("the latest completed dev push run did not fail this test")
            continue
        suite = next((s for prefix, s in QUARANTINE_SUITES if name.startswith(prefix)), "")
        if not suite:
            no("the name does not start with Darling.Tests. or Lite.Tests.")
            continue
        if not isinstance(why, dict):
            no("no usable test map selection, so the pull request's reach is unknown and the test runs")
            continue
        segments = set(re.split(r"[.+/]", name))
        hit = next(((cls, reason) for per in why.values() if isinstance(per, dict) for cls, reason in per.items()
                    if cls in segments and reason in QUARANTINE_REACH), None)
        if hit:
            no(f"this pull request reaches it ({hit[0]}: {hit[1]}), so it runs the test")
            continue
        honored.append({"name": name, "suite": suite, "issue": e["issue"], "run": e["run"]})
    return {"honored": honored, "ignored": ignored}


def quarantine_args(path: str, suite: str) -> list[str]:
    """The runner arguments that leave a suite's honored tests out: `-method- <name>` for each. [] when the decision
    file is missing or unreadable (nothing is left out)."""
    try:
        with open(path, encoding="utf-8") as fh:
            decided = json.load(fh)
        honored = [h for h in decided["honored"] if isinstance(h, dict) and h.get("suite") == suite
                   and isinstance(h.get("name"), str)]
    except (OSError, ValueError, KeyError, TypeError):
        return []
    out: list[str] = []
    for h in honored:
        out += ["-method-", h["name"]]
    return out


def _bool(s: str) -> bool:
    return s.strip().lower() == "true"


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--replay", action="store_true", help="replay the failure corpus against today's rules")
    ap.add_argument("--corpus", default=CORPUS)
    ap.add_argument("--map", help="with --replay: also replay the corpus against this class-to-file map (reports only)")
    ap.add_argument("--misses-out", help="with --map: write every map miss, one JSON row per line, to this file")
    ap.add_argument("--symbols", metavar="BASE", help="with --map: also count the classes each row selects before and "
                                                      "after the changed-symbol rule, diffing each head commit this "
                                                      "clone has against its merge base with BASE (#5798)")
    sub = ap.add_subparsers(dest="cmd")

    ls = sub.add_parser("lite-scope", help="how much of the Lite suite a leg runs (workflow step)")
    ls.add_argument("--event", required=True)
    ls.add_argument("--shard", required=True)
    for k in ("lite", "core", "root", "reads", "linked"):
        ls.add_argument(f"--{k}", default="false")

    lm = sub.add_parser("lite-mode", help="the Lite scope of a pull request from the files it changes (lite-scope job, #5669)")
    lm.add_argument("--event", default="pull_request")
    lm.add_argument("--files-from", required=True, help="one changed path per line; a missing or empty list prints mode=unknown")

    ds = sub.add_parser("darling-scope", help="what the build job's no-store Darling pass runs (workflow step)")
    ds.add_argument("--event", required=True)
    ds.add_argument("--gate", default="false", help="the Darling PostgreSQL gate's answer (darling-paths-filter.yml)")
    for k in ("full", "core", "root"):
        ds.add_argument(f"--{k}", default="true", help="a path that runs the whole Darling suite changed")

    ts = sub.add_parser("tree-scope", help="what darling-tree-guards runs once it runs (workflow step)")
    ts.add_argument("--event", required=True)
    ts.add_argument("--all-count", type=int, default=0)
    ts.add_argument("--lite-count", type=int, default=0)

    sk = sub.add_parser("slow-skip", help="the Cost=Slow classes a pull request run leaves out (workflow step)")
    sk.add_argument("--suite", required=True, choices=sorted(SUITE_DIRS))
    sk.add_argument("--event", required=True)
    sk.add_argument("--base-ref", default="", help="the pull request's base branch (github.base_ref); main skips nothing")
    sk.add_argument("--repo", default="")
    sk.add_argument("--pr", default="")
    sk.add_argument("--list", action="store_true", help="print every Cost=Slow class of the suite instead")
    sk.add_argument("files", nargs="*", help="changed files; with --repo and --pr they come from the API instead")

    dc = sub.add_parser("decide", help="print every decision for a list of changed files")
    dc.add_argument("--event", default="pull_request")
    dc.add_argument("files", nargs="*")

    ms = sub.add_parser("map-select", help="which classes a pull request runs, from a class-to-file map (workflow step)")
    ms.add_argument("--map", required=True, help="the test map (JSON, or .gz)")
    ms.add_argument("--event", default="pull_request")
    ms.add_argument("--base-ref", default="", help="the pull request's base branch (github.base_ref); main runs everything")
    ms.add_argument("--base", help="the merge base; the drift is `git diff --no-renames <map sha>..<base>`")
    ms.add_argument("--drift-file", help="the drift as one path per line, instead of --base (`-` for none)")
    ms.add_argument("--root", default=ROOT, help="the tree whose test classes are listed (default: this repository)")
    ms.add_argument("--files-from", help="the changed files as one path per line, in place of the arguments")
    ms.add_argument("--diff-file", help="the pull request's `git diff -U0` text, in place of reading it from --base: "
                                        "a shared-project change is FULL only for a const, an enum or a registration "
                                        "file, and without a diff it is FULL (#5951)")
    ms.add_argument("--guard-stage-separate", action="store_true",
                    help="the caller runs the whole Stage=Guard trait itself (the pre-push gate): leave Guard classes "
                         "out of the selection (#5894)")
    ms.add_argument("--out", help="also write the selection JSON to this file (stdout keeps the notice and the JSON)")
    ms.add_argument("--summary", help="also write the shadow step-summary markdown to this file")
    ms.add_argument("files", nargs="*")

    lc = sub.add_parser("local-classes", help="the test classes whose source names a symbol the change declares or edits (#5776)")
    lc.add_argument("--base", default="origin/dev", help="diff the working tree against the merge base of this and HEAD")
    lc.add_argument("--map", help="also print this test map's coverage answer for the changed files (JSON, or .gz)")
    lc.add_argument("--wide", action="store_true", help="also match every identifier of 6+ characters on a changed line")
    lc.add_argument("--cap", type=int, help="drop (and print as NOISY) a token named in more test files "
                                         "(default: 1%% of the test files, at least 25)")
    lc.add_argument("--root", default=ROOT, help="the repository (default: this one)")

    lf = sub.add_parser("local-filter", help="the test filter CI's selection gives this branch, per suite, for a lane (#5798)")
    lf.add_argument("--base", default="origin/dev", help="diff the working tree against the merge base of this and HEAD")
    lf.add_argument("--map", help="the nightly test map (JSON, or .gz), for CI's whole answer")
    lf.add_argument("--cap", type=int, help="as local-classes --cap")
    lf.add_argument("--root", default=ROOT, help="the repository (default: this one)")

    mk = sub.add_parser("map-keep", help="the classes a shard keeps from a selection; FULL when it cannot be trusted (workflow step)")
    mk.add_argument("--selection", required=True, help="the JSON map-select --out wrote; missing or unreadable counts as FULL")
    mk.add_argument("--suite", required=True, choices=("darling", "lite"))

    qr = sub.add_parser("quarantine", help="which entries of the CI quarantine file this pull request honors (test-map-select job, #5942)")
    qr.add_argument("--file", default=QUARANTINE_FILE, help="the quarantine file (default: .github/ci-quarantine.txt)")
    qr.add_argument("--repo", default="", help="owner/name: the repository whose latest completed dev push run is read")
    qr.add_argument("--selection", help="the JSON map-select --out wrote; missing, unreadable or FULL honors nothing")
    qr.add_argument("--dev-failures-file", help="instead of the Actions API: one failed test per line (a missing file counts as unreadable)")
    qr.add_argument("--now", help="the clock as ISO 8601 UTC (default: now)")
    qr.add_argument("--out", help="also write the decision JSON to this file")

    rc = sub.add_parser("reads-classes", help="the test classes that read the other product's files, one full name a line (workflow step, #5961)")
    rc.add_argument("--suite", required=True, choices=["darling", "lite"],
                    help="darling: Darling.Tests classes that read a Lite path; lite: Lite.Tests classes that read a Darling path")
    rc.add_argument("--args", action="store_true", help="print `-class` / name line pairs for a runner instead of bare names")
    rc.add_argument("--root", default=ROOT, help="the repository (default: this one)")

    qa = sub.add_parser("quarantine-args", help="the runner arguments that leave a suite's honored quarantined tests out (workflow step)")
    qa.add_argument("--quarantine", required=True, help="the JSON `quarantine --out` wrote; missing or unreadable leaves nothing out")
    qa.add_argument("--suite", required=True, choices=("darling", "lite"))

    sc = sub.add_parser("shadow-check", help="classes that failed but a map selection would not have run (workflow step)")
    sc.add_argument("--selection", help="the JSON map-select --out wrote; missing or unreadable counts as FULL")
    sc.add_argument("--reports", required=True, help="a folder with one subfolder per timing artifact")
    sc.add_argument("--out", required=True, help="the one-row JSONL file to write")
    sc.add_argument("--meta", default="{}", help="JSON object merged into the row (run, sha, pr...)")
    sc.add_argument("--attempt", type=int, help="the run attempt; accepted for callers, but it no longer filters: "
                                                "staleness is decided per artifact folder (the `attempt` of --meta "
                                                "still lands in the row)")
    sc.add_argument("--jobs", help="the latest attempt's jobs, one {name, conclusion} JSON object per line; "
                                   "the row lists the failed ones")

    args = ap.parse_args(argv)
    if args.replay:
        misses = replay(args.corpus)
        if args.map:
            map_replay(load_map(args.map), args.corpus, misses_out=args.misses_out,
                       symbols_base=args.symbols)  # reports; does not fail the replay yet
        return 1 if misses else 0
    if args.cmd == "map-select":
        files = list(args.files)
        if args.files_from:
            with open(args.files_from, encoding="utf-8") as fh:
                files += [ln.strip() for ln in fh if ln.strip()]
        test_map = load_map(args.map)
        drift: "list[str] | None" = None
        if args.drift_file == "-":
            drift = []
        elif args.drift_file:
            with open(args.drift_file, encoding="utf-8") as fh:
                drift = [ln.strip() for ln in fh if ln.strip()]
        elif args.base and isinstance(test_map, dict) and isinstance(test_map.get("sha"), str):
            drift = git_drift(test_map["sha"], args.base)
        discovered = scan_classes(args.root)
        if isinstance(test_map, dict) and isinstance(test_map.get("classes"), dict):
            # The map holds the suites the nightly instruments (darling, lite). The deprecated suites in the tree
            # are not in it and no shard runs them as a test-map class, so they are not "new classes".
            discovered = {s: v for s, v in discovered.items() if s in test_map["classes"]}
        # #5776: with the base, the classes that name a changed symbol are one more reason (it only adds classes).
        named = symbol_classes(args.root, args.base) if args.base else None
        # #5951: the diff a shared-project change is judged by: a planted file, else the change against the base.
        shared_diff: "str | None" = None
        if args.diff_file:
            with open(args.diff_file, encoding="utf-8") as fh:
                shared_diff = fh.read()
        elif args.base:
            sd = symbol_diff(args.root, args.base, untracked=False)
            shared_diff = sd[1] if sd else None
        result = map_select(test_map, files, drift, discovered, event=args.event, base_ref=args.base_ref,
                            named=named, guard_stage_separate=args.guard_stage_separate, shared_diff=shared_diff,
                            **selection_args(Rules()))
        if result.get("docs_only"):
            print(f"::notice::test map: {result['reason']}")
        if result["full"]:
            print(f"::notice::test map: running everything ({result['reason']})")
        for f, rule in sorted((result.get("unknown") or {}).items()):
            print(f"::notice::test map: unknown file: {f}: {rule}")
        print(json.dumps(result, indent=1))
        if args.out:
            with open(args.out, "w", encoding="utf-8", newline="\n") as fh:
                json.dump(result, fh)
        if args.summary:
            with open(args.summary, "w", encoding="utf-8", newline="\n") as fh:
                fh.write(shadow_summary(result))
        return 0
    if args.cmd == "local-classes":
        code, lines = local_classes(args.root, args.base, args.map, args.wide, args.cap)
        print("\n".join(lines))
        return code
    if args.cmd == "local-filter":
        code, lines, notes = local_filter(args.root, args.base, args.map, args.cap)
        print("\n".join(notes), file=sys.stderr)
        if lines:
            print("\n".join(lines))
        return code
    if args.cmd == "map-keep":
        # Line 1 is `SELECTED <n>` (then one class name per line) or `FULL <reason>`; the exit code is always 0, so a
        # caller that cannot read the answer runs everything.
        try:
            with open(args.selection, encoding="utf-8") as fh:
                chosen = json.load(fh)
        except (OSError, ValueError):
            chosen = None
        names, why = map_keep(chosen, args.suite)
        if names is None:
            print(f"FULL {why}")
        else:
            print(f"SELECTED {len(names)}")
            print("\n".join(sorted(names)))
        return 0
    if args.cmd == "quarantine":
        try:
            with open(args.file, encoding="utf-8") as fh:
                entries, bad = parse_quarantine(fh.read())
        except OSError:
            entries, bad = [], []
        clock = _parse_time(args.now) if args.now else datetime.datetime.now(datetime.timezone.utc)
        decided: dict = {"honored": [], "ignored": []}
        if entries and clock is not None:
            dev_run = ""
            dev_failed: "set[str] | None" = None
            if args.dev_failures_file:
                try:
                    with open(args.dev_failures_file, encoding="utf-8") as fh:
                        dev_failed = {ln.strip() for ln in fh if ln.strip()}
                except OSError:
                    dev_failed = None
            elif args.repo:
                dev_run, dev_failed = dev_failures(args.repo)
            selection = None
            if args.selection:
                try:
                    with open(args.selection, encoding="utf-8") as fh:
                        selection = json.load(fh)
                except (OSError, ValueError):
                    selection = None
            decided = quarantine_decide(entries, clock, dev_failed, selection)
            decided["dev_run"] = dev_run
        elif entries:
            decided["ignored"] = [{"name": e["name"], "reason": "the clock could not be read"} for e in entries]
        for b in bad:
            decided["ignored"].append({"name": b, "reason": "malformed line (expected: test | dev run id | UTC time | #issue)"})
        for h in decided["honored"]:
            print(f"::notice title=CI quarantine honored::{h['name']} is left out of the {h['suite']} run on this pull request "
                  f"(dev run {h['run']} failed it; {h['issue']})")
        for i in decided["ignored"]:
            print(f"::notice title=CI quarantine ignored::{i['name']}: {i['reason']}")
        print(f"quarantine: {len(decided['honored'])} honored, {len(decided['ignored'])} ignored, {len(entries)} entries")
        if args.out:
            with open(args.out, "w", encoding="utf-8", newline="\n") as fh:
                json.dump(decided, fh)
        return 0
    if args.cmd == "reads-classes":
        names = sorted(reads_classes(args.root, (args.suite,))[args.suite])
        lines = [t for n in names for t in (("-class", n) if args.args else (n,))]
        if lines:
            print("\n".join(lines))
        return 0
    if args.cmd == "quarantine-args":
        print("\n".join(quarantine_args(args.quarantine, args.suite)))
        return 0
    if args.cmd == "shadow-check":
        selection = None
        if args.selection:
            try:
                with open(args.selection, encoding="utf-8") as fh:
                    selection = json.load(fh)
            except (OSError, ValueError):
                selection = None
        try:
            meta = json.loads(args.meta)
        except ValueError:
            meta = {}
        meta = meta if isinstance(meta, dict) else {}
        failed, seen, stale = failed_classes(args.reports)
        row = shadow_row(selection, failed, seen, meta, stale, failed_jobs(args.jobs))
        with open(args.out, "w", encoding="utf-8", newline="\n") as fh:
            fh.write(json.dumps(row, sort_keys=True) + "\n")
        for m in row["misses"]:
            print(f"::notice title=Shadow miss::{m['suite']} {m['class']} failed and the test map would not have run it")
        print(f"shadow-check: full={row['full']} misses={len(row['misses'])} reports={row['reports']}")
        return 0
    if args.cmd == "lite-scope":
        mode = lite_scope(args.event, _bool(args.lite), _bool(args.core), _bool(args.root), _bool(args.reads),
                          _bool(args.linked))
        run = mode == "full" or (mode == "reads" and args.shard == "0")
        print(f"mode={mode}")
        print(f"run={'true' if run else 'false'}")
        return 0
    if args.cmd == "lite-mode":
        # The same answer the Lite shard legs reach from their own path filters (decide() reads
        # .github/lite-paths-filter.yml), made once before the shards start so a diff the Lite suite does not depend on (mode=none)
        # does not take four Windows runners. Anything but a clean answer prints unknown, and the shards then run
        # their own scope step as before.
        try:
            with open(args.files_from, encoding="utf-8") as fh:
                changed = [ln.strip() for ln in fh if ln.strip()]
        except OSError:
            changed = []
        print(f"mode={decide(changed, args.event, Rules())['lite_mode'] if changed else 'unknown'}")
        return 0
    if args.cmd == "darling-scope":
        scope = darling_scope(args.event, True, args.event != "release" and _bool(args.gate),
                              _bool(args.full) or _bool(args.core) or _bool(args.root))
        print(f"scope={scope}")
        print("filter=" + scope_filter(scope))
        return 0
    if args.cmd == "tree-scope":
        scope = tree_scope(args.event, args.all_count, args.lite_count)
        print(f"scope={scope}")
        print("filter=" + scope_filter(scope))
        return 0
    if args.cmd == "slow-skip":
        index = SlowIndex()
        if args.list:
            print("\n".join(sorted(index.slow[args.suite])))
            return 0
        files = args.files
        if not files and args.repo and args.pr.isdigit() and args.event == "pull_request":
            files = changed_files_of_pr(args.repo, args.pr)
        print("\n".join(index.skip(files, args.event, args.base_ref)[args.suite]))
        return 0
    if args.cmd == "decide":
        print(json.dumps(decide(args.files, args.event, Rules()), indent=1))
        return 0
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
