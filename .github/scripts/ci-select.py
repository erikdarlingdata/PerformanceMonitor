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
  corpus against such a map and reports what it would miss (it does not fail on that yet).
* `--replay` reads .github/ci-history/failures.jsonl (a trimmed record of every run with a real test failure)
  and checks that each failing class would have been selected for that run's changed files. A cut to the rules
  lands only while the replay finds 0 misses. A Guard-tagged test runs it on every pull request.

The path patterns are NOT copied here. They are read from the filter blocks in .github/workflows/build.yml and
from .github/darling-paths-filter.yml, the same text dorny/paths-filter evaluates, so the replay and the
workflow cannot disagree about which area a file belongs to. What lives here is the decision logic that
used to be spread over `if:` expressions and a bash if-chain.

Standard library only (the Windows and Linux runners both have Python 3).
"""
from __future__ import annotations

import argparse
import datetime
import gzip
import json
import os
import re
import subprocess
import sys
import xml.etree.ElementTree as ET

ROOT = os.path.normpath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".."))
WORKFLOW = os.path.join(ROOT, ".github", "workflows", "build.yml")
DARLING_FILTER = os.path.join(ROOT, ".github", "darling-paths-filter.yml")
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

    def __init__(self, workflow: str = WORKFLOW, darling_filter: str = DARLING_FILTER) -> None:
        wf = load_workflow_filters(workflow)
        self.build = wf["build"]
        self.lite_shards = wf["lite-tests"]
        self.tree = wf["darling-tree-guards"]
        self.guard = wf["guard-tests"]
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
    """The Darling.Tests runner arguments for a scope ('' is the whole suite)."""
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
    area_names = ("root", "core", "installer_core", "dashboard", "lite", "installer", "darling")
    fastpath = (not release) and docs_only and not any(a[n] for n in area_names)

    build_lite = release or a["lite"] or a["core"] or a["root"]
    build_installer = release or a["installer"] or a["installer_core"] or a["root"]
    build_dashboard = release or a["dashboard"] or a["core"] or a["installer_core"] or a["root"]
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
        "build_installer_tests": build_installer,
        "build_dashboard_tests": build_dashboard,
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
    "dashboard": os.path.join("deprecated", "Dashboard.Tests"),
    "installer": os.path.join("deprecated", "Installer.Tests"),
}
SUITE_BY_PREFIX = {
    "Darling.Tests": "darling",
    "PerformanceMonitor.Darling.Tests": "darling",
    "Lite.Tests": "lite",
    "PerformanceMonitorLite.Tests": "lite",
    "Dashboard.Tests": "dashboard",
    "Installer.Tests": "installer",
}


def split_failed_class(cls: str) -> tuple[str, str]:
    """(namespace prefix, simple class name) of a failing test class's full name. A nested class is reported as
    `Ns.Outer+Inner` (some runners use `Ns.Outer/Inner`): the prefix is the namespace of the OUTER class, the text
    before the first '+', and the simple name is the innermost class, which is the name _walk_classes yields for a
    nested declaration. Splitting on the last '.' alone left `Outer+Inner` as the "simple name", found no such class
    in the tree, and so never counted a nested failure as a miss (#5459)."""
    head = re.split(r"[+/]", cls, maxsplit=1)[0]
    return head.rpartition(".")[0], re.split(r"[.+/]", cls)[-1]


def _walk_classes(root: str):
    """Yield (suite, repo-relative file with '/', simple class name, {"Key=Value", ...}) for every class declaration."""
    for suite, rel in SUITE_DIRS.items():
        base = os.path.join(root, rel)
        for dp, dn, fn in os.walk(base):
            dn[:] = [d for d in dn if d not in ("bin", "obj")]
            for f in fn:
                if not f.endswith(".cs"):
                    continue
                path = os.path.join(dp, f)
                try:
                    with open(path, encoding="utf-8-sig") as fh:
                        lines = fh.read().split("\n")
                except OSError:
                    continue
                relfile = os.path.relpath(path, root).replace(os.sep, "/")
                gated = any(GATE_ENV_LITERAL.search(line) for line in lines)
                for n, line in enumerate(lines):
                    m = _CLASS_DECL.match(line)
                    if not m:
                        continue
                    traits: set[str] = {GATE_TRAIT} if gated else set()
                    j = n - 1
                    while j >= 0:
                        s = lines[j].strip()
                        if s.startswith(("[", "///", "//", "*", "/*")) or s.endswith("*/") or s == "" and False:
                            traits.update(f"{k}={v}" for k, v in _TRAIT.findall(s))
                            j -= 1
                        else:
                            break
                    yield suite, relfile, m.group(1), traits


def scan_classes(root: str = ROOT) -> dict[str, dict[str, set[str]]]:
    """{suite: {simple class name: {"Key=Value", ...}}} read from the test sources."""
    out: dict[str, dict[str, set[str]]] = {suite: {} for suite in SUITE_DIRS}
    for suite, _file, name, traits in _walk_classes(root):
        out[suite].setdefault(name, set()).update(traits)
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
    if suite == "dashboard":
        return d["build_dashboard_tests"]
    if suite == "installer":
        return d["build_installer_tests"]
    return True


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
                      ".github/scripts/run-darling-pg-shard.ps1", ".github/scripts/run-lite-shard.ps1",
                      ".github/scripts/lite-shard-pack.py"}
_BUILD_INPUT_SUFFIXES = (".csproj", ".props", ".targets", ".sln", ".slnx")
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
                    with open(os.path.join(self.root, rel), encoding="utf-8-sig") as fh:
                        parts.append(fh.read())
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
                    or any(re.search(r"(?<![\w-])" + re.escape(x) + r"(?![\w-])", text) for x in odd_stems) \
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
)

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


def map_select(test_map: object, changed: list[str], drift: "list[str] | None", discovered: dict,
               *, event: str = "pull_request", keep_full: "tuple[str, ...] | list[str]" = KEEP_FULL,
               doc_patterns: "tuple[str, ...] | list[str]" = DOC_PATTERNS,
               now: "datetime.datetime | None" = None, max_age_days: "float | None" = MAP_MAX_AGE_DAYS,
               max_drift: int = MAP_MAX_DRIFT, base_ref: str = "") -> dict:
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
    scan_classes() returns it, which is how a class the map has never seen is found.

    A class is selected when it carries Stage=Guard, its own file changed, it covers a changed file, a text pattern
    of it matches a changed file, it reads the whole tree and any non-documentation file changed, or the map has
    never seen it. A file changed since the map counts as changed too (it may have grown new call edges).
    A keep-full file forces FULL only when the pull request's own change holds it: one that changed on dev since
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
    effective = list(dict.fromkeys(list(changed) + list(drift)))
    live = [f for f in effective if not any(matches(p, f) for p in doc_patterns)]
    live_names = set(live)

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

    docs_changed = [f for f in effective if f not in live_names]

    def text_hit(cls: str) -> bool:
        g = globs.get(cls)
        if g == "tree":
            return bool(live)
        if not isinstance(g, list):
            return False
        # A class that reads a documentation file by name (the migration ladder reads CHANGELOG.md) is selected by a
        # change to exactly that file; the documentation allowlist only keeps wildcard patterns from matching it.
        return (any(matches(p, f) for p in g for f in live)
                or any(p == f for p in g if "*" not in p for f in docs_changed))

    new_classes = any(cls not in parsed.get(suite, {}) for suite, found in discovered.items() for cls in found)
    for f in live:
        if f in known:
            continue
        if any(isinstance(g, list) and any(matches(p, f) for p in g) for g in globs.values()):
            continue
        if f.endswith(".cs"):
            if _TEST_PROJECT_FILE.search(f) and new_classes:
                continue  # a new test file: its classes are found through `discovered`
            return _full(f"new file not in the map: {f}")
        return _full(f"{f} is in no class, no text pattern and not in the map")

    # A class the map has never seen runs because `discovered` lists it. If the tree scan found no class at all for a
    # suite the map has classes for, the scan itself is broken and every new class would be silently left out.
    for suite, entries in parsed.items():
        if entries and not discovered.get(suite):
            return _full(f"no test class was found in the tree for suite {suite}")

    live_set = set(live)
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
            elif text_hit(cls):
                reasons[cls] = "reads the tree" if globs.get(cls) == "tree" else "text pattern"
        for cls, traits in found.items():
            if cls not in mapped:
                reasons[cls] = "new class"
            elif "Stage=Guard" in traits:
                reasons.setdefault(cls, "guard stage")
            if GATE_TRAIT in traits:
                reasons.setdefault(cls, "gated on an environment the map shards lack")
        selected[suite] = sorted(reasons)
        why[suite] = {c: reasons[c] for c in sorted(reasons)}
        seconds[suite] = round(sum(_seconds(classes[suite][c]) for c in reasons if c in mapped), 3)
        total[suite] = len(set(mapped) | set(found))
    return {"full": False, "reason": "", "selected": selected, "why": why, "seconds": seconds, "total": total}


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
    for name in ("root", "core", "installer_core"):
        keep.extend(rules.build.get(name, []))
    return {"keep_full": tuple(dict.fromkeys(keep)), "doc_patterns": tuple(rules.build["docs"])}


def map_replay(test_map: "dict | None", corpus: str = CORPUS, root: str = ROOT, quiet: bool = False,
               misses_out: "str | None" = None) -> int:
    """Like replay(), but asks the map: would each failing class of each corpus row have been selected, taking the map
    as current (no drift, no age check)? Returns the miss count. Reports only; the caller does not fail on it yet.
    The printed list is capped at 40; `misses_out` names a file that gets EVERY miss as one JSON row per line
    (date, event, head_sha, suite, class, changed_files), so none goes unclassified (#5459)."""
    extra = selection_args(Rules())
    accepted = load_accepted()
    tree = scan_classes(root)
    rows = checked = misses = full_rows = accepted_seen = 0
    missed: list[str] = []
    rows_out: list[dict] = []
    with open(corpus, encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            row = json.loads(line)
            rows += 1
            sel = map_select(test_map, row["changed_files"], [], tree, event=row["event"], max_age_days=None, **extra)
            full_rows += bool(sel["full"])
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
    if not isinstance(names, list) or not names or not all(isinstance(n, str) and n for n in names):
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

def _bool(s: str) -> bool:
    return s.strip().lower() == "true"


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--replay", action="store_true", help="replay the failure corpus against today's rules")
    ap.add_argument("--corpus", default=CORPUS)
    ap.add_argument("--map", help="with --replay: also replay the corpus against this class-to-file map (reports only)")
    ap.add_argument("--misses-out", help="with --map: write every map miss, one JSON row per line, to this file")
    sub = ap.add_subparsers(dest="cmd")

    ls = sub.add_parser("lite-scope", help="how much of the Lite suite a leg runs (workflow step)")
    ls.add_argument("--event", required=True)
    ls.add_argument("--shard", required=True)
    for k in ("lite", "core", "root", "reads", "linked"):
        ls.add_argument(f"--{k}", default="false")

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
    ms.add_argument("--out", help="also write the selection JSON to this file (stdout keeps the notice and the JSON)")
    ms.add_argument("--summary", help="also write the shadow step-summary markdown to this file")
    ms.add_argument("files", nargs="*")

    mk = sub.add_parser("map-keep", help="the classes a shard keeps from a selection; FULL when it cannot be trusted (workflow step)")
    mk.add_argument("--selection", required=True, help="the JSON map-select --out wrote; missing or unreadable counts as FULL")
    mk.add_argument("--suite", required=True, choices=("darling", "lite"))

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
            map_replay(load_map(args.map), args.corpus, misses_out=args.misses_out)  # reports; does not fail the replay yet
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
        result = map_select(test_map, files, drift, discovered, event=args.event, base_ref=args.base_ref,
                            **selection_args(Rules()))
        if result["full"]:
            print(f"::notice::test map: running everything ({result['reason']})")
        print(json.dumps(result, indent=1))
        if args.out:
            with open(args.out, "w", encoding="utf-8", newline="\n") as fh:
                json.dump(result, fh)
        if args.summary:
            with open(args.summary, "w", encoding="utf-8", newline="\n") as fh:
                fh.write(shadow_summary(result))
        return 0
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
