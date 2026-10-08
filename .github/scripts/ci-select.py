#!/usr/bin/env python3
"""The ONE place CI's test-selection rules live (#5459).

Input: the changed files of a run (and the event). Output: which test jobs run and in what scope.

These readers use the same code:

* build.yml's jobs call `lite-scope`, `darling-scope` and `tree-scope` with the area answers
  dorny/paths-filter already computed, and get the decision back as `key=value` lines for $GITHUB_OUTPUT.
* `map-select` (slice 1 of change 4; nothing in the workflow calls it yet) answers, from a class-to-file map, which
  test classes a pull request needs. See map_select() for the map's schema. `--replay --map FILE` also replays the
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


def scan_classes(root: str = ROOT) -> dict[str, dict[str, set[str]]]:
    """{suite: {simple class name: {"Key=Value", ...}}} read from the test sources."""
    out: dict[str, dict[str, set[str]]] = {}
    for suite, rel in SUITE_DIRS.items():
        classes: dict[str, set[str]] = out.setdefault(suite, {})
        base = os.path.join(root, rel)
        for dp, dn, fn in os.walk(base):
            dn[:] = [d for d in dn if d not in ("bin", "obj")]
            for f in fn:
                if not f.endswith(".cs"):
                    continue
                try:
                    with open(os.path.join(dp, f), encoding="utf-8-sig") as fh:
                        lines = fh.read().split("\n")
                except OSError:
                    continue
                for n, line in enumerate(lines):
                    m = _CLASS_DECL.match(line)
                    if not m:
                        continue
                    traits: set[str] = set()
                    j = n - 1
                    while j >= 0:
                        s = lines[j].strip()
                        if s.startswith(("[", "///", "//", "*", "/*")) or s.endswith("*/") or s == "" and False:
                            traits.update(f"{k}={v}" for k, v in _TRAIT.findall(s))
                            j -= 1
                        else:
                            break
                    classes.setdefault(m.group(1), set()).update(traits)
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
            for cls in row["failed_classes"]:
                prefix, _, simple = cls.rpartition(".")
                suite = SUITE_BY_PREFIX.get(prefix)
                if suite is None:
                    continue
                traits = tree.get(suite, {}).get(simple)
                if traits is None:
                    exempt += 1
                    gone.add(cls)
                    continue
                checked += 1
                if not class_selected(suite, traits, d):
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
    "docs/**/*.{md,svg,png,jpg,jpeg,gif}",
    "Screenshots/**/*.{md,svg,png,jpg,jpeg,gif}",
)


_TEST_PROJECT_FILE = re.compile(r"(^|/)[^/]*Tests/")


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
               max_drift: int = MAP_MAX_DRIFT) -> dict:
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

    Returns `{"full": bool, "reason": str, "selected": {suite: [Class, ...]}, "why": {suite: {Class: reason}},
    "seconds": {suite: float}, "total": {suite: int}}`. FULL (never "nothing") whenever the map cannot be trusted:
    missing, unreadable, wrong schema, too old, drift over `max_drift` files, a keep-full file, a file the map knows
    nothing about, or any event but a pull request. That includes a new product .cs file: types found by reflection,
    DI or attribute scanning change what a census class sees without any file it covers changing. The one exception
    is a new .cs file under a test project (a `*Tests/` or `*.Tests/` directory) while `discovered` holds a class the
    map has never seen; that class runs as a "new class"."""
    if event != "pull_request":
        return _full(f"event {event or '(none)'} always runs everything")
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

    effective = list(dict.fromkeys(list(changed) + list(drift)))
    for f in effective:
        if any(matches(p, f) for p in keep_full):
            return _full(f"{f} is a keep-full file")
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
                prefix, _, simple = cls.rpartition(".")
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


def shadow_summary(result: dict) -> str:
    """The step-summary text of a shadow run: how many classes the map would have run, and the estimated seconds."""
    lines = ["### Test map shadow (#5459): nothing was skipped, every shard ran everything", ""]
    if result.get("full"):
        lines.append(f"Selection: FULL ({result.get('reason') or 'no reason given'}).")
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


def failed_classes(reports: str) -> "tuple[dict[str, set[str]], dict[str, int]]":
    """The classes with a failed test in the shards' xunit reports under `reports`, one subfolder per uploaded
    artifact (`darling-tests-timing-N`, `lite-tests-timing-N`). Returns ({suite: {Class}}, {suite: report count})."""
    failed: dict[str, set[str]] = {"darling": set(), "lite": set()}
    seen: dict[str, int] = {"darling": 0, "lite": 0}
    if not os.path.isdir(reports):
        return failed, seen
    for dirpath, _, names in os.walk(reports):
        rel = os.path.relpath(dirpath, reports).replace("\\", "/")
        suite = "darling" if rel.startswith("darling") else "lite" if rel.startswith("lite") else ""
        if not suite:
            continue
        for name in names:
            if not name.endswith(".xml"):
                continue
            seen[suite] += 1
            try:
                for _, el in ET.iterparse(os.path.join(dirpath, name), events=("end",)):
                    if el.tag == "test" and (el.get("result") or "").lower() == "fail":
                        failed[suite].add(re.split(r"[.+/]", el.get("type") or "")[-1])
                    el.clear()
            except (ET.ParseError, OSError):
                seen[suite] -= 1  # an unreadable report counts as no report
    return failed, seen


def shadow_row(selection: "dict | None", failed: "dict[str, set[str]]", seen: "dict[str, int]", meta: dict) -> dict:
    """One JSONL row per run: the selection's size and the classes that failed although it would not have run them.
    A FULL (or missing) selection selects every class, so it can have no miss."""
    sel = selection if isinstance(selection, dict) else _full("no selection was written")
    misses: list[dict] = []
    if not sel.get("full"):
        for suite in sorted(failed):
            chosen = set(sel.get("selected", {}).get(suite, []))
            misses += [{"suite": suite, "class": c} for c in sorted(failed[suite]) if c not in chosen]
    return {"v": 1, **meta, "full": bool(sel.get("full")), "reason": sel.get("reason", ""),
            "selected": {s: len(v) for s, v in sel.get("selected", {}).items()},
            "total": sel.get("total", {}), "seconds": sel.get("seconds", {}),
            "reports": seen, "failed": {s: sorted(v) for s, v in failed.items()}, "misses": misses}


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

    dc = sub.add_parser("decide", help="print every decision for a list of changed files")
    dc.add_argument("--event", default="pull_request")
    dc.add_argument("files", nargs="*")

    ms = sub.add_parser("map-select", help="which classes a pull request runs, from a class-to-file map (not wired in yet)")
    ms.add_argument("--map", required=True, help="the test map (JSON, or .gz)")
    ms.add_argument("--event", default="pull_request")
    ms.add_argument("--base", help="the merge base; the drift is `git diff --no-renames <map sha>..<base>`")
    ms.add_argument("--drift-file", help="the drift as one path per line, instead of --base (`-` for none)")
    ms.add_argument("--root", default=ROOT, help="the tree whose test classes are listed (default: this repository)")
    ms.add_argument("--files-from", help="the changed files as one path per line, in place of the arguments")
    ms.add_argument("--out", help="also write the selection JSON to this file (stdout keeps the notice and the JSON)")
    ms.add_argument("--summary", help="also write the shadow step-summary markdown to this file")
    ms.add_argument("files", nargs="*")

    sc = sub.add_parser("shadow-check", help="classes that failed but a map selection would not have run (workflow step)")
    sc.add_argument("--selection", help="the JSON map-select --out wrote; missing or unreadable counts as FULL")
    sc.add_argument("--reports", required=True, help="a folder with one subfolder per timing artifact")
    sc.add_argument("--out", required=True, help="the one-row JSONL file to write")
    sc.add_argument("--meta", default="{}", help="JSON object merged into the row (run, sha, pr...)")

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
        result = map_select(test_map, files, drift, discovered, event=args.event, **selection_args(Rules()))
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
    if args.cmd == "shadow-check":
        selection = None
        if args.selection:
            try:
                with open(args.selection, encoding="utf-8") as fh:
                    selection = json.load(fh)
            except (OSError, ValueError):
                selection = None
        failed, seen = failed_classes(args.reports)
        try:
            meta = json.loads(args.meta)
        except ValueError:
            meta = {}
        row = shadow_row(selection, failed, seen, meta if isinstance(meta, dict) else {})
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
    if args.cmd == "decide":
        print(json.dumps(decide(args.files, args.event, Rules()), indent=1))
        return 0
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
