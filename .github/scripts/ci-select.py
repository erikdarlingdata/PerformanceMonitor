#!/usr/bin/env python3
"""The ONE place CI's test-selection rules live (#5459).

Input: the changed files of a run (and the event). Output: which test jobs run and in what scope.

Two readers use the same code:

* build.yml's jobs call `lite-scope` (and, from the cut that adds it, `darling-scope`) with the area answers
  dorny/paths-filter already computed, and get the decision back as `key=value` lines for $GITHUB_OUTPUT.
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
import json
import os
import re
import sys

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
        return out


# ---------------------------------------------------------------------------------------------------------
# The decisions.
# ---------------------------------------------------------------------------------------------------------

def darling_scope(event: str, runs: bool, shards_run: bool) -> str:
    """What the build job's no-store "Run Darling tests" step runs: full, guard (only the Stage=Guard classes) or none.

    The PostgreSQL shards run every Darling.Tests class, with a live store, whenever the Darling gate
    (.github/darling-paths-filter.yml) matches. The no-store pass would repeat their non-live classes, so while the
    shards run it keeps only the Guard stage (cut 3b of #5459; the replay finds no failing class it stops covering).
    A release always runs the whole suite."""
    if not runs:
        return "none"
    if event != "release" and shards_run:
        return "guard"
    return "full"


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
        "darling_scope": darling_scope(event, build_darling, pg_run),
        "darling_pg": pg_run,
        "darling_linux": release or a["gate_darling"],
        "lite_mode": mode,
        "tree_guards": tree_run,
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
        return d["darling_scope"] == "full" or d["darling_pg"] or d["tree_guards"] \
            or (d["darling_scope"] == "guard" and guard)
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
#     (the workflow, this script, a props/csproj/lock file, global.json).
# When the changed file list is unknown (the API call failed, or it came back empty) nothing is skipped.

SLOW_TRAIT = "Cost=Slow"
_BUILD_INPUT_NAMES = {"directory.build.props", "directory.build.targets", "directory.packages.props", "global.json",
                      "nuget.config", "packages.lock.json"}
_BUILD_INPUT_PATHS = {".github/workflows/build.yml", ".github/scripts/ci-select.py", ".github/darling-paths-filter.yml"}
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

    def skip(self, files: list[str], event: str) -> dict[str, list[str]]:
        """{suite: [simple class names a leg leaves out]} for one change. Empty for every event but pull_request,
        and when the changed file list is unknown."""
        out: dict[str, list[str]] = {suite: [] for suite in SUITE_DIRS}
        if event != "pull_request" or not files:
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
# CLI.
# ---------------------------------------------------------------------------------------------------------

def _bool(s: str) -> bool:
    return s.strip().lower() == "true"


def main(argv: list[str]) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--replay", action="store_true", help="replay the failure corpus against today's rules")
    ap.add_argument("--corpus", default=CORPUS)
    sub = ap.add_subparsers(dest="cmd")

    ls = sub.add_parser("lite-scope", help="how much of the Lite suite a leg runs (workflow step)")
    ls.add_argument("--event", required=True)
    ls.add_argument("--shard", required=True)
    for k in ("lite", "core", "root", "reads", "linked"):
        ls.add_argument(f"--{k}", default="false")

    ds = sub.add_parser("darling-scope", help="what the build job's no-store Darling pass runs (workflow step)")
    ds.add_argument("--event", required=True)
    ds.add_argument("--gate", default="false", help="the Darling PostgreSQL gate's answer (darling-paths-filter.yml)")

    sk = sub.add_parser("slow-skip", help="the Cost=Slow classes a pull request run leaves out (workflow step)")
    sk.add_argument("--suite", required=True, choices=sorted(SUITE_DIRS))
    sk.add_argument("--event", required=True)
    sk.add_argument("--repo", default="")
    sk.add_argument("--pr", default="")
    sk.add_argument("--list", action="store_true", help="print every Cost=Slow class of the suite instead")
    sk.add_argument("files", nargs="*", help="changed files; with --repo and --pr they come from the API instead")

    dc = sub.add_parser("decide", help="print every decision for a list of changed files")
    dc.add_argument("--event", default="pull_request")
    dc.add_argument("files", nargs="*")

    args = ap.parse_args(argv)
    if args.replay:
        return 1 if replay(args.corpus) else 0
    if args.cmd == "lite-scope":
        mode = lite_scope(args.event, _bool(args.lite), _bool(args.core), _bool(args.root), _bool(args.reads),
                          _bool(args.linked))
        run = mode == "full" or (mode == "reads" and args.shard == "0")
        print(f"mode={mode}")
        print(f"run={'true' if run else 'false'}")
        return 0
    if args.cmd == "darling-scope":
        scope = darling_scope(args.event, True, args.event != "release" and _bool(args.gate))
        print(f"scope={scope}")
        print("filter=" + ("-trait Stage=Guard" if scope == "guard" else ""))
        return 0
    if args.cmd == "slow-skip":
        index = SlowIndex()
        if args.list:
            print("\n".join(sorted(index.slow[args.suite])))
            return 0
        files = args.files
        if not files and args.repo and args.pr.isdigit() and args.event == "pull_request":
            files = changed_files_of_pr(args.repo, args.pr)
        print("\n".join(index.skip(files, args.event)[args.suite]))
        return 0
    if args.cmd == "decide":
        print(json.dumps(decide(args.files, args.event, Rules()), indent=1))
        return 0
    ap.print_help()
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
