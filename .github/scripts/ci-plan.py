#!/usr/bin/env python3
"""The plan job of the pull-request workflow pr.yml (#6656): what a run does, from the event and the changed files.

Inputs: the event (pull_request or push), the base branch, the head branch, the draft flag, the labels and the
changed file list. Outputs, as `key=value` lines (stdout, and $GITHUB_OUTPUT when --github-output is given):

  run              true when the run does any work; false for a merge-train PR
  outcome          run | train-blocked  (train-blocked: the verdict job must fail, see below)
  why              one plain sentence for the job log
  areas            docs-only | darling | lite | all | none | classes
  darling_scope    full | reads-lite | classes | none   (reads-lite: the Stage=Guard classes plus the classes that read a Lite file)
  lite_scope       full | reads | classes | none        (reads: the Lite classes that read a Darling file)
                   classes: only the test classes the pull request changed (#6734), named in darling_classes and lite_classes
  darling_classes  comma list of simple class names, set when darling_scope is classes
  lite_classes     comma list of simple class names, set when lite_scope is classes
                   plan() returns both lists. The command line writes each as a file of one name a line when it is given
                   --class-lists-dir (#7734): darling-classes.txt and lite-classes.txt, and the outputs say class_lists=true
                   and the two counts instead of the lists. A Windows environment variable holds 32,767 characters, so a
                   list as an output and an environment variable stopped at about 980 Darling classes; a file has no limit.
  build            true when the shared test build is needed (any run that is not docs-only)
  test_minutes     estimated test time of the selected classes, in minutes
  legs             a pull request: N = ceil(test_minutes / MINUTES_PER_LEG), at most MAX_LEGS; a dev push: the same, at least DEV_PUSH_MIN_LEGS and at most DEV_PUSH_MAX_LEGS;
                   0 when nothing runs
  legs_json        [1, 2, ..., N], for the test job's matrix
  actionlint       true when a workflow file changed
  check_web_js     true when a dashboard JavaScript file changed
  pg_old_targets   true when a pull request changed a file the old-PostgreSQL-target job covers (#6985, PG_OLD_TARGET_PATHS); never on a push, the nightly runs it

The pattern sets are NOT copied here. The areas come from ci-select.py's Rules, which reads the docs allowlist and the
area filters from the same files dorny/paths-filter reads, and the Lite-reads classes from ci-select.py's
scan_classes (Reads=Lite and Reads=Darling traits, derived from the test sources).

Rules, in order:
  1. Any event that is not a pull request always runs. (PR 1 of #6656 gated pull requests on a label; PR 2 removed it.)
  2. A `train/` branch runs (a merge-train pull request exists only to run CI).
  3. A `merge-train` label does not run; the verdict job turns that into a failed `CI result` (today's rule: only an
     admin merges it, after the train covered it).
  4. A draft runs (#7717): agent pull requests open as drafts and need their first build and test run on CI, not on the
     laptop. The draft flag does not gate the run. It narrows the scope (rule 7).
  5. Areas: base other than dev, a push, a path no area knows, an empty list, or a list the API may have cut: all.
     Only documentation: docs-only. Otherwise Darling files give the Darling suite, Lite files the Lite suite, and
     each side keeps its cross-reading classes of the other.
  6. Areas classes (#6734): every changed file is documentation or a .cs file under Darling/Darling.Tests or Lite.Tests
     whose every class holds tests and is no base class. Only those classes run. Any other file (a product file, a
     helper, a fixture, a .csproj, a lock file), a file the test map does not know, or a missing map: rule 5.
  7. A draft that changes product files (#7727): rule 5 would give the full Darling suite (12 to 15 legs), so a draft
     runs only the test classes in the test files it changed (the rule 6 file table) plus the Darling Stage=Guard
     classes, as areas classes. A draft that changed no test file runs the build and the Guard classes only. When a
     changed test file is not safe in the file table (a helper, a fixture, a base class, a .csproj, a file the map does
     not know), the whole pull request runs as rule 5 says, and `why` says so. The full suite runs when the pull
     request is marked ready for review (`ready_for_review` starts a run), so every merge still has a full green run
     behind it. A dev push, a docs-only change and a test-only change are as before.
  8. A pull request that is ready for review and changes product files (#7728): the test classes the newest nightly
     `test-map` selects (ci-select.py's map_select), plus the classes of its changed test files, plus the Darling
     Stage=Guard classes, as areas classes. FULL (rule 5) instead, with `why` saying which: the map is missing or cannot be
     read, it is older than MAP_MAX_AGE_HOURS (48), map_select answers FULL (a keep-full file, a file the map does not know,
     a new product file, a constant or enum of a shared project), the diff could not be read, no class is selected, or a class
     list is longer than the runner's environment variable allows. There is no drift rule: a changed or new test file is
     always picked, and a dev push still runs everything. The map is never a reason for a red run.

Standard library only. Run: python -I .github/scripts/ci-plan.py --event pull_request --base dev --files-file FILE
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import re
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", ".."))

TRAIN_LABEL = "merge-train"
TRAIN_BRANCH_PREFIX = "train/"
# Legs. A pull request is cut for a short wait: MINUTES_PER_LEG test minutes a leg, so a full all-areas run (about 131
# weighted minutes, from run 40001582504's per-class times) is 16 legs. A leg also pays about 1.3 minutes that are not tests,
# measured on that run's 9 legs (checkout, build download, .NET and PostgreSQL start: 52 to 103 s, mean 70 s, plus about 7 s
# of post steps), so a leg is about 8.5 + 1.3 = 9.8 minutes. A dev push uses the same estimate, at least DEV_PUSH_MIN_LEGS
# legs (4 timed out) and at most DEV_PUSH_MAX_LEGS (GitHub Pro runs 40 jobs at once across the repo, and a busy hour
# fills them, so a push must leave runners for open PRs). A full push was 8 legs: about 131 / 8 + 1.3 = 17.7 minutes a leg, under the
# 25-minute step timeout, and 12 runners stay free for PRs.
# By 2026-10-29 the suites had grown to about 152 weighted minutes (the refreshed tables), which made 8 legs 152 / 8 + 1.3 =
# 20.3 minutes a leg even when perfectly balanced, and the stale tables left the slowest leg at 22.9 weighted minutes: leg 8
# of dev run 40959511081 took 23.5 minutes and leg 8 of run 40953502699 timed out the 25-minute step (#6941). The same leg's
# Darling test seconds also moved by up to 270 s between runs (live-postgres contention), so a push is 10 legs: 152 / 10 + 1.3
# = 16.5 minutes a leg and 10 runners stay free for PRs.
# A fixed 4 legs put a push's Darling.Tests alone at 1266 to 1452 s a leg, and every dev push run (40034434899,
# 40031691584, 40020225933, 40015830041) timed out the 25-minute test step (#6656).
MINUTES_PER_LEG = 8.5
MAX_LEGS = 16
DEV_PUSH_MIN_LEGS = 6
DEV_PUSH_MAX_LEGS = 10
# The GitHub pull request files API returns at most 3000 files; a list that long may have been cut short.
FILES_API_CAP = 3000

# Weights are wall-seconds per class (darling-shard-weights.json). A class a table does not know weighs the mean.
DARLING_WEIGHTS = os.path.join(HERE, "darling-shard-weights.json")
LITE_WEIGHTS = os.path.join(HERE, "lite-shard-weights.json")
# Lite's estimate when lite-shard-weights.json is missing (it is new in #6656): the full suite, and the reads subset.
LITE_FULL_FALLBACK_MINUTES = 28.0
LITE_READS_FALLBACK_MINUTES = 3.0

WEB_JS_MARKER = "/wwwroot/js/"
# #6985: the files whose change runs the old-PostgreSQL-target job (every PostgreSQL collector against a real PostgreSQL 10-17).
PG_OLD_TARGET_PATHS = (
    "PerformanceMonitor.Collectors/Pg",
    "PerformanceMonitor.Collectors/CollectorCatalog.cs",
    "Darling/Darling.Tests/PgCollectorsOnOldTargetsLiveTests.cs",
    "Darling/tools/fetch-pg-target.ps1",
    ".github/scripts/start-test-pg.ps1",
)
# #7728: a ready pull request picks its classes from the newest nightly test map, and only while the map is at most this old.
# The nightly builds one at 06:00 and one at 14:00 UTC, so a healthy map is at most about 16 hours old.
MAP_MAX_AGE_HOURS = 48
# The test trees a changed class file may sit in for the changed-classes plan (#6734).
TEST_TREES = {"darling": "Darling/Darling.Tests/", "lite": "Lite.Tests/"}


def _load_ci_select():
    spec = importlib.util.spec_from_file_location("ci_select", os.path.join(HERE, "ci-select.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ci_select"] = mod
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------------------------------------
# The gate: does this run do any work?
# ---------------------------------------------------------------------------------------------------------
def gate(event: str, head_ref: str, labels: "list[str]") -> "tuple[str, str]":
    """(outcome, why). outcome is run or train-blocked."""
    if event != "pull_request":
        return "run", f"a {event} event always runs."
    if head_ref.startswith(TRAIN_BRANCH_PREFIX):
        return "run", "a merge-train pull request exists to run CI."
    if TRAIN_LABEL in labels:
        return "train-blocked", "the pull request carries the merge-train label, so its train run already covered it."
    return "run", "a pull request runs, draft or ready for review."


# ---------------------------------------------------------------------------------------------------------
# The areas.
# ---------------------------------------------------------------------------------------------------------
def unknown_paths(rules, files: "list[str]") -> "list[str]":
    """Files that are not documentation and that no area filter names."""
    patterns: "list[str]" = []
    for name, pats in rules.build.items():
        if name not in ("all", "docs"):
            patterns += pats
    for name, pats in rules.gate.items():
        if name != "all":  # the catch-all counters match every path
            patterns += pats
    for pats in rules.lite_shards.values():
        patterns += pats
    docs = set(rules._hits(rules.build["docs"], files))
    known = set(rules._hits(patterns, files))
    return [f for f in files if f not in docs and f not in known]


def changed_test_classes(rules, files: "list[str]", catalog: "dict | None") -> "dict[str, list[str]] | None":
    """{suite: [class names]} when every file is documentation or a test-class file, else None (#6734).
    A test-class file is a .cs file the catalog's file table knows, that declares a class holding a test, with no base class
    in it and no helper class that another test file uses. Only the classes that hold a test are picked: a helper or fake of
    the same file runs no test, so the test host's class listing may not name it, and the class beside it is what changes.
    A file the table does not know (a helper, a fixture, a .csproj, a lock file, a deleted or new file, a missing table)
    gives None, and so does a product file."""
    table = (catalog or {}).get("files") or {}
    docs = set(rules._hits(rules.build["docs"], files))
    picks: "dict[str, list[str]]" = {"darling": [], "lite": []}
    for f in files:
        if f in docs:
            continue
        row = table.get(f) if f.endswith(".cs") else None
        if not row or row["suite"] not in picks or not f.startswith(TEST_TREES[row["suite"]]) or not row["safe"]:
            return None
        picks[row["suite"]] += [c for c in row["tests"] if c not in picks[row["suite"]]]
    return picks if any(picks.values()) else None


def draft_picks(files: "list[str]", catalog: "dict | None", darling_scope: str, lite_scope: str,
                named: "dict[str, list[str]] | None" = None) -> "tuple[dict[str, list[str]] | None, str]":
    """(picks, why) for a draft that changed product files (#7727): the test classes of the changed test files, plus the
    Darling Stage=Guard classes, plus (#7734) `named`: the classes whose source names a changed product file by path or file
    name (ci-select.py classes_naming_files). A named class runs whatever the suite's scope is: a pin that reads the file
    as text is what the rule is for. picks is None when a changed test file is not safe in the file table (its suite then
    runs as today, and so does the whole pull request: areas classes cannot mix a narrowed suite with a full one)."""
    table = (catalog or {}).get("files") or {}
    classes = (catalog or {}).get("classes") or {}
    if not table or not classes.get("darling"):
        return None, "the test map is missing"
    changed: "dict[str, list[str]]" = {"darling": [], "lite": []}
    for f in files:
        for suite, tree in TEST_TREES.items():
            if not f.startswith(tree):
                continue
            row = table.get(f) if f.endswith(".cs") else None
            if not row or row["suite"] != suite or not row["safe"]:
                return None, f"{f} is not a plain test class file, so its suite runs in full"
            changed[suite] += [c for c in row["tests"] if c not in changed[suite]]
    guard = [c for c, t in classes["darling"].items() if "Stage=Guard" in t and _CS.NO_TESTS_TRAIT not in t]
    picks = {"darling": (changed["darling"] + [c for c in guard if c not in changed["darling"]]) if darling_scope != "none" else [],
             "lite": changed["lite"] if lite_scope != "none" else []}
    for suite in ("darling", "lite"):
        runs = {c for c, t in classes.get(suite, {}).items() if _CS.NO_TESTS_TRAIT not in t}
        picks[suite] += [c for c in dict.fromkeys((named or {}).get(suite, [])) if c in runs and c not in picks[suite]]
    return (picks, "") if any(picks.values()) else (None, "no class is selected")


def _map_age(selection: dict) -> str:
    """The map's build time and age for a `why` line, or a word on why there is none."""
    test_map = selection.get("map")
    built = _CS._parse_time(test_map.get("built_at")) if isinstance(test_map, dict) else None
    if built is None:
        return "no readable map"
    now = selection.get("now") or _CS.datetime.datetime.now(_CS.datetime.timezone.utc)
    hours = (now - built).total_seconds() / 3600.0
    return f"map built {built:%Y-%m-%d %H:%M} UTC, {hours:.1f} hours old"


def ready_picks(rules, files: "list[str]", base: str, catalog: dict, selection: dict
                ) -> "tuple[dict[str, list[str]] | None, str]":
    """(picks, why) for a pull request that is ready for review (#7728): the classes map_select picks from the nightly map,
    plus the classes of the test files it changed, plus the Darling Stage=Guard classes. picks is None, with the reason,
    when the pull request must run as rule 5 says (see the module docstring, rule 8). Never raises: any failure is FULL.
    selection: {map: the parsed map or None, error: why there is none, now: the clock (tests), shared_diff: the -U0 diff text
    of the shared-project files, named: map_select's `named`}."""
    age = _map_age(selection)
    try:
        test_map = selection.get("map")
        if not isinstance(test_map, dict):
            return None, selection.get("error") or "no test map was given"
        sel = _CS.map_select(test_map, files, [], catalog["classes"], event="pull_request", base_ref=base,
                             now=selection.get("now"), max_age_days=MAP_MAX_AGE_HOURS / 24.0, named=selection.get("named"),
                             shared_diff=selection.get("shared_diff"), **_CS.selection_args(rules))
        if sel["full"]:
            return None, f"{sel['reason']} ({age})"
        table = catalog.get("files") or {}
        picks: "dict[str, list[str]]" = {}
        for suite in ("darling", "lite"):
            tree = catalog["classes"].get(suite, {})
            runs = {c for c, t in tree.items() if _CS.NO_TESTS_TRAIT not in t}
            names = [c for c in sel["selected"].get(suite, []) if c in runs]
            for f in files:
                row = table.get(f) if f.endswith(".cs") else None
                if row and row["suite"] == suite:
                    names += [c for c in row["tests"] if c in runs]
            if suite == "darling":
                names += [c for c, t in tree.items() if c in runs and "Stage=Guard" in t]
            picks[suite] = list(dict.fromkeys(names))
        if not any(picks.values()):
            return None, f"the map selected no class ({age})"
        return picks, (f"a ready pull request runs the classes the nightly test map selects, {len(picks['darling'])} Darling "
                       f"and {len(picks['lite'])} Lite, with the Darling Guard classes and the classes of its changed test files "
                       f"({age}).")
    except Exception as exc:  # the map is never a reason for a red run
        return None, f"the map selection failed ({type(exc).__name__}: {str(exc)[:80]})"


def classify(rules, event: str, base: str, files: "list[str]", catalog: "dict | None" = None, draft: bool = False,
             selection: "dict | None" = None, named: "dict[str, list[str]] | None" = None, named_error: str = "") -> dict:
    """The areas of one change: {areas, darling_scope, lite_scope, why}. draft: a pull request that is a draft (#7727).
    selection: the nightly map and what the pick reads (#7728, see ready_picks); None leaves that rule off."""
    everything = {"areas": "all", "darling_scope": "full", "lite_scope": "full"}
    if event != "pull_request":
        return {**everything, "reason": f"a {event} runs every area."}
    if base != "dev":
        return {**everything, "reason": f"the base branch is '{base}', not dev, so every area runs."}
    if not files:
        return {**everything, "reason": "no changed files were listed, so every area runs."}
    if len(files) >= FILES_API_CAP:
        return {**everything, "reason": f"{len(files)} files may be a cut-off list, so every area runs."}
    odd = unknown_paths(rules, files)
    if odd:
        return {**everything, "reason": f"no area knows {odd[0]}, so every area runs."}

    a = rules.areas(files)
    area_names = ("root", "core", "lite", "darling")
    if a["all_count"] == a["docs_count"] and not any(a[n] for n in area_names):
        return {"areas": "docs-only", "darling_scope": "none", "lite_scope": "none",
                "reason": "only documentation changed."}

    cs = _CS
    if any(f.endswith(".cs") and f.startswith(tuple(TEST_TREES.values())) for f in files):
        picks = changed_test_classes(rules, files, catalog if catalog is not None else load_catalog())
        if picks:
            return {"areas": "classes", "darling_scope": "classes" if picks["darling"] else "none",
                    "lite_scope": "classes" if picks["lite"] else "none",
                    "darling_classes": picks["darling"], "lite_classes": picks["lite"],
                    "reason": "only documentation and test classes changed, so only those classes run."}
    darling_full = a["gate_darling"] or a["darling_full"] or a["core"] or a["root"]
    lite_mode = cs.lite_scope("pull_request", a["lite_shard"], a["core_shard"], a["root_shard"],
                              a["darling_reads_shard"], a["lite_linked_shard"])
    if darling_full:
        darling_scope = "full"
    elif a["darling"] or a["gate_darling"]:
        darling_scope = "reads-lite"
    else:
        darling_scope = "none"
    if darling_scope == "full" and lite_mode == "full":
        areas = "all"
    elif darling_scope == "full":
        areas = "darling"
    elif lite_mode == "full":
        areas = "lite"
    else:
        # Only a cross-reading subset on either side (a Lite XAML file a Darling parity pin reads, say).
        areas = "none" if darling_scope == "none" and lite_mode == "none" else "darling"
    reason = f"Darling scope {darling_scope}, Lite scope {lite_mode}."
    if draft and areas != "none":
        picks, why = ((None, named_error) if named_error else
                      draft_picks(files, catalog if catalog is not None else load_catalog(), darling_scope, lite_mode, named))
        if picks:
            extra = sum(len(v) for v in (named or {}).values())
            return {"areas": "classes", "darling_scope": "classes" if picks["darling"] else "none",
                    "lite_scope": "classes" if picks["lite"] else "none",
                    "darling_classes": picks["darling"], "lite_classes": picks["lite"],
                    "reason": "a draft runs only its own test classes and the Guard classes"
                              + (f", and the {extra} classes whose source names a changed file (#7734)" if extra else "")
                              + "; the full suite runs when it is marked ready for review."}
        reason += f" It is a draft, but {why}, so it runs as a pull request that is ready for review."
    if not draft and selection is not None and areas != "none":
        picks, why = ready_picks(rules, files, base, catalog if catalog is not None else load_catalog(), selection)
        if picks:
            return {"areas": "classes", "darling_scope": "classes" if picks["darling"] else "none",
                    "lite_scope": "classes" if picks["lite"] else "none",
                    "darling_classes": picks["darling"], "lite_classes": picks["lite"], "reason": why}
        reason += f" A ready pull request picks its classes from the nightly test map, but {why}, so it runs as rule 5 says."
    return {"areas": areas, "darling_scope": darling_scope, "lite_scope": lite_mode, "reason": reason}


# ---------------------------------------------------------------------------------------------------------
# The leg count.
# ---------------------------------------------------------------------------------------------------------
def load_weights(path: str) -> "dict[str, float]":
    """{simple class name: seconds} from a weights file, or {} when it is missing or unreadable."""
    try:
        with open(path, encoding="utf-8") as fh:
            table = json.load(fh).get("classes", {})
    except (OSError, ValueError, AttributeError):
        return {}
    return {name.rsplit(".", 1)[-1]: float(v) for name, v in table.items() if isinstance(v, (int, float))}


def load_catalog(root: str = ROOT) -> dict:
    """The test classes of the tree with their traits, and the weight tables."""
    classes = _CS.scan_classes(root)
    return {"classes": classes, "files": _file_table(root, classes), "darling_weights": load_weights(DARLING_WEIGHTS),
            "lite_weights": load_weights(LITE_WEIGHTS)}


def _file_table(root: str, classes: "dict[str, dict[str, set[str]]]") -> "dict[str, dict]":
    """{repo-relative .cs file: {suite, classes, tests, safe}} for the darling and lite test trees (#6734). classes: every
    class the file declares. tests: those that hold a test (the only ones the plan names). safe: the file
    declares at least one class that holds a test, none of its classes is the base class of another class (a derived
    class would run its tests), and every class of it that holds no test (a helper, a fake) is named in no other test
    file, so no other class can break when this file changes. A class with the same name in two files is unsafe."""
    rows = [r for r in _CS._walk_classes(root) if r[0] in TEST_TREES]
    bases = {x[len(_CS._BASE_MARK):] for _s, _f, _n, t in rows for x in t if x.startswith(_CS._BASE_MARK)}
    owners: "dict[str, set[str]]" = {}
    for _s, rel, name, _t in rows:
        owners.setdefault(name, set()).add(rel)
    table: "dict[str, dict]" = {}
    helpers: "dict[str, list[str]]" = {}
    for suite, rel, name, _traits in rows:
        ent = table.setdefault(rel, {"suite": suite, "classes": [], "tests": [], "safe": True})
        ent["classes"].append(name)
        holds = _CS.NO_TESTS_TRAIT not in classes[suite].get(name, {_CS.NO_TESTS_TRAIT})
        if holds:
            ent["tests"].append(name)
        if name in bases or len(owners[name]) > 1:
            ent["safe"] = False
        elif not holds:
            helpers.setdefault(name, []).append(rel)
    for rel, ent in table.items():
        if not ent["tests"]:
            ent["safe"] = False
    if helpers:
        for rel in table:
            used = set(re.findall(r"\w+", _CS.read_source(os.path.join(root, rel), "replace"))) & helpers.keys()
            for name in used:
                for home in helpers[name]:
                    if home != rel:
                        table[home]["safe"] = False
    return table


def _selected(classes: "dict[str, set[str]]", scope: str, reads_trait: str, picks: "list[str] | None" = None) -> "list[str]":
    runs = [c for c, t in classes.items() if _CS.NO_TESTS_TRAIT not in t]
    if scope == "full":
        return runs
    if scope == "classes":
        return [c for c in runs if c in (picks or ())]
    if scope == "reads-lite":
        return [c for c in runs if "Stage=Guard" in classes[c] or reads_trait in classes[c]]
    if scope == "reads":
        return [c for c in runs if reads_trait in classes[c]]
    return []


def suite_minutes(names: "list[str]", weights: "dict[str, float]", fallback: float) -> float:
    if not names:
        return 0.0
    if not weights:
        return fallback
    mean = sum(weights.values()) / len(weights)
    return sum(weights.get(n, mean) for n in names) / 60.0


def estimate(catalog: dict, darling_scope: str, lite_scope: str, picks: "dict | None" = None) -> "tuple[float, float]":
    """(Darling minutes, Lite minutes) of the selected classes."""
    picks = picks or {}
    d = _selected(catalog["classes"]["darling"], darling_scope, "Reads=Lite", picks.get("darling"))
    lt = _selected(catalog["classes"]["lite"], lite_scope, "Reads=Darling", picks.get("lite"))
    fallback = LITE_FULL_FALLBACK_MINUTES if lite_scope == "full" else LITE_READS_FALLBACK_MINUTES
    return (suite_minutes(d, catalog["darling_weights"], 0.0),
            suite_minutes(lt, catalog["lite_weights"], fallback))


def leg_count(test_minutes: float, event: str = "pull_request") -> int:
    if test_minutes <= 0:
        return 0
    n = min(MAX_LEGS, max(1, math.ceil(test_minutes / MINUTES_PER_LEG)))
    return n if event == "pull_request" else min(DEV_PUSH_MAX_LEGS, max(DEV_PUSH_MIN_LEGS, n))


# ---------------------------------------------------------------------------------------------------------
# The plan.
# ---------------------------------------------------------------------------------------------------------
def plan(event: str, base: str, head_ref: str, labels: "list[str]", files: "list[str]",
         rules, catalog: "dict | None" = None, draft: bool = False, selection: "dict | None" = None,
         named: "dict[str, list[str]] | None" = None, named_error: str = "") -> "dict[str, str]":
    outcome, why = gate(event, head_ref, labels)
    out = {"run": "true" if outcome == "run" else "false", "outcome": outcome, "why": why,
           "areas": "none", "darling_scope": "none", "lite_scope": "none", "build": "false",
           "darling_classes": "", "lite_classes": "", "test_minutes": "0", "legs": "0", "legs_json": "[]",
           "actionlint": "false", "check_web_js": "false", "pg_old_targets": "false"}
    if outcome != "run":
        return out

    picks_from_map = selection is not None and event == "pull_request" and not draft and not _all_docs(rules, files)
    catalog = catalog if catalog is not None else (load_catalog() if draft or picks_from_map or _may_narrow(files) else None)
    c = classify(rules, event, base, files, catalog, draft and event == "pull_request", selection, named, named_error)
    out.update(darling_classes=",".join(c.get("darling_classes", [])), lite_classes=",".join(c.get("lite_classes", [])))
    out.update(areas=c["areas"], darling_scope=c["darling_scope"], lite_scope=c["lite_scope"])
    out["why"] = f"{why} {c['reason']}"
    out["build"] = "false" if c["areas"] == "docs-only" else "true"
    out["actionlint"] = _bool(any(f.startswith(".github/workflows/") for f in files))
    out["check_web_js"] = _bool(any(WEB_JS_MARKER in "/" + f for f in files))
    out["pg_old_targets"] = _bool(event == "pull_request" and any(f.startswith(PG_OLD_TARGET_PATHS) for f in files))
    if out["build"] == "true":
        catalog = catalog if catalog is not None else load_catalog()
        picks = {"darling": c.get("darling_classes", []), "lite": c.get("lite_classes", [])}
        d_min, l_min = estimate(catalog, c["darling_scope"], c["lite_scope"], picks)
        total = d_min + l_min
        n = leg_count(total, event)
        if c["areas"] == "classes":
            # Never more legs than classes, so no leg is empty on the duration cut. The hash cut (a class the weight table does not
            # know) can still leave one empty: the runners let such a leg pass under -Areas classes, and the other legs run every class.
            sizes = [len(_selected(catalog["classes"][s], c[f"{s}_scope"], "", picks[s])) for s in ("darling", "lite") if picks[s]]
            n = max(1, min([n] + sizes))
        out["test_minutes"] = f"{total:.1f}"
        out["legs"] = str(n)
        out["legs_json"] = json.dumps(list(range(1, n + 1)), separators=(",", ":"))
    return out


def _all_docs(rules, files: "list[str]") -> bool:
    """True when every file is documentation, so the class scan is not needed."""
    return bool(files) and set(rules._hits(rules.build["docs"], files)) >= set(files)


def _may_narrow(files: "list[str]") -> bool:
    return any(f.endswith(".cs") and f.startswith(tuple(TEST_TREES.values())) for f in files)


def _bool(v: bool) -> str:
    return "true" if v else "false"


def parse_bool(text: str) -> bool:
    return str(text).strip().lower() in ("true", "1", "yes")


def parse_labels(text: str) -> "list[str]":
    """toJSON(labels.*.name) gives a JSON array; a comma list is accepted too."""
    text = (text or "").strip()
    if not text:
        return []
    if text.startswith("["):
        try:
            return [str(x) for x in json.loads(text)]
        except ValueError:
            pass
    return [x.strip() for x in text.split(",") if x.strip()]


def build_selection(map_path: str, map_error: str, root: str = ROOT) -> dict:
    """The input of ready_picks for a pull request run (#7728): the map file the workflow downloaded, or why there is none, and
    the two things map_select reads from the pull request's own change. The checkout is the pull request's merge commit, so its
    change is `HEAD^1..HEAD`. A map that cannot be read, or a diff that cannot be read, leaves `map` None: the run is FULL."""
    out: dict = {"map": None, "error": map_error or "no test map was downloaded", "now": None, "shared_diff": None, "named": None}
    if not map_path or not os.path.isfile(map_path):
        return out
    loaded = _CS.load_map(map_path)
    if not isinstance(loaded, dict):
        out["error"] = "the downloaded test map cannot be read"
        return out
    try:
        if _CS._git(root, "rev-parse", "-q", "--verify", "HEAD^2") is None:
            out["error"] = "the checkout is not the pull request's merge commit, so its change cannot be read"
            return out
        text = _CS.symbol_diff_text(root, "HEAD^1", "HEAD")
        if text is None:
            out["error"] = "the pull request's diff could not be read"
            return out
        tokens, _tests = _CS.changed_symbols(text)
        out["named"] = _CS.named_classes(_CS.symbol_matches(tokens, set(), _CS.test_file_index(root), None))
        out["shared_diff"] = text
    except Exception as exc:  # never red because of the map
        out["error"] = f"the pull request's diff could not be read ({type(exc).__name__})"
        return out
    out["map"], out["error"] = loaded, ""
    return out


CLASS_LIST_KEYS = ("darling_classes", "lite_classes")


def write_class_lists(directory: str, out: "dict[str, str]") -> "list[str]":
    """Write each non-empty class list of a plan as `<suite>-classes.txt`, one simple name a line, and return the `key=value`
    lines that say so: class_lists and the two counts (#7734). A list as an output and an environment variable stopped at
    about 980 Darling classes (a Windows variable holds 32,767 characters); the legs read the file instead. An empty list
    writes no file, so a leg finds only the suites the plan runs."""
    counts = {}
    for suite in ("darling", "lite"):
        names = [x for x in out.get(f"{suite}_classes", "").split(",") if x]
        counts[suite] = len(names)
        if names:
            os.makedirs(directory, exist_ok=True)
            with open(os.path.join(directory, f"{suite}-classes.txt"), "w", encoding="utf-8", newline="\n") as fh:
                fh.write("\n".join(names) + "\n")
    return [f"class_lists={_bool(any(counts.values()))}", f"darling_class_count={counts['darling']}",
            f"lite_class_count={counts['lite']}"]


def main(argv: "list[str]") -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--event", required=True, choices=["pull_request", "push"])
    ap.add_argument("--base", default="dev", help="the base branch of a pull request; the pushed branch for a push")
    ap.add_argument("--head-ref", default="", help="the head branch of a pull request")
    ap.add_argument("--draft", default="false", help="true when the pull request is a draft (it narrows the scope, #7727)")
    ap.add_argument("--map", default=None, help="the nightly test-map.json.gz a pull request picks its classes from (#7728); give it "
                                                "(even empty) to turn the map rule on")
    ap.add_argument("--map-error", default="", help="why there is no map file: the plan says it in `why` and runs every class")
    ap.add_argument("--labels", default="", help="a JSON array or a comma list")
    ap.add_argument("--files-file", help="changed files, one a line (default: stdin)")
    ap.add_argument("--github-output", help="also append the outputs to this file ($GITHUB_OUTPUT)")
    ap.add_argument("--class-lists-dir", default=None,
                    help="write the selected class lists here, one name a line, as darling-classes.txt and lite-classes.txt (#7734), "
                         "and leave the lists out of the outputs; without it the outputs carry the comma lists")
    args = ap.parse_args(argv)

    if args.files_file:
        with open(args.files_file, encoding="utf-8") as fh:
            text = fh.read()
    else:
        text = sys.stdin.read()
    files = [ln.strip() for ln in text.splitlines() if ln.strip()]

    draft = parse_bool(args.draft)
    selection = None
    if args.event == "pull_request" and args.map is not None and not draft:
        selection = build_selection(args.map, args.map_error)
    named, named_error = None, ""
    if args.event == "pull_request" and draft:
        # #7734: a draft also runs the classes whose source names a changed product file. A scan that fails runs the pull request as
        # one that is ready for review (every class its areas select), never red.
        try:
            named, _path_only = _CS.classes_naming_files(files)
        except Exception as exc:
            named_error = f"the scan for test classes that name a changed file failed ({type(exc).__name__}: {str(exc)[:80]})"
    out = plan(args.event, args.base, args.head_ref, parse_labels(args.labels), files,
               _CS.Rules(), draft=draft, selection=selection, named=named, named_error=named_error)
    lines = [f"{k}={v}" for k, v in out.items()]
    if args.class_lists_dir is not None:
        lines = [f"{k}={v}" for k, v in out.items() if k not in CLASS_LIST_KEYS]
        lines += write_class_lists(args.class_lists_dir, out)
    print("\n".join(lines))
    if out["legs"] != "0":
        print(f"legs: {out['legs']} chosen ({out['test_minutes']} weighted test minutes)")
    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8", newline="\n") as fh:
            fh.write("\n".join(lines) + "\n")
    return 0


_CS = _load_ci_select()

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
