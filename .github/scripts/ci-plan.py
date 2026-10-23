#!/usr/bin/env python3
"""The plan job of the pull-request workflow pr.yml (#6656): what a run does, from the event and the changed files.

Inputs: the event (pull_request or push), the base branch, the head branch, the draft flag, the labels and the
changed file list. Outputs, as `key=value` lines (stdout, and $GITHUB_OUTPUT when --github-output is given):

  run              true when the run does any work; false for a draft or a merge-train PR
  outcome          run | skip-draft | train-blocked  (train-blocked: the verdict job must fail, see below)
  why              one plain sentence for the job log
  areas            docs-only | darling | lite | all | none
  darling_scope    full | reads-lite | none   (reads-lite: the Stage=Guard classes plus the classes that read a Lite file)
  lite_scope       full | reads | none        (reads: the Lite classes that read a Darling file)
  build            true when the shared test build is needed (any run that is not docs-only)
  test_minutes     estimated test time of the selected classes, in minutes
  legs             a pull request: N = ceil(test_minutes / MINUTES_PER_LEG), at most MAX_LEGS; a dev push: DEV_PUSH_LEGS;
                   0 when nothing runs
  legs_json        [1, 2, ..., N], for the test job's matrix
  actionlint       true when a workflow file changed
  check_web_js     true when a dashboard JavaScript file changed
  deprecated_tests true when a file under deprecated/ changed (the Installer and Dashboard tests run)

The pattern sets are NOT copied here. The areas come from ci-select.py's Rules, which reads the docs allowlist and the
area filters from the same files dorny/paths-filter reads, and the Lite-reads classes from ci-select.py's
scan_classes (Reads=Lite and Reads=Darling traits, derived from the test sources).

Rules, in order:
  1. Any event that is not a pull request always runs. (PR 1 of #6656 gated pull requests on a label; PR 2 removed it.)
  2. A `train/` branch runs, draft or not (a merge-train pull request exists only to run CI).
  3. A `merge-train` label does not run; the verdict job turns that into a failed `CI result` (today's rule: only an
     admin merges it, after the train covered it).
  4. A draft does not run and gets no status; marking it ready starts the run.
  5. Areas: base other than dev, a push, a path no area knows, an empty list, or a list the API may have cut: all.
     Only documentation: docs-only. Otherwise Darling files give the Darling suite, Lite files the Lite suite, and
     each side keeps its cross-reading classes of the other.

Standard library only. Run: python -I .github/scripts/ci-plan.py --event pull_request --base dev --files-file FILE
"""
from __future__ import annotations

import argparse
import importlib.util
import json
import math
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.normpath(os.path.join(HERE, "..", ".."))

TRAIN_LABEL = "merge-train"
TRAIN_BRANCH_PREFIX = "train/"
# Legs. A pull request is cut for a short wait: MINUTES_PER_LEG test minutes a leg, so a full all-areas run (about 131
# weighted minutes, from run 40001582504's per-class times) is 16 legs. A leg also pays about 1.3 minutes that are not tests,
# measured on that run's 9 legs (checkout, build download, .NET and PostgreSQL start: 52 to 103 s, mean 70 s, plus about 7 s
# of post steps), so a leg is about 8.5 + 1.3 = 9.8 minutes. A dev push waits for nobody and the free plan runs about 20
# jobs at once across the repo, so a push is a fixed 4 legs and pull requests keep the runners.
MINUTES_PER_LEG = 8.5
MAX_LEGS = 16
DEV_PUSH_LEGS = 4
# The GitHub pull request files API returns at most 3000 files; a list that long may have been cut short.
FILES_API_CAP = 3000

# Weights are wall-seconds per class (darling-shard-weights.json). A class a table does not know weighs the mean.
DARLING_WEIGHTS = os.path.join(HERE, "darling-shard-weights.json")
LITE_WEIGHTS = os.path.join(HERE, "lite-shard-weights.json")
# Lite's estimate when lite-shard-weights.json is missing (it is new in #6656): the full suite, and the reads subset.
LITE_FULL_FALLBACK_MINUTES = 28.0
LITE_READS_FALLBACK_MINUTES = 3.0

WEB_JS_MARKER = "/wwwroot/js/"


def _load_ci_select():
    spec = importlib.util.spec_from_file_location("ci_select", os.path.join(HERE, "ci-select.py"))
    mod = importlib.util.module_from_spec(spec)
    sys.modules["ci_select"] = mod
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------------------------------------------------
# The gate: does this run do any work?
# ---------------------------------------------------------------------------------------------------------
def gate(event: str, head_ref: str, draft: bool, labels: "list[str]") -> "tuple[str, str]":
    """(outcome, why). outcome is run, skip-draft or train-blocked."""
    if event != "pull_request":
        return "run", f"a {event} event always runs."
    if head_ref.startswith(TRAIN_BRANCH_PREFIX):
        return "run", "a merge-train pull request exists to run CI."
    if TRAIN_LABEL in labels:
        return "train-blocked", "the pull request carries the merge-train label, so its train run already covered it."
    if draft:
        return "skip-draft", "the pull request is a draft. Marking it ready for review starts the run."
    return "run", "a pull request that is ready for review runs."


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


def classify(rules, event: str, base: str, files: "list[str]") -> dict:
    """The areas of one change: {areas, darling_scope, lite_scope, why}."""
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
    area_names = ("root", "core", "installer_core", "dashboard", "lite", "installer", "darling")
    if a["all_count"] == a["docs_count"] and not any(a[n] for n in area_names):
        return {"areas": "docs-only", "darling_scope": "none", "lite_scope": "none",
                "reason": "only documentation changed."}

    cs = _CS
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
    return {"areas": areas, "darling_scope": darling_scope, "lite_scope": lite_mode,
            "reason": f"Darling scope {darling_scope}, Lite scope {lite_mode}."}


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
    return {"classes": _CS.scan_classes(root), "darling_weights": load_weights(DARLING_WEIGHTS),
            "lite_weights": load_weights(LITE_WEIGHTS)}


def _selected(classes: "dict[str, set[str]]", scope: str, reads_trait: str) -> "list[str]":
    runs = [c for c, t in classes.items() if _CS.NO_TESTS_TRAIT not in t]
    if scope == "full":
        return runs
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


def estimate(catalog: dict, darling_scope: str, lite_scope: str) -> "tuple[float, float]":
    """(Darling minutes, Lite minutes) of the selected classes."""
    d = _selected(catalog["classes"]["darling"], darling_scope, "Reads=Lite")
    lt = _selected(catalog["classes"]["lite"], lite_scope, "Reads=Darling")
    fallback = LITE_FULL_FALLBACK_MINUTES if lite_scope == "full" else LITE_READS_FALLBACK_MINUTES
    return (suite_minutes(d, catalog["darling_weights"], 0.0),
            suite_minutes(lt, catalog["lite_weights"], fallback))


def leg_count(test_minutes: float, event: str = "pull_request") -> int:
    if test_minutes <= 0:
        return 0
    if event != "pull_request":
        return DEV_PUSH_LEGS
    return min(MAX_LEGS, max(1, math.ceil(test_minutes / MINUTES_PER_LEG)))


# ---------------------------------------------------------------------------------------------------------
# The plan.
# ---------------------------------------------------------------------------------------------------------
def plan(event: str, base: str, head_ref: str, draft: bool, labels: "list[str]", files: "list[str]",
         rules, catalog: "dict | None" = None) -> "dict[str, str]":
    outcome, why = gate(event, head_ref, draft, labels)
    out = {"run": "true" if outcome == "run" else "false", "outcome": outcome, "why": why,
           "areas": "none", "darling_scope": "none", "lite_scope": "none", "build": "false",
           "test_minutes": "0", "legs": "0", "legs_json": "[]",
           "actionlint": "false", "check_web_js": "false", "deprecated_tests": "false"}
    if outcome != "run":
        return out

    c = classify(rules, event, base, files)
    out.update(areas=c["areas"], darling_scope=c["darling_scope"], lite_scope=c["lite_scope"])
    out["why"] = f"{why} {c['reason']}"
    out["build"] = "false" if c["areas"] == "docs-only" else "true"
    out["actionlint"] = _bool(any(f.startswith(".github/workflows/") for f in files))
    out["check_web_js"] = _bool(any(WEB_JS_MARKER in "/" + f for f in files))
    out["deprecated_tests"] = _bool(any(f.startswith("deprecated/") for f in files))

    if out["build"] == "true":
        d_min, l_min = estimate(catalog if catalog is not None else load_catalog(), c["darling_scope"], c["lite_scope"])
        total = d_min + l_min
        n = leg_count(total, event)
        out["test_minutes"] = f"{total:.1f}"
        out["legs"] = str(n)
        out["legs_json"] = json.dumps(list(range(1, n + 1)), separators=(",", ":"))
    return out


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


def main(argv: "list[str]") -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("--event", required=True, choices=["pull_request", "push"])
    ap.add_argument("--base", default="dev", help="the base branch of a pull request; the pushed branch for a push")
    ap.add_argument("--head-ref", default="", help="the head branch of a pull request")
    ap.add_argument("--draft", default="false")
    ap.add_argument("--labels", default="", help="a JSON array or a comma list")
    ap.add_argument("--files-file", help="changed files, one a line (default: stdin)")
    ap.add_argument("--github-output", help="also append the outputs to this file ($GITHUB_OUTPUT)")
    args = ap.parse_args(argv)

    if args.files_file:
        with open(args.files_file, encoding="utf-8") as fh:
            text = fh.read()
    else:
        text = sys.stdin.read()
    files = [ln.strip() for ln in text.splitlines() if ln.strip()]

    out = plan(args.event, args.base, args.head_ref, parse_bool(args.draft), parse_labels(args.labels), files,
               _CS.Rules())
    lines = [f"{k}={v}" for k, v in out.items()]
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
