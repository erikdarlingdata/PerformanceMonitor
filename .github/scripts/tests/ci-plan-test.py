#!/usr/bin/env python3
"""Checks for ci-plan.py (#6656). Run: python -I .github/scripts/tests/ci-plan-test.py

The decision logic runs against small stand-in filter files (written to a temp dir) and a stand-in class catalog, so
an edit to the real filters cannot move these answers. The last block runs the real filters on three stable paths."""
import contextlib
import importlib.util
import io
import json
import os
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("cp", os.path.join(here, "..", "ci-plan.py"))
cp = importlib.util.module_from_spec(spec)
sys.modules["cp"] = cp
spec.loader.exec_module(cp)
cs = cp._CS

failures = []


def check(name, actual, expected):
    if actual != expected:
        failures.append(f"{name}: expected {expected!r}, got {actual!r}")


WORKFLOW = """\
jobs:
  build:
    steps:
      - uses: dorny/paths-filter
        with:
          filters: |
            root:
              - 'global.json'
            core:
              - 'PerformanceMonitor.Common/**/!(*.md)'
            lite:
              - 'Lite/**/!(*.md)'
            darling:
              - 'Darling/**/!(*.md)'
              - 'Lite/**/*.xaml'
              - 'Lite/**/*.cs'
            darling_full:
              - 'Darling/**/!(*.md)'
            docs:
              - '**/*.md'
              - 'LICENSE'
              - 'docs/**/*.{md,png}'
            all:
              - '**'
"""
TREE_FILTER = """\nworkflow:
  - '.github/workflows/build.yml'
lite_tree:
  - 'Lite/**/!(*.md)'
"""
DARLING_FILTER = """\
darling:
  - 'Darling/**/!(*.md)'
  - 'PerformanceMonitor.Common/**/!(*.md)'
"""
LITE_FILTER = """\
root_shard:
  - 'global.json'
core_shard:
  - 'PerformanceMonitor.Common/**/!(*.md)'
lite_shard:
  - 'Lite/**/!(*.md)'
  - 'Lite.Tests/**/!(*.md)'
darling_reads_shard:
  - 'Darling/Darling.Tests/**/!(*.md)'
  - 'Darling/PerformanceMonitor.Darling.Service/**/!(*.md)'
lite_linked_shard:
  - 'Darling/Darling.Tests/Probe.cs'
"""
tmp = tempfile.mkdtemp()
paths = {}
for name, text in (("build.yml", WORKFLOW), ("darling.yml", DARLING_FILTER), ("lite.yml", LITE_FILTER), ("tree.yml", TREE_FILTER)):
    paths[name] = os.path.join(tmp, name)
    with open(paths[name], "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
rules = cs.Rules(paths["build.yml"], paths["darling.yml"], paths["lite.yml"], paths["tree.yml"])


def classes(names, reads=None, guard=()):
    out = {}
    for n in names:
        t = set()
        if reads and n in reads[0]:
            t.add(reads[1])
        if n in guard:
            t.add("Stage=Guard")
        out[n] = t
    return out


# Darling: 4 classes of 60 s each (4 min); one Guard, one reads Lite, one helper that holds no test.
d_cls = classes(["D1", "D2", "D3", "D4"], reads=({"D2"}, "Reads=Lite"), guard={"D1"})
d_cls["Helper"] = {cs.NO_TESTS_TRAIT}
# Lite: 3 classes of 600 s each (30 min); one reads Darling.
l_cls = classes(["L1", "L2", "L3"], reads=({"L3"}, "Reads=Darling"))
catalog = {"classes": {"darling": d_cls, "lite": l_cls},
           "darling_weights": {"D1": 60, "D2": 60, "D3": 60, "D4": 60},
           "lite_weights": {"L1": 600, "L2": 600, "L3": 600}}
big = {"classes": {"darling": classes([f"B{i}" for i in range(100)]), "lite": l_cls},
       "darling_weights": {f"B{i}": 600 for i in range(100)}, "lite_weights": {"L1": 600, "L2": 600, "L3": 600}}


SERVICE = "Darling/PerformanceMonitor.Darling.Service/X.cs"


def run(files, event="pull_request", base="dev", head="feature/x", labels=(), cat=catalog):
    return cp.plan(event, base, head, list(labels), files, rules, cat)


# --- the gate -------------------------------------------------------------------------------------------------
check("gate: a PR without labels runs", run(["README.md"], labels=())["outcome"], "run")
check("gate: an unrelated label runs", run(["README.md"], labels=("bug",))["outcome"], "run")
check("gate: no label, tests planned", run([SERVICE], labels=())["legs"], "2")
check("gate: draft runs", run(["Darling/X.cs"])["outcome"], "run")
check("gate: draft is run", run(["Darling/X.cs"])["run"], "true")
check("gate: train label", run(["Darling/X.cs"], labels=("merge-train",))["outcome"], "train-blocked")
check("gate: train label is not run", run(["Darling/X.cs"], labels=("merge-train",))["run"], "false")
check("gate: train branch runs", run(["Darling/X.cs"], head="train/2026-10-23")["outcome"], "run")
check("gate: train branch runs despite the train label",
      run(["Darling/X.cs"], head="train/a", labels=("merge-train",))["outcome"], "run")
check("gate: a dev push ignores the label", run(["README.md"], event="push", labels=())["outcome"], "run")

# --- the areas -------------------------------------------------------------------------------------------------
r = run(["README.md", "docs/a.png"])
check("docs-only: areas", r["areas"], "docs-only")
check("docs-only: no build", r["build"], "false")
check("docs-only: no legs", r["legs"], "0")
check("docs-only: scopes", (r["darling_scope"], r["lite_scope"]), ("none", "none"))

r = run([SERVICE])
check("darling: areas", r["areas"], "darling")
check("darling: scopes", (r["darling_scope"], r["lite_scope"]), ("full", "reads"))
check("darling: build", r["build"], "true")
check("darling: minutes (4 darling + the 10 min Lite reads class)", r["test_minutes"], "14.0")
check("darling: legs", (r["legs"], r["legs_json"]), ("2", "[1,2]"))

r = run(["Lite/Services/X.cs"])
check("lite: areas", r["areas"], "lite")
check("lite: scopes", (r["darling_scope"], r["lite_scope"]), ("reads-lite", "full"))
check("lite: minutes (Guard + reads-lite Darling = 2, Lite 30)", r["test_minutes"], "32.0")
check("lite: legs (32 minutes / 8.5)", r["legs"], "4")

r = run(["Lite/Controls/X.xaml", "README.md"])
check("lite xaml with docs: scopes", (r["darling_scope"], r["lite_scope"]), ("reads-lite", "full"))

r = run(["Darling/X.cs", "Lite/X.cs"])
check("darling + lite: all", r["areas"], "all")
check("darling + lite: minutes", r["test_minutes"], "34.0")

r = run(["PerformanceMonitor.Common/X.cs"])
check("shared library: all", r["areas"], "all")

r = run(["global.json"])
check("root build file: all", r["areas"], "all")

# --- all areas -------------------------------------------------------------------------------------------------
r = run(["tools/odd.txt"])
check("unknown path: all", (r["areas"], r["darling_scope"], r["lite_scope"]), ("all", "full", "full"))
check("unknown path among known ones: all", run(["README.md", "tools/odd.txt"])["areas"], "all")
check("unknown path says which", "tools/odd.txt" in run(["tools/odd.txt"])["why"], True)
check("base main: all", run(["README.md"], base="main")["areas"], "all")
check("base main: builds", run(["README.md"], base="main")["build"], "true")
check("base release branch: all", run(["README.md"], base="release/3.10")["areas"], "all")
check("dev push: all", run(["README.md"], event="push", base="dev")["areas"], "all")
check("dev push of Lite files: all", run(["Lite/X.cs"], event="push")["areas"], "all")
check("empty file list: all", run([])["areas"], "all")
check("a list as long as the API cap: all", run(["README.md"] * cp.FILES_API_CAP)["areas"], "all")
check("a list under the cap: not widened", run(["README.md"] * (cp.FILES_API_CAP - 1))["areas"], "docs-only")

# --- the leg count ---------------------------------------------------------------------------------------------
check("legs: 0 minutes", cp.leg_count(0), 0)
check("legs: 1 second", cp.leg_count(0.02), 1)
check("legs: exactly one leg of minutes", cp.leg_count(cp.MINUTES_PER_LEG), 1)
check("legs: just over one leg of minutes", cp.leg_count(cp.MINUTES_PER_LEG + 0.1), 2)
check("legs: 131 weighted minutes, a full run, is 16", cp.leg_count(131), 16)
check("legs: capped at the PR maximum", cp.leg_count(500), cp.MAX_LEGS)
check("legs: the PR maximum is 16", cp.MAX_LEGS, 16)
check("legs: a small push gets the floor of 6", cp.leg_count(0.02, "push"), 6)
check("legs: a push just over the floor follows the estimate", cp.leg_count(cp.MINUTES_PER_LEG * 7 - 0.1, "push"), 7)
check("legs: a push of 131 minutes is 10", cp.leg_count(131, "push"), 10)
check("legs: a push is capped at 10", cp.leg_count(500, "push"), cp.DEV_PUSH_MAX_LEGS)
check("legs: the push maximum is 10", cp.DEV_PUSH_MAX_LEGS, 10)
check("legs: a push with no tests is 0", cp.leg_count(0, "push"), 0)

r = run(["Darling/X.cs"], cat=big)
check("big suite: capped legs", (r["legs"], r["legs_json"]), ("16", json.dumps(list(range(1, 17)), separators=(",", ":"))))
full = {"classes": {"darling": classes([f"F{i}" for i in range(131)]), "lite": l_cls},
        "darling_weights": {f"F{i}": 60 for i in range(131)}, "lite_weights": {"L1": 0, "L2": 0, "L3": 0}}
check("full all-areas PR run: 16 legs", run(["tools/odd.txt"], cat=full)["legs"], "16")
check("same change as a dev push: 10 legs", run(["tools/odd.txt"], event="push", cat=full)["legs"], "10")
check("a dev push's legs_json", run(["README.md"], event="push", cat=full)["legs_json"], json.dumps(list(range(1, 11)), separators=(",", ":")))

# an unknown class weighs the mean; a missing Lite table falls back to the stand-in constants
mean_cat = {"classes": {"darling": classes(["D1", "D2", "New"]), "lite": {}},
            "darling_weights": {"D1": 60, "D2": 180}, "lite_weights": {}}
check("unweighted class weighs the mean", cp.estimate(mean_cat, "full", "none")[0], (60 + 180 + 120) / 60)
no_lite = {"classes": {"darling": {}, "lite": l_cls}, "darling_weights": {}, "lite_weights": {}}
check("missing Lite weights: full fallback", cp.estimate(no_lite, "none", "full")[1], cp.LITE_FULL_FALLBACK_MINUTES)
check("missing Lite weights: reads fallback", cp.estimate(no_lite, "none", "reads")[1], cp.LITE_READS_FALLBACK_MINUTES)
check("missing Lite weights: nothing selected, nothing owed", cp.estimate(no_lite, "none", "none"), (0.0, 0.0))

# --- the tool checks -------------------------------------------------------------------------------------------
check("actionlint: a workflow changed", run([".github/workflows/pr.yml"])["actionlint"], "true")
check("actionlint: not otherwise", run(["Darling/X.cs"])["actionlint"], "false")
check("actionlint: a script beside the workflows", run([".github/scripts/x.py"])["actionlint"], "false")
js = "Darling/PerformanceMonitor.Darling.Service/wwwroot/js/app.js"
check("web js: changed", run([js])["check_web_js"], "true")
check("web js: other Service file", run(["Darling/PerformanceMonitor.Darling.Service/X.cs"])["check_web_js"], "false")
check("web js: a root-level wwwroot/js path", run(["wwwroot/js/a.js"])["check_web_js"], "true")
check("web js: not on a skipped run", run([js], labels=("merge-train",))["check_web_js"], "false")
check("actionlint: not on a skipped run", run([".github/workflows/pr.yml"], labels=("merge-train",))["actionlint"], "false")
check("pg old targets: a collector file", run(["PerformanceMonitor.Collectors/PgVersionSql.cs"])["pg_old_targets"], "true")
check("pg old targets: the catalog", run(["PerformanceMonitor.Collectors/CollectorCatalog.cs"])["pg_old_targets"], "true")
check("pg old targets: the fetch script", run(["Darling/tools/fetch-pg-target.ps1"])["pg_old_targets"], "true")
check("pg old targets: the start script", run([".github/scripts/start-test-pg.ps1"])["pg_old_targets"], "true")
check("pg old targets: the test class", run(["Darling/Darling.Tests/PgCollectorsOnOldTargetsLiveTests.cs"])["pg_old_targets"], "true")
check("pg old targets: a SQL Server collector", run(["PerformanceMonitor.Collectors/WaitStatsCollector.cs"])["pg_old_targets"], "false")
check("pg old targets: not on a push", run(["PerformanceMonitor.Collectors/PgVersionSql.cs"], event="push")["pg_old_targets"], "false")
check("pg old targets: not on a skipped run", run(["PerformanceMonitor.Collectors/PgVersionSql.cs"], labels=("merge-train",))["pg_old_targets"], "false")

# --- parsing ---------------------------------------------------------------------------------------------------
check("labels: JSON", cp.parse_labels('["ci-next", "bug"]'), ["ci-next", "bug"])
check("labels: comma list", cp.parse_labels("ci-next, bug"), ["ci-next", "bug"])
check("labels: empty", cp.parse_labels(""), [])
check("labels: empty array", cp.parse_labels("[]"), [])

# --- the changed-classes plan (#6734) ---------------------------------------------------------------------------
DT = "Darling/Darling.Tests/"
LT = "Lite.Tests/"
cc_files = {DT + "D1Tests.cs": {"suite": "darling", "classes": ["D1"], "tests": ["D1"], "safe": True},
            DT + "D3Tests.cs": {"suite": "darling", "classes": ["D3", "D4"], "tests": ["D3", "D4"], "safe": True},
            DT + "Helpers.cs": {"suite": "darling", "classes": ["Helper"], "tests": [], "safe": False},
            LT + "L1Tests.cs": {"suite": "lite", "classes": ["L1"], "tests": ["L1"], "safe": True},
            LT + "L2Tests.cs": {"suite": "lite", "classes": ["L2"], "tests": ["L2"], "safe": True}}
for i in range(14):
    cc_files[DT + f"B{i}Tests.cs"] = {"suite": "darling", "classes": [f"B{i}"], "tests": [f"B{i}"], "safe": True}
cc_cat = {**catalog, "files": cc_files}
big_cc = {**big, "files": cc_files}


def cc(files, cat=cc_cat):
    return run(files, cat=cat)


r = cc(["README.md", DT + "D1Tests.cs"])
check("classes: docs + one test class: areas", r["areas"], "classes")
check("classes: docs + one test class: scope", (r["darling_scope"], r["lite_scope"]), ("classes", "none"))
check("classes: docs + one test class: the class", r["darling_classes"], "D1")
check("classes: docs + one test class: build and legs", (r["build"], r["legs"], r["legs_json"]), ("true", "1", "[1]"))
check("classes: a file with two classes", cc([DT + "D3Tests.cs"])["darling_classes"], "D3,D4")
check("classes: a file with two classes runs one leg", cc([DT + "D3Tests.cs"])["legs"], "1")
r = cc(["README.md"] + [DT + f"B{i}Tests.cs" for i in range(14)], big_cc)
check("classes: docs + 14 test classes: scope", (r["areas"], r["darling_scope"]), ("classes", "classes"))
check("classes: 14 classes of 10 min: legs", r["legs"], "14")
check("classes: 14 classes named", r["darling_classes"], ",".join(f"B{i}" for i in range(14)))
check("classes: a helper beside a class is full", cc([DT + "D1Tests.cs", DT + "Helpers.cs"])["darling_scope"], "full")
check("classes: a helper beside a class is areas darling", cc([DT + "D1Tests.cs", DT + "Helpers.cs"])["areas"], "darling")
for odd in (DT + "Darling.Tests.csproj", DT + "packages.lock.json", DT + "Unmapped.cs"):
    r = cc(["README.md", DT + "D1Tests.cs", odd])
    check(f"classes: {odd} is full", (r["areas"], r["darling_scope"], r["darling_classes"]), ("darling", "full", ""))
r = cc([DT + "D1Tests.cs", SERVICE])
check("classes: product file keeps today's rules", (r["areas"], r["darling_scope"], r["darling_classes"]), ("darling", "full", ""))
r = cc([DT + "D1Tests.cs", "PerformanceMonitor.Common/X.cs"])
check("classes: core file keeps today's rules", (r["areas"], r["darling_scope"]), ("all", "full"))
check("classes: a class missing from the map is full", cc([DT + "Missing.cs"])["darling_scope"], "full")
check("classes: no file table at all is full", run([DT + "D1Tests.cs"], cat=catalog)["darling_scope"], "full")
check("classes: docs only", cc(["README.md"])["areas"], "docs-only")
check("classes: docs only builds nothing", (cc(["README.md"])["build"], cc(["README.md"])["legs"]), ("false", "0"))
r = cc(["README.md", LT + "L1Tests.cs"])
check("classes lite: scope", (r["areas"], r["darling_scope"], r["lite_scope"], r["lite_classes"]), ("classes", "none", "classes", "L1"))
check("classes lite: build and legs", (r["build"], r["legs"]), ("true", "1"))
check("classes lite: two classes", cc([LT + "L1Tests.cs", LT + "L2Tests.cs"])["lite_classes"], "L1,L2")
r = cc([LT + "L1Tests.cs", DT + "D1Tests.cs"])
check("classes both: scopes", (r["areas"], r["darling_scope"], r["lite_scope"]), ("classes", "classes", "classes"))
check("classes lite: csproj is full", cc([LT + "L1Tests.cs", LT + "Lite.Tests.csproj"])["lite_scope"], "full")
check("classes lite: lock file is full", cc([LT + "L1Tests.cs", LT + "packages.lock.json"])["lite_scope"], "full")
check("classes lite: missing from the map is full", cc([LT + "Gone.cs"])["lite_scope"], "full")
check("classes lite: product file keeps today's rules", cc([LT + "L1Tests.cs", "Lite/X.cs"])["lite_scope"], "full")
check("classes: a push is never narrowed", run([DT + "D1Tests.cs"], event="push", base="dev", cat=cc_cat)["areas"], "all")
check("classes: a non-dev base is never narrowed", run([DT + "D1Tests.cs"], base="main", cat=cc_cat)["areas"], "all")

# A same-file helper (no test) stays safe but is never picked: the test host's class listing may not name it (#6734).
with tempfile.TemporaryDirectory() as hroot:
    hdir = os.path.join(hroot, "Darling", "Darling.Tests")
    os.makedirs(hdir)
    with open(os.path.join(hdir, "WithHelperTests.cs"), "w", encoding="utf-8") as fh:
        fh.write("namespace N;\npublic class WithHelperTests\n{\n    [Fact]\n    public void A() { new FakeBox(); }\n}\n"
                 "internal class FakeBox\n{\n}\n")
    h_tbl = cp._file_table(hroot, cs.scan_classes(hroot))
    h_row = h_tbl[DT + "WithHelperTests.cs"]
    check("helper file: both classes recorded", h_row["classes"], ["WithHelperTests", "FakeBox"])
    check("helper file: only the test class is a test", (h_row["tests"], h_row["safe"]), (["WithHelperTests"], True))
    h_cat = {**catalog, "files": h_tbl}
    r = run(["README.md", DT + "WithHelperTests.cs"], cat=h_cat)
    check("helper file: the picks name only the test class", (r["areas"], r["darling_classes"]), ("classes", "WithHelperTests"))

# The real file table: a class file maps to its classes, a helper and a missing map do not narrow.
real_cat = cp.load_catalog()
# --- a draft narrows the scope (#7727) -------------------------------------------------------------------------
def draft_run(files, draft=True, event="pull_request", base="dev", **kw):
    """The plan of a draft (or, with draft=False, of a pull request that is ready). A plan that has no draft input answers a draft
    with nothing, so its checks fail."""
    try:
        return cp.plan(event, base, "feature/x", [], files, rules, kw.get("cat", cc_cat), draft=draft)
    except TypeError:  # a plan with no draft input: a ready pull request still plans as before, a draft has no answer
        return {} if draft else cp.plan(event, base, "feature/x", [], files, rules, kw.get("cat", cc_cat))


LITE_SRC = "Lite/Services/X.cs"
r = draft_run([SERVICE, DT + "D3Tests.cs"])
check("draft: product file + changed test file: areas", r.get("areas"), "classes")
check("draft: product file + changed test file: scope", (r.get("darling_scope"), r.get("lite_scope")), ("classes", "none"))
check("draft: product file + changed test file: the class and the Guard classes", r.get("darling_classes"), "D3,D4,D1")
check("draft: product file + changed test file: minutes and legs", (r.get("test_minutes"), r.get("legs")), ("3.0", "1"))
check("draft: the why line names the rule", "a draft runs only its own test classes and the Guard classes" in r.get("why", ""), True)
r = draft_run([SERVICE])
check("draft: product file only: Guard classes only", (r.get("areas"), r.get("darling_scope"), r.get("darling_classes")), ("classes", "classes", "D1"))
check("draft: product file only: no Lite", (r.get("lite_scope"), r.get("lite_classes"), r.get("build")), ("none", "", "true"))
r = draft_run([SERVICE, DT + "D1Tests.cs"])
check("draft: a changed Guard class is picked once", r.get("darling_classes"), "D1")
r = draft_run([SERVICE, LITE_SRC, DT + "D3Tests.cs", LT + "L2Tests.cs"])
check("draft: both suites: classes", (r.get("darling_classes"), r.get("lite_classes")), ("D3,D4,D1", "L2"))
check("draft: both suites: scope", (r.get("areas"), r.get("darling_scope"), r.get("lite_scope")), ("classes", "classes", "classes"))
r = draft_run([LITE_SRC])
check("draft: a Lite-only product file: Darling Guard, no Lite", (r.get("areas"), r.get("darling_classes"), r.get("lite_scope")), ("classes", "D1", "none"))
r = draft_run([LITE_SRC, LT + "L1Tests.cs"])
check("draft: a Lite product file + Lite test: both", (r.get("darling_classes"), r.get("lite_classes")), ("D1", "L1"))
# A changed test file that is not safe: the whole pull request runs as it does today, and the why line says so.
r = draft_run([SERVICE, DT + "Helpers.cs"])
check("draft: an unsafe test file: full", (r.get("areas"), r.get("darling_scope")), ("darling", "full"))
check("draft: an unsafe test file: the why line", "Helpers.cs is not a plain test class file" in r.get("why", ""), True)
r = draft_run([SERVICE, DT + "NoSuchTests.cs"])
check("draft: a test file the map does not know: full", r.get("darling_scope"), "full")
r = draft_run([SERVICE, DT + "Probe.csproj"])
check("draft: a .csproj in the test tree: full", r.get("darling_scope"), "full")
# The same files on a pull request that is ready for review: as today.
for files in ([SERVICE, DT + "D3Tests.cs"], [SERVICE]):
    r = draft_run(files, draft=False)
    check(f"ready: {len(files)} file(s): full", (r.get("areas"), r.get("darling_scope"), r.get("darling_classes")), ("darling", "full", ""))
check("ready: the why line has no draft rule", "draft runs only" in draft_run([SERVICE], draft=False).get("why", ""), False)
# Docs-only, test-only, a dev push and a base other than dev: as today, draft or not.
for flag in (True, False):
    check(f"draft={flag}: docs-only", draft_run(["README.md"], draft=flag).get("areas"), "docs-only")
    r = draft_run(["README.md", DT + "D3Tests.cs"], draft=flag)
    check(f"draft={flag}: test-only has no Guard classes", (r.get("areas"), r.get("darling_classes")), ("classes", "D3,D4"))
    check(f"draft={flag}: a dev push is every area", draft_run([SERVICE], draft=flag, event="push").get("darling_scope"), "full")
    check(f"draft={flag}: a base other than dev is every area",
          draft_run([SERVICE], draft=flag, base="main").get("areas"), "all")
    check(f"draft={flag}: a path no area knows is every area",
          draft_run([SERVICE, "no-such-area/odd.bin"], draft=flag).get("areas"), "all")
check("draft: parse_bool", [cp.parse_bool(x) for x in ("true", "True", "false", "", "no")] if hasattr(cp, "parse_bool") else None,
      [True, True, False, False, False])

tbl = real_cat["files"]
one = next(f for f, e in sorted(tbl.items()) if e["suite"] == "darling" and e["safe"] and len(e["classes"]) == 1)
r = cp.plan("pull_request", "dev", "x", [], ["README.md", one], live_rules := cs.Rules(), real_cat)
check("real table: one class file narrows", (r["areas"], r["darling_classes"]), ("classes", tbl[one]["classes"][0]))
check("real table: every row has a class", all(e["classes"] for e in tbl.values()), True)
no_test = {(s, c) for s, m in real_cat["classes"].items() for c, t in m.items() if cs.NO_TESTS_TRAIT in t}
check("real table: no picked name is a class without tests",
      sorted((e["suite"], c) for e in tbl.values() for c in e["tests"] if (e["suite"], c) in no_test), [])
check("real table: some file has a helper beside its tests", any(set(e["classes"]) - set(e["tests"]) and e["tests"] for e in tbl.values()), True)
check("real table: every safe file picks a class", all(e["tests"] for e in tbl.values() if e["safe"]), True)
unsafe = [f for f, e in tbl.items() if not e["safe"]]
check("real table: some helper files are unsafe", bool(unsafe), True)
r = cp.plan("pull_request", "dev", "x", [], ["README.md", one, unsafe[0]], live_rules, real_cat)
check("real table: an unsafe file is full", r["darling_scope"], "full")

# --- a ready pull request picks its classes from the nightly test map (#7728) ----------------------------------
import datetime as _dt

NOW = _dt.datetime(2026, 11, 5, 12, 0, tzinfo=_dt.timezone.utc)
SVC_Y = "Darling/PerformanceMonitor.Darling.Service/Y.cs"
MAP_FILES = [SERVICE, SVC_Y, LITE_SRC, DT + "D1Tests.cs", DT + "D3Tests.cs", LT + "L1Tests.cs", LT + "L2Tests.cs",
             "PerformanceMonitor.Common/Shared.cs"]


def fid(path):
    return MAP_FILES.index(path)


def make_map(built="2026-11-05T06:00:00Z", gone=False):
    """A stand-in nightly map: D2 covers SERVICE, D3 covers Y.cs, L2 covers LITE_SRC; D1 is the Guard class."""
    darling = {"D1": {"own": fid(DT + "D1Tests.cs"), "files": [], "seconds": 60.0},
               "D2": {"own": fid(DT + "D3Tests.cs"), "files": [fid(SERVICE)], "seconds": 60.0},
               "D3": {"own": fid(DT + "D3Tests.cs"), "files": [fid(SVC_Y)], "seconds": 60.0},
               "D4": {"own": fid(DT + "D3Tests.cs"), "files": [], "seconds": 60.0}}
    if gone:  # a class the tree no longer has
        darling["Gone"] = {"own": fid(DT + "D3Tests.cs"), "files": [fid(SERVICE)], "seconds": 60.0}
    lite = {"L1": {"own": fid(LT + "L1Tests.cs"), "files": [], "seconds": 600.0},
            "L2": {"own": fid(LT + "L2Tests.cs"), "files": [fid(LITE_SRC)], "seconds": 600.0},
            "L3": {"own": fid(LT + "L2Tests.cs"), "files": [], "seconds": 600.0}}
    return {"schema": 1, "sha": "abc", "built_at": built, "tool": "ci-plan-test", "files": MAP_FILES,
            "classes": {"darling": darling, "lite": lite}, "text_patterns": {}}


def sel(**kw):
    return {"map": make_map(), "error": "", "now": NOW, "shared_diff": "", "named": None, **kw}


def ready_run(files, selection="default", draft=False, event="pull_request", base="dev", cat=cc_cat):
    """A plan with the map input. A plan that has no such input answers as it did before the map, so only the checks that need the
    map fail."""
    selection = sel() if selection == "default" else selection
    try:
        return cp.plan(event, base, "feature/x", [], files, rules, cat, draft=draft, selection=selection)
    except TypeError:
        return cp.plan(event, base, "feature/x", [], files, rules, cat, draft=draft)


r = ready_run([SERVICE])
check("map: a mapped product file selects its class and the Guard class", (r.get("areas"), r.get("darling_classes")), ("classes", "D1,D2"))
check("map: a mapped product file: Lite is not run", (r.get("darling_scope"), r.get("lite_scope"), r.get("lite_classes")), ("classes", "none", ""))
check("map: a mapped product file: minutes and legs", (r.get("test_minutes"), r.get("legs"), r.get("build")), ("2.0", "1", "true"))
check("map: the why line names the rule and the map's age",
      ("ready pull request runs the classes the nightly test map selects" in r.get("why", ""),
       "map built 2026-11-05 06:00 UTC, 6.0 hours old" in r.get("why", "")), (True, True))
r = ready_run([LITE_SRC])
check("map: a Lite product file: its Lite class and the Darling Guard class", (r.get("darling_classes"), r.get("lite_classes")), ("D1", "L2"))
check("map: a Lite product file: scopes", (r.get("areas"), r.get("darling_scope"), r.get("lite_scope")), ("classes", "classes", "classes"))
r = ready_run([SERVICE, DT + "D3Tests.cs"])
check("map: a changed test file's classes join the map's", r.get("darling_classes"), "D1,D2,D3,D4")
r = ready_run([SERVICE, DT + "D3Tests.cs", LT + "L1Tests.cs"])
check("map: a changed Lite test file's class joins", r.get("lite_classes"), "L1")
# A new test class the map has never seen runs, whatever the map holds.
new_cat = {**cc_cat, "classes": {"darling": {**d_cls, "NewOne": set()}, "lite": l_cls},
           "files": {**cc_files, DT + "NewOneTests.cs": {"suite": "darling", "classes": ["NewOne"], "tests": ["NewOne"], "safe": True}}}
r = ready_run([SERVICE, DT + "NewOneTests.cs"], cat=new_cat)
check("map: a new test file's class is picked", r.get("darling_classes"), "D1,D2,NewOne")
# A class the map holds and the tree no longer has is never named.
r = ready_run([SERVICE], selection=sel(map=make_map(gone=True)))
check("map: a class the tree lost is not named", r.get("darling_classes"), "D1,D2")
# Keep-full files and files the map does not know are every class of the areas.
for label, files in (("global.json", [SERVICE, "global.json"]), (".csproj", [SERVICE, "Darling/PerformanceMonitor.Darling.Service/X.csproj"])):
    r = ready_run(files)
    check(f"map: {label} is a keep-full file", (r.get("areas"), r.get("darling_scope")), ("all" if label == "global.json" else "darling", "full"))
    check(f"map: {label}: the why line", "is a keep-full file" in r.get("why", ""), True)
r = ready_run([SERVICE, "Darling/PerformanceMonitor.Darling.Service/NewFile.cs"])
check("map: an unknown product file is FULL", (r.get("areas"), r.get("darling_scope"), r.get("lite_scope")), ("darling", "full", "reads"))
check("map: an unknown product file: the why line", "unknown file: Darling/PerformanceMonitor.Darling.Service/NewFile.cs" in r.get("why", ""), True)
r = ready_run(["PerformanceMonitor.Common/Shared.cs"], selection=sel(shared_diff=None))
check("map: a shared-project file whose diff cannot be read is FULL", r.get("darling_scope"), "full")
# A stale map is FULL, a fresh one is not, and the why line says the age.
r = ready_run([SERVICE], selection=sel(map=make_map(built="2026-11-03T11:00:00Z")))   # 49 hours
check("map: a map 49 hours old is FULL", (r.get("areas"), r.get("darling_scope"), r.get("darling_classes")), ("darling", "full", ""))
check("map: a stale map: the why line", ("older than 2 days" in r.get("why", ""), "49.0 hours old" in r.get("why", "")), (True, True))
r = ready_run([SERVICE], selection=sel(map=make_map(built="2026-11-03T13:00:00Z")))   # 47 hours
check("map: a map 47 hours old still selects", (r.get("areas"), r.get("darling_classes")), ("classes", "D1,D2"))
r = ready_run([SERVICE], selection=sel(map=make_map(built="not a time")))
check("map: a map with no build time is FULL", r.get("darling_scope"), "full")
# A missing, unreadable or unconsulted map is FULL, with the reason in `why`.
r = ready_run([SERVICE], selection={"map": None, "error": "no nightly run has a test-map artifact", "now": NOW})
check("map: a missing map is FULL", (r.get("areas"), r.get("darling_scope"), r.get("darling_classes")), ("darling", "full", ""))
check("map: a missing map: the why line", "no nightly run has a test-map artifact" in r.get("why", ""), True)
check("map: a map that is not a map is FULL", ready_run([SERVICE], selection={"map": ["x"], "error": "", "now": NOW}).get("darling_scope"), "full")
check("map: a broken map never raises", ready_run([SERVICE], selection=sel(map={"schema": 1, "files": 5, "classes": 7})).get("darling_scope"), "full")
check("map: no selection input leaves the rule off", cp.plan("pull_request", "dev", "x", [], [SERVICE], rules, cc_cat)["darling_scope"], "full")
# #7734: a class list no longer has a length cap, because the legs read it from a file. 1,200 classes stay a selection.
big_map = make_map()
for i in range(1200):
    big_map["classes"]["darling"][f"Big{i}Tests"] = {"own": fid(DT + "D3Tests.cs"), "files": [fid(SERVICE)], "seconds": 1.0}
big_sel_cat = {**cc_cat, "classes": {"darling": {**d_cls, **{f"Big{i}Tests": set() for i in range(1200)}}, "lite": l_cls}}
r = ready_run([SERVICE], selection=sel(map=big_map), cat=big_sel_cat)
check("map: a 1,200 class selection stays a selection", (r.get("areas"), len(r.get("darling_classes", "").split(","))), ("classes", 1202))
check("map: no cap is named in the why line", "characters" in r.get("why", ""), False)
class_dir = os.path.join(tmp, "lists")
lines = cp.write_class_lists(class_dir, {"darling_classes": r["darling_classes"], "lite_classes": ""})
with open(os.path.join(class_dir, "darling-classes.txt"), encoding="utf-8") as fh:
    written_names = fh.read().splitlines()
check("class lists: the Darling file holds one name a line", (len(written_names), written_names[0]), (1202, r["darling_classes"].split(",")[0]))
check("class lists: an empty Lite list writes no file", os.path.exists(os.path.join(class_dir, "lite-classes.txt")), False)
check("class lists: the output lines say so", lines, ["class_lists=true", "darling_class_count=1202", "lite_class_count=0"])
check("class lists: nothing selected says false", cp.write_class_lists(os.path.join(tmp, "none"), {"darling_classes": "", "lite_classes": ""})[0], "class_lists=false")
# --- a draft also runs the classes whose source names a changed product file (#7734) --------------------------------
r = cp.plan("pull_request", "dev", "feature/x", [], [SERVICE], rules, cc_cat, draft=True, named={"darling": ["D3", "Gone"], "lite": ["L2"]})
check("draft named: the named class joins the Guard class, a class the tree lacks is dropped", r.get("darling_classes"), "D1,D3")
check("draft named: a named Lite class runs although no Lite file changed", (r.get("lite_scope"), r.get("lite_classes")), ("classes", "L2"))
check("draft named: the why line counts the classes that run, not the ones dropped", "2 classes whose source names a changed file" in r.get("why", ""), True)
# #7744: a named class the test host does not list (a private nested fixture) never reaches a plan, whatever the scope.
listed_cat = {**cc_cat, "listed": lambda suite, name: name != "D3"}
r = cp.plan("pull_request", "dev", "feature/x", [], [SERVICE], rules, listed_cat, draft=True, named={"darling": ["D3", "D2"], "lite": ["L2"]})
check("draft named: a class in the tree that the host does not list is dropped", r.get("darling_classes"), "D1,D2")
check("draft named: the why line counts only the listed ones", "2 classes whose source names a changed file" in r.get("why", ""), True)
r = cp.plan("pull_request", "dev", "feature/x", [], [SERVICE], rules, cc_cat, draft=True, named=None)
check("draft named: no named classes is the #7727 rule", (r.get("darling_classes"), "names a changed file" in r.get("why", "")), ("D1", False))
r = cp.plan("pull_request", "dev", "feature/x", [], [SERVICE], rules, cc_cat, draft=True, named_error="the scan failed")
check("draft named: a failed scan runs the pull request as a ready one", (r.get("areas"), r.get("darling_scope"), "the scan failed" in r.get("why", "")), ("darling", "full", True))
# classes_naming_files over a small tree: a path built from literals names the file, a comment does not, and a file name that
# too many classes name is matched by path only.
tree = os.path.join(tmp, "namedtree")
dt_dir = os.path.join(tree, "Darling", "Darling.Tests")
os.makedirs(dt_dir, exist_ok=True)
for fname, body in (("OneTests.cs", 'Path.Combine(root, "A", "Program.cs")'), ("TwoTests.cs", 'Path.Combine(root, "B", "Program.cs")'),
                    ("CommentTests.cs", "// reads A/Program.cs\n var x = 1;"), ("PathTests.cs", 'File.ReadAllText("PerformanceMonitor.Collectors/Foo.cs")')):
    with open(os.path.join(dt_dir, fname), "w", encoding="utf-8") as fh:
        fh.write("namespace Darling.Tests;\npublic class " + fname[:-3] + " {\n [Fact] public void T() { " + body + "; }\n}\n")
got, common = cs.classes_naming_files(["A/Program.cs"], tree, common=100)
check("naming: a file name names the classes that hold it in a literal, not a comment", (got["darling"], common), (["OneTests", "TwoTests"], []))
got, common = cs.classes_naming_files(["A/Program.cs"], tree, common=1)
check("naming: a common file name is matched by path", (got["darling"], common), (["OneTests"], ["Program.cs"]))
got, _ = cs.classes_naming_files(["PerformanceMonitor.Collectors/Foo.cs", "Darling/Darling.Tests/OneTests.cs", "README.md"], tree)
check("naming: a full path in a literal names the file, a test file and a document name nothing", got["darling"], ["PathTests"])
# --- the five shared projects are judged by the diff, like Common and PlanAnalysis (#7734) ------------------------
check("shared: all seven projects are shared projects", len(cs.SHARED_PROJECT_DIRS), 7)
live_args = cs.selection_args(cs.Rules())
check("shared: none of the core filter's projects is keep-full wholesale",
      [p for p in live_args["keep_full"] if p.startswith(cs.SHARED_PROJECT_DIRS)], [])
COLL = "PerformanceMonitor.Collectors/Foo.cs"
plain = "diff --git a/%s b/%s\n--- a/%s\n+++ b/%s\n@@ -1,1 +1,1 @@ class Foo\n-    var a = 1;\n+    var a = 2;\n" % (COLL, COLL, COLL, COLL)
const_diff = plain.replace("var a = 2;", "public const int Limit = 2;")
check("shared: an ordinary change in Collectors selects by the map", cs.shared_change_reason([COLL], plain), None)
check("shared: a const change in Collectors is FULL", "const" in (cs.shared_change_reason([COLL], const_diff) or ""), True)
check("shared: a Collectors change with no diff is FULL", cs.shared_change_reason([COLL], None) is not None, True)
check("shared: the Ui theme loader is a registration file",
      cs.shared_change_reason(["PerformanceMonitor.Ui/ThemeManager.cs"], plain) is not None, True)
# A draft keeps the #7727 rule, a dev push and a base other than dev run everything, docs-only and test-only are as before.
r = ready_run([SERVICE], draft=True)
check("map: a draft keeps its rule", (r.get("areas"), r.get("darling_classes"), "a draft runs only" in r.get("why", "")), ("classes", "D1", True))
check("map: a draft never uses the map", "nightly test map" in ready_run([SERVICE], draft=True).get("why", ""), False)
r = ready_run([SERVICE], event="push")
check("map: a dev push is FULL", (r.get("areas"), r.get("darling_scope"), r.get("lite_scope")), ("all", "full", "full"))
check("map: a base other than dev is FULL", ready_run([SERVICE], base="main").get("areas"), "all")
check("map: docs-only stays docs-only", ready_run(["README.md"]).get("areas"), "docs-only")
r = ready_run(["README.md", DT + "D3Tests.cs"])
check("map: test-only stays test-only (no Guard class)", (r.get("areas"), r.get("darling_classes")), ("classes", "D3,D4"))
r = ready_run([SERVICE, "no-such-area/odd.bin"])
check("map: a path no area knows is every area", r.get("areas"), "all")

# The command line: a pull request with a map error is FULL and the why line says why; a map file that is not at hand says so.
real_rules2 = cs.Rules
cs.Rules = lambda: rules
try:
    for extra, needle in ((["--map", "", "--map-error", "no nightly run has a test-map artifact"], "no nightly run has a test-map artifact"),
                          (["--map", os.path.join(tmp, "absent.json.gz")], "no test map was downloaded")):
        with open(os.path.join(tmp, "svc.txt"), "w", encoding="utf-8") as fh:
            fh.write(SERVICE + "\n")
        buf = io.StringIO()
        try:
            with contextlib.redirect_stdout(buf), contextlib.redirect_stderr(io.StringIO()):
                code = cp.main(["--event", "pull_request", "--base", "dev", "--files-file", os.path.join(tmp, "svc.txt")] + extra)
        except SystemExit as exc:  # a command line that does not know --map
            code = exc.code
        lines = dict(x.split("=", 1) for x in buf.getvalue().splitlines() if "=" in x)
        check(f"main: map error {needle!r}: exit and scope", (code, lines.get("darling_scope")), (0, "full"))
        check(f"main: map error {needle!r}: the why line", needle in lines.get("why", ""), True)
finally:
    cs.Rules = real_rules2

# --- the command line writes $GITHUB_OUTPUT --------------------------------------------------------------------
files_path = os.path.join(tmp, "files.txt")
out_path = os.path.join(tmp, "out.txt")
with open(files_path, "w", encoding="utf-8") as fh:
    fh.write("README.md\n\n")
real_rules = cs.Rules
cs.Rules = lambda: rules  # the command line builds its own Rules; give it the stand-ins
try:
    with contextlib.redirect_stdout(io.StringIO()):
        code = cp.main(["--event", "pull_request", "--base", "dev", "--labels", '["bug"]',
                        "--files-file", files_path, "--github-output", out_path])
finally:
    cs.Rules = real_rules
check("main: exit code", code, 0)
with open(out_path, encoding="utf-8") as fh:
    written = dict(ln.split("=", 1) for ln in fh.read().splitlines())
check("main: output file", (written["areas"], written["run"], written["legs"]), ("docs-only", "true", "0"))

# --- CI scripts only (#7743) ------------------------------------------------------------------------------------
# A change to files under .github/scripts that no area knows runs the script tests, the build and the Guard classes of both suites,
# not every class. One of the selection or leg scripts also adds a last leg that runs the classes the map selects for a sample diff.
sc_cls = {"darling": d_cls, "lite": {**l_cls, "LG": {"Stage=Guard"}}}
sc_cat = {**cc_cat, "classes": sc_cls, "lite_weights": {**cc_cat["lite_weights"], "LG": 60}}
SCRIPT = ".github/scripts/ci-map-helper.py"          # a script that is no trigger
TRIGGER = ".github/scripts/ci-select.py"
SAMPLE = getattr(cp, "SELF_TEST_SAMPLE", "Darling/README.md")
MAXC = getattr(cp, "SELF_TEST_MAX_CLASSES", 150)


def scripts_run(files, map_=None, error="", cat=sc_cat, event="pull_request", draft=False, fetched=None):
    def lazy():
        if fetched is not None:
            fetched.append(1)
        return map_, error
    try:
        return cp.plan(event, "dev", "feature/x", [], files, rules, cat, draft=draft, self_test_map=lazy, now=NOW)
    except TypeError:  # a plan without the scripts area (dev's) has no such input: its answer is what the checks measure
        return cp.plan(event, "dev", "feature/x", [], files, rules, cat, draft=draft)


sc_map = make_map()
sc_map["text_patterns"] = {"D3": [SAMPLE], "L2": [SAMPLE]}      # the classes that read the sample document as text
r = scripts_run([SCRIPT])
check("scripts: a script is its own area, not every area", (r.get("areas", ""), r.get("darling_scope", ""), r.get("lite_scope", "")), ("classes", "classes", "classes"))
check("scripts: the Guard classes of both suites", (r.get("darling_classes", ""), r.get("lite_classes", "")), ("D1", "LG"))
check("scripts: the build runs, the script tests run", (r.get("build", ""), r.get("script_tests", "")), ("true", "true"))
check("scripts: no self-test leg for a script that decides no selection", (r.get("self_test_darling", ""), r.get("self_test_lite", ""), r.get("self_test_note", ""), r.get("legs", "")), ("", "", "", "1"))
check("scripts: the why line names the rule", "CI scripts only: script tests, build and Guard." in r.get("why", ""), True)
r = scripts_run([TRIGGER], sc_map)
check("scripts: a selection script adds the self-test leg, the last one", (r.get("legs", ""), r.get("legs_json", "")), ("2", "[1,2]"))
check("scripts: the self-test classes are the map's for the sample", (r.get("self_test_darling", ""), r.get("self_test_lite", "")), ("D3", "L2"))
check("scripts: the main classes are still the Guard classes", (r.get("darling_classes", ""), r.get("lite_classes", "")), ("D1", "LG"))
check("scripts: the why line names the rule with the map-selected leg",
      "CI scripts only: script tests, build, Guard, and one map-selected leg." in r.get("why", ""), True)
check("scripts: the note names the sample and the map's age", (SAMPLE in r.get("self_test_note", ""), "6.0 hours old" in r.get("self_test_note", "")), (True, True))
check("scripts: a script of the run itself is a trigger too",
      [scripts_run([".github/scripts/" + n], sc_map).get("legs", "") for n in ("ci-plan.py", "run-ci-leg.ps1", "test-map.py", "ci-map-replay.py")], ["2"] * 4)
check("scripts: a test file is no trigger", scripts_run([".github/scripts/tests/ci-plan-test.py"], sc_map).get("legs", ""), "1")
check("scripts: documentation beside the script", scripts_run(["README.md", TRIGGER], sc_map).get("areas", ""), "classes")
check("scripts: a draft gets the same plan", scripts_run([TRIGGER], sc_map, draft=True).get("self_test_darling", ""), "D3")
fetched = []
scripts_run([SCRIPT], fetched=fetched)
scripts_run([SERVICE], fetched=fetched)
check("scripts: the map is fetched only for a self-test leg", fetched, [])
scripts_run([TRIGGER], sc_map, fetched=fetched)
check("scripts: the map is fetched for a self-test leg", fetched, [1])
# A script together with a file of any other area is the old rule: every area.
r = scripts_run([TRIGGER, SERVICE], sc_map)
check("scripts: with a product file it is every area", (r.get("areas", ""), r.get("darling_scope", ""), r.get("script_tests", "")), ("all", "full", "false"))
check("scripts: with a path no area knows it is every area", scripts_run([TRIGGER, "no-such-area/odd.bin"], sc_map).get("areas", ""), "all")
check("scripts: a dev push is every area", scripts_run([TRIGGER], sc_map, event="push").get("areas", ""), "all")
check("scripts: no Guard class in the catalog is every area", scripts_run([TRIGGER], sc_map, cat={**cc_cat, "classes": {"darling": {"D3": set()}, "lite": l_cls}}).get("areas"), "all")
# The self-test leg never fails for the map: the Guard classes run instead, and the note says why.
for label, kw, word in (("a missing map", {"map_": None, "error": "no nightly run has a test-map artifact"}, "no nightly run has a test-map artifact"),
                        ("a map nobody gave", {"map_": None}, "no test map was given"),
                        ("an old map", {"map_": make_map(built="2026-11-03T11:00:00Z")}, "older than 2 days"),
                        ("a broken map", {"map_": {"schema": 1, "files": 5, "classes": 7}}, "map")):
    r = scripts_run([TRIGGER], **kw)
    check(f"scripts: {label} runs the Guard classes in the last leg", (r.get("self_test_darling", ""), r.get("self_test_lite", ""), r.get("legs", "")), ("D1", "LG", "2"))
    check(f"scripts: {label}: the note says why", ("instead of a map-selected list" in r.get("self_test_note", ""), word in r.get("self_test_note", "")), (True, True))
no_hit = make_map()
no_hit["text_patterns"] = {}
check("scripts: a sample that selects no class runs the Guard classes", "selected no class" in scripts_run([TRIGGER], no_hit).get("self_test_note", ""), True)
many = make_map()
for i in range(MAXC + 1):
    many["classes"]["darling"][f"Many{i}"] = {"own": fid(DT + "D3Tests.cs"), "files": [], "seconds": 1.0}
    many["text_patterns"][f"Many{i}"] = [SAMPLE]
many_cat = {**sc_cat, "classes": {**sc_cls, "darling": {**d_cls, **{f"Many{i}": set() for i in range(MAXC + 1)}}}}
check("scripts: a sample that selects too many classes runs the Guard classes",
      f"more than {MAXC}" in scripts_run([TRIGGER], many, cat=many_cat).get("self_test_note", ""), True)
# write_leg_extras: the files run-ci-leg.ps1 reads.
if not hasattr(cp, "write_leg_extras"):
    failures.append("leg extras: ci-plan.py has no write_leg_extras")
    print("\n".join(failures))
    sys.exit(1)
extras = os.path.join(tmp, "extras")
r = scripts_run([TRIGGER], sc_map)
cp.write_leg_extras(extras, r)
with open(os.path.join(extras, "script-tests.txt"), encoding="utf-8") as fh:
    listed_tests = fh.read().splitlines()
check("leg extras: the script tests are every .py of the tests folder",
      (listed_tests == sorted(listed_tests), all(t.startswith(".github/scripts/tests/") and t.endswith(".py") for t in listed_tests),
       ".github/scripts/tests/ci-plan-test.py" in listed_tests), (True, True, True))
with open(os.path.join(extras, "self-test-darling-classes.txt"), encoding="utf-8") as fh:
    check("leg extras: the self-test Darling list", fh.read(), "D3\n")
with open(os.path.join(extras, "self-test.txt"), encoding="utf-8") as fh:
    check("leg extras: the note", SAMPLE in fh.read(), True)
check("leg extras: the self-test Lite list", os.path.exists(os.path.join(extras, "self-test-lite-classes.txt")), True)
r = scripts_run([SCRIPT])
cp.write_leg_extras(os.path.join(tmp, "e2"), r)
check("leg extras: no self-test leg writes the script tests and no self-test file",
      (os.path.exists(os.path.join(tmp, "e2", "script-tests.txt")), os.path.exists(os.path.join(tmp, "e2", "self-test.txt"))), (True, False))
cp.write_leg_extras(os.path.join(tmp, "e3"), run([SERVICE]))
check("leg extras: an ordinary plan writes none", os.path.exists(os.path.join(tmp, "e3")), False)

# --- the real filters, three stable paths ----------------------------------------------------------------------
live = cs.Rules()
check("real filters: README.md is docs-only", cp.classify(live, "pull_request", "dev", ["README.md"])["areas"], "docs-only")
check("real filters: Darling file is the Darling suite",
      cp.classify(live, "pull_request", "dev", ["Darling/PerformanceMonitor.Darling.Service/X.cs"])["darling_scope"], "full")
check("real filters: a stray path runs everything",
      cp.classify(live, "pull_request", "dev", ["no-such-area/odd.bin"])["areas"], "all")
check("real filters: a CI script alone is the scripts area, not every area",
      cp.classify(live, "pull_request", "dev", [".github/scripts/ci-plan.py", ".github/scripts/tests/ci-plan-test.py"], sc_cat)["areas"], "classes")
for wf in (".github/workflows/pr.yml", ".github/workflows/build.yml"):
    check(f"real filters: {wf} with a CI script keeps the rule, every area",
          cp.classify(live, "pull_request", "dev", [".github/scripts/ci-plan.py", wf], sc_cat)["areas"], "all")
check("real filters: a workflow file alone is every area",
      cp.classify(live, "pull_request", "dev", [".github/workflows/pr.yml"], sc_cat)["areas"], "all")

if failures:
    print("\n".join(failures))
    sys.exit(1)
print("ci-plan: all checks passed")
