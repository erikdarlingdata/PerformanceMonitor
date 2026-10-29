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
            installer_core:
              - 'PerformanceMonitor.Installer/**/!(*.md)'
            dashboard:
              - 'deprecated/Dashboard/**/!(*.md)'
            lite:
              - 'Lite/**/!(*.md)'
            installer:
              - 'deprecated/Installer/**/!(*.md)'
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


def run(files, event="pull_request", base="dev", head="feature/x", draft=False, labels=(), cat=catalog):
    return cp.plan(event, base, head, draft, list(labels), files, rules, cat)


# --- the gate -------------------------------------------------------------------------------------------------
check("gate: a PR without labels runs", run(["README.md"], labels=())["outcome"], "run")
check("gate: an unrelated label runs", run(["README.md"], labels=("bug",))["outcome"], "run")
check("gate: no label, tests planned", run([SERVICE], labels=())["legs"], "2")
check("gate: draft", run(["Darling/X.cs"], draft=True)["outcome"], "skip-draft")
check("gate: draft is not run", run(["Darling/X.cs"], draft=True)["run"], "false")
check("gate: draft without labels still skips", run(["README.md"], labels=(), draft=True)["outcome"], "skip-draft")
check("gate: train label", run(["Darling/X.cs"], labels=("merge-train",))["outcome"], "train-blocked")
check("gate: train label is not run", run(["Darling/X.cs"], labels=("merge-train",))["run"], "false")
check("gate: train label beats draft", run(["Darling/X.cs"], draft=True, labels=("merge-train",))["outcome"],
      "train-blocked")
check("gate: train branch runs a draft", run(["Darling/X.cs"], draft=True, head="train/2026-10-23")["outcome"], "run")
check("gate: train branch runs despite the train label",
      run(["Darling/X.cs"], head="train/a", labels=("merge-train",))["outcome"], "run")
check("gate: a dev push ignores the label", run(["README.md"], event="push", labels=())["outcome"], "run")
check("gate: a push ignores draft", run(["README.md"], event="push", draft=True)["outcome"], "run")

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

r = run(["deprecated/Installer/X.cs"])
check("deprecated only: no area", r["areas"], "none")
check("deprecated only: builds, no legs", (r["build"], r["legs"]), ("true", "0"))
check("deprecated only: installer tests", r["deprecated_tests"], "true")
check("deprecated file in an all-area change", run(["deprecated/Installer/X.cs", "Darling/X.cs"])["deprecated_tests"],
      "true")
check("no deprecated file, no installer tests", run(["Darling/X.cs"])["deprecated_tests"], "false")

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
check("web js: not on a skipped run", run([js], draft=True)["check_web_js"], "false")
check("actionlint: not on a skipped run", run([".github/workflows/pr.yml"], draft=True)["actionlint"], "false")

# --- parsing ---------------------------------------------------------------------------------------------------
check("labels: JSON", cp.parse_labels('["ci-next", "bug"]'), ["ci-next", "bug"])
check("labels: comma list", cp.parse_labels("ci-next, bug"), ["ci-next", "bug"])
check("labels: empty", cp.parse_labels(""), [])
check("labels: empty array", cp.parse_labels("[]"), [])
check("bool", [cp.parse_bool(x) for x in ("true", "True", "false", "", "null")], [True, True, False, False, False])

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
tbl = real_cat["files"]
one = next(f for f, e in sorted(tbl.items()) if e["suite"] == "darling" and e["safe"] and len(e["classes"]) == 1)
r = cp.plan("pull_request", "dev", "x", False, [], ["README.md", one], live_rules := cs.Rules(), real_cat)
check("real table: one class file narrows", (r["areas"], r["darling_classes"]), ("classes", tbl[one]["classes"][0]))
check("real table: every row has a class", all(e["classes"] for e in tbl.values()), True)
no_test = {(s, c) for s, m in real_cat["classes"].items() for c, t in m.items() if cs.NO_TESTS_TRAIT in t}
check("real table: no picked name is a class without tests",
      sorted((e["suite"], c) for e in tbl.values() for c in e["tests"] if (e["suite"], c) in no_test), [])
check("real table: some file has a helper beside its tests", any(set(e["classes"]) - set(e["tests"]) and e["tests"] for e in tbl.values()), True)
check("real table: every safe file picks a class", all(e["tests"] for e in tbl.values() if e["safe"]), True)
unsafe = [f for f, e in tbl.items() if not e["safe"]]
check("real table: some helper files are unsafe", bool(unsafe), True)
r = cp.plan("pull_request", "dev", "x", False, [], ["README.md", one, unsafe[0]], live_rules, real_cat)
check("real table: an unsafe file is full", r["darling_scope"], "full")

# --- the command line writes $GITHUB_OUTPUT --------------------------------------------------------------------
files_path = os.path.join(tmp, "files.txt")
out_path = os.path.join(tmp, "out.txt")
with open(files_path, "w", encoding="utf-8") as fh:
    fh.write("README.md\n\n")
real_rules = cs.Rules
cs.Rules = lambda: rules  # the command line builds its own Rules; give it the stand-ins
try:
    with contextlib.redirect_stdout(io.StringIO()):
        code = cp.main(["--event", "pull_request", "--base", "dev", "--draft", "false", "--labels", '["bug"]',
                        "--files-file", files_path, "--github-output", out_path])
finally:
    cs.Rules = real_rules
check("main: exit code", code, 0)
with open(out_path, encoding="utf-8") as fh:
    written = dict(ln.split("=", 1) for ln in fh.read().splitlines())
check("main: output file", (written["areas"], written["run"], written["legs"]), ("docs-only", "true", "0"))

# --- the real filters, three stable paths ----------------------------------------------------------------------
live = cs.Rules()
check("real filters: README.md is docs-only", cp.classify(live, "pull_request", "dev", ["README.md"])["areas"], "docs-only")
check("real filters: Darling file is the Darling suite",
      cp.classify(live, "pull_request", "dev", ["Darling/PerformanceMonitor.Darling.Service/X.cs"])["darling_scope"], "full")
check("real filters: a stray path runs everything",
      cp.classify(live, "pull_request", "dev", ["no-such-area/odd.bin"])["areas"], "all")

if failures:
    print("\n".join(failures))
    sys.exit(1)
print("ci-plan: all checks passed")
