#!/usr/bin/env python3
"""Checks that a plan names only classes the test host lists (#7739). Run: python -I .github/scripts/tests/ci-plan-listed-test.py

A small source tree is written to a temp dir. Its test file holds a real class, a private nested fixture class with tests
(xunit lists only public classes), a `class W` with a [Fact] inside a test's string, an abstract base with a derived class
and a new public test class the map has never seen. The nightly map knows the real classes only. A plan that names any
other class stops its leg (#6734), so the plan must name the real ones and nothing else."""
import datetime
import importlib.util
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


FILTERS = {
    "build.yml": "jobs:\n  build:\n    steps:\n      - uses: dorny/paths-filter\n        with:\n          filters: |\n"
                 "            root:\n              - 'global.json'\n            core:\n              - 'PerformanceMonitor.Common/**/!(*.md)'\n"
                 "            lite:\n              - 'Lite/**/!(*.md)'\n            darling:\n              - 'Darling/**/!(*.md)'\n"
                 "            darling_full:\n              - 'Darling/**/!(*.md)'\n            docs:\n              - '**/*.md'\n"
                 "            all:\n              - '**'\n",
    "darling.yml": "darling:\n  - 'Darling/**/!(*.md)'\n",
    "lite.yml": "root_shard:\n  - 'global.json'\ncore_shard:\n  - 'PerformanceMonitor.Common/**/!(*.md)'\n"
                "lite_shard:\n  - 'Lite/**/!(*.md)'\ndarling_reads_shard:\n  - 'Darling/Darling.Tests/**/!(*.md)'\n"
                "lite_linked_shard:\n  - 'Darling/Darling.Tests/Probe.cs'\n",
    "tree.yml": "workflow:\n  - '.github/workflows/build.yml'\nlite_tree:\n  - 'Lite/**/!(*.md)'\n",
}
SOURCES = {
    "Darling/Darling.Tests/CensusTests.cs": '''namespace X;

public sealed class CensusTests
{
    [Fact]
    public void Real() { }

    [Fact]
    public void TheScan_ReadsASampleSource()
    {
        const string Source = @"
class W
{
    [Fact]
    public void Leaks() { }
}";
        Assert.NotEmpty(Source);
    }

    // Fixtures for the self-test. Not public, so xunit does not run them.
    private sealed class FixtureCensus
    {
        [Fact]
        public void Runs() { }
    }
}

public sealed class NewRealTests
{
    [Fact]
    public void Works() { }
}
''',
    "Darling/Darling.Tests/InheritTests.cs": '''namespace X;

public abstract class BaseTests
{
    [Fact]
    public void FromBase() { }
}

public sealed class DerivedTests : BaseTests
{
}
''',
}
PRODUCT = "Darling/PerformanceMonitor.Darling.Service/X.cs"
CENSUS, INHERIT = "Darling/Darling.Tests/CensusTests.cs", "Darling/Darling.Tests/InheritTests.cs"

root = tempfile.mkdtemp()
for rel, text in SOURCES.items():
    full = os.path.join(root, *rel.split("/"))
    os.makedirs(os.path.dirname(full), exist_ok=True)
    with open(full, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
paths = {}
for name, text in FILTERS.items():
    paths[name] = os.path.join(root, name)
    with open(paths[name], "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)
rules = cs.Rules(paths["build.yml"], paths["darling.yml"], paths["lite.yml"], paths["tree.yml"])

# The map holds what the test host listed at the nightly: the real classes of the tree, not the new one.
universe = [PRODUCT, CENSUS, INHERIT]
nightly = {"schema": 1, "sha": "abc", "built_at": "2026-11-05T06:00:00Z", "tool": "ci-plan-listed-test", "files": universe,
           "classes": {"darling": {"CensusTests": {"own": 1, "files": [0], "seconds": 5.0},
                                   "DerivedTests": {"own": 2, "files": [], "seconds": 5.0}},
                       "lite": {}},
           "text_patterns": {}}
now = datetime.datetime(2026, 11, 5, 12, 0, tzinfo=datetime.timezone.utc)
# Every class the tree declares is named by a changed symbol, which is the rule that put FixtureCensus and Sample in a plan.
named = {"darling": {"CensusTests", "NewRealTests", "FixtureCensus", "W", "BaseTests", "DerivedTests"}}

catalog = cp.load_catalog(root)
plan = cp.plan("pull_request", "dev", "feature/x", [], [PRODUCT, CENSUS], rules, catalog,
               selection={"map": nightly, "error": "", "now": now, "shared_diff": "", "named": named})
chosen = sorted(c for c in plan.get("darling_classes", "").split(",") if c)
check("plan: the map picks classes", plan.get("areas"), "classes")
check("plan: only the classes the test host lists", chosen, ["CensusTests", "DerivedTests", "NewRealTests"])

# The same answer for a draft, which names the classes of its changed test files (draft_picks) and for a change that touches
# test files only (changed_test_classes): neither may name the fixture, the string class or the abstract base.
draft = cp.plan("pull_request", "dev", "feature/x", [], [PRODUCT, CENSUS], rules, catalog, draft=True)
check("draft: only the classes the test host lists",
      sorted(c for c in draft.get("darling_classes", "").split(",") if c), ["CensusTests", "NewRealTests"])
only_tests = cp.plan("pull_request", "dev", "feature/x", [], [CENSUS], rules, catalog)
check("test files only: only the classes the test host lists",
      sorted(c for c in only_tests.get("darling_classes", "").split(",") if c), ["CensusTests", "NewRealTests"])

# The check itself, by class: a name the map holds is never asked, so the answer is about source the map has not seen.
ask = getattr(cs, "listed_checker", lambda _root: lambda _suite, _cls: None)(root)
for cls, want in (("CensusTests", True), ("NewRealTests", True), ("DerivedTests", True), ("FixtureCensus", False),
                  ("W", False), ("BaseTests", False), ("NoSuchClass", False)):
    check(f"listed: {cls}", ask("darling", cls), want)

if failures:
    print("\n".join(failures))
    sys.exit(1)
print("ci-plan-listed: all checks passed")
