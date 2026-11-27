#!/usr/bin/env python3
"""Checks for ci-select.py's GATE_ENV_DEPS (#9260): a Gate=Env class that names its dependency globs is selected when a
changed file matches one, not on every narrowed pull request. Run: python -I .github/scripts/tests/ci-select-gate-deps-test.py"""
import datetime
import importlib.util
import os
import sys

here = os.path.dirname(os.path.abspath(__file__))
spec = importlib.util.spec_from_file_location("cs", os.path.join(here, "..", "ci-select.py"))
cs = importlib.util.module_from_spec(spec)
spec.loader.exec_module(cs)

failures = []


def check(name, actual, expected):
    if actual != expected:
        failures.append(f"{name}: expected {expected!r}, got {actual!r}")


CLS = "PgServerLogTailCsvJsonRotationLiveTests"
TEST_MAP = {
    "schema": 1, "sha": "0" * 7, "built_at": datetime.datetime.now(datetime.timezone.utc).isoformat(), "tool": "synthetic",
    "files": ["Darling/Darling.Tests/Gated.cs", "Darling/Darling.Tests/Other.cs", "Darling/PerformanceMonitor.Darling.Service/Svc.cs",
              "PerformanceMonitor.Collectors/PgServerLogTail.cs", "PerformanceMonitor.Common/Helper.cs",
              ".github/scripts/start-test-pg.ps1"],
    "classes": {"darling": {CLS: {"own": 0, "files": [], "seconds": 1.0}, "OtherGatedTests": {"own": 1, "files": [], "seconds": 1.0}}},
    "text_patterns": {},
}
DISCOVERED = {"darling": {CLS: {"Gate=Env"}, "OtherGatedTests": {"Gate=Env"}}}


def pick(*changed):
    r = cs.map_select(TEST_MAP, list(changed), [], DISCOVERED, event="pull_request", base_ref="dev", max_age_days=None,
                      shared_dirs=())
    return r["full"], sorted(r["selected"]["darling"])


check("a service file leaves the listed class out, the unlisted gated class stays",
      pick("Darling/PerformanceMonitor.Darling.Service/Svc.cs"), (False, ["OtherGatedTests"]))
check("a Collectors file selects both", pick("PerformanceMonitor.Collectors/PgServerLogTail.cs"), (False, ["OtherGatedTests", CLS]))
check("a Common file selects both", pick("PerformanceMonitor.Common/Helper.cs"), (False, ["OtherGatedTests", CLS]))
check("the rig script selects both", pick(".github/scripts/start-test-pg.ps1"), (False, ["OtherGatedTests", CLS]))
check("its own file selects it", pick("Darling/Darling.Tests/Gated.cs"), (False, ["OtherGatedTests", CLS]))

if failures:
    print("\n".join(failures))
    sys.exit(1)
print("ci-select gate deps: all checks passed")
