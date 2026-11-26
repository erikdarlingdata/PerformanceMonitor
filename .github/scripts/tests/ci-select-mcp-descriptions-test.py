#!/usr/bin/env python3
"""Checks for the MCP description rule of ci-select.py and ci-plan.py (#9779): a changed file that declares MCP tools selects the
classes that measure served descriptions. Behaviour only: each check builds a small repository on disk, runs the selector or the
plan on a changed-file list and compares the classes it picks.
Run: python -I .github/scripts/tests/ci-select-mcp-descriptions-test.py"""
import importlib.util
import json
import os
import subprocess
import sys
import tempfile

here = os.path.dirname(os.path.abspath(__file__))


def load(name, file):
    spec = importlib.util.spec_from_file_location(name, os.path.join(here, "..", file))
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


cp = load("ci_plan", "ci-plan.py")
cs = cp._CS

failures = []


def check(name, actual, expected):
    if actual != expected:
        failures.append(f"{name}: expected {expected!r}, got {actual!r}")


TEST = '''using Xunit;
namespace {ns};
public sealed class {name}
{{
    [Fact]
    public void Holds() {{ Assert.True(true); }}
}}
'''
HELPER = '''namespace {ns};
public sealed class {name}
{{
    public int Value;
}}
'''
TOOLS = '''using ModelContextProtocol.Server;
namespace {ns};
[McpServerToolType]
public static class {name}
{{
    [McpServerTool, Description("A served description.")]
    public static string Read() => "x";
}}
'''
NO_TOOLS = '''namespace {ns};
/// <summary>The method is a public <c>[McpServerTool]</c> method by the SDK's convention, but this class declares none.</summary>
public static class {name}
{{
    public static string Read() => "x";
}}
'''

DESCRIPTION_CLASSES = ["McpToolGuideTests", "McpToolsListBudgetTests", "McpToolGuideHeadsAlertingTests", "McpToolGuideHeadsDataTests"]
OTHER_CLASSES = ["OtherTests", "McpToolGuideHeadsNotATestRows"]
LITE_TOOL = "Lite/Mcp/McpFooTools.cs"
LITE_PLAIN = "Lite/Mcp/McpFooPolicy.cs"
LITE_OTHER = "Lite/Services/Bar.cs"
DARLING_TOOL = "Darling/PerformanceMonitor.Darling.Service/Mcp/DarlingMcpFooTools.cs"
DARLING_PLAIN = "Darling/PerformanceMonitor.Darling.Service/StoreHostProfileCache.cs"
DARLING_OTHER = "Darling/PerformanceMonitor.Darling.Service/Other.cs"
COMMON_TOOL = "PerformanceMonitor.Common/Mcp/McpSharedTools.cs"
LITE_TEST_TOOL = "Lite.Tests/McpFooToolsTests.cs"


def write(root, rel, text):
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        fh.write(text)


def build_tree(root, edit=""):
    """A repository with a Lite and a Darling MCP tool file, files that declare none, and both suites' description classes."""
    write(root, LITE_TOOL, TOOLS.format(ns="Lite.Mcp", name="McpFooTools") + edit)
    write(root, LITE_PLAIN, NO_TOOLS.format(ns="Lite.Mcp", name="McpFooPolicy"))
    write(root, LITE_OTHER, NO_TOOLS.format(ns="Lite.Services", name="Bar"))
    write(root, DARLING_TOOL, TOOLS.format(ns="Darling.Mcp", name="DarlingMcpFooTools") + edit)
    write(root, DARLING_PLAIN, NO_TOOLS.format(ns="Darling.Service", name="StoreHostProfileCache"))
    write(root, DARLING_OTHER, NO_TOOLS.format(ns="Darling.Service", name="Other"))
    write(root, COMMON_TOOL, TOOLS.format(ns="Common.Mcp", name="McpSharedTools"))
    for suite_dir, ns in (("Lite.Tests", "Lite.Tests"), ("Darling/Darling.Tests", "Darling.Tests")):
        for name in DESCRIPTION_CLASSES + ["McpFooToolsTests", "OtherTests"]:
            write(root, f"{suite_dir}/{name}.cs", TEST.format(ns=ns, name=name))
        # A class of the same family that holds no test is not run, and an unrelated class is not part of the family.
        write(root, f"{suite_dir}/McpToolGuideHeadsHelper.cs", HELPER.format(ns=ns, name="McpToolGuideHeadsHelperTests"))
        write(root, f"{suite_dir}/McpToolGuideHeadsNotATest.cs", TEST.format(ns=ns, name="McpToolGuideHeadsNotATestRows"))


with tempfile.TemporaryDirectory() as root, tempfile.TemporaryDirectory() as map_dir:
    build_tree(root)
    want = set(DESCRIPTION_CLASSES)

    # --- the selector -------------------------------------------------------------------------------------------------
    check("a Lite MCP tool file selects the Lite description classes", cs.description_classes([LITE_TOOL], root), {"lite": want})
    check("a Darling service MCP tool file selects the Darling twins", cs.description_classes([DARLING_TOOL], root), {"darling": want})
    check("both product files select both suites",
          cs.description_classes([LITE_TOOL, DARLING_TOOL, LITE_OTHER], root), {"lite": want, "darling": want})
    check("a Lite file that declares no tool selects nothing", cs.description_classes([LITE_OTHER], root), {})
    check("a Lite file whose doc comment names the attribute selects nothing", cs.description_classes([LITE_PLAIN], root), {})
    check("a Darling file whose doc comment names the attribute selects nothing", cs.description_classes([DARLING_PLAIN, DARLING_OTHER], root), {})
    check("a tool file outside both product trees selects nothing", cs.description_classes([COMMON_TOOL], root), {})
    check("a changed test file that declares a tool selects nothing", cs.description_classes([LITE_TEST_TOOL], root), {})
    check("a deleted tool file selects nothing", cs.description_classes(["Lite/Mcp/McpGone.cs"], root), {})
    check("an empty change selects nothing", cs.description_classes([], root), {})
    check("the Lite rule never names a Darling class",
          set(cs.description_classes([LITE_TOOL], root)), {"lite"})

    classes = cs.scan_classes(root)
    check("a family class that holds no test is not selected",
          "McpToolGuideHeadsHelperTests" in classes["lite"] and "McpToolGuideHeadsHelperTests" in cs.description_classes([LITE_TOOL], root, classes).get("lite", set()),
          False)
    check("a class outside the family is not selected", "McpToolGuideHeadsNotATestRows" in cs.description_classes([LITE_TOOL], root, classes)["lite"], False)

    # --- joining the rule to a selection: it only adds ----------------------------------------------------------------
    check("no extra class leaves None as None", cs.with_description_classes(None, [LITE_OTHER], root), None)
    check("no extra class leaves a selection as it was",
          cs.with_description_classes({"lite": {"A"}}, [LITE_OTHER], root), {"lite": {"A"}})
    joined = cs.with_description_classes({"lite": {"A"}, "darling": {"B"}}, [LITE_TOOL], root)
    check("the classes join the ones already named", joined, {"lite": {"A"} | want, "darling": {"B"}})
    check("None joins to the rule's classes alone", cs.with_description_classes(None, [DARLING_TOOL], root), {"darling": want})
    given = {"lite": {"A"}}
    cs.with_description_classes(given, [LITE_TOOL], root)
    check("the caller's selection is not changed", given, {"lite": {"A"}})

    # --- local-filter (a lane's view of CI's selection) ---------------------------------------------------------------
    def git(*args, cwd=root):
        subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", "-c", "commit.gpgsign=false", *args],
                       cwd=cwd, check=True, capture_output=True)

    git("init", "-q", "-b", "dev")
    git("add", "-A")
    git("commit", "-q", "-m", "base")
    git("checkout", "-q", "-b", "feature")
    write(root, LITE_TOOL, TOOLS.format(ns="Lite.Mcp", name="McpFooTools") + "// a longer description\n")
    code, lines, _notes = cs.local_filter(root, "dev")
    lite_line = next((ln for ln in lines if ln.startswith("lite classes:")), "")
    check("local-filter: a Lite MCP tool edit lists the Lite description classes",
          (code, want <= set(lite_line.split()[2:]), any(ln.startswith("darling classes:") for ln in lines)), (0, True, False))
    git("checkout", "-q", "--", LITE_TOOL)
    write(root, LITE_OTHER, NO_TOOLS.format(ns="Lite.Services", name="Bar") + "// an edit\n")
    code, lines, _notes = cs.local_filter(root, "dev")
    check("local-filter: a Lite file that declares no tool lists no description class",
          (code, [ln for ln in lines if "McpToolGuide" in ln or "McpToolsListBudget" in ln]), (0, []))
    git("checkout", "-q", "--", LITE_OTHER)

    # --- the plan: a ready pull request and a draft --------------------------------------------------------------------
    def test_map(covering):
        """A nightly map in which McpFooToolsTests covers the changed product file and no map class covers a description class'
        product code (the case the rule exists for: the tool file is covered, the measuring classes are not)."""
        universe = [LITE_TOOL, LITE_OTHER, DARLING_TOOL, DARLING_OTHER, "Lite.Tests/McpFooToolsTests.cs",
                    "Darling/Darling.Tests/McpFooToolsTests.cs"] + [f"Lite.Tests/{c}.cs" for c in DESCRIPTION_CLASSES + OTHER_CLASSES] \
            + [f"Darling/Darling.Tests/{c}.cs" for c in DESCRIPTION_CLASSES + OTHER_CLASSES]

        def entry(path, files):
            return {"own": universe.index(path), "files": [universe.index(f) for f in files], "seconds": 30.0}

        return {"schema": 1, "sha": "abc", "built_at": "2026-11-26T06:00:00Z", "tool": "test", "files": universe,
                "classes": {
                    "lite": {"McpFooToolsTests": entry("Lite.Tests/McpFooToolsTests.cs", covering[0]),
                             **{c: entry(f"Lite.Tests/{c}.cs", []) for c in DESCRIPTION_CLASSES + OTHER_CLASSES}},
                    "darling": {"McpFooToolsTests": entry("Darling/Darling.Tests/McpFooToolsTests.cs", covering[1]),
                                **{c: entry(f"Darling/Darling.Tests/{c}.cs", []) for c in DESCRIPTION_CLASSES + OTHER_CLASSES}}},
                "text_patterns": {}}

    map_path = os.path.join(map_dir, "nightly-map.json")
    with open(map_path, "w", encoding="utf-8") as fh:
        json.dump(test_map(([LITE_TOOL, LITE_OTHER], [DARLING_TOOL, DARLING_OTHER])), fh)
    rules = cs.Rules()
    catalog = cp.load_catalog(root)
    now = cs.datetime.datetime(2026, 11, 26, 12, 0, tzinfo=cs.datetime.timezone.utc)

    def commit_edit(rel, text):
        git("checkout", "-q", "dev")
        git("checkout", "-q", "-B", "feature")
        write(root, rel, text)
        git("add", "-A")
        git("commit", "-q", "-m", "edit")

    def ready(files, with_files=True):
        selection = cp.build_selection(map_path, "", root, "push", files=files if with_files else None)
        selection["now"] = now
        return cp.plan("push", "dev", "", [], files, rules, catalog, selection=selection)

    def picked(plan, suite):
        return set(filter(None, plan[f"{suite}_classes"].split(",")))

    commit_edit(LITE_TOOL, TOOLS.format(ns="Lite.Mcp", name="McpFooTools") + "// edited\n")
    before, after = ready([LITE_TOOL], with_files=False), ready([LITE_TOOL])
    check("plan: without the rule the Lite edit runs only the class that covers the file", picked(before, "lite"), {"McpFooToolsTests"})
    check("plan: with the rule a Lite tool edit adds the Lite description classes", picked(after, "lite"), {"McpFooToolsTests"} | want)
    check("plan: the rule only adds classes (Lite)", picked(before, "lite") <= picked(after, "lite"), True)
    check("plan: the Darling picks are what they were for a Lite edit", picked(before, "darling"), picked(after, "darling"))
    check("plan: the added Lite classes stay in the same areas", (before["areas"], after["areas"]), ("classes", "classes"))

    commit_edit(DARLING_TOOL, TOOLS.format(ns="Darling.Mcp", name="DarlingMcpFooTools") + "// edited\n")
    before, after = ready([DARLING_TOOL], with_files=False), ready([DARLING_TOOL])
    check("plan: with the rule a Darling tool edit adds the Darling twins", picked(after, "darling"), picked(before, "darling") | want)
    check("plan: a Darling tool edit adds no Lite class", picked(after, "lite"), picked(before, "lite"))

    commit_edit(LITE_OTHER, NO_TOOLS.format(ns="Lite.Services", name="Bar") + "// edited\n")
    before, after = ready([LITE_OTHER], with_files=False), ready([LITE_OTHER])
    check("plan: a Lite file that declares no tool picks what it picked before",
          ({s: picked(before, s) for s in ("darling", "lite")}, before["lite_scope"]),
          ({s: picked(after, s) for s in ("darling", "lite")}, after["lite_scope"]))
    check("plan: a Lite file that declares no tool adds no description class", picked(after, "lite") & want, set())

    # A draft runs the classes that name a changed file, and the description classes join them.
    def draft(files):
        named, _ = cs.classes_naming_files(files, root)
        named = {s: list(dict.fromkeys(list(named.get(s, [])) + sorted(v)))
                 for s, v in (cs.with_description_classes(named, files, root) or {}).items()}
        return cp.plan("pull_request", "dev", "feature/x", [], files, rules, catalog, draft=True, named=named)

    d = draft([LITE_TOOL])
    check("plan: a draft that edits a Lite tool file runs the Lite description classes", want <= picked(d, "lite"), True)
    d = draft([LITE_OTHER])
    check("plan: a draft that edits a Lite file declaring no tool runs no description class", picked(d, "lite") & want, set())

if failures:
    print("\n".join(failures))
    sys.exit(1)
print("ci-select mcp descriptions: all checks passed")
