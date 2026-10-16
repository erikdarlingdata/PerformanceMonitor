#!/usr/bin/env python3
"""Checks for ci-select.py's reads detectors (#5961). Run: python -I .github/scripts/tests/ci-select-reads-test.py"""
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


# --- reads_lite_path: what GuardStageScanner.ReadsALitePath answered -----------------------------------------------
for src in (
    'var p = "Lite/Services/X.cs";',
    'var p = @"..\\Lite\\Services\\X.cs";',
    'var p = Path.Combine(root, "Lite", "Mcp", "X.cs");',
    'var s = RepoFile.ReadRepoFile("Lite", "Analysis", "X.cs");',
    'var p = "Lite.Tests/DataStartBannerQueriesTabTests.cs";',
    'var p = "Lite/PerformanceMonitorLite.csproj";',
    'var p = "PerformanceMonitorLite.Tests";',
):
    check("reads lite: " + src, cs.reads_lite_path(src), True)
for src in (
    '// "Lite/Services/X.cs" in prose\nvar a = 1;',
    '/* "Lite/Services/X.cs" */ var a = 1;',
    'Assert.Contains("Lite: an unseeded store", head);',
    'var p = "Darling/Darling.Tests/X.cs";',
    'var p = "Delite/X.cs";',
):
    check("not lite: " + src, cs.reads_lite_path(src), False)

# --- a recursive enumeration from the repo root reads every Lite file without naming one ---------------------------
check("walk root var", cs.reads_lite_path(
    'var root = RepoRoot();\nvar f = Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories);'), True)
check("walk root call", cs.reads_lite_path(
    'var f = Directory.GetFiles(ParitySource.RepoRoot(), "*.csproj", SearchOption.AllDirectories);'), True)
check("walk options", cs.reads_lite_path(
    'string root = ParitySource.RepoRoot();\nvar o = new EnumerationOptions { RecurseSubdirectories = true };\n'
    'var f = Directory.EnumerateFiles(root, "*.props", o);'), True)
check("walk narrower than the root", cs.reads_lite_path(
    'var root = RepoRoot();\nvar f = Directory.EnumerateFiles(Path.Combine(root, "Darling"), "*.cs", SearchOption.AllDirectories);'), False)
check("walk top directory only", cs.reads_lite_path(
    'var root = RepoRoot();\nvar f = Directory.EnumerateFiles(root, "*.cs", SearchOption.TopDirectoryOnly);'), False)
check("walk in a comment", cs.reads_lite_path(
    'var root = RepoRoot();\n// Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories)\nvar a = 1;'), False)

# --- the walker: char literals, verbatim, raw and interpolated strings must not desynchronise it -------------------
check("char quote then path", cs.reads_lite_path('var q = \'"\'; var p = "Lite/X.cs";'), True)
check("char quote hides nothing", cs.reads_lite_path('var q = \'"\'; var a = 1; // "Lite/X.cs"'), False)
check("raw string", cs.reads_lite_path('var p = """Lite/Services/X.cs""";'), True)
check("interpolated hole is code, its literal is read", cs.reads_lite_path('var p = $"{Path.Combine(a, "Lite", "X")}";'), True)
check("verbatim with doubled quote", cs.reads_lite_path('var r = @"a""b"; var p = "Lite/X.cs";'), True)

# --- reads_darling_path: the Lite.Tests twin -----------------------------------------------------------------------
for src in (
    'var p = "Darling/PerformanceMonitor.Darling.Service/X.cs";',
    'var p = @"..\\Darling\\Darling.Tests\\X.cs";',
    'var p = "PerformanceMonitor.Darling.Viewer";',
    'var p = Path.Combine(root, "Darling", "Darling.Tests", "X.cs");',
    'var s = new[] { "Darling", "PerformanceMonitor.Darling.Viewer" };',
):
    check("reads darling: " + src, cs.reads_darling_path(src), True)
for src in (
    '// "Darling/PerformanceMonitor.Darling.Service/X.cs" in prose\nvar a = 1;',
    '/* "Darling/Darling.Tests/X.cs" */ var a = 1;',
    'Assert.Contains("Darling: an unseeded store", head);',
    'var p = "Lite/Services/X.cs";',
    'var p = "Lite.Tests/Darling.txt";',
):
    check("not darling: " + src, cs.reads_darling_path(src), False)
check("darling walk", cs.reads_darling_path(
    'var root = RepoRoot();\nvar f = Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories);'), True)

# --- test_classes_of_text: full names, own-body tests only, nesting, block namespaces -------------------------------
file_scoped = ("namespace Darling.Tests;\n\npublic sealed class AlphaTests\n{\n    [Fact]\n    public void A() { }\n}\n\n"
               "internal static class Helper\n{\n    public static string P() => \"Lite/X.cs\";\n}\n")
check("file-scoped namespace, helper skipped", cs.test_classes_of_text(file_scoped), ["Darling.Tests.AlphaTests"])
block = ("namespace Lite.Tests\n{\n    public sealed class Outer\n    {\n        [Fact]\n        public void A() { }\n\n"
         "        public sealed class InnerTests\n        {\n            [Theory]\n            public void B() { }\n        }\n    }\n}\n")
check("nested class does not take its enclosing tests", sorted(cs.test_classes_of_text(block)),
      ["Lite.Tests.Outer", "Lite.Tests.Outer+InnerTests"])
helper_only = "namespace N;\npublic class Outer\n{\n    public class Inner\n    {\n        [Fact]\n        public void B() { }\n    }\n}\n"
check("outer without its own test", cs.test_classes_of_text(helper_only), ["N.Outer+Inner"])
check("a test attribute in a comment is no test", cs.test_classes_of_text("namespace N;\npublic class C\n{\n    // [Fact]\n}\n"), [])

# --- reads_classes: file-level, both suites, on a throwaway tree ---------------------------------------------------
import tempfile

with tempfile.TemporaryDirectory() as root:
    def put(rel, text):
        path = os.path.join(root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as fh:
            fh.write(text)

    put("Darling/Darling.Tests/ReaderTests.cs",
        'namespace Darling.Tests;\npublic sealed class ReaderTests\n{\n    [Fact]\n    public void A() { var p = "Lite/X.cs"; }\n}\n')
    put("Darling/Darling.Tests/PlainTests.cs",
        'namespace Darling.Tests;\npublic sealed class PlainTests\n{\n    [Fact]\n    public void A() { }\n}\n')
    # The attribute that used to mark a class is itself a "Lite" literal in a file: it must not be needed, and a class
    # that carries it but reads nothing is not in the set (this is what the hand-written roster got wrong).
    put("Darling/Darling.Tests/TaggedOnlyTests.cs",
        'namespace Darling.Tests;\n[Trait("Stage", "Other")]\npublic sealed class TaggedOnlyTests\n{\n    [Fact]\n    public void A() { }\n}\n')
    put("Lite.Tests/DarlingReaderTests.cs",
        'namespace Lite.Tests;\npublic sealed class DarlingReaderTests\n{\n    [Fact]\n    public void A() { var p = "Darling/X.cs"; }\n}\n')
    put("Lite.Tests/LiteOnlyTests.cs",
        'namespace Lite.Tests;\npublic sealed class LiteOnlyTests\n{\n    [Fact]\n    public void A() { var p = "Lite/X.cs"; }\n}\n')
    got = cs.reads_classes(root)
    check("darling suite reads lite", sorted(got["darling"]), ["Darling.Tests.ReaderTests"])
    check("lite suite reads darling", sorted(got["lite"]), ["Lite.Tests.DarlingReaderTests"])
    check("one suite only", cs.reads_classes(root, ("lite",))["darling"], set())

    # The class scan gives a reader the Reads trait with no attribute in the source.
    scanned = cs.scan_classes(root)
    check("scan_classes trait lite", "Reads=Lite" in scanned["darling"]["ReaderTests"], True)
    check("scan_classes no trait", "Reads=Lite" in scanned["darling"]["PlainTests"], False)
    check("scan_classes trait darling", "Reads=Darling" in scanned["lite"]["DarlingReaderTests"], True)

if failures:
    print("\n".join(failures))
    sys.exit(1)
print("ci-select reads checks passed")
