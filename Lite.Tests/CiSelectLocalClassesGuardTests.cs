/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins <c>ci-select.py local-classes</c> and the "names a changed symbol" reason of <c>map-select</c> (#5776).
///
/// <para><b>Why.</b> A lane runs the test classes it thinks its change reaches, and on three pull requests twelve
/// classes it never ran went red in CI: each named a symbol the change declared or edited (a source scan, a pinned
/// constant, a census of callers), and the class-to-file map did not link them. <c>local-classes</c> lists the classes
/// whose source names a changed symbol, and <c>map-select</c> takes the same answer as one more reason. These cases
/// plant a change in a throwaway git repository (base commit, then an uncommitted edit, which is what a lane has
/// before it commits) and pin each rule: a changed method selects the class that names it, a stop word selects
/// nothing, a token over the cap is dropped and printed as NOISY, over 300 classes prints the FULL line, and
/// map-select adds a class for the name reason.</para>
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Darling")]
public sealed class CiSelectLocalClassesGuardTests : IDisposable
{
    private readonly string _repo = Path.Combine(Path.GetTempPath(), "ci-local-classes-" + Guid.NewGuid().ToString("N"));

    public CiSelectLocalClassesGuardTests()
    {
        Directory.CreateDirectory(_repo);
        Write("Lite/Services/Probe.cs", "namespace Lite.Services;\n\npublic sealed class ProbeService\n{\n    public int ReadThing() => 1;\n}\n");
        Write("Lite/Services/Unrelated.cs", "namespace Lite.Services;\n\npublic sealed class UnrelatedService\n{\n}\n");
        Write("Lite.Tests/NamesProbeTests.cs",
            "public class NamesProbeTests\n{\n    public void T() => Probe().FrobnicateWidget();\n}\n");
        Write("Lite.Tests/LiveProbeTests.cs",
            "public class LiveProbeTests\n{\n    private static readonly string? Pg = System.Environment.GetEnvironmentVariable(\"DARLING_TEST_PG\");\n"
            + "    public void T() => FrobnicateWidget();\n}\n");
        Write("Lite.Tests/OtherTests.cs", "public class OtherTests\n{\n    public string Server => \"x\";\n}\n");
        Git("init", "-q");
        Git("add", "-A");
        Git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "base");
        Git("tag", "base");
    }

    public void Dispose()
    {
        try
        {
            foreach (var f in Directory.EnumerateFiles(_repo, "*", SearchOption.AllDirectories))
            {
                File.SetAttributes(f, FileAttributes.Normal); // git marks its object files read-only
            }

            Directory.Delete(_repo, recursive: true);
        }
        catch (IOException)
        {
        }
        catch (UnauthorizedAccessException)
        {
        }
    }

    private void Write(string rel, string text)
    {
        var path = Path.Combine(_repo, rel);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, text);
    }

    /// <summary>The planted, uncommitted change: a method added to the product class.</summary>
    private void ChangeProbe(string member) =>
        Write("Lite/Services/Probe.cs",
            "namespace Lite.Services;\n\npublic sealed class ProbeService\n{\n    public int ReadThing() => 1;\n\n    " + member + "\n}\n");

    private static string Script() => Path.Combine(ParitySource.RepoRoot(), ".github", "scripts", "ci-select.py");

    private static (int ExitCode, string Output) Run(string workingDirectory, string file, params string[] args)
    {
        var psi = new ProcessStartInfo(file)
        {
            WorkingDirectory = workingDirectory,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var a in args)
        {
            psi.ArgumentList.Add(a);
        }

        using var p = Process.Start(psi) ?? throw new InvalidOperationException(file + " did not start");
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        Assert.True(p.WaitForExit(120_000), file + " did not finish in two minutes");
        return (p.ExitCode, stdout.Result + stderr.Result);
    }

    private void Git(params string[] args)
    {
        var (code, output) = Run(_repo, "git", args);
        Assert.True(code == 0, "git " + string.Join(' ', args) + " failed:\n" + output);
    }

    private (int ExitCode, string Output) RunScript(params string[] args) =>
        Run(ParitySource.RepoRoot(), "python", new[] { "-I", Script() }.Concat(args).ToArray());

    private (int ExitCode, string Output) LocalClasses(params string[] extra) =>
        RunScript(new[] { "local-classes", "--base", "base", "--root", _repo }.Concat(extra).ToArray());

    [Fact]
    public void ChangedMethod_SelectsTheClassThatNamesIt_AndMarksTheRigClass()
    {
        ChangeProbe("public int FrobnicateWidget() => 2;");

        var (code, output) = LocalClasses();

        Assert.True(code == 0, output);
        Assert.Contains("lite NamesProbeTests FrobnicateWidget\n", output.Replace("\r", "", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.Contains("lite LiveProbeTests FrobnicateWidget  needs rig", output, StringComparison.Ordinal);
        Assert.DoesNotContain("OtherTests", output, StringComparison.Ordinal);
        Assert.Contains("RUN lite: -class \"*.NamesProbeTests\"", output, StringComparison.Ordinal);
        Assert.Contains("RUN lite (needs rig, DARLING_TEST_PG): -class \"*.LiveProbeTests\"", output, StringComparison.Ordinal);
    }

    [Fact]
    public void StopWord_SelectsNothing()
    {
        // OtherTests names Server, but Server is on the stop list: it says nothing about which class to run.
        ChangeProbe("public string Server => \"y\";");

        var (code, output) = LocalClasses();

        Assert.True(code == 0, output);
        Assert.Contains("no test class names a changed symbol", output, StringComparison.Ordinal);
        Assert.DoesNotContain("OtherTests", output, StringComparison.Ordinal);
    }

    [Fact]
    public void TokenOverTheCap_IsDropped_AndPrintedAsNoisy()
    {
        ChangeProbe("public int FrobnicateWidget() => 2;");

        // Two test files name the token; a cap of 1 makes it noisy and it selects nothing.
        var (code, output) = LocalClasses("--cap", "1");

        Assert.True(code == 0, output);
        Assert.Contains("NOISY FrobnicateWidget 2", output, StringComparison.Ordinal);
        Assert.DoesNotContain("NamesProbeTests", output, StringComparison.Ordinal);
        Assert.Contains("no test class names a changed symbol", output, StringComparison.Ordinal);
    }

    [Fact]
    public void OverThreeHundredClasses_PrintsTheFullLine_AndListsTheFirstThreeHundred()
    {
        var many = new StringBuilder();
        for (var i = 0; i < 301; i++)
        {
            many.Append("public class Many").Append(i).Append("Tests { void T() => Widgetize(); }\n");
        }

        Write("Lite.Tests/ManyTests.cs", many.ToString());
        Git("add", "-A");
        Git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "many");
        Git("tag", "-f", "base");
        ChangeProbe("public void Widgetize() { }");

        var (code, output) = LocalClasses();

        Assert.True(code == 0, output);
        Assert.Contains("301 classes: CI will run FULL; run the first 300 below", output, StringComparison.Ordinal);
        var classLines = output.Split('\n').Count(l => l.StartsWith("lite Many", StringComparison.Ordinal));
        Assert.Equal(300, classLines);
    }

    [Fact]
    public void MapSelect_WithTheBase_AddsTheClassThatNamesAChangedSymbol()
    {
        ChangeProbe("public int FrobnicateWidget() => 2;");
        var map = Path.Combine(_repo, "..", Path.GetFileName(_repo) + "-map.json");
        File.WriteAllText(map, """
            {"schema": 1, "sha": "0000000", "built_at": "@BUILT@", "tool": "synthetic",
             "files": ["Lite/Services/Probe.cs", "Lite.Tests/NamesProbeTests.cs", "Lite.Tests/OtherTests.cs",
                       "Lite/Services/Unrelated.cs", "Lite.Tests/LiveProbeTests.cs"],
             "classes": {"lite": {
                "NamesProbeTests": {"own": 1, "files": [3], "seconds": 1.0},
                "LiveProbeTests":  {"own": 4, "files": [3], "seconds": 1.0},
                "OtherTests":      {"own": 2, "files": [0], "seconds": 1.0}}},
             "text_patterns": {}}
            """.Replace("@BUILT@", DateTime.UtcNow.ToString("o"), StringComparison.Ordinal));
        try
        {
            var args = new[] { "map-select", "--map", map, "--root", _repo, "--drift-file", "-", "Lite/Services/Probe.cs" };

            var (withCode, withBase) = RunScript(args.Concat(new[] { "--base", "base" }).ToArray());
            var (_, without) = RunScript(args);

            Assert.True(withCode == 0, withBase);
            var why = Json(withBase).GetProperty("why").GetProperty("lite");
            Assert.Equal("covers a changed file", why.GetProperty("OtherTests").GetString());
            Assert.Equal("names a changed symbol", why.GetProperty("NamesProbeTests").GetString());
            Assert.Equal("names a changed symbol", why.GetProperty("LiveProbeTests").GetString());
            var whyWithout = Json(without).GetProperty("why").GetProperty("lite");
            Assert.False(whyWithout.TryGetProperty("NamesProbeTests", out _), "without --base no class is named:\n" + without);
            Assert.Equal("covers a changed file", whyWithout.GetProperty("OtherTests").GetString());
        }
        finally
        {
            File.Delete(map);
        }
    }

    /// <summary>
    /// #5798: the extractor reads declarations from C# and SQL hunks, a store migration as its V-number, and nothing
    /// else: not a call, not a parameter, not a name only a test file declares.
    /// </summary>
    [Fact]
    public void SymbolExtractor_ReadsTheDeclarationsOfCSharpAndSqlHunks()
    {
        var diff = string.Join('\n',
            "diff --git a/Darling/Store/PgMigrations.cs b/Darling/Store/PgMigrations.cs",
            "--- a/Darling/Store/PgMigrations.cs",
            "+++ b/Darling/Store/PgMigrations.cs",
            "@@ -10,0 +11,4 @@ public static class PgMigrations",
            "+    private const string V184Sql = @\"",
            "+    public static int CountWidgetSpans(int limit) => limit;",
            "+        return ReadWidgetThing(limit);",
            "+        new Migration(184, \"widget-spans\", V184Sql),",
            "diff --git a/install/52_widget.sql b/install/52_widget.sql",
            "--- a/install/52_widget.sql",
            "+++ b/install/52_widget.sql",
            "@@ -1,0 +1,2 @@",
            "+CREATE TABLE IF NOT EXISTS collect.widget_spans (id int);",
            "+CREATE OR ALTER VIEW report.v_widget_rollup AS SELECT 1;",
            "diff --git a/Lite.Tests/WidgetTests.cs b/Lite.Tests/WidgetTests.cs",
            "--- a/Lite.Tests/WidgetTests.cs",
            "+++ b/Lite.Tests/WidgetTests.cs",
            "@@ -1,0 +1,1 @@",
            "+    public void OnlyInATestFile() { }",
            "");
        var diffFile = Path.Combine(_repo, "fixture.diff");
        File.WriteAllText(diffFile, diff);
        const string Code = "import importlib.util, json, sys\n"
            + "spec = importlib.util.spec_from_file_location('ci_select', sys.argv[1])\n"
            + "m = importlib.util.module_from_spec(spec)\n"
            + "spec.loader.exec_module(m)\n"
            + "tokens, tests = m.changed_symbols(open(sys.argv[2], encoding='utf-8').read())\n"
            + "print(json.dumps({'tokens': sorted(tokens), 'tests': sorted(tests)}))\n";

        var (code, output) = Run(ParitySource.RepoRoot(), "python", "-I", "-c", Code, Script(), diffFile);

        Assert.True(code == 0, output);
        var json = Json(output);
        var tokens = json.GetProperty("tokens").EnumerateArray().Select(t => t.GetString()).ToList();
        Assert.Equal(
            new[] { "CountWidgetSpans", "PgMigrations", "V184", "V184Sql", "v_widget_rollup", "widget_spans" },
            tokens);
        Assert.Equal("Lite.Tests/WidgetTests.cs", Assert.Single(json.GetProperty("tests").EnumerateArray()).GetString());
    }

    /// <summary>
    /// #5798: <c>local-filter</c> prints, per suite, the test executable's class filter and then the class list, and
    /// last the guard set. A helper class with no test of its own and a Stage=Guard class are not in the list.
    /// </summary>
    [Fact]
    public void LocalFilter_PrintsTheClassFilter_ThenTheClassList_ThenTheGuardSet()
    {
        Write("Lite.Tests/HoldsTestsTests.cs",
            "public class HoldsTestsTests\n{\n    [Fact]\n    public void T() => FrobnicateWidget();\n\n"
            + "    private sealed class FakeWidgetHelper\n    {\n        public void F() => FrobnicateWidget();\n    }\n\n"
            + "    [Fact]\n    public void U() { }\n}\n");
        Write("Lite.Tests/WidgetGuardTests.cs",
            "[Trait(\"Stage\", \"Guard\")]\npublic class WidgetGuardTests\n{\n    [Fact]\n    public void T() => FrobnicateWidget();\n}\n");
        Git("add", "-A");
        Git("-c", "user.name=t", "-c", "user.email=t@t", "commit", "-q", "-m", "holders");
        Git("tag", "-f", "base");
        ChangeProbe("public int FrobnicateWidget() => 2;");

        var (code, output) = RunScript("local-filter", "--base", "base", "--root", _repo);

        Assert.True(code == 0, output);
        var text = output.Replace("\r", "", StringComparison.Ordinal);
        // NamesProbeTests and LiveProbeTests name the method but declare no [Fact], so they are not test classes.
        Assert.Contains("lite: -class \"*.HoldsTestsTests\"\nlite classes: HoldsTestsTests\nguard: -trait Stage=Guard\n",
            text, StringComparison.Ordinal);
        Assert.DoesNotContain("FakeWidgetHelper", text, StringComparison.Ordinal);
        Assert.DoesNotContain("WidgetGuardTests", text, StringComparison.Ordinal);
        Assert.Contains("no --map:", text, StringComparison.Ordinal);
    }

    private static JsonElement Json(string output)
    {
        var start = output.IndexOf('{', StringComparison.Ordinal);
        Assert.True(start >= 0, "map-select printed no JSON:\n" + output);
        return JsonDocument.Parse(output[start..]).RootElement.Clone();
    }
}
