/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text.Json;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the class-to-file map selection in .github/scripts/ci-select.py (#5459, change 4, slice 1).
///
/// <para><b>Why.</b> <c>map-select</c> will decide which test classes a pull request runs, so its failure mode must
/// be "run everything", never "run nothing". These cases use a tiny synthetic map and a tiny synthetic test tree
/// (so they do not depend on today's repository) and pin the rule for each input: the class's own file, a file it
/// covers, a file changed since the map was built, a text pattern, a class the map has never seen, and every
/// reason the answer must be FULL (a missing, wrong-schema, old or over-drifted map, a build file, a file the map
/// knows nothing about, and any event but a pull request). Nothing in build.yml calls <c>map-select</c> yet.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class CiSelectMapSelectGuardTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ci-map-select-" + Guid.NewGuid().ToString("N"));

    public CiSelectMapSelectGuardTests()
    {
        // The synthetic test tree: one class per file, one of them a Guard class, one the map never saw.
        var tests = Path.Combine(_dir, "tree", "Lite.Tests");
        Directory.CreateDirectory(tests);
        File.WriteAllText(Path.Combine(tests, "Own.cs"), "public class OwnTests { }\n");
        File.WriteAllText(Path.Combine(tests, "Cov.cs"), "public class CoverTests { }\n");
        File.WriteAllText(Path.Combine(tests, "Text.cs"), "public class TextTests { }\n");
        File.WriteAllText(Path.Combine(tests, "Fresh.cs"), "public class FreshTests\n{" + AFact + "}\n");
        File.WriteAllText(Path.Combine(tests, "G.cs"), "[Trait(\"Stage\", \"Guard\")]\npublic class GateTests\n{" + AFact + "}\n");
        // #5872: a helper type a test file declares holds no test, so it is never a "new class".
        File.WriteAllText(Path.Combine(tests, "Helper.cs"), "public sealed class CapturingThing : IDisposable { public void Dispose() { } }\n");
    }

    /// <summary>One test method, so the tree scan sees the class holds a test (#5872).</summary>
    private const string AFact = "\n    [Fact]\n    public void Runs() { }\n";

    public void Dispose()
    {
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch (IOException)
        {
        }
    }

    private static string Script() => Path.Combine(ParitySource.RepoRoot(), ".github", "scripts", "ci-select.py");

    private string WriteMap(int schema = 1, string? builtAt = null, string textPatterns = "\"install/**/*.sql\"")
    {
        var path = Path.Combine(_dir, "map-" + Guid.NewGuid().ToString("N") + ".json");
        builtAt ??= DateTime.UtcNow.ToString("o");
        // ids: 0 Lite/Services/A.cs, 1 Lite/Services/B.cs, 2 Own.cs, 3 assets/data.json, 4 Cov.cs, 5 Text.cs
        File.WriteAllText(path, """
            {"schema": @SCHEMA@, "sha": "0000000", "built_at": "@BUILT@", "tool": "synthetic",
             "files": ["Lite/Services/A.cs", "Lite/Services/B.cs", "Lite.Tests/Own.cs", "assets/data.json",
                       "Lite.Tests/Cov.cs", "Lite.Tests/Text.cs"],
             "classes": {"lite": {
                "OwnTests":   {"own": 2, "files": [0], "seconds": 1.5},
                "CoverTests": {"own": 4, "files": [1, 3], "seconds": 2.5},
                "TextTests":  {"own": 5, "files": [], "seconds": 4.0}}},
             "text_patterns": {"TextTests": [@PATTERNS@]}}
            """.Replace("@SCHEMA@", schema.ToString(System.Globalization.CultureInfo.InvariantCulture), StringComparison.Ordinal)
                .Replace("@BUILT@", builtAt, StringComparison.Ordinal)
                .Replace("@PATTERNS@", textPatterns, StringComparison.Ordinal));
        return path;
    }

    private (int ExitCode, JsonElement Result) MapSelect(string map, string[] files, string[]? drift = null,
        string eventName = "pull_request", string? root = null, string? baseRef = null, bool guardStageSeparate = false)
    {
        var psi = new ProcessStartInfo("python")
        {
            WorkingDirectory = ParitySource.RepoRoot(),
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("-I");
        psi.ArgumentList.Add(Script());
        psi.ArgumentList.Add("map-select");
        psi.ArgumentList.Add("--map");
        psi.ArgumentList.Add(map);
        psi.ArgumentList.Add("--root");
        psi.ArgumentList.Add(root ?? Path.Combine(_dir, "tree"));
        psi.ArgumentList.Add("--event");
        psi.ArgumentList.Add(eventName);
        if (guardStageSeparate)
        {
            psi.ArgumentList.Add("--guard-stage-separate");
        }

        if (baseRef is not null)
        {
            psi.ArgumentList.Add("--base-ref");
            psi.ArgumentList.Add(baseRef);
        }

        psi.ArgumentList.Add("--drift-file");
        if (drift is null || drift.Length == 0)
        {
            psi.ArgumentList.Add("-");
        }
        else
        {
            var driftPath = Path.Combine(_dir, "drift-" + Guid.NewGuid().ToString("N") + ".txt");
            File.WriteAllLines(driftPath, drift);
            psi.ArgumentList.Add(driftPath);
        }

        foreach (var f in files)
        {
            psi.ArgumentList.Add(f);
        }

        using var p = Process.Start(psi) ?? throw new InvalidOperationException("python did not start");
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        Assert.True(p.WaitForExit(120_000), "ci-select.py did not finish in two minutes");
        var text = stdout.Result;
        var start = text.IndexOf('{', StringComparison.Ordinal);
        Assert.True(start >= 0, "map-select printed no JSON:\n" + text + stderr.Result);
        return (p.ExitCode, JsonDocument.Parse(text[start..]).RootElement.Clone());
    }

    private static Dictionary<string, string> Why(JsonElement result)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        if (result.GetProperty("why").TryGetProperty("lite", out var lite))
        {
            foreach (var c in lite.EnumerateObject())
            {
                map[c.Name] = c.Value.GetString() ?? "";
            }
        }

        return map;
    }

    private static string[] Selected(JsonElement result)
    {
        return result.GetProperty("selected").TryGetProperty("lite", out var lite)
            ? lite.EnumerateArray().Select(e => e.GetString() ?? "").OrderBy(n => n, StringComparer.Ordinal).ToArray()
            : [];
    }

    private static void AssertFull(JsonElement result, string reasonPart)
    {
        Assert.True(result.GetProperty("full").GetBoolean(), "expected FULL:\n" + result);
        Assert.Contains(reasonPart, result.GetProperty("reason").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void ClassOwnFile_Changed_SelectsThatClassOnly()
    {
        var (code, r) = MapSelect(WriteMap(), new[] { "Lite.Tests/Own.cs" });

        Assert.Equal(0, code);
        Assert.False(r.GetProperty("full").GetBoolean());
        var why = Why(r);
        Assert.Equal("own file changed", why["OwnTests"]);
        Assert.DoesNotContain("CoverTests", why.Keys);
        Assert.DoesNotContain("TextTests", why.Keys);
    }

    [Fact]
    public void CoveredFile_Changed_SelectsTheClassesThatRunIt()
    {
        var (_, r) = MapSelect(WriteMap(), new[] { "Lite/Services/B.cs" });

        var why = Why(r);
        Assert.Equal("covers a changed file", why["CoverTests"]);
        Assert.DoesNotContain("OwnTests", why.Keys);
        Assert.Equal(2.5, r.GetProperty("seconds").GetProperty("lite").GetDouble());
    }

    [Fact]
    public void DataFile_InTheMapsUniverse_SelectsItsCoveringClass_AndNeverFull()
    {
        var (_, r) = MapSelect(WriteMap(), new[] { "assets/data.json" });

        Assert.False(r.GetProperty("full").GetBoolean());
        Assert.Equal("covers a changed file", Why(r)["CoverTests"]);
    }

    [Fact]
    public void FileChangedSinceTheMap_CountsAsChanged_WhenThePullRequestChangesProductSource()
    {
        // #5872: B.cs is product source the map knows, so a drifted file may have grown a call edge into it; A.cs changed
        // between the map and the merge base, and the class that runs A.cs is selected for it.
        var (_, r) = MapSelect(WriteMap(), new[] { "Lite/Services/B.cs" }, new[] { "Lite/Services/A.cs" });

        Assert.Equal("covers a changed file", Why(r)["OwnTests"]);
        Assert.Equal("Lite/Services/A.cs", r.GetProperty("via").GetProperty("lite").GetProperty("OwnTests").GetString());
        Assert.Equal(1, r.GetProperty("drift").GetInt32());
    }

    [Theory]
    [InlineData("README.md")]
    [InlineData("docs/alerts.md")]
    [InlineData("llms.txt")]
    [InlineData("Lite.Tests/Own.cs")]
    [InlineData(".github/scripts/pre-push.sh")]
    public void DocsScriptOrLeafTestChange_GetsNoDrift(string file)
    {
        // #5872: nothing grows a call edge into a doc, a script under .github/ or a test file, so the drift (A.cs and B.cs
        // changed on dev since the map) selects nothing for this pull request.
        var (_, r) = MapSelect(WriteMap(), new[] { file }, new[] { "Lite/Services/A.cs", "Lite/Services/B.cs" });

        Assert.False(r.GetProperty("full").GetBoolean(), r.ToString());
        var why = Why(r);
        Assert.DoesNotContain("CoverTests", why.Keys);
        Assert.NotEqual("covers a changed file", why.GetValueOrDefault("OwnTests"));
        Assert.Equal(0, r.GetProperty("drift").GetInt32());
    }

    [Fact]
    public void DocsOnly_SelectsOnlyTheClassThatReadsTheDoc()
    {
        // #5872 (the shape of 1ec666f50): README.md and docs pages. TextTests reads README.md by name, so it still runs.
        // #5894: nothing else does: CI's docs fast path runs no test for such a change, so the Guard and new classes
        // that every other narrowed run keeps are left out.
        var map = WriteMap(textPatterns: "\"README.md\"");

        var (_, r) = MapSelect(map, new[] { "README.md", "docs/alerts.md", "llms.txt" }, new[] { "Lite/Services/B.cs" });

        Assert.False(r.GetProperty("full").GetBoolean(), r.ToString());
        Assert.True(r.GetProperty("docs_only").GetBoolean(), r.ToString());
        Assert.Equal(new Dictionary<string, string> { ["TextTests"] = "text pattern" }, Why(r));
        Assert.Equal(["TextTests"], Selected(r));
    }

    [Fact]
    public void DocsOnly_ThatNoClassReads_SelectsNothing()
    {
        // #5894: a docs-only diff (one clause in a skill file) ran 367 classes locally while CI ran none. The guard stage,
        // the classes the map never saw and the environment-gated classes are not reasons to run for a document.
        var (_, r) = MapSelect(WriteMap(), new[] { ".claude/skills/lane-orders/SKILL.md" });

        Assert.False(r.GetProperty("full").GetBoolean(), r.ToString());
        Assert.True(r.GetProperty("docs_only").GetBoolean(), r.ToString());
        Assert.Contains("documentation-only", r.GetProperty("reason").GetString(), StringComparison.Ordinal);
        Assert.Empty(Selected(r));
        Assert.Empty(Why(r));
    }

    [Fact]
    public void DocsAndCode_Together_IsNotDocsOnly_AndKeepsTheGuardClass()
    {
        var (_, r) = MapSelect(WriteMap(), new[] { ".claude/skills/lane-orders/SKILL.md", "Lite.Tests/Own.cs" });

        Assert.False(r.GetProperty("docs_only").GetBoolean(), r.ToString());
        Assert.Contains("GateTests", Selected(r));
        Assert.Contains("OwnTests", Selected(r));
    }

    [Fact]
    public void GuardStageSeparate_LeavesGuardClassesOutOfTheSelection_ButOnlyWhenAsked()
    {
        // #5894: the pre-push gate runs the whole Stage=Guard trait as its own stage, so the selection's batches ran every
        // Guard class a second time. With the flag the Guard class is not selected (its `why` entry stays for the
        // summary); without it, the selection is as before (CI's shards still need it).
        var files = new[] { "Lite.Tests/Own.cs" };

        var (_, withFlag) = MapSelect(WriteMap(), files, guardStageSeparate: true);
        var (_, without) = MapSelect(WriteMap(), files);

        Assert.DoesNotContain("GateTests", Selected(withFlag));
        Assert.Equal("run in the guard stage", Why(withFlag)["GateTests"]);
        Assert.Equal(["FreshTests", "OwnTests"], Selected(withFlag));
        Assert.Contains("GateTests", Selected(without));
        Assert.Equal("new class", Why(without)["GateTests"]);
    }

    [Fact]
    public void UnknownScriptUnderDotGithub_IsNamed_AndSelectsNothingOfItsOwn()
    {
        // #5872 (the shape of aa4b889a9): a script no class runs or reads is reported by name with the rule that caught
        // it, and the selection is not FULL.
        var (_, r) = MapSelect(WriteMap(), new[] { ".github/scripts/tests/new-test.sh" });

        Assert.False(r.GetProperty("full").GetBoolean(), r.ToString());
        Assert.Equal("selecting the classes that read it as text (none)",
            r.GetProperty("unknown").GetProperty(".github/scripts/tests/new-test.sh").GetString());
        Assert.Equal(["FreshTests", "GateTests"], Why(r).Keys.OrderBy(k => k, StringComparer.Ordinal));
    }

    [Fact]
    public void AHelperTypeWithNoTest_IsNeverANewClass()
    {
        // #5872: 377 Darling and 143 Lite helper types (fakes, loggers, harnesses) were "new classes" on every pull request.
        var (_, r) = MapSelect(WriteMap(), new[] { "Lite.Tests/Own.cs" });

        Assert.DoesNotContain("CapturingThing", Why(r).Keys);
    }

    [Fact]
    public void NewProductCsFile_NotInTheMap_RunsEverything()
    {
        // Reflection, DI and attribute scanning find types no covered file mentions.
        var (_, r) = MapSelect(WriteMap(), new[] { "Lite/Services/Brand.cs" }, new[] { "Lite/Services/A.cs" });

        AssertFull(r, "unknown file: Lite/Services/Brand.cs: selecting everything");
    }

    [Fact]
    public void NewTestProjectFile_WithADiscoveredClass_SelectsThatClass_AndIsNotFull()
    {
        var (_, r) = MapSelect(WriteMap(), new[] { "Lite.Tests/Fresh.cs" });

        Assert.False(r.GetProperty("full").GetBoolean());
        Assert.Equal("new class", Why(r)["FreshTests"]);
    }

    [Fact]
    public void TextPattern_Hit_SelectsTheClassThatReadsTheFile()
    {
        var (_, r) = MapSelect(WriteMap(), new[] { "install/05_x.sql" });

        Assert.False(r.GetProperty("full").GetBoolean());
        var why = Why(r);
        Assert.Equal("text pattern", why["TextTests"]);
        Assert.DoesNotContain("OwnTests", why.Keys);
    }

    [Fact]
    public void ClassTheMapNeverSaw_IsSelected_AndGuardClassesStay()
    {
        var (_, r) = MapSelect(WriteMap(), new[] { "Lite.Tests/Own.cs" });

        var why = Why(r);
        Assert.Equal("new class", why["FreshTests"]);
        Assert.Equal("new class", why["GateTests"]);
    }

    [Fact]
    public void DocumentationOnly_SelectsNoMappedClass_ButIsNotFull()
    {
        var (_, r) = MapSelect(WriteMap(), new[] { "README.md" });

        Assert.False(r.GetProperty("full").GetBoolean());
        var why = Why(r);
        Assert.DoesNotContain("OwnTests", why.Keys);
        Assert.DoesNotContain("CoverTests", why.Keys);
        Assert.DoesNotContain("TextTests", why.Keys);
    }

    [Theory]
    [InlineData("Directory.Build.props")]
    [InlineData(".github/workflows/build.yml")]
    [InlineData("Lite/Lite.csproj")]
    [InlineData("global.json")]
    [InlineData("PerformanceMonitor.Common/Anything.cs")]
    public void KeepFullFile_InThePullRequest_RunsEverything(string file)
    {
        var (_, r) = MapSelect(WriteMap(), new[] { "Lite.Tests/Own.cs", file });

        AssertFull(r, "keep-full");
    }

    [Fact]
    public void KeepFullFile_OnlyInTheDrift_GoesThroughTheClassLinks_AndIsNotFull()
    {
        // #5459: build.yml changed on dev after the map, and the pull request never touched it. A class that reads it
        // as text is selected; the rest are not, and the run is not FULL.
        var map = WriteMap(textPatterns: "\"install/**/*.sql\", \".github/workflows/build.yml\"");

        // A.cs (product source) is in the change, so the drift counts (#5872).
        var (_, r) = MapSelect(map, new[] { "Lite.Tests/Own.cs", "Lite/Services/A.cs" }, new[] { ".github/workflows/build.yml" });

        Assert.False(r.GetProperty("full").GetBoolean(), r.ToString());
        var why = Why(r);
        Assert.Equal("text pattern", why["TextTests"]);
        Assert.Equal("own file changed", why["OwnTests"]);
        Assert.DoesNotContain("CoverTests", why.Keys);
    }

    [Fact]
    public void KeepFullFile_InTheDrift_ThatNoClassKnows_IsFullForThatReason_NotAsAKeepFullFile()
    {
        var (_, r) = MapSelect(WriteMap(), new[] { "Lite.Tests/Own.cs", "Lite/Services/A.cs" }, new[] { "Directory.Packages.props" });

        AssertFull(r, "unknown file: Directory.Packages.props: selecting everything");
    }

    [Fact]
    public void KeepFullFile_InBoth_TheDriftAndThePullRequest_IsFullBecauseOfThePullRequest()
    {
        var map = WriteMap(textPatterns: "\".github/workflows/build.yml\"");

        var (_, r) = MapSelect(map, new[] { ".github/workflows/build.yml" }, new[] { ".github/workflows/build.yml" });

        AssertFull(r, ".github/workflows/build.yml is a keep-full file");
    }

    [Fact]
    public void ANewTestClass_AddedSinceTheMap_AlwaysRuns_EvenWhenOnlyDriftBroughtIt()
    {
        // The drift holds the new test file; the pull request's own change is unrelated. The class is not in the map
        // and the tree has it, so it runs as a "new class" and nothing else about the run is FULL.
        var (_, r) = MapSelect(WriteMap(), new[] { "Lite.Tests/Own.cs" }, new[] { "Lite.Tests/Fresh.cs" });

        Assert.False(r.GetProperty("full").GetBoolean(), r.ToString());
        Assert.Equal("new class", Why(r)["FreshTests"]);
    }

    [Fact]
    public void ATreeScan_ThatFindsNoClass_RunsEverything_SoNoNewClassIsLeftOutSilently()
    {
        var empty = Path.Combine(_dir, "empty-tree");
        Directory.CreateDirectory(empty);

        var (_, r) = MapSelect(WriteMap(), new[] { "Lite.Tests/Own.cs" }, root: empty);

        AssertFull(r, "no test class was found in the tree for suite lite");
    }

    [Fact]
    public void UnknownNonCsFile_RunsEverything()
    {
        var (_, r) = MapSelect(WriteMap(), new[] { "Lite.Tests/Own.cs", "somewhere/new-fixture.dat" });

        AssertFull(r, "somewhere/new-fixture.dat");
    }

    [Fact]
    public void MissingMap_RunsEverything()
    {
        var (_, r) = MapSelect(Path.Combine(_dir, "no-such-map.json"), new[] { "Lite.Tests/Own.cs" });

        AssertFull(r, "no test map");
    }

    [Fact]
    public void UnreadableMap_RunsEverything()
    {
        var bad = Path.Combine(_dir, "bad.json");
        File.WriteAllText(bad, "{ not json");

        var (_, r) = MapSelect(bad, new[] { "Lite.Tests/Own.cs" });

        AssertFull(r, "no test map");
    }

    [Fact]
    public void WrongSchema_RunsEverything()
    {
        var (_, r) = MapSelect(WriteMap(schema: 2), new[] { "Lite.Tests/Own.cs" });

        AssertFull(r, "schema");
    }

    [Fact]
    public void MapOlderThanAWeek_RunsEverything()
    {
        var old = DateTime.UtcNow.AddDays(-8).ToString("o");

        var (_, r) = MapSelect(WriteMap(builtAt: old), new[] { "Lite.Tests/Own.cs" });

        AssertFull(r, "older than");
    }

    [Fact]
    public void DriftOverThreeHundredFiles_RunsEverything()
    {
        var drift = Enumerable.Range(0, 301).Select(i => $"Lite/Drift/F{i}.cs").ToArray();

        var (_, r) = MapSelect(WriteMap(), new[] { "Lite.Tests/Own.cs" }, drift);

        AssertFull(r, "301 files changed since the map");
    }

    [Theory]
    [InlineData("push")]
    [InlineData("merge_group")]
    [InlineData("release")]
    public void NonPullRequestEvent_StaysFull(string eventName)
    {
        var (_, r) = MapSelect(WriteMap(), new[] { "Lite.Tests/Own.cs" }, eventName: eventName);

        AssertFull(r, "always runs everything");
    }

    [Fact]
    public void PullRequestIntoMain_IsFull_WhereTheSameOneIntoDevIsNarrowed()
    {
        // #5459: a good fresh map, zero drift and a one-file diff the map knows. Into dev it narrows; into main (the
        // release gate) it always runs everything.
        var map = WriteMap();
        var files = new[] { "Lite.Tests/Own.cs" };

        var (_, intoMain) = MapSelect(map, files, baseRef: "main");
        AssertFull(intoMain, "a pull request into main always runs everything");
        Assert.Equal("a pull request into main always runs everything", intoMain.GetProperty("reason").GetString());

        var (_, intoDev) = MapSelect(map, files, baseRef: "dev");
        Assert.False(intoDev.GetProperty("full").GetBoolean(), "a pull request into dev must still narrow:\n" + intoDev);
        Assert.Equal("own file changed", Why(intoDev)["OwnTests"]);

        // No base branch at all (the replay corpus has none) narrows as before.
        var (_, noBase) = MapSelect(map, files);
        Assert.False(noBase.GetProperty("full").GetBoolean(), "an absent base branch keeps today's behaviour:\n" + noBase);
    }

    [Fact]
    public void AClassGatedOnAnEnvironmentTheMapShardsLack_IsAlwaysSelected_ForAnUnrelatedChange()
    {
        // #5459: the map shards set only DARLING_TEST_PG and DARLING_TEST_PGRUNTIME, so a class gated on any other
        // DARLING_TEST_PG_* / DARLING_TEST_PGRUNTIME_* variable skips at map build and the map has no coverage edge for what
        // its tests would run. The tree scan finds those classes from the source, so a change to code under one still runs it.
        var tree = Path.Combine(_dir, "gated-tree");
        var tests = Path.Combine(tree, "Lite.Tests");
        Directory.CreateDirectory(tests);
        File.WriteAllText(Path.Combine(tests, "Gated.cs"),
            "public class GatedTests\n{" + AFact + "string? T => Environment.GetEnvironmentVariable(\"DARLING_TEST_PG_CSVLOG\"); }\n");
        File.WriteAllText(Path.Combine(tests, "Plain.cs"),
            "public class PlainTests\n{" + AFact + "string? T => Environment.GetEnvironmentVariable(\"DARLING_TEST_PG\"); }\n");
        File.WriteAllText(Path.Combine(tests, "Runtime.cs"),
            "public class RuntimeTests\n{" + AFact + "string? T => Environment.GetEnvironmentVariable(\"DARLING_TEST_PGRUNTIME\"); }\n");
        File.WriteAllText(Path.Combine(tests, "Upgrade.cs"),
            "public class UpgradeTests\n{" + AFact + "string? T => Environment.GetEnvironmentVariable(\"DARLING_TEST_PGRUNTIME_OLD\"); }\n");
        File.WriteAllText(Path.Combine(tests, "Comment.cs"),
            "/// <c>DARLING_TEST_PG_CSVLOG</c> in a comment only\npublic class CommentTests\n{" + AFact + "}\n");
        var map = Path.Combine(_dir, "gated-map.json");
        File.WriteAllText(map, """
            {"schema": 1, "sha": "0000000", "built_at": "@BUILT@", "tool": "synthetic",
             "files": ["Lite/Services/A.cs", "Lite/Services/B.cs", "Lite.Tests/Gated.cs", "Lite.Tests/Plain.cs",
                       "Lite.Tests/Runtime.cs", "Lite.Tests/Upgrade.cs", "Lite.Tests/Comment.cs"],
             "classes": {"lite": {
                "GatedTests":   {"own": 2, "files": [0], "seconds": 0.01},
                "PlainTests":   {"own": 3, "files": [0], "seconds": 1.0},
                "RuntimeTests": {"own": 4, "files": [0], "seconds": 1.0},
                "UpgradeTests": {"own": 5, "files": [0], "seconds": 1.0},
                "CommentTests": {"own": 6, "files": [0], "seconds": 1.0}}},
             "text_patterns": {}}
            """.Replace("@BUILT@", DateTime.UtcNow.ToString("o"), StringComparison.Ordinal));

        // B.cs is in the map's universe but covered by no class: no class would be selected from the map alone.
        var (_, r) = MapSelect(map, new[] { "Lite/Services/B.cs" }, root: tree);

        Assert.False(r.GetProperty("full").GetBoolean(), r.ToString());
        var why = Why(r);
        Assert.Equal(["GatedTests", "UpgradeTests"], why.Keys.OrderBy(k => k, StringComparer.Ordinal));
        Assert.Equal("gated on an environment the map shards lack", why["GatedTests"]);
    }

    [Fact]
    public void OneTestFileChange_SelectsThatClass_ItsCollectionMates_AndNothingElse()
    {
        // #5872 (the shape of a5b6360e3): one test file whose [Collection] attribute changed. Its class and the classes
        // in the same xunit collection run; a class in another collection, a class the walk does not reach and the
        // drift (product files changed on dev since the map) do not. Live0.cs changed too: its collection of 61 is over the
        // mate cap (live-postgres, about 650 in the tree), so it adds no mates.
        var tree = Path.Combine(_dir, "coll-tree");
        var tests = Path.Combine(tree, "Lite.Tests");
        Directory.CreateDirectory(tests);
        File.WriteAllText(Path.Combine(tests, "Moved.cs"),
            "[Collection(\"app-logger-statics\")]\npublic class MovedTests\n{" + AFact + "}\n");
        File.WriteAllText(Path.Combine(tests, "Mate.cs"), "[Collection(\"app-logger-statics\")]\npublic class MateTests\n{" + AFact + "}\n");
        File.WriteAllText(Path.Combine(tests, "Other.cs"), "[Collection(\"other\")]\npublic class OtherTests\n{" + AFact + "}\n");
        for (var i = 0; i < 61; i++)
        {
            File.WriteAllText(Path.Combine(tests, $"Live{i}.cs"), $"[Collection(\"live-postgres\")]\npublic class Live{i}Tests\n{{" + AFact + "}\n");
        }

        var files = string.Join(", ", new[] { "\"Lite/Services/A.cs\"", "\"Lite.Tests/Moved.cs\"", "\"Lite.Tests/Mate.cs\"", "\"Lite.Tests/Other.cs\"" }
            .Concat(Enumerable.Range(0, 61).Select(i => $"\"Lite.Tests/Live{i}.cs\"")));
        var classes = string.Join(", ", new[] { "\"MovedTests\": {\"own\": 1, \"files\": [0]}", "\"MateTests\": {\"own\": 2, \"files\": [0]}",
                "\"OtherTests\": {\"own\": 3, \"files\": [0]}" }
            .Concat(Enumerable.Range(0, 61).Select(i => $"\"Live{i}Tests\": {{\"own\": {i + 4}, \"files\": [0]}}")));
        var map = Path.Combine(_dir, "coll-map.json");
        File.WriteAllText(map, "{\"schema\": 1, \"sha\": \"0000000\", \"built_at\": \"" + DateTime.UtcNow.ToString("o") + "\", \"tool\": \"synthetic\", "
            + "\"files\": [" + files + "], \"classes\": {\"lite\": {" + classes + "}}, \"text_patterns\": {}}");

        var (_, r) = MapSelect(map, new[] { "Lite.Tests/Moved.cs", "Lite.Tests/Live0.cs" }, new[] { "Lite/Services/A.cs" }, root: tree);

        Assert.False(r.GetProperty("full").GetBoolean(), r.ToString());
        Assert.Equal(new Dictionary<string, string>
        {
            ["MateTests"] = "shares a collection with a changed class",
            ["Live0Tests"] = "own file changed",
            ["MovedTests"] = "own file changed",
        }, Why(r));
    }

    [Fact]
    public void TheMapShardsEnvironment_AndTheGateRule_CannotDriftApart()
    {
        // ci-select.py's MAP_SHARD_ENV must be exactly what nightly.yml's map shards set, and every DARLING_TEST_* variable
        // either workflow sets must be a map-shard variable or one the gate rule finds in the source (the PG_ and PGRUNTIME_
        // families). A new CI-set gate variable outside both fails here instead of silently never selecting its class.
        var root = ParitySource.RepoRoot();
        var script = File.ReadAllText(Path.Combine(root, ".github", "scripts", "ci-select.py"));
        var declared = System.Text.RegularExpressions.Regex.Match(script, "MAP_SHARD_ENV = frozenset\\(\\{([^}]*)\\}\\)");
        Assert.True(declared.Success, "ci-select.py no longer declares MAP_SHARD_ENV");
        var mapEnv = System.Text.RegularExpressions.Regex.Matches(declared.Groups[1].Value, "\"(DARLING_TEST_\\w+)\"")
            .Select(m => m.Groups[1].Value).OrderBy(v => v, StringComparer.Ordinal).ToList();

        var nightly = File.ReadAllLines(Path.Combine(root, ".github", "workflows", "nightly.yml"));
        var step = Array.FindIndex(nightly, l => l.Contains("- name: Run this shard's classes (instrumented", StringComparison.Ordinal));
        Assert.True(step >= 0, "nightly.yml no longer has the map shards' run step");
        var stepKeys = new List<string>();
        foreach (var line in nightly.Skip(step + 1))
        {
            if (line.TrimStart().StartsWith("- name:", StringComparison.Ordinal))
            {
                break;
            }

            var key = System.Text.RegularExpressions.Regex.Match(line, "^\\s+(DARLING_TEST_\\w+):");
            if (key.Success)
            {
                stepKeys.Add(key.Groups[1].Value);
            }
        }

        Assert.Equal(mapEnv, stepKeys.Distinct().OrderBy(v => v, StringComparer.Ordinal));

        var gateShape = new System.Text.RegularExpressions.Regex("^DARLING_TEST_PG(RUNTIME)?_[A-Z0-9_]+$");
        foreach (var wf in new[] { "build.yml", "nightly.yml" })
        {
            foreach (var line in File.ReadAllLines(Path.Combine(root, ".github", "workflows", wf)))
            {
                var key = System.Text.RegularExpressions.Regex.Match(line, "^\\s+(DARLING_TEST_\\w+):");
                if (key.Success)
                {
                    var name = key.Groups[1].Value;
                    Assert.True(mapEnv.Contains(name) || gateShape.IsMatch(name),
                        wf + " sets " + name + ", which is neither a map-shard variable nor one ci-select.py's GATE_ENV_LITERAL finds");
                }
            }
        }
    }
}
