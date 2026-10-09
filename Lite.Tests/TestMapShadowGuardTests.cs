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
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the test-class map's selection and its shadow check (#5459, change 4): the rules that close the holes the
/// replay found, the shadow-check row, and the workflow wiring that lets exactly three jobs narrow their classes.
///
/// <para><b>Why.</b> The selection job computes the selection for every pull request, and the Darling PG shards, the
/// Lite shards and the build job's no-store Darling pass keep only the classes it names (the shard scripts' `map-keep`
/// call). The one thing it must never do is run LESS than the full suite by accident, so the pins below fail if a
/// FULL, missing, unreadable or empty selection narrows anything, if any other job reads the selection, if the
/// selection job can fail the run, or if a push, the nightly or a release run gets a selection. The rule cases pin
/// what the replay of the failure corpus asked for: a path written only in a comment, a class named by a sibling
/// test file, a partial type declared in many files, a class that would otherwise be a hole, and a documentation
/// file a test reads by name.</para>
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Darling")]
public sealed class TestMapShadowGuardTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "testmap-shadow-" + Guid.NewGuid().ToString("N"));

    public TestMapShadowGuardTests() => Directory.CreateDirectory(_dir);

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

    private static string Script(string name) => Path.Combine(ParitySource.RepoRoot(), ".github", "scripts", name);

    private static (int ExitCode, string Output) Python(string script, params string[] args)
    {
        var psi = new ProcessStartInfo("python")
        {
            WorkingDirectory = ParitySource.RepoRoot(),
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        psi.ArgumentList.Add("-I");
        psi.ArgumentList.Add(Script(script));
        foreach (var a in args)
        {
            psi.ArgumentList.Add(a);
        }

        using var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        p.WaitForExit();
        return (p.ExitCode, stdout.Result + stderr.Result);
    }

    private void Write(string relative, string content)
    {
        var path = Path.Combine(_dir, relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, content);
    }

    private static string ShardJson(string suite, int shard, params (string Full, string[] Files)[] rows)
    {
        var classes = rows.ToDictionary(r => r.Full, r => new Dictionary<string, object>
        {
            ["files"] = r.Files,
            ["seconds"] = 1.0,
            ["tests"] = 1,
            ["skipped"] = 0,
            ["failed"] = 0,
            ["seen"] = true,
        });
        return JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["suite"] = suite,
            ["shard"] = shard,
            ["tests_exit"] = 0,
            ["listed"] = rows.Select(r => r.Full).ToArray(),
            ["classes"] = classes,
        });
    }

    /// <summary>Builds a map over a small checkout that holds one case per rule; no coverage is given for the classes.</summary>
    private JsonElement BuildRuleMap(out JsonDocument doc)
    {
        var tracked = new List<string>
        {
            "Darling/Darling.Tests/TwinTests.cs", "Darling/Darling.Tests/BaseTests.cs", "Darling/Darling.Tests/SiblingTests.cs",
            "Darling/Darling.Tests/UsesBigTests.cs", "Darling/Darling.Tests/EmptyHoleTests.cs", "Lite/Analysis/Detector.cs",
            "src/Big.cs",
        };
        Write("Darling/Darling.Tests/TwinTests.cs", """
            /// <summary>Holds the Darling detector to Lite's (Lite/Analysis/Detector.cs:120).</summary>
            public sealed class TwinTests { public void Runs() { } }
            """);
        Write("Darling/Darling.Tests/BaseTests.cs", "public sealed class BaseTests { public void Runs() { } }\n");
        Write("Darling/Darling.Tests/SiblingTests.cs", """
            /// <summary>Same rig as <see cref="BaseTests"/>.</summary>
            public sealed class SiblingTests { public void Runs() { } }
            """);
        Write("Darling/Darling.Tests/UsesBigTests.cs", """
            public sealed class UsesBigTests { public void Pins() { var s = Big.OnlyHereSql; } }
            """);
        Write("Darling/Darling.Tests/EmptyHoleTests.cs", "public sealed class EmptyHoleTests { public void Nothing() { } }\n");
        Write("Lite/Analysis/Detector.cs", "public sealed class Detector { }\n");
        // A partial type declared in 7 files (over the 5-file cap of the type-name rule); one of them holds the member.
        foreach (var part in new[] { "", ".A", ".B", ".C", ".D", ".E", ".F" })
        {
            var rel = "src/Big" + part + ".cs";
            tracked.Add(rel);
            Write(rel, "public partial class Big { " + (part == ".F" ? "public const string OnlyHereSql = \"x\";" : "") + " }\n");
        }

        // A partial type whose files are not named after it: a class that names only the type would be a hole.
        for (var i = 1; i <= 7; i++)
        {
            tracked.Add("src/W" + i + ".cs");
            Write("src/W" + i + ".cs", "public partial class Wide { }\n");
        }

        tracked.Add("Darling/Darling.Tests/FallbackTests.cs");
        Write("Darling/Darling.Tests/FallbackTests.cs", "public sealed class FallbackTests { public void Runs() { _ = new Wide(); } }\n");
        tracked = tracked.Distinct().ToList();
        Write("tracked.txt", string.Join("\n", tracked));
        Write("shards/a/shard-darling-0.json", ShardJson("darling", 0,
            ("Ns.TwinTests", Array.Empty<string>()), ("Ns.BaseTests", Array.Empty<string>()), ("Ns.SiblingTests", Array.Empty<string>()),
            ("Ns.UsesBigTests", Array.Empty<string>()), ("Ns.EmptyHoleTests", Array.Empty<string>()),
            ("Ns.FallbackTests", Array.Empty<string>())));
        Write("shards/a/shard-lite-0.json", ShardJson("lite", 0, ("Ns.LiteOnlyTests", Array.Empty<string>())));
        Write("Lite.Tests/LiteOnlyTests.cs", "public sealed class LiteOnlyTests { public void Runs() { } }\n");
        File.AppendAllText(Path.Combine(_dir, "tracked.txt"), "\nLite.Tests/LiteOnlyTests.cs");

        var (code, output) = Python("test-map.py", "build", "--root", _dir, "--shards", Path.Combine(_dir, "shards"),
            "--sha", "0123456789abcdef", "--built-at", "2026-10-08T00:00:00Z", "--tracked", Path.Combine(_dir, "tracked.txt"),
            "--out", Path.Combine(_dir, "map.json.gz"), "--summary", Path.Combine(_dir, "summary.md"));
        Assert.True(code == 0, output);
        using var gz = new System.IO.Compression.GZipStream(File.OpenRead(Path.Combine(_dir, "map.json.gz")),
            System.IO.Compression.CompressionMode.Decompress);
        doc = JsonDocument.Parse(gz);
        return doc.RootElement;
    }

    private static List<string> Strings(JsonElement e) => e.EnumerateArray().Select(x => x.GetString()!).ToList();

    private static List<string> FilesOf(JsonElement map, string suite, string cls)
    {
        var universe = Strings(map.GetProperty("files"));
        return map.GetProperty("classes").GetProperty(suite).GetProperty(cls).GetProperty("files")
            .EnumerateArray().Select(i => universe[i.GetInt32()]).ToList();
    }

    [Fact]
    public void ARepoPath_WrittenOnlyInAComment_SelectsTheClass()
    {
        BuildRuleMap(out var doc);
        using (doc)
        {
            var patterns = Strings(doc.RootElement.GetProperty("text_patterns").GetProperty("TwinTests"));
            Assert.Contains("Lite/Analysis/Detector.cs", patterns);
        }
    }

    [Fact]
    public void ATestFile_ThatNamesAClass_SelectsThatClassWhenItChanges()
    {
        BuildRuleMap(out var doc);
        using (doc)
        {
            var patterns = Strings(doc.RootElement.GetProperty("text_patterns").GetProperty("BaseTests"));
            Assert.Contains("Darling/Darling.Tests/SiblingTests.cs", patterns);
        }
    }

    [Fact]
    public void APartialType_DeclaredInManyFiles_MapsToTheFilesThatDeclareTheNamedMember_AndTheFilesNamedAfterIt()
    {
        BuildRuleMap(out var doc);
        using (doc)
        {
            var files = FilesOf(doc.RootElement, "darling", "UsesBigTests");
            Assert.Contains("src/Big.F.cs", files);   // Big.OnlyHereSql is declared there
            Assert.Contains("src/Big.cs", files);     // the files named after the type
        }
    }

    [Fact]
    public void AClass_ThatWouldBeAHole_NeverEndsWithNothing()
    {
        BuildRuleMap(out var doc);
        using (doc)
        {
            // FallbackTests names only a partial type spread over 7 differently named files: it takes all of them.
            Assert.Contains("src/W4.cs", FilesOf(doc.RootElement, "darling", "FallbackTests"));
            var summary = File.ReadAllText(Path.Combine(_dir, "summary.md"));
            // EmptyHoleTests names nothing and covers nothing: it stays a hole. UsesBigTests names only a member of
            // a partial type declared in more files than the type-name rule's cap, and was a hole before the rule.
            Assert.Contains("- darling EmptyHoleTests (", summary, StringComparison.Ordinal);
            Assert.DoesNotContain("UsesBigTests (", summary, StringComparison.Ordinal);
            Assert.DoesNotContain("TwinTests (", summary, StringComparison.Ordinal);
            Assert.DoesNotContain("- darling BaseTests (", summary, StringComparison.Ordinal);
        }
    }

    private (int ExitCode, JsonElement Result) MapSelect(string files, string patternsJson)
    {
        var map = Path.Combine(_dir, "sel-map.json");
        File.WriteAllText(map, """
            {"schema": 1, "sha": "0000000", "built_at": "@BUILT@", "tool": "synthetic",
             "files": ["Lite/Services/A.cs", "Lite.Tests/Own.cs", "CHANGELOG.md"],
             "classes": {"lite": {"OwnTests": {"own": 1, "files": [0], "seconds": 1.5},
                                  "LadderTests": {"own": 1, "files": [], "seconds": 2.0}}},
             "text_patterns": @PATTERNS@}
            """.Replace("@BUILT@", DateTime.UtcNow.ToString("o"), StringComparison.Ordinal)
               .Replace("@PATTERNS@", patternsJson, StringComparison.Ordinal));
        Directory.CreateDirectory(Path.Combine(_dir, "tree", "Lite.Tests"));
        File.WriteAllText(Path.Combine(_dir, "tree", "Lite.Tests", "Own.cs"), "public class OwnTests { }\npublic class LadderTests { }\n");
        var (code, output) = Python("ci-select.py", "map-select", "--map", map, "--root", Path.Combine(_dir, "tree"),
            "--drift-file", "-", "--out", Path.Combine(_dir, "sel.json"), files);
        Assert.True(code == 0, output);
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(_dir, "sel.json")));
        return (code, doc.RootElement.Clone());
    }

    [Fact]
    public void ADocumentationFile_ARead_ByName_SelectsTheClass_ButAWildcardDoesNot()
    {
        var (_, byName) = MapSelect("CHANGELOG.md", """{"LadderTests": ["CHANGELOG.md", "docs/**"]}""");
        Assert.False(byName.GetProperty("full").GetBoolean(), byName.ToString());
        Assert.Contains("LadderTests", Strings(byName.GetProperty("selected").GetProperty("lite")));

        var (_, byWildcard) = MapSelect("docs/guide.md", """{"LadderTests": ["CHANGELOG.md", "docs/**"]}""");
        Assert.DoesNotContain("LadderTests", Strings(byWildcard.GetProperty("selected").GetProperty("lite")));
    }

    private void WriteReport(string artifact, string type, string result, string file = "x.xml")
    {
        Write(Path.Combine("reports", artifact, file),
            "<assemblies><assembly><collection><test name=\"n\" type=\"" + type + "\" method=\"m\" time=\"1\" result=\"" + result + "\" />"
            + "</collection></assembly></assemblies>");
    }

    private JsonElement ShadowCheck(string? selectionJson, string meta = "{\"run\": \"1\"}", string? jobsLines = null)
    {
        var args = new List<string> { "shadow-check", "--reports", Path.Combine(_dir, "reports"), "--out", Path.Combine(_dir, "row.jsonl"),
            "--meta", meta };
        if (selectionJson is not null)
        {
            Write("selection.json", selectionJson);
            args.Add("--selection");
            args.Add(Path.Combine(_dir, "selection.json"));
        }

        if (jobsLines is not null)
        {
            Write("jobs.jsonl", jobsLines);
            args.Add("--jobs");
            args.Add(Path.Combine(_dir, "jobs.jsonl"));
        }

        var (code, output) = Python("ci-select.py", args.ToArray());
        Assert.True(code == 0, output);
        var lines = File.ReadAllLines(Path.Combine(_dir, "row.jsonl"));
        Assert.Single(lines);
        return JsonDocument.Parse(lines[0]).RootElement;
    }

    [Fact]
    public void TheShadowCheck_ListsEveryFailedClassTheSelectionWouldNotHaveRun_InOneRow()
    {
        WriteReport("darling-tests-timing-0", "Darling.Tests.PickedTests", "Fail");
        WriteReport("lite-tests-timing-1", "Lite.Tests.SkippedTests", "Fail");
        Write(Path.Combine("reports", "lite-tests-timing-2", "y.xml"),
            "<assemblies><assembly><collection><test name=\"n\" type=\"Lite.Tests.FineTests\" method=\"m\" time=\"1\" result=\"Pass\" /></collection></assembly></assemblies>");
        var row = ShadowCheck("""
            {"full": false, "reason": "", "selected": {"darling": ["PickedTests"], "lite": ["FineTests"]},
             "total": {"darling": 5, "lite": 9}, "seconds": {"darling": 12.5, "lite": 3}, "why": {}}
            """);
        Assert.False(row.GetProperty("full").GetBoolean());
        Assert.Equal("1", row.GetProperty("run").GetString());
        var misses = row.GetProperty("misses").EnumerateArray().ToList();
        Assert.Single(misses);
        Assert.Equal("lite", misses[0].GetProperty("suite").GetString());
        Assert.Equal("SkippedTests", misses[0].GetProperty("class").GetString());
        Assert.Equal(1, row.GetProperty("reports").GetProperty("darling").GetInt32());
        Assert.Equal(2, row.GetProperty("reports").GetProperty("lite").GetInt32());
    }

    private const string OnePickedSelection = """
        {"full": false, "reason": "", "selected": {"darling": ["PickedTests"], "lite": []},
         "total": {"darling": 5, "lite": 9}, "seconds": {}, "why": {}}
        """;

    [Fact]
    public void TheGuardTestsJobsFailedClasses_AreInTheRow_AndCountAsMissesWhenNotSelected()
    {
        // #5459: the Guard tests job's xunit report is uploaded as guard-tests-timing-<suite>; the shard reports alone
        // left five runs with a failing Guard class and an empty row.
        WriteReport("guard-tests-timing-darling", "Darling.Tests.RepoFileAdoptionTests", "Fail", "guard-darling-a1.xml");
        WriteReport("guard-tests-timing-lite", "Lite.Tests.WpfStaGateCensusTests", "Fail", "guard-lite-a1.xml");
        WriteReport("darling-tests-timing-0", "Darling.Tests.PickedTests", "Pass", "darling-timing-0-1-a1.xml");

        var row = ShadowCheck(OnePickedSelection, """{"run": "1", "attempt": "1"}""");

        Assert.Equal(new[] { "RepoFileAdoptionTests" }, row.GetProperty("failed").GetProperty("darling").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(new[] { "WpfStaGateCensusTests" }, row.GetProperty("failed").GetProperty("lite").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(2, row.GetProperty("misses").GetArrayLength());
        Assert.Equal(2, row.GetProperty("reports").GetProperty("darling").GetInt32());
    }

    [Fact]
    public void AFolderKeepsItsHighestAttempt_AndCountsLowerOnesStale()
    {
        // #5459, run 37776652739: shard 0 failed in attempt 1 and passed in attempt 2. The check ran in attempt 2 and
        // listed the attempt-1 class. Staleness is per artifact folder: the folder's highest attempt counts, a lower
        // one in the same folder is stale. The run's own attempt is not the filter.
        WriteReport("lite-tests-timing-0", "Lite.Tests.ScreenReaderRowNamesTests", "Fail", "lite-timing-0-1-a1.xml");
        WriteReport("lite-tests-timing-0", "Lite.Tests.ScreenReaderRowNamesTests", "Pass", "lite-timing-0-1-a2.xml");

        foreach (var attempt in new[] { "1", "2" })
        {
            var row = ShadowCheck(OnePickedSelection, "{\"run\": \"1\", \"attempt\": \"" + attempt + "\"}");

            Assert.Empty(row.GetProperty("failed").GetProperty("lite").EnumerateArray());
            Assert.Empty(row.GetProperty("misses").EnumerateArray());
            Assert.Equal(1, row.GetProperty("reports").GetProperty("lite").GetInt32());
            Assert.Equal(1, row.GetProperty("stale_reports").GetInt32());
        }
    }

    [Fact]
    public void RerunFailedJobs_KeepsThePassingShardsAttempt1Reports()
    {
        // "Re-run failed jobs" does not re-run a shard that passed in attempt 1, so its artifact still holds -a1 files
        // when the check runs in attempt 2. They belong to the run and must count, not be dropped as stale.
        WriteReport("darling-tests-timing-0", "Darling.Tests.PickedTests", "Pass", "darling-timing-0-1-a1.xml");
        WriteReport("darling-tests-timing-1", "Darling.Tests.OtherTests", "Pass", "darling-timing-1-1-a2.xml");
        WriteReport("lite-tests-timing-0", "Lite.Tests.LiteOnlyTests", "Pass", "lite-timing-0-1-a1.xml");

        var row = ShadowCheck(OnePickedSelection, """{"run": "1", "attempt": "2"}""");

        Assert.Equal(2, row.GetProperty("reports").GetProperty("darling").GetInt32());
        Assert.Equal(1, row.GetProperty("reports").GetProperty("lite").GetInt32());
        Assert.Equal(0, row.GetProperty("stale_reports").GetInt32());
    }

    [Fact]
    public void AJobThatFailedWithoutAFailedTest_IsListedAsAFailedJob()
    {
        // #5459: the whole-tree guards job can end red on a leaked foreground thread, with 0 failed tests.
        var jobs = """
            {"name":"Guard tests","conclusion":"success"}
            {"name":"Darling whole-tree guards","conclusion":"failure"}
            {"name":"Lite tests (0)","conclusion":"skipped"}
            """;

        var row = ShadowCheck(OnePickedSelection, """{"run": "1", "attempt": "1"}""", jobs);

        Assert.Equal(new[] { "Darling whole-tree guards" }, row.GetProperty("failed_jobs").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Empty(row.GetProperty("misses").EnumerateArray());
    }

    [Fact]
    public void ACancelledSelectionJob_IsNamedInTheReason_NotReportedAsAScriptFault()
    {
        // #5459, run 37832135291: a newer push cancelled the run one second into the selection job.
        var row = ShadowCheck(null, """{"run": "1", "selection_job": "cancelled"}""");

        Assert.True(row.GetProperty("full").GetBoolean());
        Assert.Contains("selection job ended cancelled", row.GetProperty("reason").GetString(), StringComparison.Ordinal);
    }

    [Fact]
    public void AFullOrMissingSelection_CannotHaveAShadowMiss()
    {
        WriteReport("darling-tests-timing-0", "Darling.Tests.AnyTests", "Fail");
        var full = ShadowCheck("""{"full": true, "reason": "no test map"}""");
        Assert.True(full.GetProperty("full").GetBoolean());
        Assert.Empty(full.GetProperty("misses").EnumerateArray());

        File.Delete(Path.Combine(_dir, "selection.json"));
        var missing = ShadowCheck(null);
        Assert.True(missing.GetProperty("full").GetBoolean());
        Assert.Empty(missing.GetProperty("misses").EnumerateArray());
    }

    private static string BuildYml() => File.ReadAllText(Path.Combine(ParitySource.RepoRoot(), ".github", "workflows", "build.yml"))
        .Replace("\r\n", "\n");

    private static string Job(string yaml, string name)
    {
        var start = yaml.IndexOf("\n  " + name + ":\n", StringComparison.Ordinal);
        Assert.True(start >= 0, "build.yml has no job " + name);
        var next = Regex.Match(yaml[(start + 1)..], @"\n  (?:#[^\n]*\n  )*[a-z][a-z0-9-]*:\n");
        return next.Success && next.Index > 0 ? yaml.Substring(start, next.Index + 1) : yaml[start..];
    }

    private static readonly string[] s_selectionConsumers = ["build", "darling-pg", "lite-tests"];

    [Fact]
    public void OnlyTheThreeTestJobs_ReadTheSelection_EachThroughOneDownloadStep_AndNoJobReadsTheMapItself()
    {
        var yml = BuildYml();
        var jobNames = Regex.Matches(yml, @"^  ([a-z][a-z0-9-]*):\s*$", RegexOptions.Multiline).Select(m => m.Groups[1].Value)
            .Where(n => n is not ("on" or "permissions" or "env" or "concurrency" or "jobs")).ToList();
        Assert.Contains("test-map-select", jobNames);
        Assert.Contains("shadow-check", jobNames);
        Assert.DoesNotContain("test-map-shadow", jobNames);
        Assert.DoesNotContain("test-map-shadow-selection", yml, StringComparison.Ordinal);

        // Before the selection went live no test job read the map; now exactly these three read the SELECTION, and
        // still never the map: the pinned map (gate outputs test_map_*) belongs to the selection job alone.
        foreach (var name in jobNames.Where(n => n is not ("gate" or "test-map-select" or "shadow-check")))
        {
            var job = Job(yml, name);
            Assert.DoesNotContain("test_map", job, StringComparison.Ordinal);
            Assert.DoesNotContain("ci-select.py map-select", job, StringComparison.Ordinal);
            Assert.DoesNotContain("shadow-check", job, StringComparison.Ordinal);
            if (s_selectionConsumers.Contains(name))
            {
                continue;
            }

            Assert.DoesNotContain("test-map-select", job, StringComparison.Ordinal);
            Assert.DoesNotContain("test-map-selection", job, StringComparison.Ordinal);
            Assert.DoesNotContain("TEST_MAP_SELECTION", job, StringComparison.Ordinal);
        }

        // The selection job and the check run on pull requests only and cannot redden the run. The selection job is
        // capped at 5 minutes because the three test jobs wait for it; the check is capped at 10.
        foreach (var (name, minutes) in new[] { ("test-map-select", 5), ("shadow-check", 10) })
        {
            var job = Job(yml, name);
            Assert.Contains("continue-on-error: true\n    permissions:", job, StringComparison.Ordinal);
            Assert.Contains("github.event_name == 'pull_request'", job, StringComparison.Ordinal);
            Assert.Contains($"timeout-minutes: {minutes}\n", job, StringComparison.Ordinal);
        }

        Assert.Contains("needs: [gate]", Job(yml, "test-map-select"), StringComparison.Ordinal);
        Assert.Contains("needs: [gate, guard-tests, test-map-select, darling-pg, lite-tests]", Job(yml, "shadow-check"), StringComparison.Ordinal);
        Assert.Contains("always()", Job(yml, "shadow-check"), StringComparison.Ordinal);

        // Each consumer waits on the selection job, never fails because of it (`!cancelled()` in its own `if`), and
        // downloads this run's artifact in ONE step that cannot fail the job.
        foreach (var name in s_selectionConsumers)
        {
            var job = Job(yml, name);
            var needs = Regex.Match(job, @"\n    needs: (\[[^\]]*\])");
            Assert.Contains("test-map-select", needs.Groups[1].Value, StringComparison.Ordinal);
            Assert.Contains("!cancelled()", Regex.Match(job, @"\n    if: [^\n]*").Value, StringComparison.Ordinal);
            var step = job[job.IndexOf("- name: Download the test map selection", StringComparison.Ordinal)..];
            step = step[..step.IndexOf("\n\n", StringComparison.Ordinal)];
            Assert.Single(Regex.Matches(job, "- name: Download the test map selection"));
            Assert.Contains("id: map-selection", step, StringComparison.Ordinal);
            Assert.Contains("if: github.event_name == 'pull_request' && needs.test-map-select.result == 'success'", step, StringComparison.Ordinal);
            Assert.Contains("continue-on-error: true", step, StringComparison.Ordinal);
            Assert.Contains("uses: actions/download-artifact@v6", step, StringComparison.Ordinal);
            Assert.Contains("name: test-map-selection", step, StringComparison.Ordinal);
            Assert.DoesNotContain("run-id", step, StringComparison.Ordinal);
            // Empty unless the download succeeded: every failure leaves the shard running everything.
            Assert.Contains("TEST_MAP_SELECTION: ${{ steps.map-selection.outcome == 'success' && format('{0}\\test-map-selection\\selection.json', github.workspace) || '' }}", job, StringComparison.Ordinal);
        }

        // No other job lists the selection job in `needs`.
        foreach (var name in jobNames.Where(n => n is not "shadow-check" && !s_selectionConsumers.Contains(n)))
        {
            var needs = Regex.Match(Job(yml, name), @"\n    needs: (\[[^\]]*\]|[^\n]*)");
            Assert.DoesNotContain("test-map", needs.Value, StringComparison.Ordinal);
            Assert.DoesNotContain("shadow", needs.Value, StringComparison.Ordinal);
        }

        // Only a pull request narrows: the nightly calls the same scripts without a selection.
        var nightly = File.ReadAllText(Path.Combine(ParitySource.RepoRoot(), ".github", "workflows", "nightly.yml"));
        Assert.DoesNotContain("SelectionFile", nightly, StringComparison.Ordinal);
        Assert.DoesNotContain("test-map-selection", nightly, StringComparison.Ordinal);
        Assert.Contains("-SelectionFile $env:TEST_MAP_SELECTION", Job(yml, "darling-pg"), StringComparison.Ordinal);
        Assert.Contains("-SelectionFile $env:TEST_MAP_SELECTION", Job(yml, "lite-tests"), StringComparison.Ordinal);
    }

    [Fact]
    public void ThePin_IsPullRequestOnly_AndCannotFailTheGate()
    {
        var gate = Job(BuildYml(), "gate");
        var step = gate[gate.IndexOf("- name: Pin the test map\n", StringComparison.Ordinal)..];
        Assert.Contains("id: testmap", step, StringComparison.Ordinal);
        Assert.Contains("if: github.event_name == 'pull_request' && steps.decide.outputs.run == 'true'", step, StringComparison.Ordinal);
        Assert.Contains("continue-on-error: true", step[..step.IndexOf("run: |", StringComparison.Ordinal)], StringComparison.Ordinal);

        // continue-on-error absorbs a step failure but not the job's own timeout: a hung gh api call would otherwise run
        // the gate into its 5-minute limit and red the whole run with no tests. A step timeout turns the hang into a step
        // failure first (#5459).
        var header = step[..step.IndexOf("run: |", StringComparison.Ordinal)];
        var stepTimeout = Regex.Match(header, @"^        timeout-minutes: (\d+)\s*$", RegexOptions.Multiline);
        Assert.True(stepTimeout.Success, "the pin step needs its own timeout-minutes");
        var gateTimeout = Regex.Match(gate, @"^    timeout-minutes: (\d+)\s*$", RegexOptions.Multiline);
        Assert.True(gateTimeout.Success, "the gate job needs a timeout-minutes");
        Assert.True(int.Parse(stepTimeout.Groups[1].Value) < int.Parse(gateTimeout.Groups[1].Value),
            "the pin step's timeout must be below the gate job's");
        Assert.Contains("nightly.yml/runs?status=success", step, StringComparison.Ordinal);
        Assert.Contains("compare/${sha}...${BASE_SHA}", step, StringComparison.Ordinal);   // the map's commit must be an ancestor of the base
        Assert.Contains("ahead|identical", step, StringComparison.Ordinal);
        Assert.Contains("expires_at > env.KEEP_UNTIL", step, StringComparison.Ordinal);
        Assert.Contains("test_map_run: ${{ steps.testmap.outputs.run_id }}", gate, StringComparison.Ordinal);
    }

    [Fact]
    public void TheSelectionSteps_TurnEveryFailureIntoAFullNotice_NeverAnErrorOrASkip()
    {
        var job = Job(BuildYml(), "test-map-select");
        foreach (var why in new[] { "no test map is pinned for this run", "the pinned map could not be downloaded",
                     "the changed files could not be listed", "the selection script failed" })
        {
            Assert.Contains("full \"" + why + "\"", job, StringComparison.Ordinal);
        }

        Assert.Contains("exit 0", job, StringComparison.Ordinal);
        Assert.Contains("timeout 120 python .github/scripts/ci-select.py map-select", job, StringComparison.Ordinal);
        // #5459: a pull request into main always runs everything, so the step hands the base branch to map-select.
        Assert.Contains("BASE_REF: ${{ github.base_ref }}", job, StringComparison.Ordinal);
        Assert.Contains("--event pull_request --base-ref \"${BASE_REF}\" --base \"${BASE_SHA}\"", job, StringComparison.Ordinal);
        var check = Job(BuildYml(), "shadow-check");
        Assert.Contains("exit 0", check, StringComparison.Ordinal);
        Assert.Contains("name: test-map-shadow-row", check, StringComparison.Ordinal);
        Assert.Contains("retention-days: 30", check, StringComparison.Ordinal);
    }

    [Fact]
    public void TheDarlingShards_UploadTheirReports_EvenWhenAClassFailed()
    {
        var yml = BuildYml();
        var step = yml[yml.IndexOf("- name: Upload Darling test timings", StringComparison.Ordinal)..];
        step = step[..step.IndexOf("\n\n", StringComparison.Ordinal)];
        Assert.Contains("if: always() && steps.filter.outputs.darling == 'true'", step, StringComparison.Ordinal);
        Assert.Contains("name: darling-tests-timing-${{ matrix.shard }}", step, StringComparison.Ordinal);
    }

    private (int ExitCode, string Output) MapKeep(string? selectionJson, string suite)
    {
        var path = Path.Combine(_dir, "keep-selection.json");
        File.Delete(path);
        if (selectionJson is not null)
        {
            File.WriteAllText(path, selectionJson);
        }

        return Python("ci-select.py", "map-keep", "--selection", path, "--suite", suite);
    }

    [Fact]
    public void MapKeep_NamesTheSelectedClassesOfOneSuite_AndAnswersFullWheneverTheSelectionCannotBeTrusted()
    {
        var ok = MapKeep("""{"full": false, "reason": "", "selected": {"darling": ["BTests", "ATests"], "lite": ["CTests"]}}""", "darling");
        Assert.Equal(0, ok.ExitCode);
        Assert.Equal(new[] { "SELECTED 2", "ATests", "BTests" }, ok.Output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries));
        Assert.Equal("SELECTED 1", MapKeep("""{"full": false, "selected": {"darling": ["BTests"], "lite": ["CTests"]}}""", "lite").Output
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)[0]);

        // Anything else is FULL on the first line, with exit code 0, so the shard runs every class it was cut.
        foreach (var (label, json) in new (string, string?)[]
                 {
                     ("a missing file", null),
                     ("not JSON", "{not json"),
                     ("a JSON array", "[1, 2]"),
                     ("a FULL selection", """{"full": true, "reason": "no test map", "selected": {"darling": ["ATests"]}}"""),
                     ("no full flag", """{"selected": {"darling": ["ATests"]}}"""),
                     ("a string full flag", """{"full": "false", "selected": {"darling": ["ATests"]}}"""),
                     ("no selected object", """{"full": false}"""),
                     ("no list for the suite", """{"full": false, "selected": {"lite": ["CTests"]}}"""),
                     ("an empty list for the suite", """{"full": false, "selected": {"darling": []}}"""),
                     ("a name that is not text", """{"full": false, "selected": {"darling": ["ATests", 7]}}"""),
                     ("an empty name", """{"full": false, "selected": {"darling": ["ATests", ""]}}"""),
                 })
        {
            var answer = MapKeep(json, "darling");
            Assert.True(answer.ExitCode == 0, label + ": " + answer.Output);
            Assert.True(answer.Output.StartsWith("FULL ", StringComparison.Ordinal), label + ": " + answer.Output);
            Assert.DoesNotContain("SELECTED", answer.Output, StringComparison.Ordinal);
        }
    }

    private static string? FindPwsh()
    {
        foreach (var dir in (Environment.GetEnvironmentVariable("PATH") ?? "").Split(Path.PathSeparator, StringSplitOptions.RemoveEmptyEntries))
        {
            foreach (var name in new[] { "pwsh.exe", "pwsh" })
            {
                try
                {
                    var candidate = Path.Combine(dir, name);
                    if (File.Exists(candidate))
                    {
                        return candidate;
                    }
                }
                catch (ArgumentException)
                {
                    // A PATH entry that is not a valid path is skipped.
                }
            }
        }

        return null;
    }

    /// <summary>Runs one shard script in a scratch folder with a fake `dotnet` that answers the class listing from
    /// <paramref name="classes"/> and prints each chunk's -class arguments. Returns the classes the chunks ran, and the output.</summary>
    private (int ExitCode, List<string> Ran, string Output) RunShardScript(string script, string suite, string[] classes, string? selectionJson,
        string eventName = "pull_request")
    {
        var pwsh = FindPwsh();
        Assert.SkipWhen(pwsh is null, "PowerShell 7 (pwsh) is not on PATH, so the shard scripts cannot be run here.");
        var work = Path.Combine(_dir, "shard-" + suite);
        Directory.CreateDirectory(Path.Combine(work, ".github", "scripts"));
        File.Copy(Script("ci-select.py"), Path.Combine(work, ".github", "scripts", "ci-select.py"), overwrite: true);
        File.WriteAllLines(Path.Combine(work, "classes.txt"), classes);
        var selection = Path.Combine(work, "selection.json");
        File.Delete(selection);
        if (selectionJson is not null)
        {
            File.WriteAllText(selection, selectionJson);
        }

        var harness = Path.Combine(work, "harness.ps1");
        File.WriteAllText(harness, """
            param([string] $Script, [string] $Suite, [string] $Selection, [string] $EventName)
            $global:ListFile = Join-Path $PWD 'classes.txt'
            function dotnet {
                if ($args -contains '-list') {
                    Write-Output 'a dotnet notice line that is not JSON'
                    Write-Output (ConvertTo-Json -Compress -InputObject @(Get-Content -Path $global:ListFile | Where-Object { $_ }))
                    $global:LASTEXITCODE = 0
                    return
                }
                $ran = @(for ($i = 0; $i -lt $args.Count; $i++) { if ($args[$i] -eq '-class') { $args[$i + 1] } })
                Write-Output ('RAN ' + ($ran -join ','))
                $global:LASTEXITCODE = 0
            }
            $env:SLOW_REPO = ''
            $env:SLOW_PR = ''
            $env:SLOW_BASE_REF = ''
            if ($Suite -eq 'darling') { & $Script -Shard 0 -ShardCount 1 -EventName $EventName -SelectionFile $Selection }
            else { & $Script -Shard 0 -ShardCount 1 -JobIndex 0 -EventName $EventName -SelectionFile $Selection }
            exit $LASTEXITCODE
            """);
        var psi = new ProcessStartInfo(pwsh!)
        {
            WorkingDirectory = work,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var a in new[] { "-NoProfile", "-NonInteractive", "-File", harness, "-Script", Script(script), "-Suite", suite,
                     "-Selection", selectionJson is null ? "" : selection, "-EventName", eventName })
        {
            psi.ArgumentList.Add(a);
        }

        using var p = Process.Start(psi)!;
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        p.WaitForExit();
        var output = stdout.Result + stderr.Result;
        var ran = output.Split('\n').Select(l => l.TrimEnd('\r')).Where(l => l.StartsWith("RAN ", StringComparison.Ordinal))
            .SelectMany(l => l[4..].Split(',', StringSplitOptions.RemoveEmptyEntries)).ToList();
        return (p.ExitCode, ran, output);
    }

    [Theory]
    [InlineData("run-darling-pg-shard.ps1", "darling", "Darling.Tests")]
    [InlineData("run-lite-shard.ps1", "lite", "Lite.Tests")]
    public void TheShardScripts_KeepOnlyTheSelectedClasses_AndRunEverythingWhenThereIsNoUsableSelection(string script, string suite, string ns)
    {
        string[] all = [.. Enumerable.Range(1, 40).Select(i => $"{ns}.Class{i:00}Tests")];
        var selected = new[] { "Class03Tests", "Class17Tests", "Class40Tests", "NotInThisShardTests" };
        var selection = JsonSerializer.Serialize(new Dictionary<string, object>
        {
            ["full"] = false,
            ["reason"] = "",
            ["selected"] = new Dictionary<string, string[]> { [suite] = selected, [suite == "darling" ? "lite" : "darling"] = ["OtherSuiteTests"] },
        });

        var narrowed = RunShardScript(script, suite, all, selection);
        Assert.True(narrowed.ExitCode == 0, narrowed.Output);
        Assert.Equal([$"{ns}.Class03Tests", $"{ns}.Class17Tests", $"{ns}.Class40Tests"], narrowed.Ran.OrderBy(c => c, StringComparer.Ordinal));
        Assert.Contains("3 to run", narrowed.Output, StringComparison.Ordinal);

        // Fail open: a FULL, missing, unreadable or empty selection, and any event but a pull request, run every class.
        var everything = new (string Label, string? Json, string Event)[]
        {
            ("FULL", """{"full": true, "reason": "a keep-full file", "selected": {}}""", "pull_request"),
            ("no file", null, "pull_request"),
            ("unreadable", "{broken", "pull_request"),
            ("an empty list", "{\"full\": false, \"selected\": {\"" + suite + "\": []}}", "pull_request"),
            ("a push", selection, "push"),
        };
        foreach (var (label, json, eventName) in everything)
        {
            var run = RunShardScript(script, suite, all, json, eventName);
            Assert.True(run.ExitCode == 0, label + ": " + run.Output);
            Assert.True(run.Ran.Count == all.Length, label + ": ran " + run.Ran.Count + " of " + all.Length + "\n" + run.Output);
        }

        // A shard none of whose classes is selected ends cleanly without reaching a runner with no -class arguments.
        var none = RunShardScript(script, suite, all, "{\"full\": false, \"selected\": {\"" + suite + "\": [\"SomewhereElseTests\"]}}");
        Assert.Equal(0, none.ExitCode);
        Assert.Empty(none.Ran);
        Assert.Contains("nothing runs", none.Output, StringComparison.Ordinal);
    }
}
