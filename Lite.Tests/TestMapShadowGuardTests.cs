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
/// Pins shadow mode for the test-class map (#5459, change 4, slice 4): the rules that close the holes the replay
/// found, the shadow-check row, and the workflow wiring that keeps shadow mode from selecting anything.
///
/// <para><b>Why.</b> Shadow mode computes the selection for every pull request and still runs everything, so the
/// one thing it must never do is change what runs. The workflow pins below fail if a job that executes tests
/// reads the map, if a shadow job can fail the run, or if anything but the shadow check waits on them. The rule
/// cases pin what the replay of the failure corpus asked for: a path written only in a comment, a class named by
/// a sibling test file, a partial type declared in many files, a class that would otherwise be a hole, and a
/// documentation file a test reads by name.</para>
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

    private void WriteReport(string artifact, string type, string result)
    {
        Write(Path.Combine("reports", artifact, "x.xml"),
            "<assemblies><assembly><collection><test name=\"n\" type=\"" + type + "\" method=\"m\" time=\"1\" result=\"" + result + "\" />"
            + "</collection></assembly></assemblies>");
    }

    private JsonElement ShadowCheck(string? selectionJson)
    {
        var args = new List<string> { "shadow-check", "--reports", Path.Combine(_dir, "reports"), "--out", Path.Combine(_dir, "row.jsonl"),
            "--meta", """{"run": "1"}""" };
        if (selectionJson is not null)
        {
            Write("selection.json", selectionJson);
            args.Add("--selection");
            args.Add(Path.Combine(_dir, "selection.json"));
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

    [Fact]
    public void ShadowMode_SelectsNothing_NoJobThatRunsTestsReadsTheMap()
    {
        var yml = BuildYml();
        var jobNames = Regex.Matches(yml, @"^  ([a-z][a-z0-9-]*):\s*$", RegexOptions.Multiline).Select(m => m.Groups[1].Value)
            .Where(n => n is not ("on" or "permissions" or "env" or "concurrency" or "jobs")).ToList();
        Assert.Contains("test-map-shadow", jobNames);
        Assert.Contains("shadow-check", jobNames);
        foreach (var name in jobNames.Where(n => n is not ("gate" or "test-map-shadow" or "shadow-check")))
        {
            var job = Job(yml, name);
            Assert.DoesNotContain("map-select", job, StringComparison.Ordinal);
            Assert.DoesNotContain("test_map", job, StringComparison.Ordinal);
            Assert.DoesNotContain("shadow-check", job, StringComparison.Ordinal);
            Assert.DoesNotContain("test-map-shadow", job, StringComparison.Ordinal);
        }

        // The two shadow jobs run on pull requests only, cannot redden the run, and only the check waits on the selector.
        foreach (var name in new[] { "test-map-shadow", "shadow-check" })
        {
            var job = Job(yml, name);
            Assert.Contains("continue-on-error: true\n    permissions:", job, StringComparison.Ordinal);
            Assert.Contains("github.event_name == 'pull_request'", job, StringComparison.Ordinal);
            Assert.Contains("timeout-minutes: 10", job, StringComparison.Ordinal);
        }

        Assert.Contains("needs: [gate]", Job(yml, "test-map-shadow"), StringComparison.Ordinal);
        Assert.Contains("needs: [gate, test-map-shadow, darling-pg, lite-tests]", Job(yml, "shadow-check"), StringComparison.Ordinal);
        Assert.Contains("always()", Job(yml, "shadow-check"), StringComparison.Ordinal);
        foreach (var name in jobNames.Where(n => n is not "shadow-check"))
        {
            var needs = Regex.Match(Job(yml, name), @"\n    needs: (\[[^\]]*\]|[^\n]*)");
            Assert.DoesNotContain("shadow", needs.Value, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void ThePin_IsPullRequestOnly_AndCannotFailTheGate()
    {
        var gate = Job(BuildYml(), "gate");
        var step = gate[gate.IndexOf("- name: Pin the test map (shadow mode)", StringComparison.Ordinal)..];
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
    public void TheShadowSteps_TurnEveryFailureIntoAFullNotice_NeverAnErrorOrASkip()
    {
        var job = Job(BuildYml(), "test-map-shadow");
        foreach (var why in new[] { "no test map is pinned for this run", "the pinned map could not be downloaded",
                     "the changed files could not be listed", "the selection script failed" })
        {
            Assert.Contains("full \"" + why + "\"", job, StringComparison.Ordinal);
        }

        Assert.Contains("exit 0", job, StringComparison.Ordinal);
        Assert.Contains("timeout 120 python .github/scripts/ci-select.py map-select", job, StringComparison.Ordinal);
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
}
