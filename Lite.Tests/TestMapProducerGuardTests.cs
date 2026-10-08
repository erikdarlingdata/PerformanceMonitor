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
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the test-map producer (#5459 change 4, slice 3): <c>.github/scripts/test-map.py</c> and the two jobs in
/// <c>nightly.yml</c> that run it.
///
/// <para><b>Why.</b> The map is only as good as what goes into it. Coverage sees the product files a class ran; the
/// producer adds the files a class only names (path and tool-name literals, the type-name rule), marks a class that
/// walks the whole checkout as text as "tree", unions the shards and any retry, and refuses to write a map when a
/// shard is missing (a class absent from the map runs as "new" on every pull request and hides what it covers).
/// These tests build a small synthetic checkout and run the script on it, so a rule that stops firing is red here
/// before a nightly spends 100 runner-minutes producing a map with a hole in it.</para>
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Darling")]
public sealed class TestMapProducerGuardTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "testmap-guard-" + Guid.NewGuid().ToString("N"));

    public TestMapProducerGuardTests() => Directory.CreateDirectory(_dir);

    public void Dispose()
    {
        try
        {
            Directory.Delete(_dir, recursive: true);
        }
        catch (IOException)
        {
            // A scratch folder under the temp directory; the OS reclaims it.
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

        using var p = Process.Start(psi) ?? throw new InvalidOperationException("python did not start");
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        Assert.True(p.WaitForExit(120_000), script + " did not finish in two minutes");
        return (p.ExitCode, stdout.Result + stderr.Result);
    }

    private void Write(string relative, string content)
    {
        var path = Path.Combine(_dir, relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        File.WriteAllText(path, content);
    }

    private static string ShardJson(string suite, int shard, params (string Full, string[] Files, double Seconds, int Tests, int Skipped)[] rows)
    {
        var classes = rows.ToDictionary(r => r.Full, r => new Dictionary<string, object>
        {
            ["files"] = r.Files,
            ["seconds"] = r.Seconds,
            ["tests"] = r.Tests,
            ["skipped"] = r.Skipped,
            ["failed"] = 0,
            ["seen"] = r.Files.Length > 0,
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

    /// <summary>A small checkout: two suites, product files, a script, a SQL file and a XAML pair.</summary>
    private void SeedCheckout()
    {
        var tracked = new[]
        {
            "Darling/Darling.Tests/AlphaTests.cs", "Darling/Darling.Tests/ReadsTests.cs", "Darling/Darling.Tests/WalkerTests.cs",
            "Darling/Darling.Tests/GuardedTests.cs", "Darling/Darling.Tests/HoleTests.cs", "Lite.Tests/BetaTests.cs",
            "src/Gamma.cs", "src/Other.cs", "src/ProductThing.cs", "src/Tools.cs", "src/Ui/Main.xaml", "src/Ui/Main.xaml.cs",
            "web/app.js", "install/01_schema.sql",
        };
        Write("tracked.txt", string.Join("\n", tracked));
        Write("Darling/Darling.Tests/AlphaTests.cs", """
            public sealed class AlphaTests
            {
                private readonly ProductThing _thing = new();
                public void Runs() { }
            }
            """);
        Write("Darling/Darling.Tests/ReadsTests.cs", """
            public sealed class ReadsTests
            {
                public void Reads()
                {
                    var js = File.ReadAllText(Path.Combine(RepoRoot(), "web", "app.js"));
                    var tool = "get_widget";
                    var sql = File.ReadAllText(Path.Combine(RepoRoot(), "install", "01_schema.sql"));
                }
            }
            """);
        Write("Darling/Darling.Tests/WalkerTests.cs", """
            public sealed class WalkerTests
            {
                public void Walks()
                {
                    foreach (var f in Directory.EnumerateFiles(RepoRoot(), "*.cs", SearchOption.AllDirectories)) { File.ReadAllText(f); }
                }
            }
            """);
        Write("Darling/Darling.Tests/GuardedTests.cs", """
            [Trait("Stage", "Guard")]
            public sealed class GuardedTests
            {
                public void Reads() { File.ReadAllText(Path.Combine(RepoRoot(), "web", "app.js")); }
            }
            """);
        Write("Darling/Darling.Tests/HoleTests.cs", "public sealed class HoleTests { public void Nothing() { } }\n");
        Write("Lite.Tests/BetaTests.cs", "public sealed class BetaTests { public void Runs() { } }\n");
        Write("src/Gamma.cs", "public sealed class Gamma { }\n");
        Write("src/Other.cs", "public sealed class Other { }\n");
        Write("src/ProductThing.cs", "public sealed class ProductThing { }\n");
        Write("src/Tools.cs", "public static class Tools { public const string Name = \"get_widget\"; }\n");
    }

    private (int ExitCode, string Output, JsonDocument? Map) Build(params string[] extra)
    {
        var args = new List<string>
        {
            "build", "--root", _dir, "--shards", Path.Combine(_dir, "shards"), "--sha", "0123456789abcdef",
            "--built-at", "2026-10-08T00:00:00Z", "--tracked", Path.Combine(_dir, "tracked.txt"),
            "--out", Path.Combine(_dir, "map.json.gz"), "--summary", Path.Combine(_dir, "summary.md"),
        };
        args.AddRange(extra);
        var (code, output) = Python("test-map.py", args.ToArray());
        if (code != 0)
        {
            return (code, output, null);
        }

        using var gz = new System.IO.Compression.GZipStream(File.OpenRead(Path.Combine(_dir, "map.json.gz")),
            System.IO.Compression.CompressionMode.Decompress);
        return (code, output, JsonDocument.Parse(gz));
    }

    private void SeedShards()
    {
        Write("shards/a/shard-darling-0.json", ShardJson("darling", 0,
            ("Ns.AlphaTests", new[] { "src/Gamma.cs", "src/Ui/Main.xaml.cs", "Darling/Darling.Tests/AlphaTests.cs" }, 2.5, 3, 0),
            ("Ns.ReadsTests", Array.Empty<string>(), 0.5, 1, 0)));
        // A second shard, and a retry of the first that saw another file and ran faster: files union, seconds take the longer.
        Write("shards/b/shard-darling-1.json", ShardJson("darling", 1,
            ("Ns.WalkerTests", Array.Empty<string>(), 1.0, 1, 0),
            ("Ns.GuardedTests", Array.Empty<string>(), 0.1, 1, 0),
            ("Ns.HoleTests", Array.Empty<string>(), 0.1, 1, 0)));
        Write("shards/c/shard-darling-0-retry.json", ShardJson("darling", 0,
            ("Ns.AlphaTests", new[] { "src/Other.cs" }, 1.0, 3, 0)));
        Write("shards/d/shard-lite-0.json", ShardJson("lite", 0,
            ("Ns.BetaTests", new[] { "src/Gamma.cs" }, 0.2, 1, 0)));
    }

    [Fact]
    public void TheMap_FollowsSchemaOne_AndUnionsShardsAndRetries()
    {
        SeedCheckout();
        SeedShards();

        var (code, output, map) = Build("--expect", "darling=2", "--expect", "lite=1");
        Assert.True(code == 0, output);
        using (map)
        {
            var root = map!.RootElement;
            Assert.Equal(1, root.GetProperty("schema").GetInt32());
            Assert.Equal("0123456789abcdef", root.GetProperty("sha").GetString());
            Assert.Equal("2026-10-08T00:00:00Z", root.GetProperty("built_at").GetString());
            Assert.Equal("altcover", root.GetProperty("tool").GetString());

            var universe = root.GetProperty("files").EnumerateArray().Select(e => e.GetString()!).ToList();
            Assert.Equal(universe.OrderBy(f => f, StringComparer.Ordinal).ToList(), universe);

            var alpha = root.GetProperty("classes").GetProperty("darling").GetProperty("AlphaTests");
            var alphaFiles = alpha.GetProperty("files").EnumerateArray().Select(e => universe[e.GetInt32()]).ToList();
            Assert.Contains("src/Gamma.cs", alphaFiles);          // coverage, shard 0
            Assert.Contains("src/Other.cs", alphaFiles);          // coverage, the retry (union)
            Assert.Contains("src/ProductThing.cs", alphaFiles);   // the type-name rule
            Assert.Contains("src/Ui/Main.xaml", alphaFiles);      // Main.xaml.cs stands for Main.xaml
            Assert.DoesNotContain("Darling/Darling.Tests/AlphaTests.cs", alphaFiles); // a class's own file is `own`
            Assert.Equal("Darling/Darling.Tests/AlphaTests.cs", universe[alpha.GetProperty("own").GetInt32()]);
            Assert.Equal(2.5, alpha.GetProperty("seconds").GetDouble());

            Assert.True(root.GetProperty("classes").GetProperty("lite").TryGetProperty("BetaTests", out _));
        }
    }

    [Fact]
    public void TextPatterns_CarryPathAndToolNameLiterals_AndTreeReaders()
    {
        SeedCheckout();
        SeedShards();

        var (code, output, map) = Build();
        Assert.True(code == 0, output);
        using (map)
        {
            var patterns = map!.RootElement.GetProperty("text_patterns");
            var reads = patterns.GetProperty("ReadsTests").EnumerateArray().Select(e => e.GetString()).ToList();
            Assert.Contains("web/app.js", reads);          // Path.Combine(root, "web", "app.js")
            Assert.Contains("install/01_schema.sql", reads);
            Assert.Contains("src/Tools.cs", reads);        // the "get_widget" tool name -> the file that declares it
            Assert.Equal("tree", patterns.GetProperty("WalkerTests").GetString());
            Assert.False(patterns.TryGetProperty("GuardedTests", out _), "a Guard class always runs and needs no pattern");
            Assert.False(patterns.TryGetProperty("HoleTests", out _));
        }
    }

    [Fact]
    public void TheSummary_NamesTheHoles_AndTheSize()
    {
        SeedCheckout();
        SeedShards();

        var (code, output, map) = Build();
        Assert.True(code == 0, output);
        map!.Dispose();
        var summary = File.ReadAllText(Path.Combine(_dir, "summary.md"));
        Assert.Contains("HoleTests", summary);
        Assert.DoesNotContain("AlphaTests (", summary);
        Assert.Contains("holes (no files and no pattern): 1", summary);
        Assert.Matches(@"size: \d+ bytes gzip", summary);
    }

    [Fact]
    public void TheMap_IsRefused_WhenAShardIsMissing()
    {
        SeedCheckout();
        SeedShards();

        var (code, output, _) = Build("--expect", "darling=3");

        Assert.NotEqual(0, code);
        Assert.Contains("no map written", output);
        Assert.False(File.Exists(Path.Combine(_dir, "map.json.gz")), "a partial map must not be written");
    }

    [Fact]
    public void TheCut_PutsEveryClassInExactlyOneShard_ByTheHashBuildYmlUses()
    {
        var names = Enumerable.Range(0, 60).Select(i => $"Ns.Sub.Class{i * 7919 % 1000}Tests").Distinct().ToList();
        File.WriteAllText(Path.Combine(_dir, "all.json"), JsonSerializer.Serialize(names));
        var seen = new List<string>();
        for (var shard = 0; shard < 3; shard++)
        {
            var outDir = Path.Combine(_dir, "cut" + shard);
            var (code, output) = Python("test-map.py", "cut", "--list", Path.Combine(_dir, "all.json"), "--shards", "3",
                "--shard", shard.ToString(), "--budget", "300", "--out", outDir);
            Assert.True(code == 0, output);
            var listed = File.ReadAllLines(Path.Combine(outDir, "listed.txt")).Where(l => l.Length > 0).ToList();
            // build.yml: BitConverter.ToUInt32(SHA256(UTF8(name)), 0) % job-total == shard
            foreach (var name in listed)
            {
                var hash = SHA256.HashData(Encoding.UTF8.GetBytes(name));
                Assert.Equal(shard, (int)(BitConverter.ToUInt32(hash, 0) % 3));
            }

            var chunks = Directory.GetFiles(outDir, "chunk-*.txt");
            Assert.True(chunks.Length > 1, "a 300 character budget must split the shard into several chunks");
            var chunked = chunks.SelectMany(c => File.ReadAllLines(c)).Where(l => l.Length > 0).ToList();
            Assert.Equal(listed.OrderBy(n => n).ToList(), chunked.OrderBy(n => n).ToList());
            seen.AddRange(listed);
        }

        Assert.Equal(names.OrderBy(n => n).ToList(), seen.OrderBy(n => n).ToList());
    }

    [Fact]
    public void TheShardReport_UnionsReports_AndCountsSkips()
    {
        var root = Path.Combine(_dir, "ws");
        Directory.CreateDirectory(root);
        string Report(string product, int uid) => $"""
            <CoverageSession>
              <Modules>
                <Module>
                  <Files><File uid="1" fullPath="{root.Replace('\\', '/')}/{product}" /></Files>
                  <Classes><Class><Methods><Method>
                    <FileRef uid="1" />
                    <MethodPoint vc="2" fileid="1"><TrackedMethodRefs><TrackedMethodRef uid="{uid}" vc="2" /></TrackedMethodRefs></MethodPoint>
                  </Method></Methods></Class></Classes>
                </Module>
                <Module>
                  <TrackedMethods>
                    <TrackedMethod uid="{uid}" name="System.Void Ns.AlphaTests::Runs()" />
                  </TrackedMethods>
                </Module>
              </Modules>
            </CoverageSession>
            """;
        File.WriteAllText(Path.Combine(_dir, "c1.xml"), Report("src/Gamma.cs", 5));
        File.WriteAllText(Path.Combine(_dir, "c2.xml"), Report("src/Other.cs", 7));
        File.WriteAllText(Path.Combine(_dir, "x1.xml"), """
            <assemblies><assembly><collection>
              <test type="Ns.AlphaTests" result="Pass" time="1.5" />
              <test type="Ns.AlphaTests" result="Skip" time="0.5" />
            </collection></assembly></assemblies>
            """);
        File.WriteAllText(Path.Combine(_dir, "listed.txt"), "Ns.AlphaTests\nNs.MissingTests\n");

        var (code, output) = Python("test-map.py", "shard-report", "--suite", "Darling.Tests", "--shard", "2", "--root", root,
            "--listed", Path.Combine(_dir, "listed.txt"), "--report", Path.Combine(_dir, "c1.xml"),
            "--report", Path.Combine(_dir, "c2.xml"), "--xml", Path.Combine(_dir, "x1.xml"), "--out", Path.Combine(_dir, "shard.json"));

        Assert.True(code == 0, output);
        using var doc = JsonDocument.Parse(File.ReadAllText(Path.Combine(_dir, "shard.json")));
        Assert.Equal("darling", doc.RootElement.GetProperty("suite").GetString());
        Assert.Equal(2, doc.RootElement.GetProperty("shard").GetInt32());
        var alpha = doc.RootElement.GetProperty("classes").GetProperty("Ns.AlphaTests");
        Assert.Equal(new[] { "src/Gamma.cs", "src/Other.cs" }, alpha.GetProperty("files").EnumerateArray().Select(e => e.GetString()).ToArray());
        Assert.Equal(2.0, alpha.GetProperty("seconds").GetDouble());
        Assert.Equal(2, alpha.GetProperty("tests").GetInt32());
        Assert.Equal(1, alpha.GetProperty("skipped").GetInt32());
        var missing = doc.RootElement.GetProperty("classes").GetProperty("Ns.MissingTests");
        Assert.False(missing.GetProperty("seen").GetBoolean());
        Assert.Equal(0, missing.GetProperty("files").GetArrayLength());
    }

    [Fact]
    public void UnwrapSkips_WithoutAPlainRun_ConvertsOnTheSkipToken()
    {
        File.WriteAllText(Path.Combine(_dir, "instr.xml"), """
            <assemblies><assembly total="2" passed="0" failed="2" skipped="0"><collection total="2" passed="0" failed="2" skipped="0">
              <test name="A.Skips" type="A" result="Fail"><failure><message>System.AggregateException: ($XunitDynamicSkip$no runtime)</message></failure></test>
              <test name="A.Breaks" type="A" result="Fail"><failure><message>Assert.Equal() Failure</message></failure></test>
            </collection></assembly></assemblies>
            """);

        var (code, output) = Python("altcover-probe.py", "unwrap-skips", "--instr", Path.Combine(_dir, "instr.xml"));

        Assert.True(code == 0, output);
        Assert.Contains("1 converted back to skips", output);
        var text = File.ReadAllText(Path.Combine(_dir, "instr.xml"));
        Assert.Contains("result=\"Skip\"", text);
        Assert.Contains("Assert.Equal() Failure", text);
        Assert.True(File.Exists(Path.Combine(_dir, "instr.raw.xml")), "the raw report is kept");
    }

    // ── the workflow ───────────────────────────────────────────────────────────────────────────────────────

    private static string Nightly() => File.ReadAllText(Path.Combine(ParitySource.RepoRoot(), ".github", "workflows", "nightly.yml"))
        .Replace("\r\n", "\n");

    private static string Job(string yaml, string name)
    {
        var start = yaml.IndexOf("\n  " + name + ":\n", StringComparison.Ordinal);
        Assert.True(start >= 0, "nightly.yml has no job " + name);
        var next = Regex.Match(yaml[(start + 1)..], @"\n  (?:#[^\n]*\n  )*[a-z][a-z0-9-]*:\n");
        return next.Success && next.Index > 0 ? yaml.Substring(start, next.Index + 1) : yaml[start..];
    }

    [Fact]
    public void TheJobs_AreOptional_AndNoOtherJobWaitsOnThem()
    {
        var yaml = Nightly();
        foreach (var name in new[] { "test-map-shard", "test-map" })
        {
            var job = Job(yaml, name);
            Assert.Contains("continue-on-error: true", job);
        }

        Assert.Contains("inputs.test_map != false", Job(yaml, "test-map-shard"));
        Assert.Matches(@"test_map:\n\s+description: [^\n]+\n\s+type: boolean\n\s+required: false\n\s+default: true", yaml);
        // "needs: [...]" and block lists: no other job may list the producer, or its failure would change that job's result.
        var checkedJobs = 0;
        foreach (Match m in Regex.Matches(yaml, @"\n  ([a-z][a-z0-9-]*):\n"))
        {
            var name = m.Groups[1].Value;
            if (name is "test-map" or "test-map-shard" or "schedule")
            {
                continue;
            }

            checkedJobs++;
            var needs = Regex.Match(Job(yaml, name), @"\n    needs: ([^\n]+)");
            if (needs.Success)
            {
                Assert.DoesNotContain("test-map", needs.Groups[1].Value);
            }
        }

        Assert.True(checkedJobs >= 6, "the job scan found too few jobs to mean anything");
    }

    [Fact]
    public void TheShardAndProbeJobs_WaitForBuildAndDarlingPg_ButStillRunAfterAFailedOne()
    {
        // The 10 windows shard legs and the AltCover probe used to need only `check`, so they raced build and darling-pg
        // for runners and build checked out about 8 minutes late (#5459). They wait now; always() keeps them running
        // after a failed build, and the check result must still be success (the check job is skipped on a schedule event).
        var yaml = Nightly();
        foreach (var name in new[] { "test-map-shard", "altcover-probe" })
        {
            var job = Job(yaml, name);
            var needs = Regex.Match(job, @"\n    needs: \[([^\]]+)\]");
            Assert.True(needs.Success, name + " has no needs list");
            var listed = needs.Groups[1].Value.Split(',', StringSplitOptions.TrimEntries | StringSplitOptions.RemoveEmptyEntries);
            Assert.Contains("check", listed);
            Assert.Contains("build", listed);
            Assert.Contains("darling-pg", listed);

            var condition = Regex.Match(job, @"\n    if: ([^\n]+)").Groups[1].Value;
            Assert.StartsWith("always() && needs.check.result == 'success' && ", condition);
        }

        // The join job keeps waiting on the shards alone.
        Assert.Matches(@"\n    needs: test-map-shard\n", Job(yaml, "test-map"));
    }

    [Fact]
    public void TheJobs_CutTheSameShardCountTheyJoin_AndPinTheProbesAltCover()
    {
        var yaml = Nightly();
        var shardJob = Job(yaml, "test-map-shard");
        var joinJob = Job(yaml, "test-map");

        foreach (var (suite, expected) in new[] { ("darling", 6), ("lite", 4) })
        {
            var legs = Regex.Matches(shardJob, @"\{ suite: " + suite + @", shard: (\d+), shards: (\d+) \}");
            Assert.Equal(Enumerable.Range(0, expected).ToList(), legs.Select(l => int.Parse(l.Groups[1].Value)).ToList());
            Assert.All(legs, l => Assert.Equal(expected, int.Parse(l.Groups[2].Value)));
            Assert.Contains($"--expect {suite}={expected}", joinJob);
        }

        var pinned = Regex.Match(Job(yaml, "altcover-probe"), @"ALTCOVER_VERSION: '([^']+)'").Groups[1].Value;
        Assert.NotEmpty(pinned);
        Assert.Contains($"ALTCOVER_VERSION: '{pinned}'", shardJob);

        // The runtime variable lights up the gated live classes; the Guard classes are listed too (a class the map never
        // saw would run as "new" on every pull request).
        Assert.Contains("DARLING_TEST_PGRUNTIME:", shardJob);
        Assert.DoesNotContain("-trait- Stage=Guard", shardJob);
        Assert.Contains("unwrap-skips", shardJob);
        Assert.Contains("retention-days: 14", joinJob);
        Assert.Contains("ref: ${{ github.sha }}", shardJob);
    }
}
