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
/// Pins the Darling slow-class gate in .github/scripts/ci-select.py (#5946). A Darling shard leaves out a
/// Cost=Slow class on a pull request only when the class has not failed lately AND the test map knows it and says the
/// change does not reach it. These cases run the real <c>map-select</c> and <c>slow-skip</c> commands on a synthetic
/// map, tree and failure corpus, so they do not depend on today's repository.
///
/// <para>The map is built long ago on purpose: that makes the ordinary selection FULL (the map is too old to narrow
/// by), which is exactly the case the gate exists for. The gate still reads the map's coverage in that case.</para>
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Darling")]
public sealed class CiSelectSlowGateGuardTests : IDisposable
{
    private readonly string _dir = Path.Combine(Path.GetTempPath(), "ci-slow-gate-" + Guid.NewGuid().ToString("N"));

    private const string AFact = "\n    [Fact]\n    public void Runs() { }\n";

    private const string Store = "Darling/PerformanceMonitor.Darling.Service/Store.cs";

    public CiSelectSlowGateGuardTests()
    {
        var tests = Path.Combine(_dir, "tree", "Darling", "Darling.Tests");
        Directory.CreateDirectory(tests);
        foreach (var name in new[] { "ReachedSlow", "StableSlow", "FailedSlow", "OldFailedSlow", "UnmappedSlow" })
        {
            File.WriteAllText(
                Path.Combine(tests, name + ".cs"),
                "[Trait(\"Cost\", \"Slow\")]\npublic class " + name + "Tests\n{" + AFact + "}\n");
        }

        File.WriteAllText(Path.Combine(tests, "Plain.cs"), "public class PlainTests\n{" + AFact + "}\n");

        // One failure inside the five weeks (FailedSlowTests) and one long before them (OldFailedSlowTests).
        var corpus = new[]
        {
            Row(DateTime.UtcNow.AddDays(-3), "Darling.Tests.FailedSlowTests"),
            Row(DateTime.UtcNow.AddDays(-80), "Darling.Tests.OldFailedSlowTests"),
        };
        File.WriteAllLines(Path.Combine(_dir, "failures.jsonl"), corpus);
    }

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

    private static string Row(DateTime when, string failed) =>
        "{\"date\":\"" + when.ToString("yyyy-MM-dd", System.Globalization.CultureInfo.InvariantCulture) + "\",\"head_sha\":\"x\","
        + "\"event\":\"pull_request\",\"changed_files\":[],\"failed_classes\":[\"" + failed + "\"],\"failed_jobs\":[]}";

    private static string Script() => Path.Combine(ParitySource.RepoRoot(), ".github", "scripts", "ci-select.py");

    /// <summary>A map built 90 days ago (so the selection is FULL), covering the classes as described below.</summary>
    private string WriteMap()
    {
        var path = Path.Combine(_dir, "map.json");
        // ids: 0 Store.cs, 1 Other.cs, 2.. the classes' own files (UnmappedSlow is not in the map at all).
        File.WriteAllText(path, """
            {"schema": 1, "sha": "0000000", "built_at": "@BUILT@", "tool": "synthetic",
             "files": ["@STORE@", "Darling/PerformanceMonitor.Darling.Service/Other.cs",
                       "Darling/Darling.Tests/ReachedSlow.cs", "Darling/Darling.Tests/StableSlow.cs",
                       "Darling/Darling.Tests/FailedSlow.cs", "Darling/Darling.Tests/OldFailedSlow.cs",
                       "Darling/Darling.Tests/Plain.cs"],
             "classes": {"darling": {
                "ReachedSlowTests":    {"own": 2, "files": [0], "seconds": 9.0},
                "StableSlowTests":     {"own": 3, "files": [1], "seconds": 9.0},
                "FailedSlowTests":     {"own": 4, "files": [1], "seconds": 9.0},
                "OldFailedSlowTests":  {"own": 5, "files": [1], "seconds": 9.0},
                "PlainTests":          {"own": 6, "files": [1], "seconds": 1.0}}},
             "text_patterns": {}}
            """.Replace("@BUILT@", DateTime.UtcNow.AddDays(-90).ToString("o"), StringComparison.Ordinal)
               .Replace("@STORE@", Store, StringComparison.Ordinal));
        return path;
    }

    private (int ExitCode, string Stdout) Run(params string[] args)
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
        foreach (var a in args)
        {
            psi.ArgumentList.Add(a);
        }

        using var p = Process.Start(psi) ?? throw new InvalidOperationException("python did not start");
        var stdout = p.StandardOutput.ReadToEndAsync();
        var stderr = p.StandardError.ReadToEndAsync();
        Assert.True(p.WaitForExit(120_000), "ci-select.py did not finish in two minutes");
        return (p.ExitCode, stdout.Result + stderr.Result);
    }

    /// <summary>Runs map-select for `files` and returns the written selection file's path.</summary>
    private string Select(string[] files, string baseRef = "dev")
    {
        var map = WriteMap();
        var selection = Path.Combine(_dir, "selection-" + Guid.NewGuid().ToString("N") + ".json");
        var args = new List<string>
        {
            "map-select", "--map", map, "--root", Path.Combine(_dir, "tree"), "--event", "pull_request",
            "--base-ref", baseRef, "--drift-file", "-", "--out", selection,
        };
        args.AddRange(files);
        var (code, output) = Run(args.ToArray());
        Assert.True(code == 0, output);
        Assert.True(File.Exists(selection), output);
        return selection;
    }

    private string[] SlowSkip(string selection, string baseRef = "dev")
    {
        var (code, output) = Run(
            "--corpus", Path.Combine(_dir, "failures.jsonl"),
            "slow-skip", "--suite", "darling", "--event", "pull_request", "--base-ref", baseRef,
            "--root", Path.Combine(_dir, "tree"), "--selection", selection);
        Assert.Equal(0, code);
        return output.Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries).ToArray();
    }

    [Fact]
    public void ASlowClassTheMapSaysTheChangeDoesNotReach_AndThatHasNotFailedLately_IsSkipped_TheRestStay()
    {
        var skipped = SlowSkip(Select(new[] { Store }));

        // StableSlow covers only Other.cs, and OldFailedSlow failed 80 days ago: both are skipped.
        Assert.Equal(new[] { "OldFailedSlowTests", "StableSlowTests" }, skipped);
        // Reached: ReachedSlow runs code from Store.cs. Failed lately: FailedSlow stays although the map misses it.
        // Not in the map: UnmappedSlow stays. Not tagged: PlainTests is never in the skip list at all.
        Assert.DoesNotContain("ReachedSlowTests", skipped);
        Assert.DoesNotContain("FailedSlowTests", skipped);
        Assert.DoesNotContain("UnmappedSlowTests", skipped);
        Assert.DoesNotContain("PlainTests", skipped);
    }

    [Fact]
    public void WhenTheMapCannotSpeakForTheChange_NothingIsSkipped()
    {
        // A product file the map and its text patterns know nothing about makes the selection FULL even with the age
        // limit off, so the map's silence about a slow class means nothing and every class stays.
        var selection = Select(new[] { "Darling/PerformanceMonitor.Darling.Service/BrandNew.cs" });
        using var doc = JsonDocument.Parse(File.ReadAllText(selection));
        Assert.True(doc.RootElement.GetProperty("full").GetBoolean());
        Assert.Empty(doc.RootElement.GetProperty("slow_unreached").EnumerateObject());
        Assert.Empty(SlowSkip(selection));
    }

    [Fact]
    public void WithoutASelection_AndForAPullRequestIntoMain_NothingIsSkipped()
    {
        var selection = Select(new[] { Store });
        Assert.NotEmpty(SlowSkip(selection));

        // main is the release gate: it runs every class, Cost=Slow ones included.
        Assert.Empty(SlowSkip(selection, baseRef: "main"));
        // No selection file (the map-select step failed): the shard skips nothing.
        Assert.Empty(SlowSkip(Path.Combine(_dir, "missing.json")));
    }
}
