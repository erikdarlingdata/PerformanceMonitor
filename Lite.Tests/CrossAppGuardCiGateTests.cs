/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// The DERIVED form of <see cref="WatermarkPolicyTests.TheLiteSuite_RunsOnEveryTreeTheHorizonPinScans"/>.
///
/// <para>That pin closed ONE instance: <c>Darling/PerformanceMonitor.Darling.Service</c> belongs only to the
/// <c>darling</c> path filter, so a Darling-only PR skipped the Lite suite that scans it. #2830 then hit the
/// same class twice more — <c>Lite.Tests/QueryStoreSliceTieBreakSourceTests.cs</c> reads DARLING'S counterpart
/// guard as source and asserts on the site total declared there, and raising that total 12 -> 18 fired
/// <c>darling</c> but not <c>lite</c>. Six checks green, merged, and the nightly on the merge commit failed
/// what the PR could not run. <c>dev</c> was red for ~20 minutes.</para>
///
/// <para><b>So this asserts the property rather than a path.</b> Every cross-app source reference in either
/// test project must be reachable by the filter that gates that project's suite. An enumerated list of trees
/// is what stopped covering the next file added — three times in one week — so the requirement is derived from
/// the source: add a read of the other app and the filter must grow, or this fails.</para>
///
/// <para>Only references that RESOLVE on disk are required, which drops message strings and the filter
/// patterns other guards assert on (those carry glob characters and are excluded outright). The glob matcher
/// is self-validated against known answers below, so a matcher bug fails loudly instead of green-washing.</para>
/// </summary>
public class CrossAppGuardCiGateTests
{
    /* Which filter gates which suite, per build.yml's "Run Lite tests" / "Run Darling tests" steps. */
    private const string LiteTestsDir = "Lite.Tests";
    private const string DarlingTestsDir = "Darling/Darling.Tests";

    [Fact]
    public void TheGlobMatcher_AgreesWithKnownAnswers()
    {
        /* Self-validation: these are the exact discriminations the assertions below depend on. If the
           matcher regressed, every coverage check would pass vacuously. */
        Assert.True(Matches("Lite/**/*.xaml", "Lite/Controls/ServerTab.xaml"));
        Assert.False(Matches("Lite/**/*.xaml", "Lite/Mcp/McpHealthTools.cs"));
        Assert.True(Matches("Lite/**/*.cs", "Lite/Mcp/McpHealthTools.cs"));
        Assert.True(Matches("Darling/Darling.Tests/**/!(*.md)", "Darling/Darling.Tests/Whatever.cs"));
        Assert.False(Matches("Darling/Darling.Tests/**/!(*.md)", "Darling/Darling.Tests/README.md"));
        Assert.True(Matches("Lite/**/!(*.md)", "Lite/Services/LocalDataService.cs"));

        /* The gap #2830 fell through: a Themes-only pattern does NOT reach the rest of the Viewer. */
        Assert.False(Matches(
            "Darling/PerformanceMonitor.Darling.Viewer/Themes/*.xaml",
            "Darling/PerformanceMonitor.Darling.Viewer/MainWindow.xaml"));
        Assert.True(Matches(
            "Darling/PerformanceMonitor.Darling.Viewer/**/!(*.md)",
            "Darling/PerformanceMonitor.Darling.Viewer/MainWindow.xaml"));

        Assert.True(Matches("README.md", "README.md"));
        Assert.False(Matches("README.md", "Lite/README.md"));
    }

    [Fact]
    public void EveryCrossAppSourceRead_IsReachableByTheFilterThatGatesItsSuite()
    {
        var repo = RepoRoot();
        var yaml = ReadBuildYaml(repo);

        var failures = new List<string>();

        Check(repo, yaml, failures,
            scannedProject: LiteTestsDir,
            otherApp: "Darling",
            filterName: "lite",
            gatingStep: "Run Lite tests");

        Check(repo, yaml, failures,
            scannedProject: DarlingTestsDir,
            otherApp: "Lite",
            filterName: "darling",
            gatingStep: "Run Darling tests");

        Assert.True(
            failures.Count == 0,
            "Cross-app guards that PR CI cannot run — add the path to the named filter in " +
            ".github/workflows/build.yml, or the guard only fires in the nightly, after the merge:\n  " +
            string.Join("\n  ", failures));
    }

    /// <summary>
    /// The suite that reads the whole tree has to run on the whole tree.
    ///
    /// <para><see cref="EveryCrossAppSourceRead_IsReachableByTheFilterThatGatesItsSuite"/> above covers reads
    /// of a NAMED path, which is what a cross-app guard does — there is a path, so a filter can name it. Some
    /// guards in <c>Darling.Tests</c> take the repository ITSELF as input instead: <c>FleetIdentifierScrubTests</c>
    /// enumerates every tracked file carrying one of nine extensions, and <c>MigrationUpgradeLadderLiveTests</c>
    /// derives the fixture it requires from <c>CHANGELOG.md</c>. Those have no path to add, and
    /// <c>darling</c> / <c>core</c> / <c>root</c> describe Darling product code rather than the tree.</para>
    ///
    /// <para>So <c>build.yml</c> answers it from the other side: the <c>build</c> job publishes whether it ran
    /// the suite, and a second job runs the suite when that reads <c>skipped</c>. Three pieces of wiring carry
    /// that, and losing any one of them leaves a job reporting success having run nothing — the step must
    /// carry the id, the job must publish its outcome, and the consuming job must actually invoke the suite.
    /// Pinned here rather than in <c>Darling.Tests</c> because the failure being pinned is a suite that does
    /// not run: a pin inside it would be gated by the thing it is checking.</para>
    /// </summary>
    [Fact]
    public void TheDarlingSuite_RunsWhereTheAreaFiltersDoNotReach()
    {
        var yaml = ReadBuildYaml(RepoRoot());

        var step = yaml.IndexOf("- name: Run Darling tests", StringComparison.Ordinal);
        Assert.True(step > 0, "the step that runs Darling.Tests was renamed — re-point this assertion before editing it");
        Assert.Contains(
            "id: darling-tests",
            yaml[step..Math.Min(step + 200, yaml.Length)],
            StringComparison.Ordinal);

        Assert.Contains("darling-tests: ${{ steps.darling-tests.outcome }}", yaml, StringComparison.Ordinal);

        var consumer = yaml.IndexOf("needs.build.outputs.darling-tests", StringComparison.Ordinal);
        Assert.True(
            consumer > 0,
            "nothing reads the build job's Darling.Tests outcome, so the suite runs only where the area "
          + "filters reach and a markdown-only or .github-only change runs none of it");

        /* From the consumer onward, so this cannot be satisfied by the darling-pg job's invocation earlier
           in the file — the job that reads the outcome is the one that has to run the suite. */
        var consumingJob = yaml[consumer..];
        Assert.Contains("= \"skipped\"", consumingJob, StringComparison.Ordinal);
        Assert.Contains(
            "dotnet run --project Darling/Darling.Tests/Darling.Tests.csproj",
            consumingJob,
            StringComparison.Ordinal);
    }

    private static void Check(
        string repo,
        string yaml,
        List<string> failures,
        string scannedProject,
        string otherApp,
        string filterName,
        string gatingStep)
    {
        var patterns = FilterPatterns(yaml, filterName);
        Assert.True(
            patterns.Count > 0,
            $"build.yml's '{filterName}' path filter is gone — find where it moved before editing this test");

        /* The step must actually consume the filter, or the entries are decoration. */
        var step = yaml.IndexOf("name: " + gatingStep, StringComparison.Ordinal);
        Assert.True(step > 0, $"the '{gatingStep}' step is gone — find where it moved before editing this test");
        Assert.Contains(
            $"steps.filter.outputs.{filterName} == 'true'",
            yaml[step..Math.Min(step + 400, yaml.Length)],
            StringComparison.Ordinal);

        foreach (var reference in CrossAppReferences(repo, scannedProject, otherApp))
        {
            if (!patterns.Any(p => Matches(p, reference.Probe)))
            {
                failures.Add(
                    $"{scannedProject} reads {reference.Raw} but the '{filterName}' filter does not reach it " +
                    $"(probe path: {reference.Probe})");
            }
        }
    }

    /// <summary>Repo-relative paths naming <paramref name="otherApp"/> that a test in
    /// <paramref name="project"/> reads, paired with a concrete file path to test coverage against.</summary>
    private static IEnumerable<(string Raw, string Probe)> CrossAppReferences(
        string repo, string project, string otherApp)
    {
        var seen = new SortedSet<string>(StringComparer.Ordinal);
        var projectRoot = Path.Combine(repo, project.Replace('/', Path.DirectorySeparatorChar));
        if (!Directory.Exists(projectRoot))
        {
            throw new DirectoryNotFoundException($"test project not found: {project}");
        }

        foreach (var file in Directory.EnumerateFiles(projectRoot, "*.cs", SearchOption.AllDirectories))
        {
            /* Build output carries copies of product source; scanning it would assert on artifacts. */
            if (file.Contains($"{Path.DirectorySeparatorChar}bin{Path.DirectorySeparatorChar}", StringComparison.Ordinal) ||
                file.Contains($"{Path.DirectorySeparatorChar}obj{Path.DirectorySeparatorChar}", StringComparison.Ordinal))
            {
                continue;
            }

            var text = File.ReadAllText(file);

            /* "Darling/Some/Path.cs" and the backslash spelling some Windows-facing pins use. */
            foreach (Match m in Regex.Matches(text, "\"(" + Regex.Escape(otherApp) + "[/\\\\][^\"]+)\""))
            {
                seen.Add(m.Groups[1].Value.Replace('\\', '/'));
            }

            /* Path.Combine("Darling", "Darling.Tests", "X.cs") */
            foreach (Match m in Regex.Matches(
                text, @"Path\.Combine\(\s*""" + Regex.Escape(otherApp) + @"""\s*,([^)]*)\)"))
            {
                var segments = Regex.Matches(m.Groups[1].Value, "\"([^\"]+)\"")
                    .Select(s => s.Groups[1].Value)
                    .ToArray();
                if (segments.Length > 0)
                {
                    seen.Add(otherApp + "/" + string.Join("/", segments));
                }
            }
        }

        foreach (var raw in seen)
        {
            /* A reference carrying glob syntax is an assertion ABOUT the filter (WatermarkPolicyTests does
               this), not a file read. Excluded rather than matched, or the guard would assert on itself. */
            if (raw.IndexOfAny(new[] { '*', '!', '(', ')', '{', '}' }) >= 0)
            {
                continue;
            }

            var onDisk = Path.Combine(repo, raw.Replace('/', Path.DirectorySeparatorChar));

            if (File.Exists(onDisk))
            {
                yield return (raw, raw);
            }
            else if (Directory.Exists(onDisk))
            {
                /* Directory reads are EnumerateCsFiles-shaped, so coverage of *.cs inside it is the ask. */
                yield return (raw + " (directory)", raw.TrimEnd('/') + "/CoverageProbe.cs");
            }

            /* Anything that resolves to neither is a message string or a moved file — not this test's business. */
        }
    }

    /// <summary>The quoted pattern entries of one dorny/paths-filter area block.</summary>
    private static List<string> FilterPatterns(string yaml, string filterName)
    {
        var at = yaml.IndexOf($"\n            {filterName}:\n", StringComparison.Ordinal);
        if (at < 0)
        {
            return new List<string>();
        }

        var rest = yaml[(at + 1)..];
        var next = Regex.Match(rest, "\n            [a-z_]+:\n");
        var block = next.Success ? rest[..next.Index] : rest;

        return Regex.Matches(block, @"^\s*-\s*'([^']+)'\s*$", RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value)
            .ToList();
    }

    /// <summary>Glob match for the dorny/paths-filter subset this repo uses: <c>**</c>, <c>*</c>, and a
    /// trailing <c>!(*.md)</c> markdown carve-out.</summary>
    private static bool Matches(string pattern, string path)
    {
        const string NotMarkdown = "/!(*.md)";
        var excludesMarkdown = pattern.EndsWith(NotMarkdown, StringComparison.Ordinal);
        if (excludesMarkdown)
        {
            if (path.EndsWith(".md", StringComparison.OrdinalIgnoreCase))
            {
                return false;
            }

            pattern = pattern[..^NotMarkdown.Length] + "/**";
        }

        var rx = new StringBuilder("^");
        for (var i = 0; i < pattern.Length; i++)
        {
            if (pattern[i] != '*')
            {
                rx.Append(Regex.Escape(pattern[i].ToString()));
                continue;
            }

            var isDouble = i + 1 < pattern.Length && pattern[i + 1] == '*';
            if (!isDouble)
            {
                rx.Append("[^/]*");
                continue;
            }

            if (i + 2 < pattern.Length && pattern[i + 2] == '/')
            {
                /* A double-star segment followed by a separator spans zero or more directories. */
                rx.Append("(?:.*/)?");
                i += 2;
            }
            else
            {
                rx.Append(".*");
                i += 1;
            }
        }

        rx.Append('$');
        return Regex.IsMatch(path, rx.ToString());
    }

    private static string ReadBuildYaml(string repo) =>
        File.ReadAllText(Path.Combine(repo, ".github", "workflows", "build.yml"))
            .Replace("\r\n", "\n", StringComparison.Ordinal);

    private static string RepoRoot([CallerFilePath] string thisFile = "")
    {
        for (var dir = new DirectoryInfo(Path.GetDirectoryName(thisFile)!); dir is not null; dir = dir.Parent)
        {
            if (Directory.Exists(Path.Combine(dir.FullName, "PerformanceMonitor.Collectors")))
            {
                return dir.FullName;
            }
        }

        throw new DirectoryNotFoundException($"could not locate the repo root walking up from {thisFile}");
    }
}
