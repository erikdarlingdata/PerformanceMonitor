/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// Pins the shape of <c>darling-upgrade-shapes.yml</c> (#5627): the check that runs this commit's install and
/// upgrade scripts over installs that real releases made.
///
/// <para><b>Why it is pinned.</b> The check only protects anyone while it runs. It runs when a pull request
/// touches <c>Darling/tools/*.ps1</c> or the check itself, and in the nightly; a trigger edited away, a shape
/// dropped from the matrix, one of the two Windows images lost, or the nightly job unwired leaves a workflow
/// that still looks like a check and gates nothing. None of those turns a test red by itself, so this class
/// reads the workflow text and says so.</para>
///
/// <para>The pull-request trigger must stay <c>pull_request</c>: the legs run the pull request's own scripts,
/// and <c>pull_request_target</c> would run them with a write token and the repository's secrets.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class DarlingUpgradeShapesWorkflowTests
{
    private const string WorkflowPath = ".github/workflows/darling-upgrade-shapes.yml";
    private const string ScriptPath = ".github/scripts/darling-upgrade-shapes.ps1";

    private static string Workflow() => ReadRepoFileLf(WorkflowPath);

    /// <summary>The workflow with comment-only lines removed, so prose may name the words the pins look for.</summary>
    private static string Code(string yaml) =>
        string.Join('\n', yaml.Split('\n').Where(line => !line.TrimStart().StartsWith('#')));

    /// <summary>The indented block of the top-level key <paramref name="key"/> (the lines after it that are
    /// indented deeper than the key), or the empty string when the key is absent.</summary>
    private static string Block(string yaml, string key)
    {
        var lines = yaml.Split('\n');
        var start = Array.FindIndex(lines, l => l.StartsWith(key + ":", StringComparison.Ordinal));
        if (start < 0)
        {
            return "";
        }

        var body = new List<string>();
        for (var i = start + 1; i < lines.Length; i++)
        {
            if (lines[i].Length > 0 && !char.IsWhiteSpace(lines[i][0]))
            {
                break;
            }

            body.Add(lines[i]);
        }

        return string.Join('\n', body);
    }

    private static List<string> MatrixList(string yaml, string name)
    {
        var m = Regex.Match(yaml, @"(?m)^\s+" + Regex.Escape(name) + @":\s*\[(?<items>[^\]]*)\]");
        Assert.True(m.Success, $"the matrix has no '{name}: [ ... ]' list.");
        return m.Groups["items"].Value.Split(',').Select(i => i.Trim()).Where(i => i.Length > 0).ToList();
    }

    [Fact]
    public void TheTriggers_AreThePullRequestPaths_TheNightlyCall_AndAManualRun()
    {
        var code = Code(Workflow());
        var on = Block(code, "on");

        Assert.Contains("\n  pull_request:\n", "\n" + on, StringComparison.Ordinal);
        Assert.DoesNotContain("pull_request_target", code, StringComparison.Ordinal);
        Assert.Contains("\n  workflow_call:", "\n" + on, StringComparison.Ordinal);
        Assert.Contains("\n  workflow_dispatch:", "\n" + on, StringComparison.Ordinal);

        var paths = Regex.Matches(on, @"(?m)^\s+-\s+'([^']+)'\s*$").Select(m => m.Groups[1].Value).ToList();
        Assert.Contains("Darling/tools/*.ps1", paths);
        Assert.Contains(WorkflowPath, paths);
        Assert.Contains(ScriptPath, paths);
    }

    [Fact]
    public void TheMatrix_CoversTheFourShapes_OnBothWindowsImages()
    {
        var yaml = Code(Workflow());

        var os = MatrixList(yaml, "os");
        Assert.Equal(new[] { "windows-2022", "windows-2025" }, os.OrderBy(o => o, StringComparer.Ordinal));

        var legs = MatrixList(yaml, "leg");
        foreach (var shape in new[] { "U1", "U2", "U3", "U4" })
        {
            Assert.Contains(legs, l => l.StartsWith(shape + "-", StringComparison.Ordinal));
        }

        // Install AND upgrade for the three shapes that keep a service; U4 is the install refusal.
        foreach (var leg in new[] { "U1-install", "U1-upgrade", "U2-install", "U2-upgrade", "U3-install", "U3-upgrade", "U4-install" })
        {
            Assert.Contains(leg, legs);
        }

        /* A leg the workflow names but the script's ValidateSet does not accept fails at its first line, and a leg
           the script accepts but the workflow never runs is dead code. The two lists must be one list. */
        var script = ReadRepoFileLf(ScriptPath);
        var set = Regex.Match(script, @"\[ValidateSet\((?<items>[^)]*)\)\]\[string\]\$Leg");
        Assert.True(set.Success, "the script's $Leg parameter has no ValidateSet.");
        var accepted = Regex.Matches(set.Groups["items"].Value, @"'([^']+)'").Select(m => m.Groups[1].Value).ToList();
        Assert.Equal(accepted.OrderBy(a => a, StringComparer.Ordinal), legs.OrderBy(l => l, StringComparer.Ordinal));
    }

    [Fact]
    public void EveryJob_IsReadOnly_Bounded_AndChecksOutTheCommitUnderTest()
    {
        var yaml = Code(Workflow());

        Assert.Matches(@"(?m)^permissions:\n  contents: read\s*$", yaml);
        Assert.DoesNotMatch(@"(?m)^\s+(contents|packages|id-token|attestations|actions|pull-requests): write", yaml);

        var jobs = Block(yaml, "jobs");
        Assert.Matches(@"(?m)^    timeout-minutes: \d+\s*$", jobs);

        Assert.Matches(@"(?m)^concurrency:\n  group: darling-upgrade-shapes-\$\{\{ github\.ref \}\}\n  cancel-in-progress: true\s*$", yaml);

        var checkouts = Regex.Matches(yaml, @"uses: actions/checkout@v\d+\n\s+with:\n\s+ref: (?<ref>[^\n]+)");
        Assert.Equal(Regex.Matches(yaml, @"uses: actions/checkout@").Count, checkouts.Count);
        Assert.NotEmpty(checkouts);
        foreach (Match c in checkouts)
        {
            Assert.Equal("${{ github.sha }}", c.Groups["ref"].Value.Trim());
        }

        // Third-party actions are pinned to a release line, never a moving branch name.
        foreach (Match u in Regex.Matches(yaml, @"uses: (?<action>[^\s@]+)@(?<ref>\S+)"))
        {
            Assert.Matches(@"^v\d+$", u.Groups["ref"].Value);
        }
    }

    [Fact]
    public void TheReleaseVersions_AreNamedOnceAtTheTop()
    {
        var yaml = Code(Workflow());
        var top = Block(yaml, "env");

        Assert.Matches(@"DARLING_CURRENT_RELEASE: '\d+\.\d+\.\d+'", top);
        Assert.Matches(@"DARLING_OLD_RELEASE: '\d+\.\d+\.\d+'", top);

        var rest = yaml.Replace(top, "", StringComparison.Ordinal);
        Assert.DoesNotMatch(@"\b\d+\.\d+\.\d+\b", rest);
    }

    /// <summary>
    /// A release zip the legs download is run elevated, so its SHA-256 is compared with the release's own SHA256SUMS.txt
    /// before it is extracted: the zip function hashes it on every call, a missing or repeated listing fails the
    /// leg, and the only extraction of a downloaded zip comes after that function.
    /// </summary>
    [Fact]
    public void TheDownloadedReleaseZips_AreCheckedAgainstTheReleasesChecksumFile_BeforeTheyAreExtracted()
    {
        var script = ReadRepoFileLf(ScriptPath);

        var zipFunction = script[script.IndexOf("function Get-Zip(", StringComparison.Ordinal)..];
        zipFunction = zipFunction[..zipFunction.IndexOf("\n}\n", StringComparison.Ordinal)];
        Assert.True(zipFunction.IndexOf("gh release download", StringComparison.Ordinal) < zipFunction.IndexOf("Assert-ZipChecksum $version $zip", StringComparison.Ordinal));
        Assert.True(zipFunction.IndexOf("Assert-ZipChecksum $version $zip", StringComparison.Ordinal) < zipFunction.IndexOf("return $zip", StringComparison.Ordinal));

        var check = script[script.IndexOf("function Assert-ZipChecksum(", StringComparison.Ordinal)..];
        check = check[..check.IndexOf("\n}\n", StringComparison.Ordinal)];
        Assert.Contains("-p 'SHA256SUMS.txt'", check, StringComparison.Ordinal);
        Assert.Contains("Get-FileHash -LiteralPath $zip -Algorithm SHA256", check, StringComparison.Ordinal);
        Assert.Contains("$expected.Count -ne 1", check, StringComparison.Ordinal);
        Assert.Contains("$actual -ne $expected[0]", check, StringComparison.Ordinal);
        Assert.Contains("Fail-Leg", check, StringComparison.Ordinal);

        // Every extraction takes a zip that came out of Get-Zip.
        var extractions = Regex.Matches(script, @"tar -xf (\$\w+)");
        Assert.NotEmpty(extractions);
        foreach (Match extraction in extractions)
        {
            Assert.Matches(@"\$zip = Get-Zip \$\w+", script[..extraction.Index][^400..]);
        }
    }

    /// <summary>
    /// The upgrade-shape check found that Windows PowerShell 5.1 leaves <c>$PSScriptRoot</c> empty inside the
    /// param() default of a <c>[CmdletBinding()]</c> script started with <c>-File</c>, so <c>-Source</c> resolved to
    /// nothing and an upgrade run from its staging folder stopped at "No -Source given" (#5627). The body applies the
    /// default; if it goes, the documented way to run the script fails on the engine an operator's prompt starts.
    /// </summary>
    [Fact]
    public void TheUpgradeScript_AppliesItsSourceDefaultInTheBody()
    {
        var script = ReadRepoFileLf("Darling/tools/upgrade-darling.ps1");

        Assert.Contains("[string]$Source = $PSScriptRoot,", script, StringComparison.Ordinal);
        Assert.Contains("if ([string]::IsNullOrWhiteSpace($Source)) { $Source = $PSScriptRoot }", script, StringComparison.Ordinal);
    }

    [Fact]
    public void TheNightly_CallsTheWorkflow_GatedLikeItsOtherJobs_AndPublishDoesNotWaitOnIt()
    {
        var nightly = ReadRepoFileLf(".github/workflows/nightly.yml");

        var job = Regex.Match(nightly, @"(?ms)^  darling-upgrade-shapes:\n(?<body>.*?)(?=^  \S)");
        Assert.True(job.Success, "nightly.yml has no darling-upgrade-shapes job.");
        var body = job.Groups["body"].Value;

        Assert.Contains("    uses: ./.github/workflows/darling-upgrade-shapes.yml\n", body, StringComparison.Ordinal);
        Assert.Contains("    needs: check\n", body, StringComparison.Ordinal);
        Assert.Contains("    if: needs.check.outputs.has_changes == 'true' || inputs.from_schedule != true\n", body, StringComparison.Ordinal);

        var publish = Regex.Match(nightly, @"(?m)^  publish:\n    needs: (?<needs>\[[^\]]*\])").Groups["needs"].Value;
        Assert.NotEmpty(publish);
        Assert.DoesNotContain("darling-upgrade-shapes", publish, StringComparison.Ordinal);
    }
}
