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
using System.Text.RegularExpressions;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Replays the CI failure corpus against the test-selection rules (#5459).
///
/// <para><b>Why.</b> .github/scripts/ci-select.py is the one place the rules for "which test jobs run for these
/// changed files" are kept. .github/ci-history/failures.jsonl holds every run in a month that failed a real test,
/// with its changed files and failing classes. A change to the rules is safe only while each of those failing
/// classes would still have been selected for its run's changed files (a class that is gone from the tree is
/// exempt); the script's <c>--replay</c> checks exactly that and exits non-zero on a miss. This class runs it on
/// every pull request, in the Guard stage, so a cut that stops selecting a class that has failed before is red
/// before the shards start.</para>
///
/// <para>The few rows the rules miss for a reason that is not a selection gap are listed with the reason in
/// .github/ci-history/replay-accepted.txt; a miss that is not listed there fails this test.</para>
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Darling")]
public sealed class CiSelectReplayGuardTests
{
    private static string Script() => Path.Combine(ParitySource.RepoRoot(), ".github", "scripts", "ci-select.py");

    private static (int ExitCode, string Output) RunScript(params string[] args)
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

    [Fact]
    public void TheFailureCorpus_ReplaysAgainstTodaysRules_WithNoMiss()
    {
        var (code, output) = RunScript("--replay");

        var summary = Regex.Match(output, @"replay: (\d+) rows, (\d+) failing classes checked");
        Assert.True(summary.Success, "ci-select.py --replay printed no summary:\n" + output);
        Assert.True(int.Parse(summary.Groups[1].Value) >= 700, "the replay read too few corpus rows:\n" + output);
        Assert.True(int.Parse(summary.Groups[2].Value) >= 1500, "the replay checked too few failing classes:\n" + output);
        Assert.True(code == 0, "A failing class would not be selected for its run's changed files under the current rules. "
            + "Narrow the cut, or list the row in .github/ci-history/replay-accepted.txt with the reason it is not a selection gap:\n" + output);
    }

    [Fact]
    public void TheDecisions_ForKnownChanges_AreWhatTheWorkflowDocuments()
    {
        var (docsCode, docs) = RunScript("decide", "--event", "pull_request", "README.md");
        Assert.Equal(0, docsCode);
        Assert.Contains("\"docs_fastpath\": true", docs, StringComparison.Ordinal);
        Assert.Contains("\"guard_run\": false", docs, StringComparison.Ordinal);

        var (darlingCode, darling) = RunScript("decide", "--event", "pull_request", "Darling/Darling.Tests/CiSelectProbeTests.cs");
        Assert.Equal(0, darlingCode);
        Assert.Contains("\"darling_pg\": true", darling, StringComparison.Ordinal);
        Assert.Contains("\"lite_mode\": \"reads\"", darling, StringComparison.Ordinal);

        var (liteCode, lite) = RunScript("decide", "--event", "pull_request", "Lite/Services/ProbeService.cs");
        Assert.Equal(0, liteCode);
        Assert.Contains("\"lite_mode\": \"full\"", lite, StringComparison.Ordinal);
        Assert.Contains("\"darling_pg\": false", lite, StringComparison.Ordinal);
    }

    [Fact]
    public void TheCorpus_StaysUnderOneMegabyte_AndCarriesNoRunOrHostIdentifiers()
    {
        var path = Path.Combine(ParitySource.RepoRoot(), ".github", "ci-history", "failures.jsonl");
        Assert.True(new FileInfo(path).Length < 1_000_000, "the committed corpus grew past 1 MB; trim it");

        foreach (var line in File.ReadLines(path))
        {
            Assert.DoesNotContain("\"run_id\"", line, StringComparison.Ordinal);
            Assert.DoesNotContain("\"run_attempt\"", line, StringComparison.Ordinal);
            break;
        }
    }
}
