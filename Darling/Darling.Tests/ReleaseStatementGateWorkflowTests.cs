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
/// The statement-filter release gate runs inside the release job, before the job packages anything (#5320, N4).
///
/// <para><b>The defect this closes was a gate nobody ran.</b> <c>StatementColumnCensusTests.StatementCensus_PendingEntries_BlockARelease</c>
/// fails while any statement-column census entry is pending, but only when <c>DARLING_RELEASE_CUT=1</c>, and the only
/// place that said to set it was a checklist step. A release that skipped the step shipped with a column still
/// unhooked and every check green. The ruling (Erik, 2026-10-06) is that the release job runs the gate itself, and that
/// no other workflow does: nightly and pull-request CI must keep flowing while columns are pending.</para>
/// </summary>
public sealed class ReleaseStatementGateWorkflowTests
{
    private const string ReleaseWorkflow = ".github/workflows/build.yml";
    private const string NightlyWorkflow = ".github/workflows/nightly.yml";
    private const string GateTest = "StatementCensus_PendingEntries_BlockARelease";

    private sealed record Step(string Job, string Name, string Text);

    /// <summary>Every step of every job in a workflow, comment lines dropped so a rationale that quotes a command is not judged as one.</summary>
    private static List<Step> Steps(string workflow)
    {
        var steps = new List<Step>();
        var job = string.Empty;
        string? name = null;
        var text = new List<string>();
        var inJobs = false;

        void Flush()
        {
            if (name is not null)
            {
                steps.Add(new Step(job, name, string.Join("\n", text)));
            }

            name = null;
            text.Clear();
        }

        foreach (var raw in ReadRepoFileLf(workflow).Split('\n'))
        {
            if (raw.TrimStart().StartsWith('#'))
            {
                continue;
            }

            if (raw.StartsWith("jobs:", StringComparison.Ordinal))
            {
                inJobs = true;
                continue;
            }

            var jobHeader = Regex.Match(raw, @"^  ([A-Za-z0-9_-]+):\s*$");
            if (inJobs && jobHeader.Success)
            {
                Flush();
                job = jobHeader.Groups[1].Value;
                continue;
            }

            var stepHeader = Regex.Match(raw, @"^      - name:\s*(.*)$");
            if (stepHeader.Success)
            {
                Flush();
                name = stepHeader.Groups[1].Value.Trim();
            }

            if (name is not null)
            {
                text.Add(raw);
            }
        }

        Flush();

        // Non-vacuity: a scan that read nothing would satisfy the "no other workflow sets it" half by having nothing to judge.
        Assert.True(steps.Count >= 10, $"only {steps.Count} steps found in {workflow}; the scan is not reading the file");
        return steps;
    }

    private static bool IsPackaging(Step s) =>
        Regex.IsMatch(s.Text, @"dotnet publish |Compress-Archive|\bvpk\b");

    private static bool SetsTheGate(Step s) =>
        Regex.IsMatch(s.Text, @"DARLING_RELEASE_CUT\s*:\s*['""]?1['""]?\s*$", RegexOptions.Multiline);

    [Fact]
    public void TheReleaseJob_RunsTheStatementGateWithTheReleaseCutOn_BeforeItsFirstPackagingStep()
    {
        var steps = Steps(ReleaseWorkflow);
        var gate = Assert.Single(steps.Where(SetsTheGate).ToList());

        Assert.Contains("github.event_name == 'release'", gate.Text, StringComparison.Ordinal);
        Assert.Contains(GateTest, gate.Text, StringComparison.Ordinal);
        Assert.DoesNotContain("continue-on-error", gate.Text, StringComparison.Ordinal);

        // The gate runs the one test: with no filter the runner executes the whole suite a second time.
        Assert.Matches(@"-method\s+""?\*?" + GateTest, gate.Text);

        var jobSteps = steps.Where(s => s.Job == gate.Job).ToList();
        var firstPackaging = jobSteps.FindIndex(s => s.Name != gate.Name && IsPackaging(s));
        Assert.True(firstPackaging >= 0, "the gate's job has no packaging step: the scan is not reading it");

        var gateIndex = jobSteps.FindIndex(s => s.Name == gate.Name);
        Assert.True(
            gateIndex < firstPackaging,
            $"the statement gate step '{gate.Name}' (step {gateIndex}) must run before '{jobSteps[firstPackaging].Name}' (step {firstPackaging}), "
            + "the job's first packaging step, so a release cannot publish while a statement column is pending (#5320)");
    }

    [Theory]
    [InlineData(NightlyWorkflow)]
    [InlineData(ReleaseWorkflow)]
    public void OnlyTheReleaseStep_SetsTheReleaseCut(string workflow)
    {
        // Nightly and pull-request CI run while columns are pending, so none may set the variable outside the release-only step.
        var offending = Steps(workflow)
            .Where(s => s.Text.Contains("DARLING_RELEASE_CUT", StringComparison.Ordinal))
            .Where(s => !(workflow == ReleaseWorkflow
                && SetsTheGate(s)
                && s.Text.Contains("github.event_name == 'release'", StringComparison.Ordinal)))
            .Select(s => s.Job + " / " + s.Name)
            .ToList();

        Assert.True(
            offending.Count == 0,
            "DARLING_RELEASE_CUT=1 fails the build while any statement column is pending; only the release-only gate step may set it (#5320):\n  "
            + string.Join("\n  ", offending));
    }
}
