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
/// The statement-filter release gate runs in the release pull request's CI, before the tag (#5320, #8756, N4).
///
/// <para><b>The defect this closes was a gate nobody ran.</b> <c>StatementColumnCensusTests.StatementCensus_PendingEntries_BlockARelease</c>
/// once failed while a statement-column census entry was pending only when <c>DARLING_RELEASE_CUT=1</c>, and the only
/// place that said to set it was a checklist step. A release that skipped the step shipped with a column still
/// unhooked and every check green. The first ruling (Erik, 2026-10-06) is that the release path runs the gate itself, and
/// that no other workflow sets the variable. Nothing is pending now, so the test fails on ANY pending entry whether or not
/// the variable is set (#5320): a pull request that adds an unjudged column fails in its own Darling test run, and the
/// release gate is the backstop, not the only place the gate bites.</para>
///
/// <para><b>The gate moved before the tag (#8756).</b> It used to be a step of the release build, which runs when the release is
/// published, so a failure could be seen only after users could download the build. It now runs in <c>pr.yml</c>, in the job
/// that builds <c>Darling.Tests</c> in Release, on a pull request whose base is <c>main</c> (the release pull request). The job's
/// failure reaches the required <c>CI result</c> through the verdict job. The release build runs no gate.</para>
/// </summary>
public sealed class ReleaseStatementGateWorkflowTests
{
    private const string PullRequestWorkflow = ".github/workflows/pr.yml";
    private const string ReleaseWorkflow = ".github/workflows/build.yml";
    private const string NightlyWorkflow = ".github/workflows/nightly.yml";
    // Bound to the test with nameof (via StatementColumnCensusTests.ReleaseGateTestName), so renaming the test fails the
    // workflow pin below rather than leaving the gate filtering on a name that matches nothing (review of #5367, B-M1).
    private const string GateTest = StatementColumnCensusTests.ReleaseGateTestName;

    // The one summary line the runner prints for exactly one passing test. The gate step must demand this whole line.
    private const string ExactGateSummary = "Total: 1, Errors: 0, Failed: 0, Skipped: 0, Not Run: 0";

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
    public void TheDarlingBuildJob_RunsTheStatementGateWithTheReleaseCutOn_OnlyOnAPullRequestToMain_BeforeTheBuildIsUploaded()
    {
        var steps = Steps(PullRequestWorkflow);
        var gate = Assert.Single(steps.Where(SetsTheGate).ToList());

        // Only the release pull request (head dev, base main) sets the variable. A pull request to dev, a push, the hourly
        // run and a manual dispatch all skip the step, so they never set it. The condition is exactly this and nothing wider.
        Assert.Matches(@"(?m)^        if:\s*github\.event_name == 'pull_request' && github\.base_ref == 'main'\s*$", gate.Text);
        Assert.DoesNotContain("continue-on-error", gate.Text, StringComparison.Ordinal);

        // The gate runs the one test: with no filter the runner executes the whole suite a second time.
        Assert.Matches(@"-method\s+""?\*?" + GateTest, gate.Text);

        // A -method filter that matches nothing exits 0 with "Total: 0", so a zero-test run would pass. The step must read the
        // runner's summary and fail unless it shows exactly the one passing test, and must also fail on the runner's own exit code.
        Assert.Contains(ExactGateSummary, gate.Text, StringComparison.Ordinal);
        Assert.Matches(@"-not\s*\(\s*Select-String[^\r\n]*" + Regex.Escape(ExactGateSummary) + @"[^\r\n]*\)\s*\)\s*\{", gate.Text);
        Assert.Matches(@"\$LASTEXITCODE\s+-ne\s+0", gate.Text);
        Assert.Matches(@"Tee-Object\s+-FilePath\s+(\S+)[\s\S]*Select-String\s+-Path\s+\1\s", gate.Text);

        // It reuses the job's own Release build of Darling.Tests (--no-build), so it must sit in the job that makes that build,
        // after the build step and before the build is trimmed and uploaded for the legs.
        Assert.Contains("--no-build", gate.Text, StringComparison.Ordinal);
        var jobSteps = steps.Where(s => s.Job == gate.Job).ToList();
        var build = jobSteps.FindIndex(s => Regex.IsMatch(s.Text, @"dotnet build Darling/Darling\.Tests/Darling\.Tests\.csproj -c Release"));
        Assert.True(build >= 0, $"the gate's job '{gate.Job}' does not build Darling.Tests in Release: the gate has nothing to run");
        var upload = jobSteps.FindIndex(s => s.Text.Contains("actions/upload-artifact", StringComparison.Ordinal));
        Assert.True(upload >= 0, "the gate's job has no artifact upload step: the scan is not reading it");

        var gateIndex = jobSteps.FindIndex(s => s.Name == gate.Name);
        Assert.True(
            build < gateIndex && gateIndex < upload,
            $"the statement gate step '{gate.Name}' (step {gateIndex}) must run after the Darling.Tests build (step {build}) and before "
            + $"the artifact upload (step {upload}), in the same job (#8756)");
    }

    [Fact]
    public void TheVerdictJob_NeedsTheGatesJob_SoAGateFailureFailsTheCiResult()
    {
        // The job that holds the gate is not itself the required check: `CI result` is set by the verdict job, which folds in the
        // result of every job it needs. A gate in a job the verdict does not need could fail without failing the pull request.
        var gateJob = Assert.Single(Steps(PullRequestWorkflow).Where(SetsTheGate).ToList()).Job;
        var lines = ReadRepoFileLf(PullRequestWorkflow).Split('\n');
        var start = Array.FindIndex(lines, l => Regex.IsMatch(l, @"^  verdict:\s*$"));
        Assert.True(start >= 0, "the verdict job was not found in pr.yml");

        var needsLine = lines.Skip(start + 1)
            .TakeWhile(l => !Regex.IsMatch(l, @"^  [A-Za-z0-9_-]+:\s*$"))
            .FirstOrDefault(l => Regex.IsMatch(l, @"^    needs:"));
        Assert.True(needsLine is not null, "the verdict job's needs list was not found in pr.yml");

        var needs = Regex.Match(needsLine!, @"\[([^\]]*)\]").Groups[1].Value.Split(',').Select(n => n.Trim()).ToList();
        Assert.Contains(gateJob, needs);
    }

    [Fact]
    public void TheGateTest_FailsOnAnyPendingEntry_WhetherOrNotTheReleaseCutIsSet()
    {
        // #5320: the ratchet is part of every Darling test run, so the test must not branch on the variable. A copy of the
        // old "only when DARLING_RELEASE_CUT=1" shape would let a pull request add an unjudged column and pass.
        var source = ReadRepoFileLf("Darling/Darling.Tests/StatementColumnCensusTests.cs");
        var start = source.IndexOf("public void " + GateTest + "()", StringComparison.Ordinal);
        Assert.True(start >= 0, "the gate test method was not found in StatementColumnCensusTests.cs");

        var end = source.IndexOf("\n    }\n", start, StringComparison.Ordinal);
        Assert.True(end > start, "the end of the gate test method was not found");

        var body = source[start..end];
        Assert.DoesNotContain("GetEnvironmentVariable", body, StringComparison.Ordinal);
        Assert.DoesNotContain("DARLING_RELEASE_CUT", body, StringComparison.Ordinal);
        Assert.Contains("Assert.True(", body, StringComparison.Ordinal);
        Assert.Contains("pending.Length == 0", body, StringComparison.Ordinal);
    }

    [Fact]
    public void OnlyTheReleasePullRequestStep_SetsTheReleaseCut()
    {
        // The test no longer reads the variable, so setting it elsewhere is redundant; the one pull-request-to-main step stays the one
        // place that names it, so the release backstop is a single pinned step rather than a variable scattered over workflows.
        var offending = Steps(PullRequestWorkflow)
            .Where(s => s.Text.Contains("DARLING_RELEASE_CUT", StringComparison.Ordinal))
            .Where(s => !(SetsTheGate(s)
                && s.Text.Contains("github.base_ref == 'main'", StringComparison.Ordinal)))
            .Select(s => s.Job + " / " + s.Name)
            .ToList();

        Assert.True(
            offending.Count == 0,
            "DARLING_RELEASE_CUT=1 fails the build while any statement column is pending; only the pull-request-to-main gate step may set it (#5320, #8756):\n  "
            + string.Join("\n  ", offending));

        // A workflow-level or job-level env would set it for every run and sit outside any step, where the scan above cannot see it.
        var mentions = ReadRepoFileLf(PullRequestWorkflow).Split('\n')
            .Count(l => !l.TrimStart().StartsWith('#') && l.Contains("DARLING_RELEASE_CUT", StringComparison.Ordinal));
        Assert.True(mentions == 1, $"pr.yml names DARLING_RELEASE_CUT on {mentions} non-comment lines; the one gate step must be the only one");
    }

    [Fact]
    public void NoOtherWorkflow_SetsTheReleaseCut_AndTheReleaseBuildRunsNoGate()
    {
        // The release build runs when the release is published, so a gate there cannot stop anything (#8756). Nightly and every other
        // workflow never name the variable either.
        var pullRequestFile = System.IO.Path.GetFileName(PullRequestWorkflow);
        var workflows = System.IO.Directory.GetFiles(PathTo(".github", "workflows"), "*.yml")
            .Select(f => System.IO.Path.GetFileName(f))
            .ToList();
        Assert.Contains(System.IO.Path.GetFileName(ReleaseWorkflow), workflows);
        Assert.Contains(System.IO.Path.GetFileName(NightlyWorkflow), workflows);
        Assert.Contains(pullRequestFile, workflows);

        var offending = new List<string>();
        foreach (var name in workflows.Where(w => w != pullRequestFile))
        {
            foreach (var line in ReadRepoFileLf(".github/workflows/" + name).Split('\n'))
            {
                if (!line.TrimStart().StartsWith('#') && line.Contains("DARLING_RELEASE_CUT", StringComparison.Ordinal))
                {
                    offending.Add(name + ": " + line.Trim());
                }
            }
        }

        Assert.True(
            offending.Count == 0,
            "DARLING_RELEASE_CUT=1 fails the build while any statement column is pending; only the pr.yml gate step for a pull request to main may set it (#5320, #8756):\n  "
            + string.Join("\n  ", offending));
    }
}
