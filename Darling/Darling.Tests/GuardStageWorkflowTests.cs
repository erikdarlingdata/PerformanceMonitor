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

namespace Darling.Tests;

/// <summary>
/// build.yml runs the Stage=Guard classes first and fails fast (#5459). These pins hold the wiring: the guard
/// job runs both suites by trait, every job that waits on it says so, the shards start on a pull request only
/// after it succeeded, and the two result jobs turn every skip that was not planned into a failure.
///
/// <para><b>What is pinned, and why each piece.</b> The guard job is not a required check, so the four required
/// checks (<c>build</c>, <c>Darling PostgreSQL tests</c>, <c>Lite tests (result)</c> and the review guard, which
/// lives in another workflow) are what show a guard failure. That only works while each of the first three lists
/// the guard job in its <c>needs</c> and reads its result, so a rename of the job, or a result job that stops
/// reading it, would let a skipped matrix read as green again. The exact <c>if:</c> of each shard job is pinned
/// as the census of by-design skips: <c>darling-pg</c> and <c>darling-linux</c> are skipped only when the gate
/// declined the run or the guards failed on a pull request, and <c>lite-tests</c> also on a release. A fourth
/// reason would be an unplanned skip, and the result jobs' <c>skipped</c> arm is an error.</para>
///
/// <para><b>Docs-only pull requests</b> still report success on all four required checks: the guard job's
/// filter is a copy of the build job's docs entries and every step after its decision is gated on it, so the
/// job succeeds having built nothing; the shards' own path filters are untouched.</para>
/// </summary>
[Trait("Stage", "Guard")]
public sealed class GuardStageWorkflowTests
{
    private const string ShardIf =
        "if: ${{ !cancelled() && needs.gate.outputs.run == 'true' && (needs.guard-tests.result == 'success' || github.event_name != 'pull_request') }}";

    private const string LiteShardIf =
        "if: ${{ !cancelled() && needs.gate.outputs.run == 'true' && github.event_name != 'release' && (needs.guard-tests.result == 'success' || github.event_name != 'pull_request') }}";

    [Fact]
    public void TheGuardJob_IsNamedOnceNotRequiredByName_AndRunsBothSuitesByTrait()
    {
        var job = JobBlock(Yaml(), "guard-tests");

        Assert.Contains("\n    name: Guard tests\n", job, StringComparison.Ordinal);
        Assert.Contains("\n    needs: gate\n", job, StringComparison.Ordinal);
        Assert.Contains(
            "\n    if: ${{ needs.gate.outputs.run == 'true' && github.event_name != 'release' }}\n",
            job,
            StringComparison.Ordinal);
        Assert.Contains("\n    runs-on: windows-latest\n", job, StringComparison.Ordinal);
        Assert.Contains("\n    timeout-minutes: 20\n", job, StringComparison.Ordinal);
        Assert.Contains("\n      contents: read\n", job, StringComparison.Ordinal);

        /* Both suites (the Lite project is matched without its folder: a Darling.Tests literal that names a Lite path
           would have to be reachable by the Darling path filter), each listed by trait first (an empty list is a hard error) and then run by trait. */
        Assert.Contains("Darling/Darling.Tests/Darling.Tests.csproj", job, StringComparison.Ordinal);
        Assert.Contains("Lite.Tests.csproj", job, StringComparison.Ordinal);
        Assert.Contains("-c Release --no-build -- -list classes/json -trait Stage=Guard", job, StringComparison.Ordinal);
        Assert.Contains("-c Release --no-build -- -trait Stage=Guard", job, StringComparison.Ordinal);
        Assert.Contains("no class carries Stage=Guard", job, StringComparison.Ordinal);
        Assert.Contains("if ($failed.Count -gt 0) { throw", job, StringComparison.Ordinal);

        /* Release builds of both projects, once, after a locked-mode restore of both, in bash with errexit. */
        Assert.Single(Regex.Matches(job, @"dotnet build Darling/Darling\.Tests/Darling\.Tests\.csproj -c Release --no-restore"));
        Assert.Single(Regex.Matches(job, @"dotnet build \S*Lite\.Tests\.csproj -c Release --no-restore"));
        var restore = Step(job, "Restore both test projects");
        Assert.Contains("shell: bash", restore, StringComparison.Ordinal);
        Assert.Contains("set -euo pipefail", restore, StringComparison.Ordinal);
        Assert.Contains("dotnet restore Darling/Darling.Tests/Darling.Tests.csproj --locked-mode", restore, StringComparison.Ordinal);
        Assert.Matches(@"dotnet restore \S*Lite\.Tests\.csproj --locked-mode", restore);

        /* No store, and nothing the Lite timing packer could read as a timing source. */
        Assert.DoesNotContain("DARLING_TEST_", job, StringComparison.Ordinal);
        Assert.DoesNotContain("pg-runtime", job, StringComparison.Ordinal);
        Assert.DoesNotContain("lite-tests-timing", job, StringComparison.Ordinal);
        Assert.DoesNotContain("upload-artifact", job, StringComparison.Ordinal);
    }

    [Fact]
    public void TheGuardJobsFilter_IsACopyOfTheBuildJobsDocsAndAllEntries()
    {
        var yaml = Yaml();
        var guard = JobBlock(yaml, "guard-tests");
        var build = JobBlock(yaml, "build");

        foreach (var name in new[] { "docs", "all" })
        {
            var original = FilterPatterns(build, name);
            var copy = FilterPatterns(guard, name);

            /* Anti-vacuity on both sides: two empty lists are equal, which is the green that would mean this
               pin had stopped reading what it is named for. */
            Assert.NotEmpty(original);
            Assert.NotEmpty(copy);
            Assert.Equal(original, copy);
        }

        /* The guard job's filter holds only those two entries. */
        Assert.Equal(
            new[] { "all", "docs" },
            Regex.Matches(guard, "\n            ([a-z_]+):\n").Select(m => m.Groups[1].Value).OrderBy(n => n, StringComparer.Ordinal).ToArray());
    }

    [Fact]
    public void ADocsOnlyChange_LeavesTheGuardJobGreenHavingBuiltNothing()
    {
        var job = JobBlock(Yaml(), "guard-tests");
        var steps = job[job.IndexOf("    steps:\n", StringComparison.Ordinal)..]
            .Split("\n      - ", StringSplitOptions.None)
            .Skip(1)
            .ToList();

        var decide = steps.FindIndex(s => s.StartsWith("name: Decide whether the guard classes run", StringComparison.Ordinal));
        Assert.True(decide > 0, "the guard job's decision step was renamed");

        Assert.Contains("[ \"${ALL_COUNT:-0}\" -eq \"${DOCS_COUNT:-0}\" ]", steps[decide], StringComparison.Ordinal);
        Assert.Contains("echo \"run=false\" >> \"$GITHUB_OUTPUT\"", steps[decide], StringComparison.Ordinal);

        var after = steps.Skip(decide + 1).ToList();
        Assert.NotEmpty(after);
        foreach (var step in after)
        {
            Assert.True(
                step.Contains("if: steps.decide.outputs.run == 'true'", StringComparison.Ordinal),
                "A guard-job step after the decision does not wait for it, so a docs-only change would build and run "
              + "for nothing (or fail): " + step.Split('\n')[0]);
        }
    }

    [Fact]
    public void EveryJobThatTheGuardJobGates_ListsItInNeeds()
    {
        var yaml = Yaml();

        foreach (var key in new[]
                 {
                     "build", "darling-pg", "darling-linux", "lite-tests", "darling-pg-result", "lite-tests-result",
                     "darling-tree-guards",
                 })
        {
            var job = JobBlock(yaml, key);
            var needs = Regex.Match(job, @"\n    needs: \[(?<list>[^\]]*)\]\n");
            Assert.True(needs.Success, $"{key} has no `needs: [..]` list");
            Assert.Contains(
                "guard-tests",
                needs.Groups["list"].Value.Split(',').Select(n => n.Trim()),
                StringComparer.Ordinal);
        }

        /* The result jobs keep the pinned `needs: [gate, ` prefix and add the guard before the matrix. */
        Assert.Contains("\n    needs: [gate, guard-tests, darling-pg]\n", JobBlock(yaml, "darling-pg-result"), StringComparison.Ordinal);
        Assert.Contains("\n    needs: [gate, guard-tests, lite-tests]\n", JobBlock(yaml, "lite-tests-result"), StringComparison.Ordinal);
    }

    /// <summary>
    /// The census of by-design skips. A shard job is skipped only when the gate declined the run, when the
    /// guard job did not succeed on a pull request, and (the Lite shards) on a release. Pinned as the whole
    /// condition so a new skip cause is a deliberate edit of this test, with the result job's reaction to it.
    /// </summary>
    [Fact]
    public void TheShardJobs_AreSkippedOnlyForThePlannedReasons()
    {
        var yaml = Yaml();

        Assert.Contains("\n    " + ShardIf + "\n", JobBlock(yaml, "darling-pg"), StringComparison.Ordinal);
        Assert.Contains("\n    " + ShardIf + "\n", JobBlock(yaml, "darling-linux"), StringComparison.Ordinal);
        Assert.Contains("\n    " + LiteShardIf + "\n", JobBlock(yaml, "lite-tests"), StringComparison.Ordinal);

        /* The darling-tree-guards job, not a shard, takes the same fail-fast so it does not start a runner. */
        Assert.Contains("\n    " + ShardIf + "\n", JobBlock(yaml, "darling-tree-guards"), StringComparison.Ordinal);
    }

    [Fact]
    public void TheResultJobs_FailOnAGuardFailure_AndOnAnUnplannedSkip_AndKeepTheDeclinedArm()
    {
        var yaml = Yaml();

        foreach (var key in new[] { "darling-pg-result", "lite-tests-result" })
        {
            var job = JobBlock(yaml, key);
            Assert.Contains("GUARD_RESULT: ${{ needs.guard-tests.result }}", job, StringComparison.Ordinal);

            var gateArm = job.IndexOf("if [ \"${GATE_RESULT}\" != \"success\" ]", StringComparison.Ordinal);
            var declinedArm = job.IndexOf("[ \"${GATE_RUN}\" != \"true\" ]", StringComparison.Ordinal);
            var guardArm = job.IndexOf("if [ \"${GUARD_RESULT}\" != \"success\" ]", StringComparison.Ordinal);
            var cases = job.IndexOf("case \"${RESULT}\" in", StringComparison.Ordinal);

            Assert.True(gateArm > 0 && declinedArm > gateArm && guardArm > declinedArm && cases > guardArm,
                $"{key}: the arms must read gate-failed, declined, guards-failed, then the matrix result");

            var arm = job[guardArm..cases];
            Assert.Contains("[ \"${EVENT_NAME}\" = \"release\" ] && [ \"${GUARD_RESULT}\" = \"skipped\" ]", arm, StringComparison.Ordinal);
            Assert.Contains("::error title=Guard tests failed::", arm, StringComparison.Ordinal);
            Assert.Contains("exit 1", arm, StringComparison.Ordinal);

            /* The skipped matrix arm is an error: it used to exit 0, which read a skipped job as a pass. */
            var skipped = job[job.IndexOf("            skipped)", StringComparison.Ordinal)..];
            skipped = skipped[..skipped.IndexOf(";;", StringComparison.Ordinal)];
            Assert.Contains("::error", skipped, StringComparison.Ordinal);
            Assert.Contains("exit 1", skipped, StringComparison.Ordinal);
            Assert.DoesNotContain("::notice", skipped, StringComparison.Ordinal);
            Assert.DoesNotContain("exit 0", job, StringComparison.Ordinal);
        }
    }

    [Fact]
    public void TheBuildJob_FailsItsSecondStepWhenTheGuardsFailedOnAPullRequest()
    {
        var build = JobBlock(Yaml(), "build");

        Assert.Contains("\n    if: ${{ !cancelled() }}\n", build, StringComparison.Ordinal);
        Assert.Contains(
            "needs.gate.outputs.run == 'true' && (github.event_name != 'pull_request' || needs.guard-tests.result == 'success') && 'windows-latest' || 'ubuntu-latest'",
            build,
            StringComparison.Ordinal);

        var steps = build[build.IndexOf("    steps:\n", StringComparison.Ordinal)..]
            .Split("\n      - ", StringSplitOptions.None)
            .Skip(1)
            .Take(3)
            .ToList();

        Assert.StartsWith("name: Fail when CI did not run", steps[0], StringComparison.Ordinal);
        Assert.StartsWith("name: Fail when the guard tests failed", steps[1], StringComparison.Ordinal);
        Assert.StartsWith("uses: actions/checkout@v7", steps[2], StringComparison.Ordinal);

        Assert.Contains(
            "if: ${{ github.event_name == 'pull_request' && needs.gate.outputs.run == 'true' && needs.guard-tests.result != 'success' }}",
            steps[1],
            StringComparison.Ordinal);
        Assert.Contains("::error title=Guard tests failed::The Guard tests job", steps[1], StringComparison.Ordinal);
        Assert.Contains("exit 1", steps[1], StringComparison.Ordinal);
    }

    private static string Yaml() => RepoFile.ReadRepoFileLf(".github", "workflows", "build.yml");

    /// <summary>One top-level job, from its key line to the next job's key line.</summary>
    private static string JobBlock(string yaml, string key)
    {
        var at = yaml.IndexOf($"\n  {key}:\n", StringComparison.Ordinal);
        Assert.True(at >= 0, $"build.yml has no job `{key}`");

        var rest = yaml[(at + 1)..];
        var next = Regex.Match(rest[(key.Length + 4)..], "\n  [A-Za-z0-9_-]+:\n");
        return next.Success ? "\n" + rest[..(key.Length + 4 + next.Index)] : "\n" + rest;
    }

    /// <summary>One step of a job block, from its name line to the next step.</summary>
    private static string Step(string job, string name)
    {
        var at = job.IndexOf($"- name: {name}\n", StringComparison.Ordinal);
        Assert.True(at >= 0, $"the job has no step `{name}`");

        var rest = job[at..];
        var next = rest.IndexOf("\n      - ", 1, StringComparison.Ordinal);
        return next < 0 ? rest : rest[..next];
    }

    /// <summary>The quoted patterns of one dorny/paths-filter entry inside a job block.</summary>
    private static List<string> FilterPatterns(string job, string filterName)
    {
        var at = job.IndexOf($"\n            {filterName}:\n", StringComparison.Ordinal);
        if (at < 0)
        {
            return new List<string>();
        }

        var rest = job[(at + 1)..];
        var next = Regex.Match(rest, "\n            [a-z_]+:\n");
        var block = next.Success ? rest[..next.Index] : rest;

        return Regex.Matches(block, @"^\s*-\s*'([^']+)'\s*$", RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value)
            .ToList();
    }
}
