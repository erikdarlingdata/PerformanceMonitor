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
/// build.yml runs the Stage=Guard classes in their own job and fails fast (#5459, #5669). These pins hold the
/// wiring: the guard job runs both suites by trait, the jobs that read it say so, the long jobs start beside it and
/// stop themselves through a watcher when it fails, and the two result jobs turn every skip that was not planned
/// into a failure.
///
/// <para><b>What is pinned, and why each piece.</b> The guard job is not a required check, so the required checks
/// (<c>build</c>, <c>Darling PostgreSQL tests</c>, <c>Lite tests (result)</c> and the review guard, which lives in
/// another workflow) are what show a guard failure. <c>build</c> shows it through its watcher and its last step; the
/// two result jobs list the guard job in their <c>needs</c> and read its result, so a rename of the job, or a result
/// job that stops reading it, would let a skipped matrix read as green again. The exact <c>if:</c> of each shard job
/// is pinned as the census of by-design skips: <c>darling-pg</c> and <c>darling-linux</c> are skipped only when the
/// gate declined the run, and <c>lite-tests</c> also on a release. A third reason would be an unplanned skip, and
/// the result jobs' <c>skipped</c> arm is an error.</para>
///
/// <para><b>Docs-only pull requests</b> still report success on all four required checks: the guard job's
/// filter is a copy of the build job's docs entries and every step after its decision is gated on it, so the
/// job succeeds having built nothing; the shards' own path filters are untouched.</para>
/// </summary>
[Trait("Stage", "Guard")]
[Trait("Reads", "Lite")]
public sealed class GuardStageWorkflowTests
{
    // #5669: the shards and the Linux build start beside the guard job, so their condition no longer reads its result.
    private const string ShardIf =
        "if: ${{ !cancelled() && needs.gate.outputs.run == 'true' }}";

    private const string LiteShardIf =
        "if: ${{ !cancelled() && needs.gate.outputs.run == 'true' && github.event_name != 'release' }}";

    // The whole-tree guards job still waits for the guard job (and for `build`), and skips when the guards failed.
    private const string TreeGuardsIf =
        "if: ${{ !cancelled() && needs.gate.outputs.run == 'true' && (needs.guard-tests.result == 'success' || github.event_name != 'pull_request') }}";

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
        /* The uploads are the built Darling test project (#5459 change 2, pinned by TheGuardJobUploadsTheBuiltDarlingTests_... below; the Lite one went with the shards' fetch, #5669) and the two xunit reports the shadow check reads (#5459). */
        Assert.Equal(3, Regex.Matches(job, "actions/upload-artifact@").Count);
    }

    /// <summary>
    /// A skipped test exits 0, and the guard job has no PostgreSQL, so a tagged test that needs a store would skip
    /// and the job would stay green (M1 of #5471's review). <c>-failSkips</c> turns that into a red job. The loop also
    /// has to let both suites run when the first fails, which needs the native-command preference set outright (L1).
    /// </summary>
    [Fact]
    public void TheGuardJobsRunStep_FailsOnASkip_AndLetsBothSuitesRunWhenOneFails()
    {
        var run = Step(JobBlock(Yaml(), "guard-tests"), "Run the Stage=Guard classes of both suites");

        /* The one dotnet run of the tests (the class listing is a different command) carries the switch, once, in the loop. */
        var runs = Regex.Matches(run, @"^\s*dotnet run --project \$suite\.Project -c Release --no-build -- -trait Stage=Guard.*$", RegexOptions.Multiline);
        Assert.Single(runs);
        /* A whole switch, not a prefix: -failSkips- is the spelling that turns failing skips back off (L2 of the second review). */
        Assert.Matches(@"\s-failSkips(?:\s|$)", runs[0].Value);

        /* An uncommented statement of its own: a commented-out line must not satisfy the pin (L2 of the second review). */
        var preferenceLine = Regex.Match(run, @"^[ \t]*\$PSNativeCommandUseErrorActionPreference = \$false[ \t]*\r?$", RegexOptions.Multiline);
        Assert.True(preferenceLine.Success, "the guard loop does not set $PSNativeCommandUseErrorActionPreference explicitly, on a line of its own that is not a comment");
        var preference = preferenceLine.Index;
        Assert.True(
            preference > run.IndexOf("$ErrorActionPreference = 'Stop'", StringComparison.Ordinal)
              && preference < run.IndexOf("foreach ($suite in $suites)", StringComparison.Ordinal),
            "the native-command preference must be set after $ErrorActionPreference and before the loop");
    }

    /// <summary>
    /// darling-tree-guards now skips on a pull request whose guards failed, so its header must not promise "always
    /// reports a result" (L2 of #5471's review): a skip counts as passing, and the comment is what keeps the next
    /// reader from making it a required check.
    /// </summary>
    [Fact]
    public void TheTreeGuardsHeader_NoLongerPromisesAResultOnEveryEvent()
    {
        var yaml = Yaml();
        var at = yaml.IndexOf("\n  darling-tree-guards:\n", StringComparison.Ordinal);
        Assert.True(at >= 0);

        var header = yaml[Math.Max(0, at - 1400)..at];
        Assert.DoesNotContain("always reports a result, so it can be a required check", header, StringComparison.Ordinal);
        Assert.Contains("on a pull request whose Guard tests job did not", header, StringComparison.Ordinal);
        Assert.Contains("is NOT a required check", header, StringComparison.Ordinal);
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

    /* #5614: a changelog splice rewrites exactly CHANGELOG.md, docs/changelog/<version>.md and
       tools/changelog/archive-census.txt. The census is not markdown, so unless its exact path is on the docs
       allowlist the splice paid the full Guard tests and whole-tree guards run (about 17 minutes) instead of the
       docs fast path. The path is exact, never a bare *.txt glob: the MCP budget pins are .txt test inputs. */
    [Fact]
    public void AChangelogSplice_IsDocsOnly_InBothBuildYmlCopiesAndCiSelect()
    {
        var yaml = Yaml();
        var splice = new[] { "CHANGELOG.md", "docs/changelog/3.10.md", "tools/changelog/archive-census.txt" };
        const string census = "tools/changelog/archive-census.txt";

        var lists = new Dictionary<string, List<string>>
        {
            ["build job"] = FilterPatterns(JobBlock(yaml, "build"), "docs"),
            ["guard-tests job"] = FilterPatterns(JobBlock(yaml, "guard-tests"), "docs"),
        };

        /* ci-select.py's DOC_PATTERNS: the fallback when no live list is passed, which must not drift from build.yml. */
        var script = RepoFile.ReadRepoFileLf(".github", "scripts", "ci-select.py");
        var start = script.IndexOf("\nDOC_PATTERNS = (\n", StringComparison.Ordinal);
        Assert.True(start >= 0, "ci-select.py has no DOC_PATTERNS tuple");
        var tuple = script[start..];
        tuple = tuple[..tuple.IndexOf("\n)\n", StringComparison.Ordinal)];
        lists["ci-select.py DOC_PATTERNS"] = Regex.Matches(tuple, @"^\s*""([^""]+)"",\s*$", RegexOptions.Multiline)
            .Select(m => m.Groups[1].Value)
            .ToList();

        var buildList = lists["build job"];
        foreach (var (name, patterns) in lists)
        {
            Assert.NotEmpty(patterns);
            Assert.Equal(buildList, patterns);

            /* The two markdown files ride on the markdown glob; the census needs its own exact entry. */
            Assert.Contains("**/*.md", patterns);
            Assert.Contains(census, patterns);
            Assert.DoesNotContain("**/*.txt", patterns);
            Assert.DoesNotContain("tools/**", patterns);

            foreach (var path in splice)
            {
                Assert.True(
                    patterns.Contains(path, StringComparer.Ordinal)
                    || (path.EndsWith(".md", StringComparison.Ordinal) && patterns.Contains("**/*.md", StringComparer.Ordinal)),
                    $"{name}: a changelog splice file is not documentation, so the splice runs the full path: {path}");
            }
        }
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

    /// <summary>
    /// #5669: the report jobs and the whole-tree guards job read the guard job, so they list it. `build`, the shards and
    /// the Linux build do NOT: they start with the guard job, and a `needs` on it would put the ten-minute wait back.
    /// </summary>
    [Fact]
    public void TheJobsThatReadTheGuardJob_ListItInNeeds_AndTheJobsThatStartBesideItDoNot()
    {
        var yaml = Yaml();

        foreach (var key in new[] { "darling-pg-result", "lite-tests-result", "darling-tree-guards", "shadow-check" })
        {
            Assert.Contains("guard-tests", NeedsList(JobBlock(yaml, key)), StringComparer.Ordinal);
        }

        /* The result jobs keep the pinned `needs: [gate, ` prefix and add the guard before the matrix. */
        Assert.Contains("\n    needs: [gate, guard-tests, darling-pg]\n", JobBlock(yaml, "darling-pg-result"), StringComparison.Ordinal);
        Assert.Contains("\n    needs: [gate, guard-tests, lite-tests]\n", JobBlock(yaml, "lite-tests-result"), StringComparison.Ordinal);

        foreach (var key in new[] { "build", "darling-pg", "darling-linux", "lite-tests" })
        {
            var list = NeedsList(JobBlock(yaml, key));
            Assert.DoesNotContain("guard-tests", list, StringComparer.Ordinal);
            Assert.Contains("gate", list, StringComparer.Ordinal);
        }

        /* The whole-tree guards job reads `build`'s outputs (darling-tests, docs-fastpath), so it keeps that wait. */
        var treeGuards = JobBlock(yaml, "darling-tree-guards");
        Assert.Contains("build", NeedsList(treeGuards), StringComparer.Ordinal);
        Assert.Contains("needs.build.outputs.darling-tests", treeGuards, StringComparison.Ordinal);
        Assert.Contains("needs.build.outputs.docs-fastpath", treeGuards, StringComparison.Ordinal);
    }

    /// <summary>
    /// The census of by-design skips. A shard job is skipped only when the gate declined the run, and (the Lite shards)
    /// on a release. The guard job no longer skips any of them (#5669): they start beside it. The whole-tree guards job
    /// still waits for the guard job and skips when it failed on a pull request. Pinned as the whole condition so a new
    /// skip cause is a deliberate edit of this test, with the result job's reaction to it.
    /// </summary>
    [Fact]
    public void TheShardJobs_AreSkippedOnlyForThePlannedReasons()
    {
        var yaml = Yaml();

        Assert.Contains("\n    " + ShardIf + "\n", JobBlock(yaml, "darling-pg"), StringComparison.Ordinal);
        Assert.Contains("\n    " + ShardIf + "\n", JobBlock(yaml, "darling-linux"), StringComparison.Ordinal);
        Assert.Contains("\n    " + LiteShardIf + "\n", JobBlock(yaml, "lite-tests"), StringComparison.Ordinal);

        /* The darling-tree-guards job, not a shard, takes the guard fail-fast so it does not start a runner. */
        Assert.Contains("\n    " + TreeGuardsIf + "\n", JobBlock(yaml, "darling-tree-guards"), StringComparison.Ordinal);
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

    /// <summary>
    /// #5669: `build` starts with the guard job, on the Windows runner whenever the gate ran the run, and no longer fails
    /// a step on the guard's result. Its second step is the checkout; the guard is read by the watcher and by the last step.
    /// </summary>
    [Fact]
    public void TheBuildJob_StartsBesideTheGuardJob_AndStopsItselfThroughTheWatcher()
    {
        var build = JobBlock(Yaml(), "build");

        Assert.Contains("\n    if: ${{ !cancelled() }}\n", build, StringComparison.Ordinal);
        Assert.Contains(
            "runs-on: ${{ needs.gate.outputs.run == 'true' && 'windows-latest' || 'ubuntu-latest' }}",
            build,
            StringComparison.Ordinal);
        Assert.DoesNotContain("needs.guard-tests", build, StringComparison.Ordinal);

        var steps = build[build.IndexOf("    steps:\n", StringComparison.Ordinal)..]
            .Split("\n      - ", StringSplitOptions.None)
            .Skip(1)
            .Take(3)
            .ToList();

        Assert.StartsWith("name: Fail when CI did not run", steps[0], StringComparison.Ordinal);
        Assert.StartsWith("uses: actions/checkout@v7", steps[1], StringComparison.Ordinal);
        Assert.StartsWith("name: Watch the Guard tests job", steps[2], StringComparison.Ordinal);
    }

    /// <summary>
    /// #5669: every job that starts beside the guard job runs the watcher right after its checkout, on a pull request that
    /// the gate ran, and ends with the check step (<c>if: always()</c>, the last step). The token can read the run's jobs.
    /// `build` is the one required check with no result job reading the guard for it, so its check waits for a guard that
    /// has not finished; the others report without waiting (their result jobs read the guard's result).
    /// </summary>
    [Theory]
    [InlineData("build", false)]
    [InlineData("darling-pg", true)]
    [InlineData("lite-tests", true)]
    [InlineData("darling-linux", true)]
    public void EveryJobThatStartsBesideTheGuardJob_RunsTheWatcherAfterCheckout_AndChecksTheGuardAtTheEnd(string key, bool noWait)
    {
        var job = JobBlock(Yaml(), key);

        var steps = job[job.IndexOf("    steps:\n", StringComparison.Ordinal)..]
            .Split("\n      - ", StringSplitOptions.None)
            .Skip(1)
            .ToList();

        var watch = steps.Single(s => s.StartsWith("name: Watch the Guard tests job", StringComparison.Ordinal));
        Assert.Contains("if: ${{ github.event_name == 'pull_request' && needs.gate.outputs.run == 'true' }}", watch, StringComparison.Ordinal);
        Assert.Contains("shell: pwsh", watch, StringComparison.Ordinal);
        Assert.Contains("GH_TOKEN: ${{ github.token }}", watch, StringComparison.Ordinal);
        Assert.Contains("./.github/scripts/guard-watch.ps1 -Mode Start", watch, StringComparison.Ordinal);

        var checkout = steps.FindIndex(s => s.StartsWith("uses: actions/checkout@v7", StringComparison.Ordinal));
        Assert.True(checkout >= 0 && steps.IndexOf(watch) == checkout + 1,
            $"{key}: the watcher must start right after the checkout, before any work");

        /* The block runs to the next job's key line, so it carries that job's leading comment: cut it off. */
        var last = steps[^1].Split("\n\n  ")[0];
        Assert.StartsWith("name: Fail with the guard when the Guard tests job did not succeed", last, StringComparison.Ordinal);
        Assert.Contains("if: ${{ always() && github.event_name == 'pull_request' && needs.gate.outputs.run == 'true' }}", last, StringComparison.Ordinal);
        Assert.Contains("GH_TOKEN: ${{ github.token }}", last, StringComparison.Ordinal);
        Assert.Contains(
            noWait ? "guard-watch.ps1 -Mode Check -NoWait" : "guard-watch.ps1 -Mode Check\n",
            last + "\n",
            StringComparison.Ordinal);
        Assert.DoesNotContain("continue-on-error", last, StringComparison.Ordinal);

        /* A job-level permissions block replaces the workflow default; the watcher needs `actions: read`. */
        var head = job.Split("\n    steps:\n")[0] + "\n";
        if (head.Contains("\n    permissions:\n", StringComparison.Ordinal))
        {
            Assert.Contains("\n      actions: read\n", head, StringComparison.Ordinal);
        }
        else
        {
            Assert.Contains("\n  actions: read\n", Yaml().Split("\njobs:\n")[0], StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// The watcher script, pinned for the one property the ruling needs: a guard failure ends the running step so the job
    /// FAILS, and the job's own check names the guard. It never cancels the run (a run-level cancel would turn a required
    /// check into "cancelled"), and it never reports a verdict from an API call that failed.
    /// </summary>
    [Fact]
    public void TheGuardWatcher_StopsTheRunningStep_NeverCancelsTheRun_AndNamesTheGuard()
    {
        var script = RepoFile.ReadRepoFileLf(".github", "scripts", "guard-watch.ps1");

        Assert.DoesNotContain("/cancel", script, StringComparison.Ordinal);
        Assert.DoesNotContain("gh run cancel", script, StringComparison.Ordinal);
        Assert.Contains("actions/runs/$($env:GITHUB_RUN_ID)/jobs?filter=latest", script, StringComparison.Ordinal);
        Assert.Contains("-Name 'Runner.Worker'", script, StringComparison.Ordinal);
        Assert.Contains("Stop-Process", script, StringComparison.Ordinal);
        Assert.Contains("::error title=Guard tests failed::", script, StringComparison.Ordinal);
        Assert.Contains("exit 1", script, StringComparison.Ordinal);

        /* An unreadable job list is not a verdict: the read returns $null and the caller polls again. */
        var read = script[script.IndexOf("function Get-GuardJob", StringComparison.Ordinal)..script.IndexOf("function Test-GuardFailed", StringComparison.Ordinal)];
        Assert.Contains("catch", read, StringComparison.Ordinal);
        Assert.Contains("return $null", read, StringComparison.Ordinal);

        /* Only a completed job that did not succeed counts as failed. */
        Assert.Contains("($job.status -eq 'completed') -and ($job.conclusion -ne 'success')", script, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5669: the kill ends only the running step's process tree. The first planted failure ended every child of the
    /// runner's worker, the console host included, and every later step (the guard check, the failure uploads, the post
    /// steps) died at start with 0xC0000142. The worker's console host and the runner's helpers are left running, in the
    /// top-level children and inside the step's tree.
    /// </summary>
    [Fact]
    public void TheGuardWatcher_LeavesTheConsoleHostAndRunnerHelpersRunning_SoTheLaterStepsStart()
    {
        var script = RepoFile.ReadRepoFileLf(".github", "scripts", "guard-watch.ps1");

        var keep = script[script.IndexOf("function Test-KeepRunning", StringComparison.Ordinal)..script.IndexOf("function Stop-CurrentStep", StringComparison.Ordinal)];
        Assert.Contains("'conhost'", keep, StringComparison.Ordinal);
        Assert.Contains("'Runner.*'", keep, StringComparison.Ordinal);

        var stop = script[script.IndexOf("function Stop-CurrentStep", StringComparison.Ordinal)..script.IndexOf("switch ($Mode)", StringComparison.Ordinal)];
        /* Checked on the worker's direct children, then again on the members of the step's tree, before any Stop-Process. */
        Assert.Equal(2, System.Text.RegularExpressions.Regex.Matches(stop, @"\(Test-KeepRunning").Count);
        Assert.True(stop.IndexOf("if (Test-KeepRunning", StringComparison.Ordinal) < stop.IndexOf("Stop-Process", StringComparison.Ordinal));
        Assert.True(stop.IndexOf("-not (Test-KeepRunning", StringComparison.Ordinal) < stop.IndexOf("Stop-Process", StringComparison.Ordinal));
    }

    /// <summary>
    /// #5459 change 2: the Guard job uploads the built Darling.Tests right after its build and BEFORE it runs a test (a
    /// guard failure still leaves the artifact), with the producer's workspace and sha beside it, and publishes the
    /// artifact name as a job output only when the upload succeeded. The one consumer is the whole-tree guards job; since
    /// #5669 the shards build for themselves, so no Lite artifact is uploaded.
    /// </summary>
    [Fact]
    public void TheGuardJobUploadsTheBuiltDarlingTests_BeforeAnyTestRuns_WithTheWorkspaceAndShaBesideIt()
    {
        var job = JobBlock(Yaml(), "guard-tests");

        var build = job.IndexOf("- name: Build both test projects\n", StringComparison.Ordinal);
        var origin = job.IndexOf("- name: Record where this build was made\n", StringComparison.Ordinal);
        var uploadDarling = job.IndexOf("- name: Upload the built Darling.Tests for the whole-tree guards job\n", StringComparison.Ordinal);
        var run = job.IndexOf("- name: Run the Stage=Guard classes of both suites\n", StringComparison.Ordinal);
        Assert.True(build > 0 && build < origin && origin < uploadDarling && uploadDarling < run,
            "the Guard job must build, record the origin, upload the Darling test project, and only then run the guard classes");

        Assert.DoesNotContain("guard-test-build-lite", job, StringComparison.Ordinal);
        Assert.DoesNotContain("lite_build_artifact", job, StringComparison.Ordinal);

        var recorded = Step(job, "Record where this build was made");
        Assert.Contains("workspace=$env:PRODUCER_WORKSPACE", recorded, StringComparison.Ordinal);
        Assert.Contains("sha=$env:PRODUCER_SHA", recorded, StringComparison.Ordinal);
        Assert.Contains("PRODUCER_WORKSPACE: ${{ github.workspace }}", recorded, StringComparison.Ordinal);
        Assert.Contains("PRODUCER_SHA: ${{ github.sha }}", recorded, StringComparison.Ordinal);

        var upload = Step(job, "Upload the built Darling.Tests for the whole-tree guards job");
        Assert.Contains("id: upload-darling-build", upload, StringComparison.Ordinal);
        Assert.Contains("if: steps.decide.outputs.run == 'true'", upload, StringComparison.Ordinal);
        Assert.Contains("uses: actions/upload-artifact@v6", upload, StringComparison.Ordinal);
        Assert.Contains("name: guard-test-build-darling\n", upload, StringComparison.Ordinal);
        Assert.Contains("guard-build-origin.txt", upload, StringComparison.Ordinal);
        Assert.Contains("Darling/Darling.Tests/bin/Release", upload, StringComparison.Ordinal);
        Assert.Contains("Darling/Darling.Tests/obj/project.assets.json", upload, StringComparison.Ordinal);
        /* A test that reads another project's build output through the repo layout (the first version failed it). */
        Assert.Contains("Darling/PerformanceMonitor.Darling.Service/bin/Release", upload, StringComparison.Ordinal);
        Assert.Contains("retention-days: 1\n", upload, StringComparison.Ordinal);
        Assert.Contains("overwrite: true", upload, StringComparison.Ordinal);
        Assert.Contains("if-no-files-found: error", upload, StringComparison.Ordinal);
        Assert.Contains(
            "darling_build_artifact: ${{ steps.upload-darling-build.outcome == 'success' && 'guard-test-build-darling' || '' }}",
            job,
            StringComparison.Ordinal);
    }

    /// <summary>
    /// #5459 change 2, rulings 2 and 3, for the one job that still builds from the Guard job's artifact: the whole-tree
    /// guards job. It fetches the artifact of THIS run (no run-id), fails with an error when its workspace or sha differs,
    /// skips its restore and build and turns off the NuGet cache only when it used the artifact, and leaves Stage=Guard
    /// out of the class listing only then. Without the artifact it builds and runs every class. The PostgreSQL and Lite
    /// shards fetch nothing since #5669: they start beside the Guard job, so there is no build to wait for.
    /// </summary>
    [Fact]
    public void TheWholeTreeGuardsJob_UsesTheGuardBuildOnlyForThisRunAndWorkspace_AndLeavesGuardOutOnlyThen()
    {
        const string gate = "steps.gate.outputs.run == 'true'";
        var yaml = Yaml();
        var job = JobBlock(yaml, "darling-tree-guards");
        Assert.Contains("needs: [gate, ", job.Split("\n    steps:\n")[0], StringComparison.Ordinal);
        Assert.Contains("guard-tests", job.Split("\n    steps:\n")[0], StringComparison.Ordinal);

        foreach (var key in new[] { "darling-pg", "lite-tests", "build", "darling-linux" })
        {
            var other = JobBlock(yaml, key);
            Assert.DoesNotContain("guard-build", other, StringComparison.Ordinal);
            Assert.DoesNotContain("GUARD_BUILD_USED", other, StringComparison.Ordinal);
            Assert.DoesNotContain("-GuardBuildUsed", other, StringComparison.Ordinal);
            Assert.DoesNotContain("guard-tests.outputs", other, StringComparison.Ordinal);
        }

        /* The fetch: this run's artifact, by the producer's output, nothing from another run; a miss is a notice. */
        var fetch = Step(job, "Fetch the Guard job's build of the test project");
        Assert.Contains("id: guard-build\n", fetch, StringComparison.Ordinal);
        Assert.Contains($"if: {gate} && needs.guard-tests.outputs.darling_build_artifact != ''", fetch, StringComparison.Ordinal);
        Assert.Contains("continue-on-error: true", fetch, StringComparison.Ordinal);
        Assert.Contains("uses: actions/download-artifact@v6", fetch, StringComparison.Ordinal);
        Assert.Contains("name: ${{ needs.guard-tests.outputs.darling_build_artifact }}", fetch, StringComparison.Ordinal);
        Assert.DoesNotContain("run-id", fetch, StringComparison.Ordinal);
        Assert.DoesNotContain("github-token", fetch, StringComparison.Ordinal);

        /* The check: a different workspace or sha is an error, no artifact is a notice and the old path. */
        var check = Step(job, "Check the fetched build is for this workspace and commit");
        Assert.Contains("id: guard-build-check\n", check, StringComparison.Ordinal);
        Assert.Contains($"if: {gate}\n", check, StringComparison.Ordinal);
        Assert.Contains("FETCH_OUTCOME: ${{ steps.guard-build.outcome }}", check, StringComparison.Ordinal);
        Assert.Contains("THIS_WORKSPACE: ${{ github.workspace }}", check, StringComparison.Ordinal);
        Assert.Contains("THIS_SHA: ${{ github.sha }}", check, StringComparison.Ordinal);
        Assert.Contains("$origin['workspace'] -ne $env:THIS_WORKSPACE -or $origin['sha'] -ne $env:THIS_SHA", check, StringComparison.Ordinal);
        Assert.Contains("::error title=Guard build is for another workspace or commit::", check, StringComparison.Ordinal);
        Assert.Contains("::notice title=Building the test project here::", check, StringComparison.Ordinal);
        Assert.Contains("'used=true'", check, StringComparison.Ordinal);
        Assert.Contains("'used=false'", check, StringComparison.Ordinal);

        /* Restore, build and the NuGet cache wait for the check and run only when the artifact was not used. */
        const string notUsed = "steps.guard-build-check.outputs.used != 'true'";
        Assert.Contains($"if: {gate} && {notUsed}\n", Step(job, "Restore Darling.Tests"), StringComparison.Ordinal);
        Assert.Contains($"if: {gate} && {notUsed}\n", Step(job, "Build Darling.Tests"), StringComparison.Ordinal);
        Assert.Contains($"cache: ${{{{ {notUsed} }}}}", Step(job, "Setup .NET 10.0"), StringComparison.Ordinal);
        Assert.True(
            job.IndexOf("- name: Check the fetched build", StringComparison.Ordinal) < job.IndexOf("- name: Setup .NET 10.0", StringComparison.Ordinal),
            "the check has to come before setup-dotnet, whose cache input reads its output");

        /* Guard is left out only through the one filter the check's output feeds; there is no literal exclusion. */
        var run = Step(job, "Run the whole-tree guards");
        Assert.Contains("GUARD_BUILD_USED: ${{ steps.guard-build-check.outputs.used }}", run, StringComparison.Ordinal);
        Assert.Contains("$guardFilter = @(if ($env:GUARD_BUILD_USED -eq 'true') { '-trait-'; 'Stage=Guard' })", run, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(run, "'-trait-'"));
        Assert.DoesNotContain("-trait- Stage=Guard", run, StringComparison.Ordinal);
        Assert.Contains("--no-build -- @guardFilter", run, StringComparison.Ordinal);
    }

    private static List<string> NeedsList(string job)
    {
        var needs = Regex.Match(job, @"\n    needs: \[(?<list>[^\]]*)\]\n");
        Assert.True(needs.Success, "the job has no `needs: [..]` list");
        return needs.Groups["list"].Value.Split(',').Select(n => n.Trim()).ToList();
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
