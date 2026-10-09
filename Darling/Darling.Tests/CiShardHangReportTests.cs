/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #5587: run 37824250187 attempt 1, Darling PG shard 3, ran to the JOB's 30-minute timeout. GitHub cancelled the
/// job, left every later step (the stop, both uploads) pending, kept no log, and the per-test timing was never
/// written, so nothing said which test was running or what it waited on. The shard test steps now carry a
/// step-level timeout (the step fails, the job goes on, and the <c>always()</c> steps run), the runner's
/// <c>-longRunning</c> report (a test that has run for 5 minutes is named in the log while the step is alive) and a
/// kept copy of the runner output. These are source pins on the workflow text, because the steps only run on a
/// Windows runner.
/// </summary>
public sealed class CiShardHangReportTests
{
    /// <summary>Under the job's 30 minutes with room for the setup before the step and the stop and uploads after it.</summary>
    private const int MaxStepTimeoutMinutes = 25;

    /// <summary>The slowest normal Darling test measured 120 s (the store-upgrade classes), so a report below it is noise.</summary>
    private const int MinLongRunningSeconds = 150;

    /// <summary>Well under the step timeout, or the report would arrive after the step was killed.</summary>
    private const int MaxLongRunningSeconds = 600;

    [Theory]
    [InlineData("Run Darling PG tests")]
    [InlineData("Run Lite tests (shard)")]
    public void ShardStep_HasAStepTimeout_UnderTheJobTimeout(string stepName)
    {
        var step = ReadStep(stepName);

        var timeout = Regex.Match(step, @"(?m)^        timeout-minutes:\s*(?<n>\d+)\s*$");
        Assert.True(timeout.Success,
            $"'{stepName}' has no step-level timeout-minutes. The job's own timeout cancels the job and leaves the upload steps pending (#5587).");
        var minutes = int.Parse(timeout.Groups["n"].Value);
        Assert.InRange(minutes, 10, MaxStepTimeoutMinutes);
    }

    [Theory]
    [InlineData("Run Darling PG tests")]
    [InlineData("Run Lite tests (shard)")]
    public void ShardStep_AsksTheRunnerToNameALongRunningTest_AndKeepsTheRunnerOutput(string stepName)
    {
        var step = ReadStep(stepName);

        var longRunning = Regex.Match(step, @"dotnet\s+run\s+[^\r\n]*@chunkArgs[^\r\n]*\s-longRunning\s+(?<n>\d+)\b");
        Assert.True(longRunning.Success, $"'{stepName}' does not pass -longRunning to the chunk's runner, so a hung test is not named.");
        Assert.InRange(int.Parse(longRunning.Groups["n"].Value), MinLongRunningSeconds, MaxLongRunningSeconds);

        Assert.Matches(@"dotnet\s+run\s+[^\r\n]*@chunkArgs[^\r\n]*\|\s*Tee-Object\s+-FilePath\s+TestResults/(darling|lite)-runner-\$\{\{ matrix\.shard \}\}\.log\s+-Append", step);
        Assert.Matches(@"New-Item\s+-ItemType\s+Directory\s+-Force\s+-Path\s+TestResults", step);
    }

    [Fact]
    public void DarlingUploads_RunWhateverTheTestStepDid()
    {
        var timings = ReadStep("Upload Darling test timings");
        Assert.Matches(@"(?m)^        if:\s*always\(\)", timings);

        var runner = ReadStep("Upload Darling runner output");
        Assert.Matches(@"(?m)^        if:\s*always\(\)", runner);
        Assert.Contains("TestResults/darling-runner-*.log", runner, StringComparison.Ordinal);
        Assert.Contains("darling-runner-log-${{ matrix.shard }}", runner, StringComparison.Ordinal);
        Assert.Matches(@"(?m)^        timeout-minutes:\s*\d+", runner);

        var failure = ReadStep("Upload PG log and test results on failure");
        Assert.Matches(@"(?m)^        if:\s*\(failure\(\) \|\| cancelled\(\)\)", failure);
    }

    [Fact]
    public void LiteUploads_RunWhateverTheTestStepDid()
    {
        var timings = ReadStep("Upload Lite test timings");
        Assert.Matches(@"(?m)^        if:\s*always\(\)", timings);

        var runner = ReadStep("Upload Lite runner output");
        Assert.Matches(@"(?m)^        if:\s*always\(\)", runner);
        Assert.Contains("TestResults/lite-runner-*.log", runner, StringComparison.Ordinal);
        Assert.Contains("lite-runner-log-${{ matrix.shard }}", runner, StringComparison.Ordinal);
    }

    /// <summary>
    /// #5587, nightly.yml: the same hang report for the single-process nightly steps. The step must still fail when the
    /// runner fails, so the pipe to <c>Tee-Object</c> is followed by a check of <c>$LASTEXITCODE</c>.
    /// </summary>
    [Theory]
    [InlineData("Run tests", "lite")]
    [InlineData("Run Darling PG tests", "darling")]
    public void NightlyStep_AsksTheRunnerToNameALongRunningTest_KeepsTheRunnerOutput_AndStillFailsWithTheRunner(string stepName, string product)
    {
        var step = ReadStep(stepName, "nightly.yml");

        var longRunning = Regex.Match(step, @"dotnet\s+run\s+[^\r\n]*\s-longRunning\s+(?<n>\d+)\b");
        Assert.True(longRunning.Success, $"nightly.yml '{stepName}' does not pass -longRunning to the runner, so a hung test is not named.");
        Assert.InRange(int.Parse(longRunning.Groups["n"].Value), MinLongRunningSeconds, MaxLongRunningSeconds);

        Assert.Matches(@"dotnet\s+run\s+[^\r\n]*\|\s*Tee-Object\s+-FilePath\s+TestResults/" + product + @"-runner-nightly\.log\s*(\r?\n)", step);
        Assert.Matches(@"New-Item\s+-ItemType\s+Directory\s+-Force\s+-Path\s+TestResults", step);
        Assert.Matches(@"Tee-Object[^\r\n]*\r?\n\s*if \(\$LASTEXITCODE -ne 0\) \{ exit \$LASTEXITCODE \}", step);
        Assert.Matches(@"(?m)^        shell:\s*pwsh\s*$", step);
    }

    /// <summary>
    /// #5587, nightly.yml: step timeouts from the last 5 completed nightly runs on dev (2026-10-06 to 2026-10-08). Lite:
    /// the step took 32.4 to 44.2 minutes and the job 4.9 minutes more at most, so with the job's 150 the timeout is
    /// 150 - 4.9 - 5 = 140, and 44.2 is 32% of it. Darling: 32.2 to 41.2 minutes in a job of 60, so 60 - 5.1 - 5 = 49 and
    /// 41.2 is 84% of it, over the 70% bar; the Darling step has no timeout of its own and says why in its comment.
    /// </summary>
    [Fact]
    public void NightlyStepTimeouts_FollowTheMeasuredNumbers()
    {
        var lite = ReadStep("Run tests", "nightly.yml");
        var timeout = Regex.Match(lite, @"(?m)^        timeout-minutes:\s*(?<n>\d+)\s*$");
        Assert.True(timeout.Success, "nightly.yml 'Run tests' (Lite) has no step-level timeout-minutes.");
        Assert.InRange(int.Parse(timeout.Groups["n"].Value), 60, 145);

        var darling = ReadStep("Run Darling PG tests", "nightly.yml");
        Assert.DoesNotMatch(@"(?m)^        timeout-minutes:", darling);
        Assert.Contains("NO step timeout, from numbers",
            File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "nightly.yml")), StringComparison.Ordinal);
    }

    [Fact]
    public void NightlyUploads_RunWhateverTheTestStepDid()
    {
        var lite = ReadStep("Upload Lite runner output", "nightly.yml");
        Assert.Matches(@"(?m)^        if:\s*always\(\)", lite);
        Assert.Contains("TestResults/lite-runner-*.log", lite, StringComparison.Ordinal);
        Assert.Contains("lite-runner-log-nightly", lite, StringComparison.Ordinal);
        Assert.Matches(@"(?m)^        timeout-minutes:\s*\d+", lite);

        var darling = ReadStep("Upload Darling runner output", "nightly.yml");
        Assert.Matches(@"(?m)^        if:\s*always\(\)", darling);
        Assert.Contains("TestResults/darling-runner-*.log", darling, StringComparison.Ordinal);
        Assert.Contains("darling-runner-log-nightly", darling, StringComparison.Ordinal);
        Assert.Matches(@"(?m)^        timeout-minutes:\s*\d+", darling);

        var failure = ReadStep("Upload PG log and test results on failure", "nightly.yml");
        Assert.Matches(@"(?m)^        if:\s*failure\(\) \|\| cancelled\(\)", failure);

        Assert.DoesNotContain("-runner-", ReadStep("Upload Lite test timings", "nightly.yml"), StringComparison.Ordinal);
    }

    [Fact]
    public void RunnerLogs_AreTheirOwnArtifacts_NotPartOfTheTimingArtifacts()
    {
        // The shadow check and the Lite shard packer read every file of the *-tests-timing-* artifacts.
        foreach (var name in new[] { "Upload Darling test timings", "Upload Lite test timings" })
        {
            Assert.DoesNotContain("-runner-", ReadStep(name), StringComparison.Ordinal);
        }
    }

    /// <summary>
    /// #5587: the shard-3 classes were read for a wait with no end. The one found is the cancelled refresh in
    /// <c>RollupBackfillLiveTests</c>: it waited on a task whose only way out was a cancel to a backend that was blocked
    /// on a lock the test itself still held, and the refresh's own command timeout is an hour. The wait is bounded now.
    /// </summary>
    [Fact]
    public void RollupBackfillLiveTests_TheCancelledRefreshWait_IsBoundedWhileTheChunkLockIsHeld()
    {
        var source = ReadSiblingSource("RollupBackfillLiveTests.cs");

        Assert.Contains("refresh.WaitAsync(CancelledRefreshLimit", source, StringComparison.Ordinal);
        Assert.Single(Regex.Matches(source, @"_ = await refresh;"));
        var released = source.IndexOf("lockTransaction.RollbackAsync(CancellationToken.None)", StringComparison.Ordinal);
        Assert.True(released >= 0, "the chunk lock is no longer released in a finally.");
        Assert.True(
            source.IndexOf("if (cancelDidNotStopTheRefresh)", released, StringComparison.Ordinal) is var guarded && guarded > released
                && source.IndexOf("_ = await refresh;", guarded, StringComparison.Ordinal) > guarded,
            "the only unbounded await of the refresh must come after the chunk lock is released.");
    }

    private static string ReadSiblingSource(string file, [CallerFilePath] string callerFile = "") =>
        File.ReadAllText(Path.Combine(Path.GetDirectoryName(callerFile)!, file)).Replace("\r\n", "\n");

    private static string ReadStep(string stepName, string workflow = "build.yml")
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", workflow);
        Assert.True(File.Exists(path), $"{workflow} was not copied beside the test binary (Darling.Tests.csproj links it into Fixtures\\).");
        var text = File.ReadAllText(path);
        var start = text.IndexOf("- name: " + stepName + "\n", StringComparison.Ordinal);
        if (start < 0)
        {
            start = text.IndexOf("- name: " + stepName + "\r\n", StringComparison.Ordinal);
        }
        Assert.True(start >= 0, $"{workflow} has no step named '{stepName}'.");
        var next = Regex.Match(text[(start + 1)..], @"\r?\n      - name: |\r?\n  [a-z][\w-]*:\r?\n");
        return next.Success ? text.Substring(start, next.Index + 1) : text[start..];
    }
}
