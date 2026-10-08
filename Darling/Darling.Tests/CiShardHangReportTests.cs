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

    private static string ReadStep(string stepName)
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "build.yml");
        Assert.True(File.Exists(path), "build.yml was not copied beside the test binary (Darling.Tests.csproj links it into Fixtures\\).");
        var text = File.ReadAllText(path);
        var start = text.IndexOf("- name: " + stepName + "\n", StringComparison.Ordinal);
        if (start < 0)
        {
            start = text.IndexOf("- name: " + stepName + "\r\n", StringComparison.Ordinal);
        }
        Assert.True(start >= 0, $"build.yml has no step named '{stepName}'.");
        var next = Regex.Match(text[(start + 1)..], @"\r?\n      - name: |\r?\n  [a-z][\w-]*:\r?\n");
        return next.Success ? text.Substring(start, next.Index + 1) : text[start..];
    }
}
