/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using PerformanceMonitorLite.Services;
using Xunit;

namespace PerformanceMonitorLite.Tests;

/// <summary>
/// #9745: a headless Lite shows nothing on screen, so the log is the only place an operator learns whether the running build is
/// current. The update check on startup writes exactly one line for each outcome, at Warning when the check itself failed, and each
/// line carries what the outcome rests on: the newer release, the latest release, or the reason the check failed.
/// </summary>
public sealed class StartupUpdateCheckTests
{
    private const string Running = "3.14.0-nightly.20261126.750";

    private sealed class Sink
    {
        public List<string> Info { get; } = [];
        public List<string> Warn { get; } = [];
        public int Total => Info.Count + Warn.Count;
    }

    private static UpdateCheckResult Release(bool updateAvailable, string latest) =>
        new(new UpdateInfo(updateAvailable, "3.14.0", latest, "https://example.invalid/release", ""), null);

    private static async Task<(StartupUpdateOutcome Outcome, Sink Log)> Run(
        Func<Task<string?>> velopack,
        Func<Task<UpdateCheckResult>> release,
        bool enabled = true,
        bool headless = true)
    {
        var sink = new Sink();
        var outcome = await StartupUpdateCheck.RunAsync(enabled, headless, Running, velopack, release, sink.Info.Add, sink.Warn.Add);
        return (outcome, sink);
    }

    private static Task<string?> VelopackFindsNothing() => Task.FromResult<string?>(null);

    private static Task<UpdateCheckResult> ReleaseCheckMustNotRun() => throw new InvalidOperationException("the release check ran");

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task VelopackFindsAnUpdate_LogsOneLineNamingTheVersion_AndSkipsTheReleaseCheck(bool headless)
    {
        var (outcome, log) = await Run(() => Task.FromResult<string?>("v3.15.0"), ReleaseCheckMustNotRun, headless: headless);

        Assert.Equal(StartupUpdateKind.Available, outcome.Kind);
        Assert.Equal("v3.15.0", outcome.Version);
        Assert.Equal(1, log.Total);
        Assert.Single(log.Info);
        Assert.Contains("v3.15.0", log.Info[0]);
        Assert.Contains(Running, log.Info[0]);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ReleaseCheckFindsANewerRelease_LogsOneLineNamingIt(bool headless)
    {
        var (outcome, log) = await Run(VelopackFindsNothing, () => Task.FromResult(Release(updateAvailable: true, "v3.15.0")), headless: headless);

        Assert.Equal(StartupUpdateKind.Available, outcome.Kind);
        Assert.Equal("v3.15.0", outcome.Version);
        Assert.Equal(1, log.Total);
        Assert.Contains("v3.15.0", Assert.Single(log.Info));
    }

    [Fact]
    public async Task ReleaseCheckFindsNothingNewer_LogsOneLineWithTheRunningAndTheLatestRelease()
    {
        var (outcome, log) = await Run(VelopackFindsNothing, () => Task.FromResult(Release(updateAvailable: false, "v3.13.2")));

        Assert.Equal(StartupUpdateKind.UpToDate, outcome.Kind);
        Assert.Equal(1, log.Total);
        var line = Assert.Single(log.Info);
        Assert.Contains(Running, line);
        Assert.Contains("v3.13.2", line);
    }

    [Fact]
    public async Task AVelopackFailure_IsNotAnOutcome_SoTheReleaseCheckDecides()
    {
        var (outcome, log) = await Run(
            () => throw new InvalidOperationException("this install is not managed by Velopack"),
            () => Task.FromResult(Release(updateAvailable: false, "v3.13.2")));

        Assert.Equal(StartupUpdateKind.UpToDate, outcome.Kind);
        Assert.Equal(1, log.Total);
        Assert.Empty(log.Warn);
    }

    [Fact]
    public async Task ReleaseCheckFails_LogsOneWarningWithTheReason()
    {
        var (outcome, log) = await Run(
            VelopackFindsNothing,
            () => Task.FromResult(new UpdateCheckResult(null, "GitHub answered 403 rate limit exceeded")));

        Assert.Equal(StartupUpdateKind.Failed, outcome.Kind);
        Assert.Equal(1, log.Total);
        var line = Assert.Single(log.Warn);
        Assert.Contains("403 rate limit exceeded", line);
        Assert.Contains(Running, line);
    }

    [Fact]
    public async Task AVelopackFailureAndAReleaseCheckFailure_ReportTheReleaseCheckReason()
    {
        var (outcome, log) = await Run(
            () => throw new InvalidOperationException("this install is not managed by Velopack"),
            () => Task.FromResult(new UpdateCheckResult(null, "HttpRequestException: no such host is known")));

        Assert.Equal(StartupUpdateKind.Failed, outcome.Kind);
        Assert.Equal(1, log.Total);
        var line = Assert.Single(log.Warn);
        Assert.Contains("no such host is known", line);
        Assert.DoesNotContain("not managed by", line);
    }

    [Fact]
    public async Task ReleaseCheckThrows_StillLogsOneWarningWithTheReason()
    {
        var (outcome, log) = await Run(VelopackFindsNothing, () => throw new TimeoutException("the request timed out"));

        Assert.Equal(StartupUpdateKind.Failed, outcome.Kind);
        Assert.Equal(1, log.Total);
        Assert.Contains("the request timed out", Assert.Single(log.Warn));
    }

    [Fact]
    public async Task ReleaseCheckWithNoReleaseAndNoReason_StillLogsOneWarning()
    {
        var (outcome, log) = await Run(VelopackFindsNothing, () => Task.FromResult(new UpdateCheckResult(null, null)));

        Assert.Equal(StartupUpdateKind.Failed, outcome.Kind);
        Assert.Equal(1, log.Total);
        Assert.Single(log.Warn);
    }

    [Fact]
    public async Task TheCheckIsOff_LogsOneLine_AndRunsNeitherCheck()
    {
        var (outcome, log) = await Run(
            () => throw new InvalidOperationException("the Velopack check ran"),
            ReleaseCheckMustNotRun,
            enabled: false);

        Assert.Equal(StartupUpdateKind.Skipped, outcome.Kind);
        Assert.Equal(1, log.Total);
        Assert.Single(log.Info);
    }

    [Fact]
    public void ADescribedFailure_NamesTheInnermostCause()
    {
        var failure = new HttpRequestException(
            "An error occurred while sending the request.",
            new System.IO.IOException("Unable to read data from the transport connection: the remote host reset it."));

        var text = UpdateCheckService.DescribeFailure(failure);

        Assert.Contains("the remote host reset it", text);
    }
}
