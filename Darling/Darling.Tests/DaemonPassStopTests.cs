/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Service;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// #9392: a background pass that meets a disposed object because the service is stopping must end its loop without a
/// Warning. The same disposed object while the service is NOT stopping is still a fault. These drive the shared pass
/// runner the external alert loop and the investigation loops use.
/// </summary>
public class DaemonPassStopTests
{
    private static int Faults(CapturingTestLogger log) =>
        log.CountAtLevel(LogLevel.Warning) + log.CountAtLevel(LogLevel.Error) + log.CountAtLevel(LogLevel.Critical);

    [Fact]
    public async Task ADisposedObject_WhileTheServiceIsStopping_EndsTheLoop_AndLogsNothingAtWarning()
    {
        var log = new CapturingTestLogger();
        using var stopping = new CancellationTokenSource();
        stopping.Cancel();

        var goOn = await DarlingWorker.RunDaemonPassAsync(
            log, () => throw new ObjectDisposedException("NpgsqlDataSource"), "External alert pass failed", stopping.Token);

        Assert.False(goOn);
        Assert.Equal(0, Faults(log));
    }

    [Fact]
    public async Task AWrappedDisposedObject_WhileTheServiceIsStopping_EndsTheLoop_AndLogsNothingAtWarning()
    {
        var log = new CapturingTestLogger();
        using var stopping = new CancellationTokenSource();
        stopping.Cancel();

        var goOn = await DarlingWorker.RunDaemonPassAsync(
            log, () => throw new NpgsqlException("wrapper", new ObjectDisposedException("NpgsqlDataSource")),
            "External alert pass failed", stopping.Token);

        Assert.False(goOn);
        Assert.Equal(0, Faults(log));
    }

    [Fact]
    public async Task APostmasterShutdown_WhileTheServiceIsStopping_EndsTheLoop_AndLogsNothingAtWarning()
    {
        var log = new CapturingTestLogger();
        using var stopping = new CancellationTokenSource();
        stopping.Cancel();

        var goOn = await DarlingWorker.RunDaemonPassAsync(
            log, () => throw new PostgresException("terminating connection due to administrator command", "FATAL", "FATAL", "57P01"),
            "External alert pass failed", stopping.Token);

        Assert.False(goOn);
        Assert.Equal(0, Faults(log));
    }

    [Fact]
    public async Task AWrappedDisposedObject_WhileTheServiceIsNotStopping_IsStillLoggedAsAWarning_AndTheLoopGoesOn()
    {
        var log = new CapturingTestLogger();
        using var running = new CancellationTokenSource();

        var goOn = await DarlingWorker.RunDaemonPassAsync(
            log, () => throw new NpgsqlException("wrapper", new ObjectDisposedException("NpgsqlDataSource")),
            "External alert pass failed", running.Token);

        Assert.True(goOn);
        Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public async Task ACancellation_WhileTheServiceIsStopping_EndsTheLoop_AndLogsNothingAtWarning()
    {
        var log = new CapturingTestLogger();
        using var stopping = new CancellationTokenSource();
        stopping.Cancel();

        var goOn = await DarlingWorker.RunDaemonPassAsync(
            log, () => throw new OperationCanceledException(stopping.Token), "External alert pass failed", stopping.Token);

        Assert.False(goOn);
        Assert.Equal(0, Faults(log));
    }

    [Fact]
    public async Task ADisposedObject_WhileTheServiceIsNotStopping_IsStillLoggedAsAWarning_AndTheLoopGoesOn()
    {
        var log = new CapturingTestLogger();
        using var running = new CancellationTokenSource();

        var goOn = await DarlingWorker.RunDaemonPassAsync(
            log, () => throw new ObjectDisposedException("NpgsqlDataSource"), "External alert pass failed", running.Token);

        Assert.True(goOn);
        Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
        Assert.Single(log.ExceptionsAtLevel(LogLevel.Warning));
        Assert.Contains("External alert pass failed", log.Joined, StringComparison.Ordinal);
    }

    [Fact]
    public async Task AnOtherFailure_WhileTheServiceIsStopping_IsStillLoggedAsAWarning()
    {
        var log = new CapturingTestLogger();
        using var stopping = new CancellationTokenSource();
        stopping.Cancel();

        var goOn = await DarlingWorker.RunDaemonPassAsync(
            log, () => throw new InvalidOperationException("boom"), "External alert pass failed", stopping.Token);

        Assert.True(goOn);
        Assert.Equal(1, log.CountAtLevel(LogLevel.Warning));
    }

    [Fact]
    public async Task APassThatSucceeds_GoesOn_AndLogsNothing()
    {
        var log = new CapturingTestLogger();
        var ran = false;

        var goOn = await DarlingWorker.RunDaemonPassAsync(
            log, () => { ran = true; return Task.CompletedTask; }, "unused", CancellationToken.None);

        Assert.True(goOn);
        Assert.True(ran);
        Assert.Equal(0, Faults(log));
    }
}
