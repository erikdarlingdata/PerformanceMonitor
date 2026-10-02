/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4970: the hourly store-maintenance tick (availability re-probe, compression job health, retention
/// re-evaluation, store-object convergence) used to be awaited inline in the collection launch loop, so a
/// slow pass held every sweep launch back once an hour. It now runs as a tracked background task, one at a
/// time, through <see cref="DarlingWorker.TryStartStoreMaintenanceTick"/>. A never-signalled
/// <see cref="TaskCompletionSource"/> stands in for the tick so "does not await" and "still running" are
/// directly observable.
/// </summary>
public sealed class StoreMaintenanceTickLaunchTests
{
    private static (DarlingWorker Worker, RecordingLogger Logger) MakeWorker()
    {
        var logger = new RecordingLogger();
        var worker = new DarlingWorker(
            logger,
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator());
        return (worker, logger);
    }

    [Fact]
    public void TheLaunchLoop_DoesNotAwaitTheHourlyTenantsInline()
    {
        var worker = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var start = worker.IndexOf("while (!stoppingToken.IsCancellationRequested)", StringComparison.Ordinal);
        var drain = worker.IndexOf("Shutdown drain for the fire-and-track bodies", StringComparison.Ordinal);
        Assert.True(start >= 0 && drain > start, "could not locate the launch loop span");
        var loop = worker[start..drain];

        foreach (var inline in new[]
                 {
                     "await ReprobeTimescaleAvailabilityAsync",
                     "await EvaluateCompressionJobHealthAsync",
                     "await ReevaluateRetentionPoliciesAsync",
                     "await ConvergeStoreObjectsAsync",
                 })
        {
            Assert.DoesNotContain(inline, loop, StringComparison.Ordinal);
        }

        Assert.Contains("TryStartStoreMaintenanceTick(", loop, StringComparison.Ordinal);
    }

    [Fact]
    public void ReturnsImmediately_WhileTheTickIsStillRunning()
    {
        var (worker, _) = MakeWorker();
        var gate = new TaskCompletionSource();

        /* The gate is never signalled: returning at all proves the launcher did not await the tick. */
        Assert.True(worker.TryStartStoreMaintenanceTick(_ => gate.Task, CancellationToken.None));

        gate.SetResult();
    }

    [Fact]
    public void DoesNotStartASecondTick_WhileTheFirstIsRunning()
    {
        var (worker, logger) = MakeWorker();
        var gate = new TaskCompletionSource();
        var secondRan = false;

        Assert.True(worker.TryStartStoreMaintenanceTick(_ => gate.Task, CancellationToken.None));
        var second = worker.TryStartStoreMaintenanceTick(
            _ =>
            {
                secondRan = true;
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.False(second);
        Assert.False(secondRan);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("skipping this hour", StringComparison.Ordinal));

        gate.SetResult();
    }

    [Fact]
    public async Task StartsTheNextTick_AfterTheFirstCompletes()
    {
        var (worker, _) = MakeWorker();
        var gate = new TaskCompletionSource();
        Assert.True(worker.TryStartStoreMaintenanceTick(_ => gate.Task, CancellationToken.None));
        gate.SetResult();
        await Task.Delay(TimeSpan.FromMilliseconds(50));

        var secondRan = false;
        Assert.True(worker.TryStartStoreMaintenanceTick(
            _ =>
            {
                secondRan = true;
                return Task.CompletedTask;
            },
            CancellationToken.None));
        await Task.Delay(TimeSpan.FromMilliseconds(50));
        Assert.True(secondRan);
    }

    [Fact]
    public async Task AFaultingTick_IsObservedAndDoesNotBlockTheNext()
    {
        var (worker, logger) = MakeWorker();
        Assert.True(worker.TryStartStoreMaintenanceTick(
            _ => throw new InvalidOperationException("boom"), CancellationToken.None));
        await Task.Delay(TimeSpan.FromMilliseconds(50));

        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Exception is InvalidOperationException);
        Assert.True(worker.TryStartStoreMaintenanceTick(_ => Task.CompletedTask, CancellationToken.None));
    }

    private sealed class RecordingLogger : ILogger<DarlingWorker>
    {
        public List<(LogLevel Level, string Message, Exception? Exception)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            lock (Entries)
            {
                Entries.Add((logLevel, formatter(state, exception), exception));
            }
        }
    }
}
