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
                     "await SweepStoreSelfMetricsAsync",
                     "_selfAlerts.EvaluateCollectorCostAsync",
                     "_selfAlerts.EvaluateToastSlackAsync",
                     "_selfAlerts.EvaluateCheckpointerPressureAsync",
                     "_selfAlerts.EvaluateFleetSweepRollupAsync",
                     "_selfAlerts.EvaluateAnalysisSinglesDigestAsync",
                 })
        {
            Assert.DoesNotContain(inline, loop, StringComparison.Ordinal);
        }

        Assert.Contains("TryStartStoreMaintenanceTick(", loop, StringComparison.Ordinal);
        Assert.Contains("TryStartStoreMetricsTick(", loop, StringComparison.Ordinal);

        /* The deadlock re-mask key is assigned on the loop, ahead of the launch. */
        var key = loop.IndexOf("_pgDeadlockRemaskKey = runner.LogHashKey;", StringComparison.Ordinal);
        Assert.True(key >= 0 && key < loop.IndexOf("TryStartStoreMetricsTick(", StringComparison.Ordinal));
    }

    [Fact]
    public void TheMetricsLauncher_TakesTheWindowOnlyWhenItLaunches_AndDefersBehindMaintenance()
    {
        var worker = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");
        var launcher = worker.IndexOf("internal bool TryStartStoreMetricsTick(", StringComparison.Ordinal);
        Assert.True(launcher > 0);
        var end = worker.IndexOf("\n    }\n", launcher, StringComparison.Ordinal);
        var body = worker[launcher..end];
        var defer = body.IndexOf("_storeMaintenanceTick is { IsCompleted: false }", StringComparison.Ordinal);
        var stamp = body.IndexOf("_nextStoreMetricsUtc = NextGridStamp(_nextStoreMetricsUtc, nowUtc, s_storeMetricsInterval);", StringComparison.Ordinal);
        var window = body.IndexOf("takeWindow()", StringComparison.Ordinal);
        var launch = body.IndexOf("RunTrackedTickAsync(", StringComparison.Ordinal);
        Assert.True(defer >= 0 && stamp > defer && window > stamp && launch > window, "defer < stamp < window < launch");
        Assert.Single(System.Text.RegularExpressions.Regex.Matches(worker, @"_checkpointSyncSampler\.TakeWindowMax"));
    }

    private static DateTime NextStoreMetrics(DarlingWorker w) =>
        (DateTime)typeof(DarlingWorker).GetField("_nextStoreMetricsUtc", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!.GetValue(w)!;

    private static readonly DateTime Now = new(2026, 10, 2, 12, 34, 5, DateTimeKind.Utc);

    [Fact]
    public void MetricsLauncher_ReturnsImmediately_WhileTheTickIsStillRunning()
    {
        var (worker, _) = MakeWorker();
        var gate = new TaskCompletionSource();
        Assert.True(worker.TryStartStoreMetricsTick(Now, () => null, (_, _) => gate.Task, CancellationToken.None));
        gate.SetResult();
    }

    [Fact]
    public void MetricsLauncher_DoesNotStartASecondTick_ButAdvancesTheStamp_AndWarns()
    {
        var (worker, logger) = MakeWorker();
        var gate = new TaskCompletionSource();
        Assert.True(worker.TryStartStoreMetricsTick(Now, () => null, (_, _) => gate.Task, CancellationToken.None));
        var taken = 0;
        var secondRan = false;
        var second = worker.TryStartStoreMetricsTick(
            Now.AddHours(1), () => { taken++; return null; }, (_, _) => { secondRan = true; return Task.CompletedTask; }, CancellationToken.None);

        Assert.False(second);
        Assert.False(secondRan);
        Assert.Equal(0, taken);
        Assert.True(NextStoreMetrics(worker) > Now);
        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Warning && e.Message.Contains("skipping this hour", StringComparison.Ordinal));
        gate.SetResult();
    }

    [Fact]
    public async Task MetricsLauncher_StartsTheNextTick_AfterTheFirstCompletes()
    {
        var (worker, _) = MakeWorker();
        var gate = new TaskCompletionSource();
        Assert.True(worker.TryStartStoreMetricsTick(Now, () => null, (_, _) => gate.Task, CancellationToken.None));
        gate.SetResult();
        await Task.Delay(TimeSpan.FromMilliseconds(50));

        var secondRan = false;
        Assert.True(worker.TryStartStoreMetricsTick(
            Now.AddHours(1), () => null, (_, _) => { secondRan = true; return Task.CompletedTask; }, CancellationToken.None));
        await Task.Delay(TimeSpan.FromMilliseconds(50));
        Assert.True(secondRan);
    }

    [Fact]
    public async Task AFaultingMetricsTick_IsObservedAndDoesNotBlockTheNext()
    {
        var (worker, logger) = MakeWorker();
        Assert.True(worker.TryStartStoreMetricsTick(
            Now, () => null, (_, _) => throw new InvalidOperationException("boom"), CancellationToken.None));
        await Task.Delay(TimeSpan.FromMilliseconds(50));

        Assert.Contains(logger.Entries, e => e.Level == LogLevel.Error && e.Exception is InvalidOperationException);
        Assert.True(worker.TryStartStoreMetricsTick(Now.AddHours(1), () => null, (_, _) => Task.CompletedTask, CancellationToken.None));
    }

    [Fact]
    public void DefersStoreMetrics_WhileTheMaintenanceTickRuns()
    {
        var (worker, _) = MakeWorker();
        var gate = new TaskCompletionSource();
        Assert.True(worker.TryStartStoreMaintenanceTick(_ => gate.Task, CancellationToken.None));
        var before = NextStoreMetrics(worker);
        var taken = 0;
        var ran = false;

        var launched = worker.TryStartStoreMetricsTick(
            Now, () => { taken++; return null; }, (_, _) => { ran = true; return Task.CompletedTask; }, CancellationToken.None);

        /* Deferred: no launch, the window is not taken, and the stamp is unmoved so the next 15 s pass retries. */
        Assert.False(launched);
        Assert.False(ran);
        Assert.Equal(0, taken);
        Assert.Equal(before, NextStoreMetrics(worker));

        gate.SetResult();
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
