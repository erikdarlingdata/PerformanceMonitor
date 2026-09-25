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
using Microsoft.Extensions.Logging.Abstractions;
using PerformanceMonitor.Darling.Analysis;
using PerformanceMonitor.Darling.Service;
using PerformanceMonitor.Darling.Service.Mcp;
using Xunit;
using static Darling.Tests.RepoFile;

namespace Darling.Tests;

/// <summary>
/// #4130: the daily retention purge moved off the fleet launch loop's inline await (346-400s deleting
/// ~839k rows, during which the loop launched no server sweeps and the whole fleet read stale) onto the
/// same fire-and-track shape the per-server sweeps and the oversized-plan backlog already use.
///
/// <para><see cref="DarlingWorker.TryStartScheduledPurge"/> is the extracted launch-loop decision, tested
/// here without driving the whole fleet loop: it must return immediately without awaiting the purge, must
/// never start a second purge while one is running, and must start the next one once the first completes
/// and the next due time arrives. A blocking <see cref="TaskCompletionSource"/> delegate stands in for the
/// real purge so "does not await" and "still running" are both directly observable rather than timing-
/// dependent.</para>
/// </summary>
public sealed class ScheduledPurgeLaunchTests
{
    private static DarlingWorker MakeWorker() => new(
        new CapturingLogger<DarlingWorker>(),
        NullLoggerFactory.Instance,
        new McpRuntimeState(),
        new WebRuntimeState(),
        new MonitoredServerRegistryState(),
        new CollectorRuntimeState(),
        new WebTlsCertificateState(),
        new BaselineCache());

    [Fact]
    public void ReturnsImmediately_WithoutAwaitingThePurge()
    {
        var worker = MakeWorker();
        var gate = new TaskCompletionSource();

        var launched = worker.TryStartScheduledPurge(
            DateTime.UtcNow, _ => gate.Task, CancellationToken.None);

        /* If this call had awaited the delegate, it would never return — the gate is never signalled in
           this test. Reaching this assertion at all is the proof. */
        Assert.True(launched);

        gate.SetResult();
    }

    [Fact]
    public void DoesNotStartASecondPurge_WhileTheFirstIsRunning()
    {
        var worker = MakeWorker();
        var gate = new TaskCompletionSource();
        var secondDelegateCalled = false;

        var firstLaunch = worker.TryStartScheduledPurge(
            DateTime.UtcNow, _ => gate.Task, CancellationToken.None);
        Assert.True(firstLaunch);

        var secondLaunch = worker.TryStartScheduledPurge(
            DateTime.UtcNow,
            _ =>
            {
                secondDelegateCalled = true;
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.False(secondLaunch);
        Assert.False(secondDelegateCalled);

        gate.SetResult();
    }

    [Fact]
    public async Task StartsTheNextOne_AfterTheFirstCompletes_AndTheTimeIsDue()
    {
        var worker = MakeWorker();
        var firstGate = new TaskCompletionSource();
        var start = DateTime.UtcNow;

        var firstLaunch = worker.TryStartScheduledPurge(
            start, _ => firstGate.Task, CancellationToken.None);
        Assert.True(firstLaunch);

        /* Still running, and the 24h cadence a launch stamps means "now" is not due either way — both
           reasons must fail this check the same way (no second launch). */
        var whileRunning = worker.TryStartScheduledPurge(
            start, _ => Task.CompletedTask, CancellationToken.None);
        Assert.False(whileRunning);

        firstGate.SetResult();

        /* Give the tracked task's continuation a turn to observe completion before the next check —
           the launch loop's own next tick is exactly this: some time has passed. Advance "now" past the
           24h cadence the first launch stamped, since that is what makes the SECOND launch due — the
           earlier "not running any more" alone would still be held off by the schedule otherwise. */
        await Task.Delay(TimeSpan.FromMilliseconds(50));
        var due = start.AddHours(25);

        var secondDelegateCalled = false;
        var secondLaunch = worker.TryStartScheduledPurge(
            due,
            _ =>
            {
                secondDelegateCalled = true;
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.True(secondLaunch);
        Assert.True(secondDelegateCalled);
    }

    [Fact]
    public void ANotYetDueCheck_NeverLaunches()
    {
        var worker = MakeWorker();

        /* _nextPurgeUtc starts at MinValue (first-run-at-start), so prime it forward with one completed
           launch before asserting the not-yet-due path — otherwise every "now" is >= MinValue and due. */
        var firstLaunch = worker.TryStartScheduledPurge(
            DateTime.UtcNow, _ => Task.CompletedTask, CancellationToken.None);
        Assert.True(firstLaunch);

        var called = false;
        var launched = worker.TryStartScheduledPurge(
            DateTime.UtcNow.AddHours(1),
            _ =>
            {
                called = true;
                return Task.CompletedTask;
            },
            CancellationToken.None);

        Assert.False(launched);
        Assert.False(called);
    }

    /* ---- census: no inline await remains in the launch loop ------------------------------------------ */

    [Fact]
    public void TheLaunchLoop_NoLongerAwaitsThePurgeInline()
    {
        /* The old inline gate ("if (DateTime.UtcNow >= _nextPurgeUtc)" awaiting PurgeAsync directly in the
           loop body) is gone from the launch site; the loop now only calls TryStartScheduledPurge and never
           reaches DarlingRetention.PurgeAsync itself — that call moved into RunScheduledPurgeAsync, which
           TryStartScheduledPurge fires and tracks rather than awaits. So the pin scopes to the launch-loop
           call site rather than the whole file: PurgeAsync's own await still exists (correctly) inside the
           extracted method that now runs off the launch loop's thread. */
        var worker = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        Assert.DoesNotContain(
            "if (DateTime.UtcNow >= _nextPurgeUtc)", worker, StringComparison.Ordinal);
        Assert.Contains("TryStartScheduledPurge(", worker, StringComparison.Ordinal);
        Assert.Contains("_purgeTask", worker, StringComparison.Ordinal);
        Assert.Contains("_purgeTask = RunTrackedAsync(startPurge, stoppingToken);", worker, StringComparison.Ordinal);
        Assert.DoesNotContain("await RunScheduledPurgeAsync", worker, StringComparison.Ordinal);
    }

    /// <summary>A no-op <see cref="ILogger{TCategoryName}"/> so <see cref="DarlingWorker"/> can be
    /// constructed without a real logging pipeline.</summary>
    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
        }
    }
}
