/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Linq;
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
/// #5592: a retention pass that stopped on its wall budget with tables left must not wait a whole day for the rest.
/// <see cref="DarlingWorker.TryStartScheduledPurge"/> stamps the next pass 24 hours out when it launches;
/// <see cref="DarlingWorker.NotePurgePassEnded"/> (called by the scheduled pass and by <c>purge_now</c> when the sweep
/// returns) pulls that stamp in to at most one hour after the pass ended. Each test drives the real launchers with a
/// delegate that reports the way the real passes do, and reads the outcome as "is the next launch due at this time".
/// </summary>
public sealed class PurgeContinuationTests
{
    private static readonly DateTime T0 = new(2026, 10, 8, 3, 0, 0, DateTimeKind.Utc);

    private static DarlingWorker MakeWorker(out CapturingTestLogger log)
    {
        var captured = new CapturingTestLogger();
        log = captured;
        return new DarlingWorker(
            new WorkerLogger(captured),
            NullLoggerFactory.Instance,
            new McpRuntimeState(),
            new WebRuntimeState(),
            new MonitoredServerRegistryState(),
            new CollectorRuntimeState(),
            new WebTlsCertificateState(),
            new BaselineCache(),
            new ReadLatencyAccumulator());
    }

    private static readonly PurgeSummary BudgetStopped = new(TablesPurged: 12, RowsDeleted: 5_000, ChunksDropped: 0, TablesLeftOnBudget: 15);
    private static readonly PurgeSummary Finished = new(TablesPurged: 27, RowsDeleted: 5_000, ChunksDropped: 0);

    /// <summary>Launches a scheduled pass at <paramref name="at"/> and lets it finish, so the slot is free.</summary>
    private static async Task LaunchAndFinishAsync(DarlingWorker worker, DateTime at, Func<DarlingWorker, Task>? body = null)
    {
        Assert.True(worker.TryStartScheduledPurge(at, async _ => { if (body is not null) { await body(worker); } }, CancellationToken.None));
        await WaitForSlotAsync(worker, at);
    }

    /// <summary>Waits until the slot is free: a launch attempt at a far-future time that is not due must not say "still running".</summary>
    private static async Task WaitForSlotAsync(DarlingWorker worker, DateTime _)
    {
        for (var i = 0; i < 200 && worker.PurgeSlotBusyForTest; i++)
        {
            await Task.Delay(10);
        }

        Assert.False(worker.PurgeSlotBusyForTest);
    }

    private static bool DueAt(DarlingWorker worker, DateTime at) =>
        worker.TryStartScheduledPurge(at, _ => Task.CompletedTask, CancellationToken.None);

    [Fact]
    public async Task ABudgetStoppedScheduledPass_MakesTheNextLaunchDueWithinAnHourOfItsEnd()
    {
        var worker = MakeWorker(out var log);
        var end = T0.AddMinutes(30);

        await LaunchAndFinishAsync(worker, T0, w =>
        {
            Assert.True(w.NotePurgePassEnded(BudgetStopped, end));
            return Task.CompletedTask;
        });

        Assert.False(DueAt(worker, end.AddMinutes(59)));
        Assert.True(DueAt(worker, end.AddMinutes(61)));
        Assert.Contains(
            log.Lines,
            line => line.StartsWith("Information:", StringComparison.Ordinal)
                && line.Contains("15 table(s) left", StringComparison.Ordinal)
                && line.Contains(end.AddHours(1).ToString("O"), StringComparison.Ordinal));
    }

    [Fact]
    public async Task AFinishedPass_KeepsTheDailyStamp()
    {
        var worker = MakeWorker(out var log);
        var end = T0.AddMinutes(30);

        await LaunchAndFinishAsync(worker, T0, w =>
        {
            Assert.False(w.NotePurgePassEnded(Finished, end));
            return Task.CompletedTask;
        });

        Assert.False(DueAt(worker, end.AddHours(2)));
        Assert.False(DueAt(worker, T0.AddHours(23)));
        Assert.True(DueAt(worker, T0.AddHours(25)));
        Assert.DoesNotContain(log.Lines, line => line.Contains("table(s) left", StringComparison.Ordinal));
    }

    [Fact]
    public async Task ABudgetStoppedPurgeNow_PullsTheStampIn()
    {
        var worker = MakeWorker(out _);
        await LaunchAndFinishAsync(worker, T0);

        /* purge_now runs in the same slot; it ends well before the daily time, and stopped on its budget. */
        var end = T0.AddHours(2);
        var outcome = worker.TryStartPurgeNow(
            _ =>
            {
                Assert.True(worker.NotePurgePassEnded(BudgetStopped, end));
                return Task.CompletedTask;
            },
            customRetentionDays: null,
            CancellationToken.None);
        Assert.True(outcome.Success);
        await WaitForSlotAsync(worker, end);

        Assert.False(DueAt(worker, end.AddMinutes(59)));
        Assert.True(DueAt(worker, end.AddMinutes(61)));
    }

    [Fact]
    public async Task AStampAlreadySoonerThanEndPlusAnHour_IsNotPushedOut()
    {
        var worker = MakeWorker(out _);
        await LaunchAndFinishAsync(worker, T0);

        /* A first stopped pass ends at T0+1h, so the stamp becomes T0+2h. */
        Assert.True(worker.NotePurgePassEnded(BudgetStopped, T0.AddHours(1)));

        /* A second one ends at T0+5h; end + 1 h is later than the stamp, so the stamp stays. */
        Assert.False(worker.NotePurgePassEnded(BudgetStopped, T0.AddHours(5)));
        Assert.True(DueAt(worker, T0.AddHours(2).AddMinutes(1)));

        /* And the daily stamp itself, when it is the sooner one, stays too: ending at 23.5 h would pull to 24.5 h. */
        var other = MakeWorker(out _);
        await LaunchAndFinishAsync(other, T0);
        Assert.False(other.NotePurgePassEnded(BudgetStopped, T0.AddHours(23.5)));
        Assert.False(DueAt(other, T0.AddHours(23)));
        Assert.True(DueAt(other, T0.AddHours(24).AddMinutes(1)));
    }

    [Fact]
    public async Task ACancelledOrFailedPass_ChangesNothing()
    {
        var worker = MakeWorker(out var log);

        /* A cancelled sweep throws out of PurgeAsync, so the note after it is never reached: the stamp stays at 24 h. */
        await LaunchAndFinishAsync(worker, T0, _ => throw new OperationCanceledException());
        Assert.False(DueAt(worker, T0.AddHours(3)));
        Assert.True(DueAt(worker, T0.AddHours(25)));

        /* A failed sweep returns a summary with no tables left on budget (the run-record carries the ERROR). */
        var failed = MakeWorker(out _);
        await LaunchAndFinishAsync(failed, T0, w =>
        {
            Assert.False(w.NotePurgePassEnded(new PurgeSummary(3, 0, 0), T0.AddMinutes(5)));
            return Task.CompletedTask;
        });
        Assert.False(DueAt(failed, T0.AddHours(3)));
        Assert.True(DueAt(failed, T0.AddHours(25)));
        Assert.DoesNotContain(log.Lines, line => line.Contains("table(s) left", StringComparison.Ordinal));
    }

    [Fact]
    public async Task TheContinuation_NeverStartsASecondPass_WhileThePassIsStillRunning()
    {
        var worker = MakeWorker(out _);
        var gate = new TaskCompletionSource();
        Assert.True(worker.TryStartScheduledPurge(T0, _ => gate.Task, CancellationToken.None));

        /* The sweep returned (stamp pulled in) but the pass is still in its follow-on chores: the slot is held. */
        Assert.True(worker.NotePurgePassEnded(BudgetStopped, T0.AddMinutes(30)));
        var secondRan = false;
        Assert.False(worker.TryStartScheduledPurge(T0.AddHours(2), _ => { secondRan = true; return Task.CompletedTask; }, CancellationToken.None));
        Assert.False(secondRan);

        gate.SetResult();
        await WaitForSlotAsync(worker, T0);
        Assert.True(DueAt(worker, T0.AddHours(2)));
    }

    [Fact]
    public void ThePulledInStamp_PassesTheStampIsDueSpanCheck()
    {
        /* #4732: a stamp more than one interval ahead is read as a clock that stepped back and counts as due. The
           continuation stamp is sooner than the daily one, so it is inside the span and decides as a plain comparison. */
        var stamp = T0.AddHours(1);
        Assert.False(DarlingWorker.StampIsDue(stamp, TimeSpan.FromHours(24), T0.AddMinutes(30)));
        Assert.True(DarlingWorker.StampIsDue(stamp, TimeSpan.FromHours(24), T0.AddMinutes(61)));
    }

    [Fact]
    public void BothRunners_NoteTheEndOfTheSweep_RightAfterItReturns()
    {
        var worker = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingWorker.cs");

        var scheduled = worker.IndexOf("var purgeSummary = await DarlingRetention.PurgeAsync(", StringComparison.Ordinal);
        Assert.True(scheduled > 0);
        Assert.Contains("NotePurgePassEnded(purgeSummary, DateTime.UtcNow);", worker[scheduled..(scheduled + 900)], StringComparison.Ordinal);

        var manual = worker.IndexOf("var summary = await DarlingRetention.PurgeAsync(", StringComparison.Ordinal);
        Assert.True(manual > 0);
        Assert.Contains("NotePurgePassEnded(summary, DateTime.UtcNow);", worker[manual..(manual + 1200)], StringComparison.Ordinal);

        /* The note is the sweep's own result: not inside a catch or finally, which would also run on a failure. */
        Assert.DoesNotContain("finally\n        {\n            NotePurgePassEnded", worker.Replace("\r\n", "\n"), StringComparison.Ordinal);
    }

    [Fact]
    public void ThePurgeSummary_ReportsTheBudgetStopFromTheYieldsOwnCount()
    {
        Assert.True(BudgetStopped.StoppedOnBudget);
        Assert.False(Finished.StoppedOnBudget);
        Assert.False(new PurgeSummary(0, 0, 0).StoppedOnBudget);

        var retention = ReadRepoFile("Darling", "PerformanceMonitor.Darling.Service", "DarlingRetention.cs");
        Assert.Contains("yieldGate?.TablesLeft ?? 0);", retention, StringComparison.Ordinal);
    }

    private sealed class WorkerLogger(CapturingTestLogger inner) : ILogger<DarlingWorker>
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception,
            Func<TState, Exception?, string> formatter) => inner.Log(logLevel, eventId, state, exception, formatter);
    }
}
