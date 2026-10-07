/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using ModelContextProtocol.Protocol;
using PerformanceMonitor.Alerting;
using PerformanceMonitor.Common;
using PerformanceMonitor.Notifications;
using Xunit;

namespace Darling.Tests;

/// <summary>
/// Serializes the classes that stage a fake start-up warm-up (#5484). The registered warm-up is process-wide, so a
/// fake that is still running while another class judges a large document would hold that class up for the wait.
/// </summary>
[CollectionDefinition("statement-filter-warm-up", DisableParallelization = true)]
public sealed class StatementFilterWarmUpCollection
{
}

/// <summary>
/// #5478, #5484: a call that judges a large document waits for a running start-up warm-up, but only until one deadline
/// (3 s after the warm-up started), only when it judges at least 256 KB, and the alert engine's fire path awaits instead
/// of blocking. The MCP and web sweep waits the same way.
/// </summary>
[Collection("statement-filter-warm-up")]
public sealed class StatementFilterWarmUpWaitTests
{
    private static long Ticks(TimeSpan span) => (long)(span.TotalSeconds * Stopwatch.Frequency);

    /// <summary>A fake warm-up that has been running for <paramref name="age"/>. Disposing it finishes it and clears the
    /// registration, so a failing assertion cannot leave a running fake for the next test to wait on.</summary>
    private sealed class StagedWarmUp : IDisposable
    {
        public StagedWarmUp(TimeSpan age)
        {
            Source = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            Started = Stopwatch.GetTimestamp() - Ticks(age);
            SensitiveStatements.SetWarmUp(Source.Task, Started);
        }

        public TaskCompletionSource Source { get; }

        public long Started { get; }

        public void Dispose()
        {
            Source.TrySetResult();
            SensitiveStatements.SetWarmUp(null, 0);
        }
    }

    /// <summary>A blocked-process report of about 300,000 characters (above the 256 KB gate) that still names the canary.</summary>
    private static string LargeReportXml() =>
        StatementFilterAlertTests.ReportXml().Replace(
            "</blocked-process-report>",
            "<note>" + new string('x', 300_000) + "</note></blocked-process-report>",
            StringComparison.Ordinal);

    private static AlertContext LargeContext() => new() { AttachmentXml = LargeReportXml() };

    private static CallToolResult LargeToolResult() => new()
    {
        Content = new List<ContentBlock> { new TextContentBlock { Text = "{\"v\":\"" + new string('x', 300_000) + "\"}" } },
    };

    // ---- the primitive: one deadline for every caller ----

    [Fact]
    public void WaitForWarmUp_ARunningWarmUp_IsWaitedForOnlyUpToTheLimit()
    {
        var running = new TaskCompletionSource();
        Assert.Equal(TimeSpan.FromSeconds(3), SensitiveStatements.WarmUpWaitLimit);
        Assert.Equal(256 * 1024, SensitiveStatements.WarmUpWaitGateChars);

        var watch = Stopwatch.StartNew();
        var finished = SensitiveStatements.WaitForWarmUp(running.Task, Stopwatch.GetTimestamp(), TimeSpan.FromMilliseconds(300));
        watch.Stop();

        Assert.False(finished);
        Assert.InRange(watch.ElapsedMilliseconds, 250, 2500);

        // The same task, finished a moment later, ends the wait early.
        _ = Task.Run(async () => { await Task.Delay(200); running.SetResult(); });
        watch.Restart();
        Assert.True(SensitiveStatements.WaitForWarmUp(running.Task, Stopwatch.GetTimestamp(), TimeSpan.FromSeconds(20)));
        Assert.InRange(watch.ElapsedMilliseconds, 0, 5000);
    }

    [Fact]
    public void WaitForWarmUp_NoWarmUp_AFinishedOne_OrOneThatAlreadyFailed_DelaysNoOne()
    {
        var failed = Task.FromException(new InvalidOperationException("boom"));
        var watch = Stopwatch.StartNew();
        Assert.True(SensitiveStatements.WaitForWarmUp(null, Stopwatch.GetTimestamp(), TimeSpan.FromSeconds(20)));
        Assert.True(SensitiveStatements.WaitForWarmUp(Task.CompletedTask, Stopwatch.GetTimestamp(), TimeSpan.FromSeconds(20)));
        Assert.True(SensitiveStatements.WaitForWarmUp(failed, Stopwatch.GetTimestamp(), TimeSpan.FromSeconds(20)));
        watch.Stop();
        Assert.True(watch.ElapsedMilliseconds < 1000, "took " + watch.ElapsedMilliseconds + " ms");
        _ = failed.Exception;
    }

    [Fact]
    public void WaitForWarmUp_AWarmUpThatFaultsWhileWaitedOn_ReturnsFalseAtOnce_AndNeverThrows()
    {
        var source = new TaskCompletionSource();
        _ = Task.Run(async () =>
        {
            await Task.Delay(100);
            source.SetException(new InvalidOperationException("boom"));
        });

        var watch = Stopwatch.StartNew();
        var finished = SensitiveStatements.WaitForWarmUp(source.Task, Stopwatch.GetTimestamp(), TimeSpan.FromSeconds(20));
        watch.Stop();

        Assert.False(finished);
        Assert.InRange(watch.ElapsedMilliseconds, 0, 5000);
        Assert.NotNull(source.Task.Exception);
    }

    [Fact]
    public void WaitForWarmUp_AfterTheDeadline_NobodyWaits_AndTheFleetPaysTheLimitOnce()
    {
        var running = new TaskCompletionSource();
        var limit = TimeSpan.FromMilliseconds(500);
        var started = Stopwatch.GetTimestamp();

        // Three callers one after another: the first waits out the deadline, the other two find it already passed.
        var watch = Stopwatch.StartNew();
        Assert.False(SensitiveStatements.WaitForWarmUp(running.Task, started, limit));
        var afterFirst = watch.ElapsedMilliseconds;
        Assert.False(SensitiveStatements.WaitForWarmUp(running.Task, started, limit));
        Assert.False(SensitiveStatements.WaitForWarmUp(running.Task, started, limit));
        watch.Stop();

        Assert.InRange(afterFirst, 400, 2500);
        Assert.True(watch.ElapsedMilliseconds - afterFirst < 250,
            "the callers after the deadline waited " + (watch.ElapsedMilliseconds - afterFirst) + " ms");

        // A warm-up that started long ago and never finishes costs a new caller nothing.
        var old = Stopwatch.GetTimestamp() - Ticks(TimeSpan.FromSeconds(10));
        watch.Restart();
        Assert.False(SensitiveStatements.WaitForWarmUp(running.Task, old, TimeSpan.FromSeconds(3)));
        Assert.True(watch.ElapsedMilliseconds < 250, "took " + watch.ElapsedMilliseconds + " ms");
        running.SetResult();
    }

    [Fact]
    public async Task WhenWarmAsync_ATimedOutAsyncWait_IsNotFollowedByASecondWaitInTheSyncOne()
    {
        var running = new TaskCompletionSource();
        var limit = TimeSpan.FromMilliseconds(500);
        var started = Stopwatch.GetTimestamp();

        var watch = Stopwatch.StartNew();
        await SensitiveStatements.WhenWarmAsync(running.Task, started, limit);
        var afterAsync = watch.ElapsedMilliseconds;
        Assert.InRange(afterAsync, 400, 2500);

        Assert.False(SensitiveStatements.WaitForWarmUp(running.Task, started, limit));
        watch.Stop();
        Assert.True(watch.ElapsedMilliseconds - afterAsync < 250,
            "the sync wait after the async one took " + (watch.ElapsedMilliseconds - afterAsync) + " ms");
        running.SetResult();
    }

    [Fact]
    public async Task WhenWarmAsync_IsCompleteWithNoWarmUp_AFinishedOne_AFailedOne_OrAnExpiredDeadline_AndNeverThrows()
    {
        var failed = new TaskCompletionSource();
        failed.SetException(new InvalidOperationException("boom"));
        var stuck = new TaskCompletionSource();

        Assert.True(SensitiveStatements.WhenWarmAsync(null, 0, TimeSpan.FromSeconds(3)).IsCompleted);
        Assert.True(SensitiveStatements.WhenWarmAsync(Task.CompletedTask, 0, TimeSpan.FromSeconds(3)).IsCompleted);
        Assert.True(SensitiveStatements.WhenWarmAsync(failed.Task, Stopwatch.GetTimestamp(), TimeSpan.FromSeconds(3)).IsCompleted);
        Assert.True(SensitiveStatements.WhenWarmAsync(
            stuck.Task, Stopwatch.GetTimestamp() - Ticks(TimeSpan.FromSeconds(10)), TimeSpan.FromSeconds(3)).IsCompleted);

        // A warm-up that faults while it is awaited ends the wait without throwing.
        var faulting = new TaskCompletionSource();
        _ = Task.Run(async () => { await Task.Delay(100); faulting.SetException(new InvalidOperationException("boom")); });
        await SensitiveStatements.WhenWarmAsync(faulting.Task, Stopwatch.GetTimestamp(), TimeSpan.FromSeconds(20))
            .WaitAsync(TimeSpan.FromSeconds(20));

        _ = failed.Task.Exception;
        _ = faulting.Task.Exception;
        stuck.SetResult();
    }

    // ---- the size gate ----

    [Fact]
    public async Task TheGate_BelowTheThreshold_NobodyWaits_AtTheThreshold_TheCallWaits()
    {
        await StatementFilterWarmUp.EnsureAsync();
        using var staged = new StagedWarmUp(TimeSpan.Zero);

        var watch = Stopwatch.StartNew();
        SensitiveStatements.WaitForWarmUp(SensitiveStatements.WarmUpWaitGateChars - 1);
        watch.Stop();
        Assert.True(watch.ElapsedMilliseconds < 500, "a call below the gate waited " + watch.ElapsedMilliseconds + " ms");

        _ = Task.Run(async () => { await Task.Delay(400); staged.Source.SetResult(); });
        watch.Restart();
        SensitiveStatements.WaitForWarmUp(SensitiveStatements.WarmUpWaitGateChars);
        watch.Stop();
        Assert.InRange(watch.ElapsedMilliseconds, 300, 2900);
    }

    [Fact]
    public async Task Apply_OfASmallAlert_DuringARunningWarmUp_DoesNotWait()
    {
        await StatementFilterWarmUp.EnsureAsync();
        using var staged = new StagedWarmUp(TimeSpan.Zero);

        var watch = Stopwatch.StartNew();
        var filtered = AlertStatementFilter.Apply(new AlertContext { AttachmentXml = StatementFilterAlertTests.ReportXml() });
        watch.Stop();

        Assert.True(watch.ElapsedMilliseconds < 1000, "took " + watch.ElapsedMilliseconds + " ms");
        StatementFilterAlertTests.AssertNoSecret(filtered!.AttachmentXml!);
    }

    // ---- the alert path through the product's own warm-up ----

    [Fact]
    public async Task Apply_DuringASlowWarmUp_WaitsForItAndThenJudgesTheContext()
    {
        // Settle the process's own warm-up first, so no other test replaces the one this test staged.
        await StatementFilterWarmUp.EnsureAsync();
        using var release = new ManualResetEventSlim(false);
        var slow = AlertStatementFilter.WarmUpAsync(() => release.Wait(TimeSpan.FromSeconds(20)));
        try
        {
            var entered = new TaskCompletionSource();
            long returnedAt = 0;
            var apply = Task.Run(() =>
            {
                entered.SetResult();
                var result = AlertStatementFilter.Apply(LargeContext());
                returnedAt = Stopwatch.GetTimestamp();
                return result;
            });

            // Apply has started; had it not waited, a judged 300 KB document would be back long before this.
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(20));
            await Task.Delay(500);
            Assert.False(apply.IsCompleted, "Apply did not wait for the warm-up that was still running");

            var releasedAt = Stopwatch.GetTimestamp();
            release.Set();
            var filtered = await apply.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.True(returnedAt >= releasedAt, "Apply returned before the warm-up was released");

            StatementFilterAlertTests.AssertNoSecret(filtered!.AttachmentXml!);
            Assert.Contains(StatementScrubCanary.PlainStatement, filtered.AttachmentXml!, StringComparison.Ordinal);
        }
        finally
        {
            release.Set();
            await slow;
        }
    }

    [Fact]
    public async Task TheWarmUpsOwnApplyCalls_NeverWaitOnTheWarmUp()
    {
        await StatementFilterWarmUp.EnsureAsync();
        long insideMs = -1;
        var warmUp = AlertStatementFilter.WarmUpAsync(() =>
        {
            // Large enough to be gated: only the warm-up's own flag keeps these from waiting the whole limit on themselves.
            var watch = Stopwatch.StartNew();
            _ = AlertStatementFilter.Apply(LargeContext());
            _ = AlertStatementFilter.Apply(new FindingAlert("A", "SRV", "1", "1", "101", LargeContext(), 0.9, 0.5, "plain", true));
            insideMs = watch.ElapsedMilliseconds;
        });

        await warmUp.WaitAsync(TimeSpan.FromSeconds(20));
        Assert.InRange(insideMs, 0, 2000);
    }

    [Fact]
    public async Task TheRegistrationIsWrittenBeforeTheWarmUpCanRun()
    {
        await StatementFilterWarmUp.EnsureAsync();
        for (var i = 0; i < 50; i++)
        {
            // The first thing the work does is ask who is registered: it must be this warm-up, never the previous one.
            Task? seenByTheWork = null;
            var warmUp = SensitiveStatements.StartWarmUp(() =>
            {
                seenByTheWork = SensitiveStatements.RegisteredWarmUp;
                return Task.CompletedTask;
            });
            await warmUp.WaitAsync(TimeSpan.FromSeconds(20));
            Assert.Same(warmUp, seenByTheWork);
        }
    }

    [Fact]
    public async Task Apply_AfterAWarmUpThatThrew_IsNotDelayed()
    {
        await StatementFilterWarmUp.EnsureAsync();
        await AlertStatementFilter.WarmUpAsync(() => throw new InvalidOperationException("boom"));

        var watch = Stopwatch.StartNew();
        var filtered = AlertStatementFilter.Apply(LargeContext());
        watch.Stop();

        Assert.True(watch.ElapsedMilliseconds < 2000, "took " + watch.ElapsedMilliseconds + " ms");
        StatementFilterAlertTests.AssertNoSecret(filtered!.AttachmentXml!);
    }

    [Fact]
    public async Task TheAlertWarmUpSwallowsAThrownException_AndRunsTheDefaultProbe()
    {
        await AlertStatementFilter.WarmUpAsync(() => throw new InvalidOperationException("boom"));
        await AlertStatementFilter.WarmUpAsync();
    }

    // ---- the engine's fire path awaits ----

    [Fact]
    public async Task TheEnginesFirePath_DuringARunningWarmUp_AwaitsInsteadOfBlockingTheCaller()
    {
        await StatementFilterWarmUp.EnsureAsync();
        var h = StatementFilterAlertTests.BlockingHarness(out _);
        h.Adapter.Blocking[0].BlockedProcessReportXml = LargeReportXml();
        using var staged = new StagedWarmUp(TimeSpan.Zero);

        var engine = h.Build();
        var watch = Stopwatch.StartNew();
        var evaluation = engine.EvaluateServerAsync(AlertEngineTests.Harness.Snapshot());
        watch.Stop();

        // A blocking wait would have held this thread for the rest of the warm-up before the call returned.
        Assert.True(watch.ElapsedMilliseconds < 1500, "the fire path blocked the caller for " + watch.ElapsedMilliseconds + " ms");
        await Task.Delay(300);
        Assert.False(evaluation.IsCompleted, "the alert was delivered while the warm-up was still running");
        Assert.Empty(h.Deliverer.Outcomes);

        staged.Source.SetResult();
        await evaluation.WaitAsync(TimeSpan.FromSeconds(30));

        var outcome = Assert.Single(h.Deliverer.Outcomes);
        StatementFilterAlertTests.AssertNoSecret(StatementFilterAlertTests.Everything(outcome));
    }

    // ---- the MCP and web sweep ----

    [Fact]
    public async Task TheSweep_OfALargeResult_WaitsForARunningWarmUp_AndASmallOneDoesNot()
    {
        await StatementFilterWarmUp.EnsureAsync();
        using var staged = new StagedWarmUp(TimeSpan.Zero);

        var small = new CallToolResult { Content = new List<ContentBlock> { new TextContentBlock { Text = "{\"v\":1}" } } };
        var watch = Stopwatch.StartNew();
        Assert.Same(small, SensitiveStatementOutputFilter.Sweep(small));
        watch.Stop();
        Assert.True(watch.ElapsedMilliseconds < 500, "a small result waited " + watch.ElapsedMilliseconds + " ms");

        _ = Task.Run(async () => { await Task.Delay(400); staged.Source.SetResult(); });
        var large = LargeToolResult();
        watch.Restart();
        var swept = SensitiveStatementOutputFilter.Sweep(large);
        watch.Stop();

        Assert.InRange(watch.ElapsedMilliseconds, 300, 2900);
        Assert.Same(large, swept);
    }

    // ---- #5484: sizing runs only while a warm-up can still make the call wait ----

    [Fact]
    public async Task WarmUpPending_IsTrueOnlyWhileAWarmUpRunsInsideItsDeadline()
    {
        await StatementFilterWarmUp.EnsureAsync();

        // No warm-up registered.
        SensitiveStatements.SetWarmUp(null, 0);
        Assert.False(SensitiveStatements.WarmUpPending);

        // One running before its 3 s deadline.
        using (var staged = new StagedWarmUp(TimeSpan.Zero))
        {
            Assert.True(SensitiveStatements.WarmUpPending);

            // The same warm-up once it finished.
            staged.Source.SetResult();
            Assert.False(SensitiveStatements.WarmUpPending);
        }

        // One still running, but past the deadline.
        using (var late = new StagedWarmUp(TimeSpan.FromSeconds(4)))
        {
            Assert.False(late.Source.Task.IsCompleted);
            Assert.False(SensitiveStatements.WarmUpPending);
        }

        // Inside the warm-up itself: it never waits on itself, so it is never pending there.
        var insideSaw = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = SensitiveStatements.StartWarmUp(() =>
        {
            insideSaw.SetResult(SensitiveStatements.WarmUpPending);
            return Task.CompletedTask;
        });
        Assert.False(await insideSaw.Task);
        await started;
        SensitiveStatements.SetWarmUp(null, 0);
    }

    [Fact]
    public void TheSizedWait_SizesTheCallOnlyWhileAWarmUpIsPending()
    {
        var sized = 0;
        long Size() { sized++; return 10; }

        SensitiveStatements.SetWarmUp(null, 0);
        SensitiveStatements.WaitForWarmUp(Size);
        Assert.Equal(0, sized);

        using (var staged = new StagedWarmUp(TimeSpan.Zero))
        {
            SensitiveStatements.WaitForWarmUp(Size);
            Assert.Equal(1, sized);

            staged.Source.SetResult();
            SensitiveStatements.WaitForWarmUp(Size);
            Assert.Equal(1, sized);
        }

        using (var late = new StagedWarmUp(TimeSpan.FromSeconds(4)))
        {
            SensitiveStatements.WaitForWarmUp(Size);
            Assert.Equal(1, sized);
        }
    }

    [Fact]
    public async Task TheSweep_SizesTheResultOnlyWhileAWarmUpIsPending()
    {
        await StatementFilterWarmUp.EnsureAsync();
        var result = LargeToolResult();

        // Seam: SensitiveStatementOutputFilter.SizingCalls counts SweptChars calls, because the structured content's
        // GetRawText (the copy that matters) is not observable from a test. The collection is serial, so no other
        // test moves the counter.
        SensitiveStatements.SetWarmUp(null, 0);
        var before = SensitiveStatementOutputFilter.SizingCalls;
        SensitiveStatementOutputFilter.Sweep(result);
        Assert.Equal(before, SensitiveStatementOutputFilter.SizingCalls);

        using (var staged = new StagedWarmUp(TimeSpan.Zero))
        {
            _ = Task.Run(async () => { await Task.Delay(100); staged.Source.SetResult(); });
            SensitiveStatementOutputFilter.Sweep(result);
            Assert.Equal(before + 1, SensitiveStatementOutputFilter.SizingCalls);

            // Finished: sized no more.
            SensitiveStatementOutputFilter.Sweep(result);
            Assert.Equal(before + 1, SensitiveStatementOutputFilter.SizingCalls);
        }

        using (var late = new StagedWarmUp(TimeSpan.FromSeconds(4)))
        {
            SensitiveStatementOutputFilter.Sweep(result);
            Assert.Equal(before + 1, SensitiveStatementOutputFilter.SizingCalls);
        }
    }
}
