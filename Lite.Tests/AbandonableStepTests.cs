/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor Lite.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Threading;
using System.Threading.Tasks;
using PerformanceMonitor.Common;
using Xunit;

namespace Lite.Tests;

/// <summary>
/// Pins the #2148 abandonment discipline — the primitive that keeps one wedged ladder step from
/// stopping ALL collection (the field failure: a step hung on an Azure elastic pool after the 3.4.0
/// upgrade and every chart went silent, permanently, with all exception armor intact). The arms that
/// matter most are the guard's: an abandoned run must QUARANTINE the step (no relaunch on top of the
/// wedged task) and must RELEASE it the moment the wedged task truly ends — both directions, because a
/// guard that never releases turns one hang into a permanently dead step, which is the bug again with
/// extra steps.
/// </summary>
public sealed class AbandonableStepTests
{
    private static readonly TimeSpan Deadline = TimeSpan.FromMilliseconds(200);
    private static readonly TimeSpan Generous = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task CompletedWithinDeadline_ReportsCompleted_AndReleasesTheGuard()
    {
        var step = new AbandonableStep();

        var result = await step.RunAsync(() => Task.CompletedTask, Generous);

        Assert.Equal(AbandonableStepOutcome.Completed, result.Outcome);
        Assert.Null(result.Exception);
        Assert.False(step.IsInFlight);
    }

    [Fact]
    public async Task Fault_ReportsFaulted_WithTheException_AndReleasesTheGuard()
    {
        var step = new AbandonableStep();

        var result = await step.RunAsync(
            () => Task.FromException(new InvalidOperationException("boom")), Generous);

        Assert.Equal(AbandonableStepOutcome.Faulted, result.Outcome);
        Assert.IsType<InvalidOperationException>(result.Exception);
        Assert.False(step.IsInFlight);
    }

    [Fact]
    public async Task SynchronousThrow_IsAFault_NotAnEscape_AndReleasesTheGuard()
    {
        /* The loop calls RunAsync inline — a delegate that throws BEFORE returning its task must not
           blow through the ladder; it is a fault like any other. */
        var step = new AbandonableStep();

        var result = await step.RunAsync(
            () => throw new InvalidOperationException("sync boom"), Generous);

        Assert.Equal(AbandonableStepOutcome.Faulted, result.Outcome);
        Assert.IsType<InvalidOperationException>(result.Exception);
        Assert.False(step.IsInFlight);
    }

    [Fact]
    public async Task DeadlineElapsed_ReportsAbandoned_AndTheLoopGetsControlBack()
    {
        var step = new AbandonableStep();
        var wedge = new TaskCompletionSource();

        var result = await step.RunAsync(() => wedge.Task, Deadline);

        Assert.Equal(AbandonableStepOutcome.Abandoned, result.Outcome);
        /* The wedged task is still running — the guard holds. */
        Assert.True(step.IsInFlight);

        wedge.SetResult();
    }

    [Fact]
    public async Task WhileWedged_NextRunIsSkipped_NeverOverlapped()
    {
        var step = new AbandonableStep();
        var wedge = new TaskCompletionSource();
        var secondRan = false;

        await step.RunAsync(() => wedge.Task, Deadline);

        var second = await step.RunAsync(
            () => { secondRan = true; return Task.CompletedTask; }, Generous);

        /* THE quarantine: the wedged task must never be overlapped by a relaunch — on the real ladder
           that would stack a second hung backfill slice (and its connection) on top of the first. */
        Assert.Equal(AbandonableStepOutcome.SkippedStillRunning, second.Outcome);
        Assert.False(secondRan);

        wedge.SetResult();
    }

    [Fact]
    public async Task WhenTheWedgedTaskFinallyEnds_TheStepRunsAgain()
    {
        var step = new AbandonableStep();
        var wedge = new TaskCompletionSource();

        await step.RunAsync(() => wedge.Task, Deadline);
        wedge.SetResult();

        /* The guard clears via the task's own completion — poll briefly for the continuation. */
        for (var i = 0; i < 100 && step.IsInFlight; i++)
        {
            await Task.Delay(10);
        }
        Assert.False(step.IsInFlight);

        var next = await step.RunAsync(() => Task.CompletedTask, Generous);
        Assert.Equal(AbandonableStepOutcome.Completed, next.Outcome);
    }

    [Fact]
    public async Task AbandonedTaskThatLaterFaults_IsObserved_AndReleasesTheGuard()
    {
        /* The nasty double: abandoned first, THEN faults. The fault must be observed (no
           UnobservedTaskException tearing anything down) and the guard must still release. */
        var step = new AbandonableStep();
        var wedge = new TaskCompletionSource();

        var result = await step.RunAsync(() => wedge.Task, Deadline);
        Assert.Equal(AbandonableStepOutcome.Abandoned, result.Outcome);

        wedge.SetException(new InvalidOperationException("late boom"));

        for (var i = 0; i < 100 && step.IsInFlight; i++)
        {
            await Task.Delay(10);
        }
        Assert.False(step.IsInFlight);

        var next = await step.RunAsync(() => Task.CompletedTask, Generous);
        Assert.Equal(AbandonableStepOutcome.Completed, next.Outcome);
    }

    [Fact]
    public async Task AbandonedThenFaulted_SurfacesTheLateFault_ThroughTheCallback()
    {
        /* Review catch: without the callback, the one exception that explains a wedge was observed
           and DISCARDED — abandoned at the deadline, faulted a minute later, nothing in any log. */
        var step = new AbandonableStep();
        var wedge = new TaskCompletionSource();
        Exception? lateFault = null;

        var result = await step.RunAsync(
            () => wedge.Task, Deadline, onLateFault: ex => lateFault = ex);
        Assert.Equal(AbandonableStepOutcome.Abandoned, result.Outcome);

        wedge.SetException(new InvalidOperationException("the wedge's own exception"));

        for (var i = 0; i < 100 && lateFault is null; i++)
        {
            await Task.Delay(10);
        }
        Assert.IsType<InvalidOperationException>(lateFault);
        Assert.Equal("the wedge's own exception", lateFault!.Message);
    }

    [Fact]
    public async Task FaultWithinDeadline_DoesNotAlsoFireTheLateCallback()
    {
        /* The awaited path already returned the exception to the caller — the callback firing too
           would double-log every ordinary failure. */
        var step = new AbandonableStep();
        var fired = false;

        var result = await step.RunAsync(
            () => Task.FromException(new InvalidOperationException("boom")), Generous,
            onLateFault: _ => fired = true);

        Assert.Equal(AbandonableStepOutcome.Faulted, result.Outcome);
        await Task.Delay(50);
        Assert.False(fired);
    }

    [Fact]
    public async Task ThrowingLateFaultCallback_StillReleasesTheGuard()
    {
        /* A logging callback that itself throws must not leave the step permanently wedged. */
        var step = new AbandonableStep();
        var wedge = new TaskCompletionSource();

        await step.RunAsync(() => wedge.Task, Deadline,
            onLateFault: _ => throw new InvalidOperationException("logger boom"));
        wedge.SetException(new InvalidOperationException("late"));

        for (var i = 0; i < 100 && step.IsInFlight; i++)
        {
            await Task.Delay(10);
        }
        Assert.False(step.IsInFlight);
    }

    [Fact]
    public async Task CallerCancellation_ReportsCancelled_NotAbandoned()
    {
        /* Shutdown must read as shutdown — an Abandoned logged at ERROR during a clean exit would
           train operators to ignore the one line that matters in the field. */
        var step = new AbandonableStep();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var wedge = new TaskCompletionSource();

        var result = await step.RunAsync(() => wedge.Task, Generous, cancellationToken: cts.Token);

        Assert.Equal(AbandonableStepOutcome.Cancelled, result.Outcome);

        wedge.SetResult();
    }
}
