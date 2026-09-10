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

    /* ──────────── #2165 handoff: the hold whose lifetime must match the guard's ────────────

       Found while working #2874 group D. Both SKUs' backfill loops took the per-server
       QueryStoreServerGate and then scoped it with `using var` INSIDE the foreach body, so the lease
       was disposed at the end of the iteration — including the iteration that ended in Abandoned,
       which is the one case where the slice really is still running against the monitored server. The
       #2148 in-flight guard went on quarantining that server's backfill while the #2165 gate stood
       open, so the tick was free to start its own heavy Query Store collection beside the wedged
       slice: exactly the ~128 MB overlap the gate was added to prevent.

       The hold is passed here now, because this type is the only thing that knows when the guard
       clears. Which makes "released exactly once, on EVERY path" this type's claim to prove — a hold
       that leaked would wedge one server's Query Store collection for the life of the process, a
       worse failure than the one being fixed. Every outcome gets its own test below, plus the two
       exits that produce no outcome at all. */

    /// <summary>
    /// A hold that counts its disposals and records what the guard looked like at that instant.
    /// Counting matters because releasing twice is as wrong as never releasing: the second Dispose
    /// would clear a flag some LATER acquirer owns. The guard snapshot pins the ordering.
    /// </summary>
    private sealed class CountingHold : IDisposable
    {
        private readonly AbandonableStep? _step;
        private readonly bool _throwOnDispose;
        private int _disposals;

        internal CountingHold(AbandonableStep? step = null, bool throwOnDispose = false)
        {
            _step = step;
            _throwOnDispose = throwOnDispose;
        }

        internal int Disposals => Volatile.Read(ref _disposals);

        /// <summary>Whether the step still held its in-flight guard when this hold was released.</summary>
        internal bool? GuardHeldAtRelease { get; private set; }

        public void Dispose()
        {
            GuardHeldAtRelease = _step?.IsInFlight;
            Interlocked.Increment(ref _disposals);
            if (_throwOnDispose)
            {
                throw new InvalidOperationException("hold dispose boom");
            }
        }
    }

    private static async Task WaitForAsync(Func<bool> condition)
    {
        for (var i = 0; i < 200 && !condition(); i++)
        {
            await Task.Delay(10);
        }
    }

    [Fact]
    public async Task Hold_OnCompleted_IsReleased()
    {
        var step = new AbandonableStep();
        var hold = new CountingHold();

        var result = await step.RunAsync(() => Task.CompletedTask, Generous, holdUntilStepEnds: hold);

        Assert.Equal(AbandonableStepOutcome.Completed, result.Outcome);
        await WaitForAsync(() => hold.Disposals == 1);
        Assert.Equal(1, hold.Disposals);
    }

    [Fact]
    public async Task Hold_OnFaulted_IsReleased()
    {
        var step = new AbandonableStep();
        var hold = new CountingHold();

        var result = await step.RunAsync(
            () => Task.FromException(new InvalidOperationException("boom")), Generous,
            holdUntilStepEnds: hold);

        Assert.Equal(AbandonableStepOutcome.Faulted, result.Outcome);
        await WaitForAsync(() => hold.Disposals == 1);
        Assert.Equal(1, hold.Disposals);
    }

    [Fact]
    public async Task Hold_OnSynchronousThrow_IsReleased()
    {
        /* No task was ever created, so no continuation will ever run — this exit has to release on
           its own or the hold is stranded. */
        var step = new AbandonableStep();
        var hold = new CountingHold();

        var result = await step.RunAsync(
            () => throw new InvalidOperationException("sync boom"), Generous, holdUntilStepEnds: hold);

        Assert.Equal(AbandonableStepOutcome.Faulted, result.Outcome);
        Assert.Equal(1, hold.Disposals);
    }

    [Fact]
    public async Task Hold_OnAbandoned_SurvivesTheDeadline_AndReleasesWhenTheWedgeEnds()
    {
        /* THE POINT, and the whole defect: the deadline handing control back to the loop must NOT
           release the hold, because the slice is still running on the monitored server. */
        var step = new AbandonableStep();
        var wedge = new TaskCompletionSource();
        var hold = new CountingHold(step);

        var result = await step.RunAsync(() => wedge.Task, Deadline, holdUntilStepEnds: hold);

        Assert.Equal(AbandonableStepOutcome.Abandoned, result.Outcome);
        Assert.True(step.IsInFlight, "the wedged task is still running, so the guard must still be held");
        Assert.Equal(0, hold.Disposals);

        /* And it is not a leak: the moment the wedged task truly ends, the hold goes with the guard. */
        wedge.SetResult();
        await WaitForAsync(() => hold.Disposals == 1);
        Assert.Equal(1, hold.Disposals);
        Assert.False(step.IsInFlight);
    }

    [Fact]
    public async Task Hold_OnAbandonedThenLateFault_IsStillReleasedExactlyOnce()
    {
        /* The nasty double: abandoned first, then the wedged task faults. The fault path runs the
           late-fault callback as well, so it is the one continuation with two things to do. */
        var step = new AbandonableStep();
        var wedge = new TaskCompletionSource();
        var hold = new CountingHold(step);
        Exception? lateFault = null;

        var result = await step.RunAsync(
            () => wedge.Task, Deadline, onLateFault: ex => lateFault = ex, holdUntilStepEnds: hold);

        Assert.Equal(AbandonableStepOutcome.Abandoned, result.Outcome);
        Assert.Equal(0, hold.Disposals);

        wedge.SetException(new InvalidOperationException("the wedge's own exception"));

        await WaitForAsync(() => hold.Disposals == 1 && lateFault is not null);
        Assert.Equal(1, hold.Disposals);
        Assert.IsType<InvalidOperationException>(lateFault);
    }

    [Fact]
    public async Task Hold_OnSkippedStillRunning_IsReleasedImmediately()
    {
        /* This call took no guard, so it has no completion of its own to wait for — the wedged run is
           an earlier one carrying its own hold. Holding on here would strand a second lease behind a
           task this call never started. */
        var step = new AbandonableStep();
        var wedge = new TaskCompletionSource();
        var firstHold = new CountingHold(step);
        var secondHold = new CountingHold(step);

        await step.RunAsync(() => wedge.Task, Deadline, holdUntilStepEnds: firstHold);

        var second = await step.RunAsync(
            () => Task.CompletedTask, Generous, holdUntilStepEnds: secondHold);

        Assert.Equal(AbandonableStepOutcome.SkippedStillRunning, second.Outcome);
        Assert.Equal(1, secondHold.Disposals);
        Assert.Equal(0, firstHold.Disposals);

        wedge.SetResult();
        await WaitForAsync(() => firstHold.Disposals == 1);
        Assert.Equal(1, firstHold.Disposals);
    }

    [Fact]
    public async Task Hold_OnCallerCancellation_ReleasesWhenTheWorkEnds_NotAtTheCancel()
    {
        /* Shutdown is not a reason to stop excluding: the statement is still on the wire, so the hold
           rides the work's own end here too. What must not happen is a hold stranded by the shutdown
           path — asserted by driving the work to completion afterwards. */
        var step = new AbandonableStep();
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(50));
        var wedge = new TaskCompletionSource();
        var hold = new CountingHold(step);

        var result = await step.RunAsync(
            () => wedge.Task, Generous, holdUntilStepEnds: hold, cancellationToken: cts.Token);

        Assert.Equal(AbandonableStepOutcome.Cancelled, result.Outcome);
        Assert.Equal(0, hold.Disposals);

        wedge.SetResult();
        await WaitForAsync(() => hold.Disposals == 1);
        Assert.Equal(1, hold.Disposals);
    }

    [Fact]
    public async Task Hold_OnArgumentValidationThrow_IsStillReleased()
    {
        /* Ownership transfers on entry, so the one exit that throws rather than returning an outcome
           releases too. A lease surviving this throw would hold its gate for the life of the process.
           Awaited rather than Assert.Throws because RunAsync is `async`, so its validation throw rides
           the returned task instead of the call. */
        var step = new AbandonableStep();
        var hold = new CountingHold();

        await Assert.ThrowsAsync<ArgumentNullException>(
            () => step.RunAsync(null!, Generous, holdUntilStepEnds: hold));

        Assert.Equal(1, hold.Disposals);
        Assert.False(step.IsInFlight);
    }

    [Fact]
    public async Task Hold_IsReleasedAfterTheGuardClears_NotBefore()
    {
        /* The ordering is what makes SkippedStillRunning unreachable for a caller that gates on the
           hold: if the hold were released first, a caller could take the gate and then be told the
           step is still wedged. Asserted from inside Dispose, which is the only place that can see it. */
        var step = new AbandonableStep();
        var hold = new CountingHold(step);

        await step.RunAsync(() => Task.CompletedTask, Generous, holdUntilStepEnds: hold);

        await WaitForAsync(() => hold.Disposals == 1);
        Assert.False(hold.GuardHeldAtRelease,
            "the in-flight guard must already be clear when the hold is released");
    }

    [Fact]
    public async Task ThrowingHoldDispose_OnTheSynchronousSkipPath_StillReturnsAnOutcome()
    {
        /* A hold whose Dispose throws is the caller's bug; it must not become the LOOP's exception.
           The skip path releases inline, so without the swallow this call throws instead of reporting
           SkippedStillRunning — and the backfill's foreach, which handles outcomes rather than throws,
           would unwind the whole fleet's loop on one bad Dispose. */
        var step = new AbandonableStep();
        var wedge = new TaskCompletionSource();

        await step.RunAsync(() => wedge.Task, Deadline);

        var second = await step.RunAsync(
            () => Task.CompletedTask, Generous,
            holdUntilStepEnds: new CountingHold(throwOnDispose: true));

        Assert.Equal(AbandonableStepOutcome.SkippedStillRunning, second.Outcome);

        wedge.SetResult();
    }

    [Fact]
    public async Task ThrowingHoldDispose_OnTheContinuationPath_DoesNotWedgeTheGuard()
    {
        /* And in the continuation, where a throw would be swallowed as an unobserved task fault: the
           guard must still be clear and the step must still be reusable. */
        var step = new AbandonableStep();
        var hold = new CountingHold(throwOnDispose: true);

        var result = await step.RunAsync(() => Task.CompletedTask, Generous, holdUntilStepEnds: hold);

        Assert.Equal(AbandonableStepOutcome.Completed, result.Outcome);
        await WaitForAsync(() => !step.IsInFlight);
        Assert.False(step.IsInFlight);
        Assert.Equal(1, hold.Disposals);

        var next = await step.RunAsync(() => Task.CompletedTask, Generous);
        Assert.Equal(AbandonableStepOutcome.Completed, next.Outcome);
    }

    [Fact]
    public async Task NoHold_IsTolerated_OnTheTwoPathsThatReleaseInline()
    {
        /* Lite's connection-check step passes no hold at all, so a null one has to be legal — and the
           two paths that release INLINE are where that matters, because a release that throws there
           reaches the caller instead of dying quietly in a discarded continuation. Asserted on those two
           deliberately: the same claim made on the Completed path cannot fail, since a continuation's
           exception is swallowed either way, and a test that cannot fail is not a test. */
        var step = new AbandonableStep();
        var wedge = new TaskCompletionSource();

        await step.RunAsync(() => wedge.Task, Deadline, holdUntilStepEnds: null);

        var skipped = await step.RunAsync(() => Task.CompletedTask, Generous, holdUntilStepEnds: null);
        Assert.Equal(AbandonableStepOutcome.SkippedStillRunning, skipped.Outcome);

        wedge.SetResult();
        await WaitForAsync(() => !step.IsInFlight);

        var threw = await step.RunAsync(
            () => throw new InvalidOperationException("sync boom"), Generous, holdUntilStepEnds: null);
        Assert.Equal(AbandonableStepOutcome.Faulted, threw.Outcome);
        Assert.False(step.IsInFlight);
    }
}
