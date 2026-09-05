/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Common;

/// <summary>How one run of an <see cref="AbandonableStep"/> ended.</summary>
public enum AbandonableStepOutcome
{
    /// <summary>The step finished within its deadline.</summary>
    Completed,

    /// <summary>The step threw; the exception rides <see cref="AbandonableStepResult.Exception"/>.</summary>
    Faulted,

    /// <summary>The deadline elapsed first. The step's task is ABANDONED, not cancelled — it may still
    /// be running; the in-flight guard keeps it from being relaunched until it truly ends.</summary>
    Abandoned,

    /// <summary>A previously-abandoned run is still wedged, so this run never started.</summary>
    SkippedStillRunning,

    /// <summary>The caller's token cancelled while waiting.</summary>
    Cancelled,
}

/// <summary>One run's outcome plus the fault when there was one.</summary>
public readonly record struct AbandonableStepResult(AbandonableStepOutcome Outcome, Exception? Exception = null);

/// <summary>
/// A sequential background-loop step that may NOT hold the loop past a deadline (#2148). Born from the
/// field failure this class exists to make impossible: Lite's collection ladder ran its steps strictly
/// in sequence, one step wedged on an Azure elastic pool ~12 minutes after a 3.4.0 upgrade, and ALL
/// collection stopped — permanently, silently, with every step's exception armor intact, because the
/// armor bounded throws and nothing bounded a HANG.
///
/// <para>The discipline is the ladder's own scheduled-analysis idiom, extracted and made reusable:
/// <see cref="Task.WhenAny(Task, Task)"/> against a deadline, and an in-flight guard cleared only when
/// the underlying task TRULY finishes — so an abandoned (possibly wedged) run is never overlapped by a
/// relaunch, and the moment it finally dies the step becomes runnable again on its own. Abandonment is
/// deliberately not cancellation: the wedged task already ignored cooperative signals by definition,
/// and the value here is that the LOOP keeps moving while the guard quarantines the stuck step.</para>
///
/// <para>Outcomes are returned, never thrown (the caller is a loop whose next steps must run; it logs
/// each outcome at its own severity). The step delegate's synchronous throws are treated as
/// <see cref="AbandonableStepOutcome.Faulted"/> like any other fault, with the guard released.</para>
/// </summary>
public sealed class AbandonableStep
{
    private int _inFlight;

    /// <summary>Whether a run is currently holding the guard — an abandoned run still counts until its
    /// task truly ends. Exposed for the caller's logging/diagnostics, racy by nature.</summary>
    public bool IsInFlight => Volatile.Read(ref _inFlight) == 1;

    /// <summary>
    /// Runs <paramref name="step"/> unless a prior run is still wedged, waiting at most
    /// <paramref name="timeout"/> before abandoning it and returning control to the loop.
    /// <paramref name="onLateFault"/> (review catch on the #2148 PR) surfaces an exception thrown by a
    /// run AFTER it was abandoned — without it that fault would be observed-but-discarded, and the one
    /// exception that explains a wedge would never reach a log. Invoked only for faults the caller's
    /// awaited path did NOT already receive; a fault landing in the microseconds between the deadline
    /// decision and the abandonment flag can be missed (never doubled), which costs one log line, not
    /// correctness — the caller already logged the abandonment itself.
    /// <para><b><paramref name="holdUntilStepEnds"/> — the lease whose lifetime must match the guard's.</b>
    /// Ownership transfers to this call, which disposes it EXACTLY ONCE on every path, at the moment the
    /// in-flight guard clears: for an abandoned run that is when the wedged task truly ends, not when the
    /// deadline hands the loop back. A caller cannot get this right on its own, because only this type knows
    /// when the guard clears — and the shape it reaches for instead, a <c>using</c> scoped to its own loop
    /// iteration, releases the lease while the abandoned work is still running, so the exclusion lapses
    /// precisely in the case it was taken for (#2165). Released one step BEHIND the guard, so a caller that
    /// finds the lease free can never then be told
    /// <see cref="AbandonableStepOutcome.SkippedStillRunning"/>.</para>
    /// <para><b>Parameter order:</b> <paramref name="cancellationToken"/> is LAST, per CA1068. It was third
    /// until #2193, which is the ordering the analyzer flags — and every call site already passed
    /// <paramref name="onLateFault"/> by name, so the move cost nothing at the callers and the compiler
    /// found all of them.</para>
    /// </summary>
    public async Task<AbandonableStepResult> RunAsync(
        Func<Task> step, TimeSpan timeout, Action<Exception>? onLateFault = null,
        IDisposable? holdUntilStepEnds = null, CancellationToken cancellationToken = default)
    {
        if (step is null)
        {
            /* Ownership transferred on entry, so even the argument-validation exit releases. A lease that
               survived a throw from here would hold its gate for the life of the process. */
            ReleaseHold(holdUntilStepEnds);
            throw new ArgumentNullException(nameof(step));
        }

        if (Interlocked.CompareExchange(ref _inFlight, 1, 0) != 0)
        {
            /* This call holds no guard, so it has nothing to hand to a task it never started — the run
               still wedged is an EARLIER one, carrying its own hold. Release now rather than pin this lease
               to a completion that was never ours. */
            ReleaseHold(holdUntilStepEnds);
            return new AbandonableStepResult(AbandonableStepOutcome.SkippedStillRunning);
        }

        Task work;
        try
        {
            work = step();
        }
        catch (Exception ex)
        {
            Interlocked.Exchange(ref _inFlight, 0);
            ReleaseHold(holdUntilStepEnds);
            return new AbandonableStepResult(AbandonableStepOutcome.Faulted, ex);
        }

        var abandoned = new StrongBox<bool>(false);

        /* The guard clears when the task TRULY ends — completion, fault, or cancellation — never when
           the deadline merely moves the loop on. Faults on the abandoned path are observed here (so an
           abandoned-then-faulted task cannot surface as UnobservedTaskException) AND handed to
           onLateFault, because a discarded exception from the wedged run is exactly the diagnostic the
           field report needs. The caller's hold rides this same moment, one step behind the guard — every
           outcome still reachable from here (Completed, Faulted, Abandoned, Cancelled) leaves the release to
           this continuation, because on all four the task itself is what decides when the work is over. */
        _ = work.ContinueWith(
            (t, state) =>
            {
                var self = (AbandonableStep)state!;
                var fault = t.Exception; /* observe unconditionally */
                if (fault is not null && Volatile.Read(ref abandoned.Value))
                {
                    try
                    {
                        onLateFault?.Invoke(fault.GetBaseException());
                    }
                    catch
                    {
                        /* A throwing log callback must not take the continuation down. */
                    }
                }

                Interlocked.Exchange(ref self._inFlight, 0);
                ReleaseHold(holdUntilStepEnds);
            },
            this,
            CancellationToken.None,
            TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default);

        var finished = await Task.WhenAny(work, Task.Delay(timeout, cancellationToken)).ConfigureAwait(false);

        if (finished != work)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return new AbandonableStepResult(AbandonableStepOutcome.Cancelled);
            }

            Volatile.Write(ref abandoned.Value, true);
            return new AbandonableStepResult(AbandonableStepOutcome.Abandoned);
        }

        try
        {
            await work.ConfigureAwait(false);
            return new AbandonableStepResult(AbandonableStepOutcome.Completed);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return new AbandonableStepResult(AbandonableStepOutcome.Cancelled);
        }
        catch (Exception ex)
        {
            return new AbandonableStepResult(AbandonableStepOutcome.Faulted, ex);
        }
    }

    /* Every release of the caller's hold goes through here, so "exactly once on every path" is one call per
       path rather than a Dispose() spelled five ways. Swallows a throwing Dispose for the same reason the
       late-fault callback is wrapped: a caller-supplied disposable must not take the continuation down and
       with it the guard's own release, which would turn one bad Dispose into a permanently dead step. */
    private static void ReleaseHold(IDisposable? hold)
    {
        try
        {
            hold?.Dispose();
        }
        catch
        {
            /* The caller's bug, and not one worth making this loop's hang. */
        }
    }
}
