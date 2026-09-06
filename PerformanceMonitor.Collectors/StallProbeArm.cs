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

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Arms exactly one out-of-band wait sample against a server-scoped read that is still in flight (#2880), and
/// disarms when the read ends.
///
/// <para><b>One timer, one evaluation, one probe — never a loop.</b> #2880's first constraint is "one shot,
/// never retried", and this is that constraint expressed as structure rather than as a flag somebody has to
/// remember to check: the arm waits <see cref="StallWaitProbePolicy.TriggerElapsedFor"/> once, asks
/// <see cref="StallWaitProbePolicy.Decide"/> once, and is finished either way.
/// <c>StallWaitProbePolicyTests.TheArm_WaitsOnce_FiresOnce_AndNeverRetries</c> counts the delays and the
/// fires, so a regression to a polling loop that re-evaluates — the obvious "improvement", and the one that
/// would put a second query on a struggling target every cycle — fails a test instead of shipping.</para>
///
/// <para><b>The probe is never awaited by the read it observes.</b> <see cref="Start"/> hands
/// <paramref name="fire"/> a task it does not join, and the arm's own task is not joined either. That is the
/// bound that matters most and the reason the sample is stored on its own row rather than folded onto the
/// stalled run's <c>collection_log</c> row: a run that waited for its own watchdog would carry the wait in
/// <c>duration_ms</c> and could push it into the residual, which is precisely the misattribution the phase
/// split exists to end — and it would delay the next collector on a server whose sweep is already blown.</para>
///
/// <para><b>Disposal cancels the arm, never the probe.</b> A fired probe has its own budget
/// (<see cref="StallWaitProbePolicy.HardBudget"/>) and must survive the read ending, because the read ending
/// IS the abandon the sample explains. Disposing the arm only stops a sample that has not fired yet.</para>
/// </summary>
public sealed class StallProbeArm : IDisposable
{
    private readonly CancellationTokenSource _disarm;
    private int _fires;
    private int _delays;
    private int _disposed;

    private StallProbeArm(CancellationTokenSource disarm) => _disarm = disarm;

    /// <summary>
    /// How many times the arm waited for its trigger. One on any armed read; zero when the collector declared
    /// no budget or the target is not SQL Server. Never more than one — see the type comment.
    /// </summary>
    public int Delays => Volatile.Read(ref _delays);

    /// <summary>How many probes this arm dispatched. Zero or one, for the life of the arm.</summary>
    public int Fires => Volatile.Read(ref _fires);

    /// <summary>
    /// Arms a read. Returns an arm to dispose when the read ends — always non-null, so a caller cannot
    /// accidentally skip disposal on the not-armed path, and <see cref="Delays"/> is what says whether a timer
    /// was ever started.
    /// </summary>
    /// <param name="engine">The target's engine; only SQL Server is armed. See <see cref="StallWaitProbePolicy.Decide"/>.</param>
    /// <param name="wallClockBudget">
    /// The collector's <c>PerItemWallClockBudget</c>. Null leaves the arm inert — no timer, no allocation
    /// beyond the arm itself — which is every collector but the four that declare one.
    /// </param>
    /// <param name="observe">
    /// Reads the live client-side state at the trigger instant. Called at most once, from the arm's own task,
    /// so it must be safe to call while the drain is reading.
    /// </param>
    /// <param name="fire">
    /// Dispatches the probe. Its task is deliberately NOT awaited (see the type comment), so it must handle
    /// its own failures; anything it throws synchronously is swallowed here rather than reaching the sweep.
    /// </param>
    /// <param name="onDecision">
    /// Optional: told every decision, fired or not, with the policy's reason. The logging seam, kept out of
    /// this type so the arm has no logger dependency and stays in the dependency-free collector library.
    /// </param>
    /// <param name="delay">
    /// Optional: how the arm waits. Defaults to <see cref="Task.Delay(TimeSpan, CancellationToken)"/>;
    /// injectable so a pin can drive the trigger deterministically and COUNT the waits, which is how the
    /// one-shot claim is asserted rather than asserted about.
    /// </param>
    public static StallProbeArm Start(
        CollectorTargetEngine engine,
        TimeSpan? wallClockBudget,
        Func<StallProbeObservation> observe,
        Func<StallProbeObservation, Task> fire,
        Action<StallProbeDecision>? onDecision = null,
        Func<TimeSpan, CancellationToken, Task>? delay = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(observe);
        ArgumentNullException.ThrowIfNull(fire);

        var arm = new StallProbeArm(CancellationTokenSource.CreateLinkedTokenSource(cancellationToken));

        if (engine != CollectorTargetEngine.SqlServer ||
            StallWaitProbePolicy.TriggerElapsedFor(wallClockBudget) is not { } trigger)
        {
            /* Inert. Deliberately without calling observe or fire, and without reporting a decision: a
               collector that is not a candidate is not a diagnosis, and an onDecision line per unbudgeted
               collector per server per cycle is ~25,000 log lines an hour saying nothing happened. */
            return arm;
        }

        _ = arm.RunAsync(engine, wallClockBudget, trigger, observe, fire, onDecision, delay ?? Task.Delay);
        return arm;
    }

    private async Task RunAsync(
        CollectorTargetEngine engine,
        TimeSpan? wallClockBudget,
        TimeSpan trigger,
        Func<StallProbeObservation> observe,
        Func<StallProbeObservation, Task> fire,
        Action<StallProbeDecision>? onDecision,
        Func<TimeSpan, CancellationToken, Task> delay)
    {
        try
        {
            Interlocked.Increment(ref _delays);
            await delay(trigger, _disarm.Token).ConfigureAwait(false);

            /* Observed ONCE. The probe row records the same observation the decision was made on, so
               "why did this fire" is answerable from the stored row rather than from a second, later
               reading of a counter that has moved since. */
            var observation = observe();
            var decision = StallWaitProbePolicy.Decide(engine, wallClockBudget, observation);
            onDecision?.Invoke(decision);

            if (!decision.Fire)
            {
                return;
            }

            Interlocked.Increment(ref _fires);

            /* Not awaited: the probe outlives this arm by design. Its own budget bounds it and its own
               failure handling records what happened, so there is nothing here to wait for and nothing to
               report if we did. */
            _ = fire(observation);
        }
        catch (OperationCanceledException)
        {
            /* The read finished before the trigger, or the service is stopping. Both are the ordinary path
               for ~99% of runs and neither is worth a word. */
        }
        catch (Exception)
        {
            /* An arm must never be able to fault a collection sweep. There is no logger here by design (see
               onDecision), and a diagnostic that cannot be taken is not a collection failure. */
        }
    }

    /// <summary>
    /// Disarms a sample that has not fired. A fired probe is untouched — see the type comment.
    ///
    /// <para><b>Idempotent, and that is load-bearing rather than defensive.</b> The call site disarms
    /// EXPLICITLY the moment the read it observes is over, and keeps a <c>using</c> for the paths that leave
    /// by exception — so two disposals is the ordinary case, not a mistake. Without the guard the second one
    /// would throw <see cref="ObjectDisposedException"/> out of a <c>using</c> during a collector's storage
    /// phase.</para>
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        _disarm.Cancel();
        _disarm.Dispose();
    }
}
