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
using PerformanceMonitor.Collectors;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #9535: the stall probe of a plan fetch phase. A plan fetch command that is still running after
/// <see cref="Trigger"/> gets one out-of-band wait sample (<see cref="StallWaitProbeRunner"/>), taken while the
/// command is still running, so the stored row names the wait the fetch was in. The trigger is half the fetch's
/// lock timeout: a command that waits on a lock is sampled inside that wait, and a command that has run that long
/// has spent a tenth of the phase's budget on one ask.
///
/// <para>One probe per phase run, never retried and never awaited by the fetch: the first command to reach the
/// trigger fires it, and every later command of that run is not watched. The probe row carries the phase's budget
/// in <c>budget_ms</c>, which is how a plan fetch probe is told from a probe of the collector's read, the phase's
/// elapsed time at the trigger, and the plans answered by then. The fetch has no counted reader, so the row's byte
/// and last-read figures are -1.</para>
/// </summary>
internal sealed class PlanFetchStallWatch
{
    /// <summary>How long one plan fetch command runs before the probe fires.</summary>
    internal static readonly TimeSpan Trigger = TimeSpan.FromMilliseconds(BoundedPlanFetch.LockTimeoutMilliseconds / 2);

    private readonly Func<StallProbeObservation, Task> _fire;
    private readonly Func<TimeSpan, CancellationToken, Task> _delay;
    private int _fires;

    /// <param name="fire">Dispatches the probe. Its task is not awaited, so it handles its own failures.</param>
    /// <param name="delay">How the watch waits for the trigger. Defaults to <see cref="Task.Delay(TimeSpan, CancellationToken)"/>.</param>
    internal PlanFetchStallWatch(Func<StallProbeObservation, Task> fire, Func<TimeSpan, CancellationToken, Task>? delay = null)
    {
        ArgumentNullException.ThrowIfNull(fire);
        _fire = fire;
        _delay = delay ?? Task.Delay;
    }

    /// <summary>How many probes this watch dispatched. Zero or one.</summary>
    internal int Fires => Volatile.Read(ref _fires);

    /// <summary>
    /// Runs <paramref name="command"/> and returns its result or its exception unchanged. A command that is still
    /// running at the trigger fires the probe with <paramref name="observe"/>'s reading, once per watch.
    /// </summary>
    internal async Task<T> WatchAsync<T>(
        Func<Task<T>> command, Func<StallProbeObservation> observe, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(command);
        ArgumentNullException.ThrowIfNull(observe);

        if (Volatile.Read(ref _fires) != 0)
        {
            return await command();
        }

        using var disarm = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var running = command();
        _ = ArmAsync(running, observe, disarm.Token);
        try
        {
            return await running;
        }
        finally
        {
            disarm.Cancel();
        }
    }

    private async Task ArmAsync(Task running, Func<StallProbeObservation> observe, CancellationToken disarmed)
    {
        try
        {
            await _delay(Trigger, disarmed).ConfigureAwait(false);
            if (running.IsCompleted || Interlocked.CompareExchange(ref _fires, 1, 0) != 0)
            {
                return;
            }

            /* Not awaited: the probe has its own budget and stores its own row. */
            _ = _fire(observe());
        }
        catch (OperationCanceledException)
        {
            /* The command ended before the trigger. */
        }
        catch (Exception)
        {
            /* A probe that cannot be taken never faults the fetch it watches. */
        }
    }
}
