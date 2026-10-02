/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;

namespace PerformanceMonitor.Collectors;

/// <summary>#4636/#4640: recurring collectors keep a fixed grid — the next due time is the previous due time
/// plus the interval, never "when this pass started" plus the interval, so a late start is not carried into
/// the next slot. A slot missed during a stall is skipped, not replayed, so a resumed server does not burst.</summary>
public static class CollectorCadence
{
    /// <summary>The next grid slot after <paramref name="now"/> (or <paramref name="due"/> plus one interval when
    /// that is still in the future).</summary>
    public static DateTime NextDue(DateTime due, DateTime now, TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);

        var next = due + interval;
        if (next > now)
        {
            return next;
        }

        var missed = SkippedSlots(due, now, interval);
        return due + TimeSpan.FromTicks(interval.Ticks * (missed + 1));
    }

    /// <summary>#4732: how many grid slots <see cref="NextDue"/> steps over when the slot at <paramref name="due"/>
    /// runs at <paramref name="now"/>: the whole slots that have also come due and will never run, because a missed
    /// slot is skipped, not replayed. 0 when the run is on time (the next slot is still ahead), which is the
    /// normal case. The same count <see cref="NextDue"/> uses to land on the next grid slot, so the two cannot
    /// disagree about how far behind a run was.</summary>
    public static long SkippedSlots(DateTime due, DateTime now, TimeSpan interval)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);

        if (due + interval > now)
        {
            return 0;
        }

        return (long)Math.Floor((now - due).Ticks / (double)interval.Ticks);
    }

    /// <summary>#4732: a stored due time that is more than one <paramref name="interval"/> ahead of
    /// <paramref name="now"/> can only come from a wall clock that stepped backwards after the time was stamped
    /// (a stamp is at most one interval ahead when it is written), so it is treated as due now instead of pausing
    /// the work for as long as the step. A due time up to one interval ahead is a normal wait and is returned
    /// unchanged. Darling's schedule reload (<c>RecomputeNextDueAsync</c>) is not this clamp: it caps a stored due time at
    /// <c>now</c> plus the interval, so a stamp that far ahead still waits one interval there instead of running at once.</summary>
    public static DateTime ClampDue(DateTime due, DateTime now, TimeSpan interval) =>
        due - now > interval ? now : due;

    /// <summary>#4732: whether <paramref name="interval"/> has passed since <paramref name="lastUtc"/>, for the work that
    /// decides from the time elapsed since its last run (a job's cadence, a retry backoff, a re-check throttle) instead of from a
    /// stored due time. The elapsed time is negative when the wall clock stepped backwards after the stamp was taken, and the work
    /// then waited out the step. A stamp ahead of the clock can only be that step (a stamp is taken from the clock at the time),
    /// so it counts as elapsed: this is <see cref="ClampDue"/> applied to <c>lastUtc + interval</c>. A stamp at or before now
    /// decides exactly as <c>nowUtc - lastUtc &gt;= interval</c> does.</summary>
    public static bool IntervalElapsed(DateTime lastUtc, DateTime nowUtc, TimeSpan interval) =>
        ClampDue(lastUtc + interval, nowUtc, interval) <= nowUtc;

    /// <summary>
    /// A deterministic, restart-stable per-server phase offset within a cadence period (#1553 cadence jitter), used
    /// to break the fleet-wide lockstep at cadence boundaries: the field incident re-herded every server at once, so
    /// at each boundary all collectors fired together. <paramref name="serverId"/> is the monitored server's id,
    /// which today is an FNV-1a hash of its name (<c>ServerIdHelper.GetDeterministicHashCode</c>), so a plain modulo
    /// spreads it across <c>[0, period)</c> without any further mixing (an extra multiply was reviewed out as
    /// unnecessary: the input is already avalanched). This is the one consumer that wants the value only as a
    /// spreading function rather than as an identity, so if #2218 ever makes ids sequential the extra mixing that was
    /// reviewed out has to come back here: consecutive integers modulo a period do not spread, they line up.
    /// Restart-stable because it is a pure function of the id, with no <see cref="Random"/>.
    ///
    /// <para>A non-positive period yields no offset (it guards the callers where a period could in principle be
    /// zero, and keeps the result well defined for tests). It is applied at initial cadence stamps and never to the
    /// steady-state advance of a grid: the Darling worker's cold-start sweep spread, its on-connect analysis stamp
    /// and, capped at 150 seconds, its seed jitter for an overdue or never-run collector (#1575). A daily
    /// collector's run time takes its fixed per-server spread from it over one hour (#4938,
    /// <see cref="CollectorRunTime.Spread"/>).</para>
    ///
    /// <para>Moved here from the Darling worker (#4938) so the run-time rules in this project can share it, with the
    /// values unchanged.</para>
    /// </summary>
    public static TimeSpan CadencePhaseOffset(int serverId, int periodSeconds)
    {
        if (periodSeconds <= 0)
        {
            return TimeSpan.Zero;
        }

        /* Cast to uint first so a negative FNV hash still maps into [0, period): a signed modulo would yield a
           negative offset and pull the due time into the past. */
        return TimeSpan.FromSeconds((uint)serverId % periodSeconds);
    }
}
