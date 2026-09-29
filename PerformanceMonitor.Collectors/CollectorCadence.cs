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
}
