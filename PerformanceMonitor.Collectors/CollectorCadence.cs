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
    /// unchanged. The same clamp <c>RecomputeNextDueAsync</c> applies on a schedule reload.</summary>
    public static DateTime ClampDue(DateTime due, DateTime now, TimeSpan interval) =>
        due - now > interval ? now : due;
}
