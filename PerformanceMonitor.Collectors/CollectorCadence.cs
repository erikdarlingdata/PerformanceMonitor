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

        var missed = (long)Math.Floor((now - due).Ticks / (double)interval.Ticks);
        return due + TimeSpan.FromTicks(interval.Ticks * (missed + 1));
    }
}
