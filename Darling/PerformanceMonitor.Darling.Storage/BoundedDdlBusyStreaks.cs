/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Storage;

/// <summary>
/// Counts, per object, how many bounded DDL attempts in a row found the table held by another session. One busy
/// pass is routine; the same object busy every pass means its settings never converge, and that has to be
/// visible above Information. Pure and thread-safe: no I/O, no clock of its own.
/// </summary>
internal sealed class BoundedDdlBusyStreaks
{
    /// <summary>Consecutive busy results on one object at which <see cref="RecordBusy"/> reports an escalation.</summary>
    internal const int EscalateAfter = 3;

    private readonly object _gate = new();
    private readonly Dictionary<string, Streak> _streaks = new(StringComparer.Ordinal);

    private sealed class Streak
    {
        public int Count;
        public DateTime FirstBusyUtc;
        public bool Escalated;
    }

    /// <summary>
    /// Records one busy result for <paramref name="key"/>. Returns true exactly once per streak, on the call that
    /// brings it to <see cref="EscalateAfter"/>; the streak's length and first busy time come back through the
    /// out parameters.
    /// </summary>
    internal bool RecordBusy(string key, DateTime utcNow, out int passes, out DateTime firstBusyUtc)
    {
        lock (_gate)
        {
            if (!_streaks.TryGetValue(key, out var streak))
            {
                streak = new Streak { FirstBusyUtc = utcNow };
                _streaks[key] = streak;
            }

            streak.Count++;
            passes = streak.Count;
            firstBusyUtc = streak.FirstBusyUtc;
            if (streak.Count == EscalateAfter)
            {
                streak.Escalated = true;
                return true;
            }

            return false;
        }
    }

    /// <summary>
    /// Clears the streak for <paramref name="key"/> after an Applied or Failed result. Returns true when the
    /// streak had escalated, so the caller can say the object converged; the passes it was busy come back in
    /// <paramref name="busyPasses"/>.
    /// </summary>
    internal bool Clear(string key, out int busyPasses)
    {
        lock (_gate)
        {
            if (_streaks.Remove(key, out var streak))
            {
                busyPasses = streak.Count;
                return streak.Escalated;
            }

            busyPasses = 0;
            return false;
        }
    }

    /// <summary>Forgets every streak.</summary>
    internal void Reset()
    {
        lock (_gate)
        {
            _streaks.Clear();
        }
    }
}
