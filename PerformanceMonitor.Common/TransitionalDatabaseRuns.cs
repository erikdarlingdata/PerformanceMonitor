/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Linq;

namespace PerformanceMonitor.Common;

/// <summary>
/// #9270: the one place that knows how a database in a transitional state (RESTORING, RECOVERING, RECOVERY_PENDING: the minutes
/// after a restart or failover, or a standby that stays restoring) is treated by <c>database_config</c> capture. Both products read it:
/// the early recapture of the collector and the change diff (<see cref="ConfigChangeDiff.DiffDatabaseConfigChanges"/>).
/// </summary>
public static class TransitionalDatabaseRuns
{
    /// <summary>
    /// A <c>database_config</c> capture that holds a database in a transitional state is taken again after this many minutes, not after a
    /// day, so a database that has since come back is captured settled within minutes.
    /// </summary>
    public const int RecaptureMinutes = 10;

    /// <summary>
    /// How many times in a row a transitional capture is retaken early. After that the collector keeps its normal interval, so a
    /// database that stays restoring for good (a log-shipping standby) does not hold a recapture every ten minutes.
    /// </summary>
    public const int MaxRecaptures = 6;

    /// <summary>
    /// A run of transitional captures of one database is stuck once it holds a capture this long after the run's first. One step short
    /// of the full hour of early recaptures, so the last early recapture always counts even with scheduling jitter. A run that ends
    /// before this (a restart or failover) is not a state change worth a row; a stuck one is.
    /// </summary>
    public static readonly TimeSpan StuckAfter = TimeSpan.FromMinutes(RecaptureMinutes * (MaxRecaptures - 1));

    /// <summary>One <c>database_config</c> capture of one database: when, which database, and its <c>state_desc</c>.</summary>
    public sealed record Capture(DateTime Time, string Database, string? State);

    /// <summary>
    /// The stateless early-recapture rule, read from the server's own stored captures (all of them; the run lengths come from the data, so a
    /// restart of either product changes nothing). True when some database in the server's newest capture has all three of: a transitional
    /// state in that capture, a settled capture before its current run of transitional captures, and fewer than
    /// <c>1 + <see cref="MaxRecaptures"/></c> captures in that run. A database transitional since it was first seen (a standby)
    /// has no settled capture before its run, and a stuck one has used its repeats, so neither holds the collector at ten minutes.
    /// A database missing from the newest capture (dropped) does not count.
    /// </summary>
    public static bool RecaptureIsDue(IEnumerable<Capture> captures)
    {
        var all = captures as IReadOnlyCollection<Capture> ?? captures.ToList();
        if (all.Count == 0)
        {
            return false;
        }

        var newest = all.Max(c => c.Time);
        foreach (var database in all.GroupBy(c => c.Database, StringComparer.Ordinal))
        {
            var history = database.OrderByDescending(c => c.Time).ToList();
            if (history[0].Time != newest || !ConfigChangeDiff.IsTransitionalDatabaseState(history[0].State))
            {
                continue;
            }

            var run = 0;
            while (run < history.Count && ConfigChangeDiff.IsTransitionalDatabaseState(history[run].State))
            {
                run++;
            }

            if (run < history.Count && run < 1 + MaxRecaptures)
            {
                return true;
            }
        }

        return false;
    }
}
