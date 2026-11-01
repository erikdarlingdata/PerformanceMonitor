/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Service;

/// <summary>The writers that share a <see cref="RepeatLogGate"/>. Each keeps its own last-message slot, so two lines about
/// the same server and collector never overwrite each other's memory.</summary>
internal enum RepeatLogChannel
{
    /// <summary>The worker's classified PostgreSQL fault warning (<c>DarlingWorker.LogPostgresFault</c>).</summary>
    PostgresFault,

    /// <summary>The runner's "failed in all N database(s)" line (<c>DarlingCollectorRunner.LogFailedInAllDatabases</c>).</summary>
    FailedInAllDatabases,
}

/// <summary>
/// #7067: decides whether a warning that a collector repeats on every cycle is written to the service log again.
/// A precondition that does not change (an extension that is not created, a grant that is not given) said the same
/// thing on every cycle, and the repeats buried the lines that carry news. For each server, collector and channel the gate
/// remembers the last message written. A message that differs from it is a new sighting and is logged at once, so a cause
/// that flips from A to B and back to A logs all three. The same message as the last one is repeated as a reminder at most
/// once a day. <see cref="Clear"/> forgets every channel of a server and collector when the collector succeeds, so a failure that comes
/// back is logged at once. Only the log file is affected: the collection_log rows, the health bands and the alerts do not
/// read this.
/// </summary>
internal sealed class RepeatLogGate
{
    /// <summary>How long an unchanged message stays quiet before it is logged again as a reminder.</summary>
    public static readonly TimeSpan ReminderInterval = TimeSpan.FromDays(1);

    private readonly object _lock = new();
    private readonly Dictionary<(string Server, string Collector, RepeatLogChannel Channel), (string Message, DateTime LoggedAt)> _last = new();

    /// <summary>The clock the reminder interval is measured on; a test steps it.</summary>
    public Func<DateTime> Clock { get; set; } = static () => DateTime.UtcNow;

    /// <summary>True when the message should be written now, and records that it was.</summary>
    public bool ShouldLog(string server, string collector, RepeatLogChannel channel, string message)
    {
        var now = Clock();
        lock (_lock)
        {
            if (_last.TryGetValue((server, collector, channel), out var last)
                && string.Equals(last.Message, message, StringComparison.Ordinal)
                && now - last.LoggedAt < ReminderInterval)
            {
                return false;
            }

            _last[(server, collector, channel)] = (message, now);
            return true;
        }
    }

    /// <summary>Forgets everything logged on every channel for the server and collector, because the collector ran and succeeded.</summary>
    public void Clear(string server, string collector)
    {
        lock (_lock)
        {
            foreach (var channel in Enum.GetValues<RepeatLogChannel>())
            {
                _last.Remove((server, collector, channel));
            }
        }
    }

    /// <summary>A short length of time for a log line: "45s", "12m", "3h 5m", "2d 4h".</summary>
    public static string FormatSpan(TimeSpan span)
    {
        if (span < TimeSpan.FromMinutes(1))
        {
            return $"{(int)Math.Max(0, span.TotalSeconds)}s";
        }

        if (span < TimeSpan.FromHours(1))
        {
            return $"{(int)span.TotalMinutes}m";
        }

        if (span < TimeSpan.FromDays(1))
        {
            return $"{(int)span.TotalHours}h {span.Minutes}m";
        }

        return $"{(int)span.TotalDays}d {span.Hours}h";
    }
}
