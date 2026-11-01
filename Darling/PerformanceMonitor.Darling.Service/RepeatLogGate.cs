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

/// <summary>
/// #7067: decides whether a warning that a collector repeats on every cycle is written to the service log again.
/// A precondition that does not change (an extension that is not created, a grant that is not given) said the same
/// thing on every cycle, and the repeats buried the lines that carry news. For each server, collector and message the
/// first sighting is logged, a changed message is a new sighting, and an unchanged one is repeated as a reminder at most
/// once a day. <see cref="Clear"/> forgets a server and collector when the collector succeeds, so a failure that comes
/// back is logged at once. Only the log file is affected: the collection_log rows, the health bands and the alerts do not
/// read this.
/// </summary>
internal sealed class RepeatLogGate
{
    /// <summary>How long an unchanged message stays quiet before it is logged again as a reminder.</summary>
    public static readonly TimeSpan ReminderInterval = TimeSpan.FromDays(1);

    /// <summary>A message that embeds a changing value would add a new entry on every cycle; past this many for one
    /// server and collector the pair is forgotten, which logs the next message at once.</summary>
    private const int MaxMessagesPerPair = 32;

    private readonly object _lock = new();
    private readonly Dictionary<(string Server, string Collector), Dictionary<string, DateTime>> _lastLogged = new();

    /// <summary>The clock the reminder interval is measured on; a test steps it.</summary>
    public Func<DateTime> Clock { get; set; } = static () => DateTime.UtcNow;

    /// <summary>True when the message should be written now, and records that it was.</summary>
    public bool ShouldLog(string server, string collector, string message)
    {
        var now = Clock();
        lock (_lock)
        {
            if (!_lastLogged.TryGetValue((server, collector), out var messages))
            {
                messages = new Dictionary<string, DateTime>(StringComparer.Ordinal);
                _lastLogged[(server, collector)] = messages;
            }

            if (messages.TryGetValue(message, out var last) && now - last < ReminderInterval)
            {
                return false;
            }

            if (messages.Count >= MaxMessagesPerPair && !messages.ContainsKey(message))
            {
                messages.Clear();
            }

            messages[message] = now;
            return true;
        }
    }

    /// <summary>Forgets everything logged for the server and collector, because the collector ran and succeeded.</summary>
    public void Clear(string server, string collector)
    {
        lock (_lock)
        {
            _lastLogged.Remove((server, collector));
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
