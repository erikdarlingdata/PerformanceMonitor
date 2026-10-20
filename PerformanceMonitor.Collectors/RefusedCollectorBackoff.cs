/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// #6186: the retry schedule for a collector the monitored server refuses on every run (a PERMISSIONS run), shared by Darling's
/// scheduler and Lite's. A refusal that repeats with the same answer is a standing condition, not a fresh failure each interval,
/// so the collector is tried less often while it repeats: the first <see cref="FreeRefusals"/> refusals run at the normal interval
/// (a grant being applied can look like one), then each further identical refusal doubles the wait, up to
/// <see cref="MaxDelay"/> (or the collector's own interval when that is longer: a backoff never makes a collector run MORE often).
/// The collector is never stopped for good, so a granted permission is picked up at the next try. The first run that is not a
/// refusal, or a refusal with a different answer, goes back to the normal interval.
/// </summary>
public sealed class RefusedCollectorBackoff
{
    /// <summary>Identical refusals that still run at the normal interval.</summary>
    public const int FreeRefusals = 2;

    /// <summary>The longest wait a 1-minute collector backs off to.</summary>
    public static readonly TimeSpan MaxDelay = TimeSpan.FromHours(1);

    private readonly object _lock = new();
    private readonly Dictionary<string, Entry> _entries = new(StringComparer.OrdinalIgnoreCase);

    private sealed class Entry
    {
        public int Streak;
        public string Answer = "";
        public DateTime NotBeforeUtc;
    }

    /// <summary>
    /// The wait before the next try after <paramref name="streak"/> identical refusals in a row, for a collector whose normal
    /// interval is <paramref name="interval"/>. The normal interval until the streak passes <see cref="FreeRefusals"/>, then
    /// doubled for each further refusal, capped at <see cref="MaxDelay"/> and never below the normal interval.
    /// </summary>
    public static TimeSpan DelayAfter(int streak, TimeSpan interval)
    {
        if (streak <= FreeRefusals)
        {
            return interval;
        }

        var doublings = Math.Min(streak - FreeRefusals, 20);
        var ceiling = interval > MaxDelay ? interval : MaxDelay;
        var scaled = interval.TotalSeconds * Math.Pow(2, doublings);
        return scaled >= ceiling.TotalSeconds ? ceiling : TimeSpan.FromSeconds(scaled);
    }

    /// <summary>
    /// Records one run of <paramref name="collector"/>. A refusal (<paramref name="refused"/>) with the same
    /// <paramref name="answer"/> as the last one extends the streak and sets the next try; anything else (a success, an error
    /// that is not a refusal, a refusal with a different answer) restarts from the normal interval. Returns the streak after the
    /// run (0 when it was not a refusal).
    /// </summary>
    public int Record(string collector, bool refused, string? answer, DateTime nowUtc, TimeSpan interval)
    {
        lock (_lock)
        {
            if (!refused)
            {
                _entries.Remove(collector);
                return 0;
            }

            var text = answer ?? "";
            if (!_entries.TryGetValue(collector, out var entry) || !string.Equals(entry.Answer, text, StringComparison.Ordinal))
            {
                entry = new Entry { Answer = text };
                _entries[collector] = entry;
            }

            entry.Streak++;
            /* A wait no longer than the collector's own interval is the ordinary schedule, which already spaces the tries: stamping
               it would only push a collector with a long interval (a daily one) past its own next slot. */
            var delay = DelayAfter(entry.Streak, interval);
            entry.NotBeforeUtc = delay > interval ? nowUtc + delay : DateTime.MinValue;
            return entry.Streak;
        }
    }

    /// <summary>
    /// Whether <paramref name="collector"/> is waiting out its backoff at <paramref name="nowUtc"/>. A stamp more than the longest
    /// possible wait ahead can only come from a wall clock that stepped backwards after it was set, so it counts as due.
    /// </summary>
    public bool IsBackedOff(string collector, DateTime nowUtc, TimeSpan interval)
    {
        lock (_lock)
        {
            if (!_entries.TryGetValue(collector, out var entry) || entry.Streak <= FreeRefusals)
            {
                return false;
            }

            var longest = interval > MaxDelay ? interval : MaxDelay;
            return nowUtc < entry.NotBeforeUtc && entry.NotBeforeUtc - nowUtc <= longest;
        }
    }

    /// <summary>The identical refusals in a row recorded for <paramref name="collector"/>; 0 when its last run was not one.</summary>
    public int Streak(string collector)
    {
        lock (_lock)
        {
            return _entries.TryGetValue(collector, out var entry) ? entry.Streak : 0;
        }
    }

    /// <summary>Moves the next try of <paramref name="collector"/> to <paramref name="notBeforeUtc"/>; for tests, standing in for the clock.</summary>
    internal void SetNotBeforeForTests(string collector, DateTime notBeforeUtc)
    {
        lock (_lock)
        {
            if (_entries.TryGetValue(collector, out var entry))
            {
                entry.NotBeforeUtc = notBeforeUtc;
            }
        }
    }

    /// <summary>Forgets every collector's streak (a server removed, or a restart of its loop state).</summary>
    public void Clear()
    {
        lock (_lock)
        {
            _entries.Clear();
        }
    }
}
