/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// A per-source delay that grows after failed credential checks, for the surfaces a caller can reach
/// before holding a credential: the web dashboard's token sign-in and the MCP host's bearer check. Without
/// it a token can be tried at line rate from inside <c>allowFrom</c> (on the MCP loopback bind, from any
/// local process); with it the cost of the Nth failure from one address doubles from the
/// <see cref="FreeFailures"/>th on, up to <see cref="MaxDelay"/>.
///
/// <para><b>The shape.</b> <see cref="RetryAfter"/> answers how long the source still has to wait (zero
/// when it may try); the caller refuses without comparing anything while it is positive, so a correct
/// credential presented inside the wait is refused too — otherwise the wait would tell the caller which
/// attempt was right. <see cref="RecordFailure"/> lengthens the next wait, <see cref="RecordSuccess"/>
/// forgets the source. An attempt refused for being inside the wait does not count as a failure, so a
/// client that retries on a timer cannot lock itself out for good.</para>
///
/// <para><b>Bounded.</b> Like <see cref="DarlingHttpRefusalLog"/>, this keys on a source address a network
/// port hands out freely. A source quiet for <see cref="ForgetAfter"/> is forgotten, and past
/// <see cref="DefaultMaxTrackedSources"/> the longest-quiet entry is dropped for a new one, so memory is
/// bounded and no aggregate bucket ever holds every client to one source's failures. A proxy in front of the
/// port makes every client one source unless it is listed in <c>web.network.trustedProxies</c>, which is why
/// that setting exists beside this. The MCP host reads no forwarded headers, so behind a proxy it sees one
/// source, and on its loopback bind every local client is one.</para>
///
/// <para>Pure with respect to the clock: every call takes <c>nowUtc</c>, so the whole policy is testable
/// without a wait.</para>
/// </summary>
internal sealed class DarlingSignInThrottle
{
    /// <summary>Failures a source may make before the first wait. Three covers a mistyped paste twice over
    /// without punishing anyone, and finding a token by trial takes far more than three.</summary>
    internal const int FreeFailures = 3;

    /// <summary>The wait after the first failure past <see cref="FreeFailures"/>; it doubles per failure.</summary>
    internal static readonly TimeSpan BaseDelay = TimeSpan.FromSeconds(2);

    /// <summary>The longest wait a source can accrue. Five minutes bounds a legitimate operator's worst case
    /// while holding a repeated-trial caller to twelve attempts an hour.</summary>
    internal static readonly TimeSpan MaxDelay = TimeSpan.FromMinutes(5);

    /// <summary>A source with no failure for this long is forgotten (its next failure is a first one).</summary>
    internal static readonly TimeSpan ForgetAfter = TimeSpan.FromMinutes(15);

    /// <summary>Distinct sources tracked at once before the longest-quiet one is dropped.</summary>
    internal const int DefaultMaxTrackedSources = 1024;

    private readonly int _maxTrackedSources;
    private readonly object _sync = new();
    private readonly Dictionary<string, Entry> _perSource = new(StringComparer.Ordinal);

    public DarlingSignInThrottle(int? maxTrackedSources = null)
    {
        _maxTrackedSources = maxTrackedSources ?? DefaultMaxTrackedSources;
    }

    /// <summary>PURE: the wait that follows <paramref name="failures"/> consecutive failures.</summary>
    internal static TimeSpan DelayAfter(int failures)
    {
        if (failures <= FreeFailures)
        {
            return TimeSpan.Zero;
        }

        var doublings = Math.Min(failures - FreeFailures - 1, 30);
        var ticks = BaseDelay.Ticks * (1L << doublings);
        return ticks >= MaxDelay.Ticks ? MaxDelay : TimeSpan.FromTicks(ticks);
    }

    /// <summary>How much longer <paramref name="source"/> must wait before its next attempt is compared;
    /// <see cref="TimeSpan.Zero"/> when it may try now.</summary>
    public TimeSpan RetryAfter(string source, DateTime nowUtc)
    {
        lock (_sync)
        {
            Forget(nowUtc);
            if (!_perSource.TryGetValue(source, out var entry))
            {
                return TimeSpan.Zero;
            }

            var until = entry.LastFailureUtc + DelayAfter(entry.Failures);
            return until > nowUtc ? until - nowUtc : TimeSpan.Zero;
        }
    }

    /// <summary>Records a failed attempt from <paramref name="source"/>: its next wait grows.</summary>
    public void RecordFailure(string source, DateTime nowUtc)
    {
        lock (_sync)
        {
            Forget(nowUtc);
            if (_perSource.TryGetValue(source, out var entry))
            {
                entry.Failures++;
                entry.LastFailureUtc = nowUtc;
                return;
            }

            if (_perSource.Count >= _maxTrackedSources)
            {
                DropLongestQuiet();
            }

            _perSource[source] = new Entry { Failures = 1, LastFailureUtc = nowUtc };
        }
    }

    /// <summary>A successful attempt forgets the source's failures.</summary>
    public void RecordSuccess(string source)
    {
        lock (_sync)
        {
            _perSource.Remove(source);
        }
    }

    /// <summary>Sources tracked right now — for the tests that assert the bound holds.</summary>
    internal int TrackedCount
    {
        get { lock (_sync) { return _perSource.Count; } }
    }

    private void Forget(DateTime nowUtc)
    {
        List<string>? stale = null;
        foreach (var pair in _perSource)
        {
            if (nowUtc - pair.Value.LastFailureUtc >= ForgetAfter)
            {
                (stale ??= new List<string>()).Add(pair.Key);
            }
        }

        if (stale is not null)
        {
            foreach (var key in stale)
            {
                _perSource.Remove(key);
            }
        }
    }

    private void DropLongestQuiet()
    {
        string? victim = null;
        var oldest = DateTime.MaxValue;
        foreach (var pair in _perSource)
        {
            if (pair.Value.LastFailureUtc < oldest)
            {
                oldest = pair.Value.LastFailureUtc;
                victim = pair.Key;
            }
        }

        if (victim is not null)
        {
            _perSource.Remove(victim);
        }
    }

    /* Mutable, and only ever touched under _sync. */
    private sealed class Entry
    {
        public int Failures;
        public DateTime LastFailureUtc;
    }
}
