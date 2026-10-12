/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Net.Sockets;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// A per-source delay that grows after failed credential checks, for the surfaces a caller can reach
/// before holding a credential: the web dashboard's token sign-in and the MCP host's bearer check. Without
/// it a token can be tried at line rate from inside <c>allowFrom</c> (on the MCP loopback bind, from any
/// local process); with it the cost of the Nth attempt from one source doubles from the
/// <see cref="FreeFailures"/>th on, up to <see cref="MaxDelay"/>.
///
/// <para><b>The shape.</b> <see cref="BeginAttempt"/> is ONE atomic step: under a single lock it checks the
/// source's wait and the total budget and, when the attempt may run, reserves it (counts it as a failure)
/// before any token is compared. A caller that is told the attempt is held answers 429 with
/// <c>Retry-After</c> and compares nothing, so a correct credential presented inside the wait is refused too
/// (otherwise the wait would tell the caller which attempt was right). Because the reservation is made up
/// front, N attempts that arrive together, or that were staged and then released together, are counted one
/// by one: exactly the allowance gets through and the rest are held. <see cref="Accepted"/> gives back the
/// reservation of the one attempt that matched, and only that one; it never clears what other clients behind
/// the same source have accumulated. A held attempt reserves nothing, so a client that retries on a timer
/// cannot lengthen its own wait.</para>
///
/// <para><b>Bounded.</b> Like <see cref="DarlingHttpRefusalLog"/>, this keys on a source address a network
/// port hands out freely. A source quiet for <see cref="ForgetAfter"/> is forgotten, and past
/// <see cref="DefaultMaxTrackedSources"/> the longest-quiet entry is dropped for a new one, so memory is
/// bounded. IPv6 sources are grouped by their /64 (<see cref="SourceKey"/>), and a total budget of
/// <see cref="GlobalMaxAttempts"/> reservations per <see cref="GlobalWindow"/> holds across every source, so
/// rotating addresses does not multiply the attempts. A proxy in front of the port makes every client one
/// source unless it is listed in <c>web.network.trustedProxies</c> (the web host only). The MCP host reads no
/// forwarded headers, so behind a proxy it sees one source, and on its loopback bind every local client is
/// one.</para>
///
/// <para><b>In memory.</b> The waits live in this object, so a service restart clears them; an operator held
/// out by a wait can restart the service. The 429 answers and the docs say so.</para>
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

    /// <summary>IPv6 sources are grouped by this many leading bits. A /64 is the smallest block a provider
    /// or a LAN hands to one subscriber, and every host in it can choose any of 2^64 addresses, so grouping
    /// by the whole address would give one client a fresh allowance per request.</summary>
    internal const int Ipv6GroupPrefixBits = 64;

    /// <summary>The window the total budget is counted over. One hour is the unit the per-source limit is
    /// documented in (twelve attempts an hour at the longest wait).</summary>
    internal static readonly TimeSpan GlobalWindow = TimeSpan.FromHours(1);

    /// <summary>The most attempts reserved across ALL sources in one <see cref="GlobalWindow"/>. Sixty is five
    /// sources' worth of the steady per-source rate (twelve an hour): more distinct sources failing in the same
    /// hour than a LAN of operators plausibly has, so a spread of addresses runs into it while a few
    /// mistyped tokens never do.</summary>
    internal const int GlobalMaxAttempts = 60;

    private readonly int _maxTrackedSources;
    private readonly int _globalMaxAttempts;
    private readonly object _sync = new();
    private readonly Dictionary<string, Entry> _perSource = new(StringComparer.Ordinal);
    private readonly List<DateTime> _global = new();

    public DarlingSignInThrottle(int? maxTrackedSources = null, int? globalMaxAttempts = null)
    {
        _maxTrackedSources = maxTrackedSources ?? DefaultMaxTrackedSources;
        _globalMaxAttempts = globalMaxAttempts ?? GlobalMaxAttempts;
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

    /// <summary>PURE: the key a remote address is throttled under. An IPv4-mapped IPv6 address is the IPv4
    /// address; any other IPv6 address is its /64 (<see cref="Ipv6GroupPrefixBits"/>); a missing address is
    /// one shared "unknown" source.</summary>
    internal static string SourceKey(IPAddress? remote)
    {
        if (remote is null)
        {
            return DarlingHttpRefusalLog.DescribeSource(null);
        }

        var ip = remote.IsIPv4MappedToIPv6 ? remote.MapToIPv4() : remote;
        if (ip.AddressFamily != AddressFamily.InterNetworkV6)
        {
            return ip.ToString();
        }

        var bytes = ip.GetAddressBytes();
        Array.Clear(bytes, Ipv6GroupPrefixBits / 8, bytes.Length - Ipv6GroupPrefixBits / 8);
        return new IPAddress(bytes) + "/" + Ipv6GroupPrefixBits.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// The one atomic step in front of every credential comparison. When the source is inside its wait, or the
    /// total budget is spent, the attempt is held: nothing is reserved and the caller must compare nothing.
    /// Otherwise the attempt is reserved (counted as a failure) and the caller may compare once; a match is
    /// reported through <see cref="Accepted"/>, a miss needs no further call.
    /// </summary>
    public SignInAttempt BeginAttempt(string source, DateTime nowUtc)
    {
        lock (_sync)
        {
            Forget(nowUtc);
            ForgetGlobal(nowUtc);

            var wait = TimeSpan.Zero;
            if (_perSource.TryGetValue(source, out var entry))
            {
                var until = entry.LastFailureUtc + DelayAfter(entry.Failures);
                if (until > nowUtc)
                {
                    wait = until - nowUtc;
                }
            }

            if (_global.Count >= _globalMaxAttempts)
            {
                var freesAt = _global[0] + GlobalWindow;
                var globalWait = freesAt > nowUtc ? freesAt - nowUtc : TimeSpan.Zero;
                wait = globalWait > wait ? globalWait : wait;
            }

            if (wait > TimeSpan.Zero)
            {
                return SignInAttempt.Held(wait);
            }

            var previous = default(DateTime);
            if (entry is not null)
            {
                previous = entry.LastFailureUtc;
                entry.Failures++;
                entry.LastFailureUtc = nowUtc;
            }
            else
            {
                if (_perSource.Count >= _maxTrackedSources)
                {
                    DropLongestQuiet();
                }

                _perSource[source] = new Entry { Failures = 1, LastFailureUtc = nowUtc };
            }

            _global.Add(nowUtc);
            return SignInAttempt.Reserved(source, nowUtc, previous);
        }
    }

    /// <summary>The reserved attempt matched: its own reservation is given back (the source's count and the
    /// total budget each go down by one). Other attempts' reservations stay, so one client's success never
    /// clears what another client behind the same source has accumulated. A held attempt reserved nothing,
    /// so this does nothing for it.</summary>
    public void Accepted(SignInAttempt attempt)
    {
        if (!attempt.IsReserved)
        {
            return;
        }

        lock (_sync)
        {
            if (_perSource.TryGetValue(attempt.Source!, out var entry))
            {
                if (entry.Failures <= 1)
                {
                    _perSource.Remove(attempt.Source!);
                }
                else
                {
                    entry.Failures--;
                    if (entry.LastFailureUtc == attempt.ReservedAtUtc && attempt.PreviousFailureUtc != default)
                    {
                        entry.LastFailureUtc = attempt.PreviousFailureUtc;
                    }
                }
            }

            var index = _global.LastIndexOf(attempt.ReservedAtUtc);
            if (index >= 0)
            {
                _global.RemoveAt(index);
            }
        }
    }

    /// <summary>Sources tracked right now — for the tests that assert the bound holds.</summary>
    internal int TrackedCount
    {
        get { lock (_sync) { return _perSource.Count; } }
    }

    /// <summary>Reservations counted against the total budget right now — for the tests.</summary>
    internal int GlobalCount
    {
        get { lock (_sync) { return _global.Count; } }
    }

    private void ForgetGlobal(DateTime nowUtc)
    {
        var expired = 0;
        while (expired < _global.Count && nowUtc - _global[expired] >= GlobalWindow)
        {
            expired++;
        }

        if (expired > 0)
        {
            _global.RemoveRange(0, expired);
        }
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

/// <summary>What <see cref="DarlingSignInThrottle.BeginAttempt"/> decided: held for <see cref="RetryAfter"/>
/// (nothing reserved, nothing may be compared), or reserved (the caller may compare once).</summary>
internal readonly struct SignInAttempt
{
    private SignInAttempt(bool isHeld, TimeSpan retryAfter, string? source, DateTime reservedAtUtc, DateTime previousFailureUtc)
    {
        IsHeld = isHeld;
        RetryAfter = retryAfter;
        Source = source;
        ReservedAtUtc = reservedAtUtc;
        PreviousFailureUtc = previousFailureUtc;
    }

    /// <summary>True when the attempt must be answered 429 without comparing a token.</summary>
    public bool IsHeld { get; }

    /// <summary>How long until the source may try again; zero when the attempt is reserved.</summary>
    public TimeSpan RetryAfter { get; }

    /// <summary>True when this attempt holds a reservation.</summary>
    public bool IsReserved => Source is not null;

    internal string? Source { get; }

    internal DateTime ReservedAtUtc { get; }

    internal DateTime PreviousFailureUtc { get; }

    internal static SignInAttempt Held(TimeSpan retryAfter) => new(true, retryAfter, null, default, default);

    internal static SignInAttempt Reserved(string source, DateTime reservedAtUtc, DateTime previousFailureUtc)
        => new(false, TimeSpan.Zero, source, reservedAtUtc, previousFailureUtc);
}
