/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace PerformanceMonitor.Darling.Service.Hosting;

/// <summary>
/// A per-source delay that grows after failed credential checks, for the surfaces a caller can reach
/// before holding a credential: the web dashboard's token sign-in and the MCP host's bearer check. Without
/// it a token can be tried at line rate from inside <c>allowFrom</c> (on the MCP loopback bind, from any
/// local process); with it the cost of the Nth attempt from one source doubles from the
/// <see cref="FreeFailures"/>th on, up to <see cref="MaxDelay"/>.
///
/// <para><b>The shape.</b> <see cref="BeginAttemptAsync"/> is ONE atomic step: under a single lock it checks
/// the source's wait and the total budget and, when the attempt may run, reserves it before any token is
/// compared. A caller that is told the attempt is held answers 429 with <c>Retry-After</c> and compares
/// nothing, so a correct credential presented inside the wait is refused too (otherwise the wait would tell
/// the caller which attempt was right). A reservation is IN FLIGHT until the caller reports the comparison:
/// <see cref="Accepted"/> gives the reservation back (the token matched), <see cref="Missed"/> turns it into
/// a recorded miss. In-flight and recorded reservations both count against the free allowance, so N
/// attempts that arrive together, or that were staged and then released together, are counted one by one:
/// at most <see cref="FreeFailures"/> + 1 tokens are compared before the source's wait starts, whatever the
/// concurrency. An attempt that arrives while the allowance is used up ONLY by in-flight reservations is not
/// answered 429, because those reservations may be right tokens that are about to be given back: it waits
/// (bounded by <see cref="InFlightWaitLimit"/>, and cancellable) for one of them to finish and is decided
/// again. If they miss, the misses are recorded, the wait starts and the waiter is held without a comparison.
/// A held attempt reserves nothing, so a client that retries on a timer cannot lengthen its own wait.</para>
///
/// <para><b>Bounded.</b> Like <see cref="DarlingHttpRefusalLog"/>, this keys on a source address a network
/// port hands out freely. A source quiet for <see cref="ForgetAfter"/> is forgotten, and past
/// <see cref="DefaultMaxTrackedSources"/> the longest-quiet entry is dropped for a new one, so memory is
/// bounded. IPv6 sources are grouped by their /64 and every loopback address is one source
/// (<see cref="SourceKey"/>), and a total budget of <see cref="GlobalMaxAttempts"/> reservations per
/// <see cref="GlobalWindow"/> holds across every source, so rotating addresses does not multiply the
/// attempts. A source that presented the right token within <see cref="RecentSuccessWindow"/> is not held
/// by the total budget (its own wait still applies), so a few failing sources cannot lock out the clients
/// that are working. A proxy in front of the port makes every client one source unless it is listed in
/// <c>web.network.trustedProxies</c> (the web host only). The MCP host reads no forwarded headers, so behind
/// a proxy it sees one source, and on its loopback bind every local client is one.</para>
///
/// <para><b>In memory.</b> The waits live in this object, so a service restart clears them; an operator held
/// out by a wait can restart the service. The 429 answers and the docs say so.</para>
///
/// <para>Pure with respect to the clock: every decision takes the time as an argument, so the whole policy is
/// testable without a wait. Only the bounded in-flight wait uses real time.</para>
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

    /// <summary>The most wrong tokens compared across ALL sources that have not recently shown the right one,
    /// in one <see cref="GlobalWindow"/>: sixty an hour, one a minute for the whole service. The purpose is
    /// a bound on comparisons when the sources are many (rotating addresses, a spread of hosts). It is far
    /// below what finding a token takes, because the floor is 16 characters (at least 2^64 values even from
    /// a 16-symbol alphabet), and well above honest mistakes: fifteen sources in one hour, each using its
    /// three free misses and the one that starts its wait, make exactly sixty. A source that presented the
    /// right token recently is not held by this budget (<see cref="RecentSuccessWindow"/>), so the budget
    /// being spent never locks out a client that is working.</summary>
    internal const int GlobalMaxAttempts = 60;

    /// <summary>How long a source counts as having "presented the right token recently", which lifts the
    /// total-budget hold for it (its own wait still applies). One hour, the same as <see cref="GlobalWindow"/>:
    /// the hold is built from reservations made inside that hour, so a client that worked inside the same
    /// hour is the working client the hold must not lock out, and the exemption lasts exactly as long as the
    /// evidence behind the hold. Every success renews it, so a client that keeps working keeps it. Only a
    /// matching token earns it, so a source cannot enter this map by failing.</summary>
    internal static readonly TimeSpan RecentSuccessWindow = GlobalWindow;

    /// <summary>Sources remembered as having presented the right token; the oldest is dropped past this.
    /// A LAN of operators and MCP clients is a few dozen sources at most.</summary>
    internal const int MaxRecentSuccessSources = 256;

    /// <summary>The longest an attempt waits for in-flight reservations to finish before it is answered 429.
    /// A reservation is in flight only for the token comparison (microseconds), so the normal wait is a
    /// few milliseconds; five seconds covers a stalled thread pool without holding a connection for long,
    /// and it is the same order as the first wait (<see cref="BaseDelay"/>).</summary>
    internal static readonly TimeSpan InFlightWaitLimit = TimeSpan.FromSeconds(5);

    /// <summary>The key every loopback address is throttled under (127.0.0.0/8, ::1 and the IPv4-mapped forms).
    /// A process on this host can pick any of them as its source address, so they are one source.</summary>
    internal const string LoopbackKey = "127.0.0.1";

    private readonly int _maxTrackedSources;
    private readonly int _globalMaxAttempts;
    private readonly TimeSpan _inFlightWaitLimit;
    private readonly object _sync = new();
    private readonly Dictionary<string, Entry> _perSource = new(StringComparer.Ordinal);
    private readonly Dictionary<string, DateTime> _recentSuccess = new(StringComparer.Ordinal);
    private readonly List<DateTime> _global = new();
    private TaskCompletionSource? _changed;

    public DarlingSignInThrottle(int? maxTrackedSources = null, int? globalMaxAttempts = null, TimeSpan? inFlightWaitLimit = null)
    {
        _maxTrackedSources = maxTrackedSources ?? DefaultMaxTrackedSources;
        _globalMaxAttempts = globalMaxAttempts ?? GlobalMaxAttempts;
        _inFlightWaitLimit = inFlightWaitLimit ?? InFlightWaitLimit;
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
    /// address; every loopback address is <see cref="LoopbackKey"/>; any other IPv6 address is its /64
    /// (<see cref="Ipv6GroupPrefixBits"/>); a missing address is one shared "unknown" source.</summary>
    internal static string SourceKey(IPAddress? remote)
    {
        if (remote is null)
        {
            return DarlingHttpRefusalLog.DescribeSource(null);
        }

        var ip = remote.IsIPv4MappedToIPv6 ? remote.MapToIPv4() : remote;
        if (IPAddress.IsLoopback(ip))
        {
            return LoopbackKey;
        }

        if (ip.AddressFamily != AddressFamily.InterNetworkV6)
        {
            return ip.ToString();
        }

        var bytes = ip.GetAddressBytes();
        Array.Clear(bytes, Ipv6GroupPrefixBits / 8, bytes.Length - Ipv6GroupPrefixBits / 8);
        return new IPAddress(bytes) + "/" + Ipv6GroupPrefixBits.ToString(CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// The one atomic step in front of every credential comparison, for a caller that can wait. It is
    /// <see cref="BeginAttempt"/> repeated while the answer is "pending": when the allowance is used up only
    /// by in-flight reservations (which may be right tokens about to be given back) the attempt waits for one
    /// of them to finish, at most <see cref="InFlightWaitLimit"/> of real time, and is decided again. If the
    /// limit passes with the in-flight reservations still unresolved, the attempt is held for
    /// <see cref="BaseDelay"/>. Cancelling <paramref name="cancellationToken"/> ends the wait with
    /// <see cref="OperationCanceledException"/>; a waiting attempt holds no reservation, so nothing is left
    /// behind. The returned attempt is reserved (compare once, then report <see cref="Accepted"/> or
    /// <see cref="Missed"/>) or held (answer 429, compare nothing).
    /// </summary>
    public async Task<SignInAttempt> BeginAttemptAsync(string source, Func<DateTime> clock, CancellationToken cancellationToken)
    {
        var started = Stopwatch.GetTimestamp();
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();

            var attempt = Decide(source, clock(), out var changed);
            if (!attempt.IsPending)
            {
                return attempt;
            }

            var remaining = _inFlightWaitLimit - Stopwatch.GetElapsedTime(started);
            if (remaining <= TimeSpan.Zero)
            {
                return SignInAttempt.Held(BaseDelay, SignInHold.Source);
            }

            using var stop = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            await Task.WhenAny(changed!, Task.Delay(remaining, stop.Token)).ConfigureAwait(false);
            stop.Cancel();
        }
    }

    /// <summary>
    /// The synchronous form of the atomic step: decides now, never waits. When the source is inside its wait,
    /// or the total budget is spent and the source has no recent success, the attempt is held: nothing is
    /// reserved and the caller must compare nothing. When the allowance is used up only by in-flight
    /// reservations the answer is pending (<see cref="SignInAttempt.IsPending"/>): nothing is reserved yet and
    /// <see cref="BeginAttemptAsync"/> is what waits and asks again. Otherwise the attempt is reserved and the
    /// caller may compare once; it then reports <see cref="Accepted"/> or <see cref="Missed"/>.
    /// </summary>
    public SignInAttempt BeginAttempt(string source, DateTime nowUtc) => Decide(source, nowUtc, out _);

    private SignInAttempt Decide(string source, DateTime nowUtc, out Task? changed)
    {
        changed = null;
        lock (_sync)
        {
            Forget(nowUtc);
            ForgetGlobal(nowUtc);
            ForgetSuccesses(nowUtc);

            /* The source's own wait comes from its RECORDED misses only. */
            var sourceWait = TimeSpan.Zero;
            _perSource.TryGetValue(source, out var entry);
            if (entry is not null && entry.Misses > 0)
            {
                var until = entry.LastMissUtc + DelayAfter(entry.Misses);
                if (until > nowUtc)
                {
                    sourceWait = until - nowUtc;
                }
            }

            /* The total hold does not apply to a source that presented the right token recently; the source's
               own wait above still does. */
            var totalWait = TimeSpan.Zero;
            if (_global.Count >= _globalMaxAttempts && !_recentSuccess.ContainsKey(source))
            {
                var freesAt = _global[0] + GlobalWindow;
                totalWait = freesAt > nowUtc ? freesAt - nowUtc : TimeSpan.Zero;
            }

            if (sourceWait > TimeSpan.Zero || totalWait > TimeSpan.Zero)
            {
                return totalWait > sourceWait
                    ? SignInAttempt.Held(totalWait, SignInHold.Total)
                    : SignInAttempt.Held(sourceWait, SignInHold.Source);
            }

            /* In-flight reservations count against the allowance exactly as recorded misses do (so the number
               compared stays exact), but they are not yet failures: if the wait would be in force only because
               of them, this attempt waits for one to finish instead of being refused. */
            if (entry is not null && entry.InFlight > 0)
            {
                var until = entry.LastReservedUtc + DelayAfter(entry.Misses + entry.InFlight);
                if (until > nowUtc)
                {
                    _changed ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                    changed = _changed.Task;
                    return SignInAttempt.Pending();
                }
            }

            if (entry is null)
            {
                entry = AddEntry(source);
            }

            entry.InFlight++;
            entry.LastReservedUtc = nowUtc;
            _global.Add(nowUtc);
            return SignInAttempt.Reserved(source, nowUtc);
        }
    }

    /// <summary>The reserved attempt matched: its own reservation is given back (the source's count and the
    /// total budget each go down by one) and the source is remembered as having presented the right token
    /// (<see cref="RecentSuccessWindow"/>). Other attempts' reservations and the misses already recorded stay,
    /// so one client's success never clears what another client behind the same source has accumulated, and
    /// never shortens the wait those misses earned. A held attempt reserved nothing, so this does nothing for
    /// it.</summary>
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
                if (entry.InFlight > 0)
                {
                    entry.InFlight--;
                }

                if (entry.InFlight == 0 && entry.Misses == 0)
                {
                    _perSource.Remove(attempt.Source!);
                }
            }

            var index = _global.LastIndexOf(attempt.ReservedAtUtc);
            if (index >= 0)
            {
                _global.RemoveAt(index);
            }

            RememberSuccess(attempt.Source!, attempt.ReservedAtUtc);
            Signal();
        }
    }

    /// <summary>The reserved attempt did not match: it becomes a recorded miss, and the wait is computed from
    /// the recorded misses. The reservation stays counted in the total budget. Attempts waiting on in-flight
    /// reservations are woken to be decided again. A held attempt reserved nothing, so this does nothing for
    /// it.</summary>
    public void Missed(SignInAttempt attempt)
    {
        if (!attempt.IsReserved)
        {
            return;
        }

        lock (_sync)
        {
            /* The entry can have been dropped for being quiet or to make room; a miss is still recorded. */
            if (!_perSource.TryGetValue(attempt.Source!, out var entry))
            {
                entry = AddEntry(attempt.Source!);
                entry.LastReservedUtc = attempt.ReservedAtUtc;
            }

            if (entry.InFlight > 0)
            {
                entry.InFlight--;
            }

            entry.Misses++;
            if (attempt.ReservedAtUtc > entry.LastMissUtc)
            {
                entry.LastMissUtc = attempt.ReservedAtUtc;
            }

            Signal();
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

    /// <summary>Sources remembered as having presented the right token — for the tests.</summary>
    internal int RecentSuccessCount
    {
        get { lock (_sync) { return _recentSuccess.Count; } }
    }

    /* Wakes the attempts waiting on in-flight reservations. Called under _sync; the continuations run
       asynchronously, so none of them runs inside the lock. */
    private void Signal()
    {
        var waiters = _changed;
        _changed = null;
        waiters?.TrySetResult();
    }

    private Entry AddEntry(string source)
    {
        if (_perSource.Count >= _maxTrackedSources)
        {
            DropLongestQuiet();
        }

        var entry = new Entry();
        _perSource[source] = entry;
        return entry;
    }

    private void RememberSuccess(string source, DateTime nowUtc)
    {
        if (!_recentSuccess.ContainsKey(source) && _recentSuccess.Count >= MaxRecentSuccessSources)
        {
            string? oldestKey = null;
            var oldest = DateTime.MaxValue;
            foreach (var pair in _recentSuccess)
            {
                if (pair.Value < oldest)
                {
                    oldest = pair.Value;
                    oldestKey = pair.Key;
                }
            }

            if (oldestKey is not null)
            {
                _recentSuccess.Remove(oldestKey);
            }
        }

        _recentSuccess[source] = nowUtc;
    }

    private void ForgetSuccesses(DateTime nowUtc)
    {
        if (_recentSuccess.Count == 0)
        {
            return;
        }

        List<string>? stale = null;
        foreach (var pair in _recentSuccess)
        {
            if (nowUtc - pair.Value >= RecentSuccessWindow)
            {
                (stale ??= new List<string>()).Add(pair.Key);
            }
        }

        if (stale is not null)
        {
            foreach (var key in stale)
            {
                _recentSuccess.Remove(key);
            }
        }
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
            if (nowUtc - pair.Value.LastActivityUtc >= ForgetAfter)
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
            if (pair.Value.LastActivityUtc < oldest)
            {
                oldest = pair.Value.LastActivityUtc;
                victim = pair.Key;
            }
        }

        if (victim is not null)
        {
            _perSource.Remove(victim);
        }
    }

    /* Mutable, and only ever touched under _sync. Misses are recorded failures; InFlight counts reservations
       whose comparison has not been reported yet. Both count against the free allowance. */
    private sealed class Entry
    {
        public int Misses;
        public DateTime LastMissUtc;
        public int InFlight;
        public DateTime LastReservedUtc;

        public DateTime LastActivityUtc => LastMissUtc > LastReservedUtc ? LastMissUtc : LastReservedUtc;
    }
}

/// <summary>Why an attempt was held: the source's own wait (its recorded misses), or the total budget across
/// every source.</summary>
internal enum SignInHold
{
    None = 0,
    Source = 1,
    Total = 2,
}

/// <summary>What <see cref="DarlingSignInThrottle"/> decided: held for <see cref="RetryAfter"/> (nothing reserved,
/// nothing may be compared), pending (nothing reserved yet; only the asynchronous form waits and asks again),
/// or reserved (the caller may compare once and then reports the result).</summary>
internal readonly struct SignInAttempt
{
    private SignInAttempt(bool isHeld, bool isPending, TimeSpan retryAfter, SignInHold hold, string? source, DateTime reservedAtUtc)
    {
        IsHeld = isHeld;
        IsPending = isPending;
        RetryAfter = retryAfter;
        Hold = hold;
        Source = source;
        ReservedAtUtc = reservedAtUtc;
    }

    /// <summary>True when the attempt must be answered 429 without comparing a token.</summary>
    public bool IsHeld { get; }

    /// <summary>True when the attempt is waiting on in-flight reservations (only the synchronous form returns
    /// this; <see cref="DarlingSignInThrottle.BeginAttemptAsync"/> waits it out).</summary>
    public bool IsPending { get; }

    /// <summary>How long until the source may try again; zero when the attempt is reserved.</summary>
    public TimeSpan RetryAfter { get; }

    /// <summary>What caused the hold; <see cref="SignInHold.None"/> when the attempt is not held.</summary>
    public SignInHold Hold { get; }

    /// <summary>True when this attempt holds a reservation.</summary>
    public bool IsReserved => Source is not null;

    internal string? Source { get; }

    internal DateTime ReservedAtUtc { get; }

    internal static SignInAttempt Held(TimeSpan retryAfter, SignInHold hold) => new(true, false, retryAfter, hold, null, default);

    internal static SignInAttempt Pending() => new(false, true, TimeSpan.Zero, SignInHold.None, null, default);

    internal static SignInAttempt Reserved(string source, DateTime reservedAtUtc)
        => new(false, false, TimeSpan.Zero, SignInHold.None, source, reservedAtUtc);
}
