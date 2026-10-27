/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Caching.Memory;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// #5506 part 3: the text of recent successful answers to long reads, one object for the process (the MCP and web hosts
/// share it through <see cref="ReadAdmission.Cache"/>). A heavy (surface, route, band) pair as
/// <see cref="ReadAdmission"/> classifies it is cached, and so is a whole-fleet read of a route in
/// <see cref="ReadAdmission.FleetCachedRoutes"/> over a long window (#6800). Other short reads never are. A hit skips admission and the store.
///
/// <para><b>Lifetime.</b> An entry lives for its band's TTL (<see cref="TtlOf"/>), or for the route's own TTL when it is a fleet read
/// (<see cref="TtlFor"/>), and its key carries the TTL bucket
/// and the write generation (<see cref="ReadCacheKey"/>), so a successful write makes every earlier entry unreachable.
/// Entries are held in a <see cref="MemoryCache"/> of 64 MB; an answer over 1 MB is never stored.</para>
///
/// <para><b>Single flight (D6).</b> Identical misses at the same time run one read. The read runs detached from any one
/// caller under its own token, which is cancelled only when every caller waiting for it has left; each caller waits
/// with its own token. The caller that started the read stays until it ends even if it leaves, because the read records
/// through that caller's <see cref="ReadScope"/> and resolves its services from that caller's <c>RequestServices</c>
/// (round 1, C3), which must outlive it. A flight whose last caller has left is closing: no later caller joins it, so a
/// caller that arrives then starts a read of its own (round 2, L1).</para>
///
/// <para><b>Joined callers (round 2, M1).</b> Only the caller that started the read records its outcome. A joined caller
/// records what it got itself (<see cref="JoinedOutcome"/>): <c>busy</c> or <c>cancelled</c>, or <c>cache_hit</c> for the
/// shared answer, whatever that answer was, so one store read counts once in <c>count</c>, <c>timeouts</c> and the
/// percentiles.</para>
/// </summary>
internal sealed class ReadResultCache : IDisposable
{
    /// <summary>The most the cache holds, counted as the UTF-16 size of the keys and texts.</summary>
    internal const long DefaultSizeLimitBytes = 64L * 1024 * 1024;

    /// <summary>An answer whose UTF-8 text is larger than this is never stored.</summary>
    internal const int MaxAnswerBytes = 1024 * 1024;

    /// <summary>Reads that change what they answer from (they store findings), so they are never cached; a successful
    /// unanchored one moves the write generation instead (D5).</summary>
    internal const string AnalyzeServer = "analyze_server";

    private readonly TimeProvider _time;
    private readonly long _sizeLimit;
    private readonly MemoryCache _memory;
    private readonly ConcurrentDictionary<string, object> _flights = new(StringComparer.Ordinal);
    private readonly object _flightLock = new();
    private long _generation;

    internal ReadResultCache()
        : this(TimeProvider.System)
    {
    }

    internal ReadResultCache(TimeProvider time, long sizeLimitBytes = DefaultSizeLimitBytes)
    {
        _time = time ?? throw new ArgumentNullException(nameof(time));
        _sizeLimit = sizeLimitBytes;
        _memory = new MemoryCache(new MemoryCacheOptions { SizeLimit = sizeLimitBytes, CompactionPercentage = 0.25 });
    }

    /// <summary>Off only in tests that measure admission itself, where identical calls would otherwise share one read.</summary>
    internal bool Enabled { get; set; } = true;

    /// <summary>The write generation: part of every key, moved by <see cref="BumpGeneration"/>.</summary>
    internal long Generation => Interlocked.Read(ref _generation);

    /// <summary>A write succeeded: every entry stored before it becomes unreachable.</summary>
    internal void BumpGeneration() => Interlocked.Increment(ref _generation);

    /// <summary>The entries held now (for tests).</summary>
    internal int Count => _memory.Count;

    /// <summary>For tests (round 2, L1): runs when a flight's last caller has left, after the flight is closed to new callers
    /// and before its read is cancelled, so a test can arrive in that gap.</summary>
    internal Action? BeforeFlightCancel { get; set; }

    /// <summary>The callers waiting on reads in flight now, summed over every key (for tests: a test waits on this rather
    /// than on a fixed delay to know a caller has joined or left).</summary>
    internal int WaitingCallers
    {
        get
        {
            var waiting = 0;
            foreach (var flight in _flights.Values)
            {
                waiting += flight is IFlight counted ? counted.Waiters : 0;
            }

            return waiting;
        }
    }

    /// <summary>
    /// Round 2, M1: the outcome a joined caller records. It ran no read, so it never records the shared read's outcome
    /// (<c>timeout</c>, <c>error</c>, <c>fallback_raw</c>, <c>gate_failed</c> or <c>ok</c>), which the caller that started
    /// the read records once: <c>cancelled</c> when this caller left or the answer it got is a cancellation, <c>busy</c>
    /// when the answer is the busy refusal, and <c>cache_hit</c> for any other shared answer.
    /// </summary>
    internal static ReadOutcome JoinedOutcome(ReadOutcome? received, CancellationToken callerToken) =>
        callerToken.IsCancellationRequested || received == ReadOutcome.Cancelled ? ReadOutcome.Cancelled
        : received == ReadOutcome.Busy ? ReadOutcome.Busy
        : ReadOutcome.CacheHit;

    /// <summary>The TTL of a band: <c>none</c> and <c>1h</c> 15 s, <c>24h</c> 30 s, <c>7d</c> 60 s, <c>7d+</c> 120 s.</summary>
    internal static TimeSpan TtlOf(string? band) => band switch
    {
        ReadWindowBand.Longer => TimeSpan.FromSeconds(120),
        ReadWindowBand.SevenDays => TimeSpan.FromSeconds(60),
        ReadWindowBand.OneDay => TimeSpan.FromSeconds(30),
        _ => TimeSpan.FromSeconds(15),
    };

    /// <summary>The TTL of an entry of <paramref name="route"/>: the route's own TTL for a whole-fleet read of a route in
    /// <see cref="ReadAdmission.FleetCachedRoutes"/> over a window of 24 hours or more (#6800,
    /// <see cref="ReadAdmission.FleetTtlApplies"/>), else the band's (<see cref="TtlOf(string?)"/>).</summary>
    internal static TimeSpan TtlFor(string route, string? band, bool fleetRead) =>
        ReadAdmission.FleetTtlApplies(route, fleetRead) ? ReadAdmission.FleetCachedRoutes[route] : TtlOf(band);

    /// <summary>The TTL bucket of now, <c>floor(now / TTL)</c>, for a key with no <c>as_of</c>.</summary>
    internal long BucketOf(TimeSpan ttl) =>
        _time.GetUtcNow().ToUnixTimeMilliseconds() / Math.Max(1L, (long)ttl.TotalMilliseconds);

    /// <summary>Whether a route may be cached at all: not a health read (D8, <see cref="ReadAdmission.SkippedRoutes"/>),
    /// not a write, and not <c>analyze_server</c>. A route that passes is cached only when its pair is heavy.</summary>
    internal static bool IsCacheableRoute(string route) =>
        !ReadAdmission.SkippedRoutes.ContainsKey(route)
        && !Mcp.DarlingMcpWritePolicy.IsWrite(route)
        && !string.Equals(route, AnalyzeServer, StringComparison.Ordinal);

    /// <summary>Whether a web request is a write that moves the generation: a 2xx answer to a non-GET <c>/api/*</c>
    /// request, except the compose run, the rule validator and the rule test, which only read.</summary>
    internal static bool IsWebWrite(string method, string path, int statusCode) =>
        statusCode is >= 200 and < 300
        && !HttpMethodsAreReads(method)
        && path.StartsWith("/api/", StringComparison.OrdinalIgnoreCase)
        && !string.Equals(path, "/api/compose/run", StringComparison.OrdinalIgnoreCase)
        && !string.Equals(path, "/api/alerts/validate", StringComparison.OrdinalIgnoreCase)
        && !string.Equals(path, "/api/alerts/test", StringComparison.OrdinalIgnoreCase);

    private static bool HttpMethodsAreReads(string method) =>
        string.Equals(method, "GET", StringComparison.OrdinalIgnoreCase)
        || string.Equals(method, "HEAD", StringComparison.OrdinalIgnoreCase)
        || string.Equals(method, "OPTIONS", StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Whether an answer's text may be stored: a JSON object or array with no <c>status</c> word other than <c>ok</c>
    /// (an envelope such as <c>empty</c>, <c>unavailable</c> or <c>error</c> is never stored), or, when
    /// <paramref name="gcf"/> is on, a GCF wire whose top-level <c>status</c> is absent or <c>ok</c>. An answer carrying
    /// the statement filter's withheld marker is never stored either: the sweep writes the same marker for a value it
    /// stopped judging at its time budget, so a later call may judge it in full.
    /// </summary>
    internal static bool IsCacheableText(string? text, bool gcf)
    {
        /* Round 2, L7: an answer too large to store is refused before it is parsed. A UTF-16 length over the cap is a UTF-8
           size over it too (no character takes fewer UTF-8 bytes than UTF-16 code units). */
        if (string.IsNullOrEmpty(text)
            || text.Length > MaxAnswerBytes
            || Encoding.UTF8.GetByteCount(text) > MaxAnswerBytes
            || text.Contains(PerformanceMonitor.Common.SensitiveStatements.PlaceholderText, StringComparison.Ordinal)
            || text.Contains(PerformanceMonitor.Common.SensitiveStatements.WithheldPlanSentence, StringComparison.Ordinal))
        {
            return false;
        }

        var first = text.AsSpan().TrimStart();
        if (first.Length > 0 && (first[0] == '{' || first[0] == '['))
        {
            try
            {
                using var document = JsonDocument.Parse(text);
                return document.RootElement.ValueKind != JsonValueKind.Object
                    || !document.RootElement.TryGetProperty("status", out var status)
                    || (status.ValueKind == JsonValueKind.String && status.GetString() == "ok");
            }
            catch (JsonException)
            {
                return false;
            }
        }

        if (!gcf)
        {
            return false;
        }

        try
        {
            var decoded = BlackwellSystems.Gcf.Gcf.DecodeGeneric(text);
            return decoded is not BlackwellSystems.Gcf.OrderedMap map
                || !map.TryGetValue("status", out var word)
                || word is string { } s && s == "ok";
        }
#pragma warning disable CA1031 // a wire that does not decode is simply not stored
        catch (Exception)
#pragma warning restore CA1031
        {
            return false;
        }
    }

    /// <summary>The stored text for <paramref name="key"/>, when it is there and its TTL has not run out.</summary>
    internal bool TryGet(string key, out string text)
    {
        if (_memory.TryGetValue(key, out Entry? entry) && entry is not null && _time.GetUtcNow() < entry.ExpiresAt)
        {
            text = entry.Text;
            return true;
        }

        text = string.Empty;
        return false;
    }

    /// <summary>Stores an answer for its TTL. False when it is over 1 MB or does not fit after a compaction.</summary>
    internal bool TryStore(string key, string text, TimeSpan ttl)
    {
        if (text.Length > MaxAnswerBytes || Encoding.UTF8.GetByteCount(text) > MaxAnswerBytes)
        {
            return false;
        }

        var size = 2L * (text.Length + key.Length) + 64;
        if (size > _sizeLimit)
        {
            return false;
        }

        var entry = new Entry(text, _time.GetUtcNow() + ttl);
        var options = new MemoryCacheEntryOptions { Size = size, AbsoluteExpirationRelativeToNow = ttl };
        _memory.Set(key, entry, options);
        if (_memory.TryGetValue(key, out _))
        {
            return true;
        }

        /* Over the cap the cache refuses the entry and compacts in the background; compact now, oldest-used first (at
           least one entry each round), and try again, so a full cache makes room for the newest answer. */
        for (var round = 0; round < 4; round++)
        {
            _memory.Compact(Math.Max(0.25, 1.0 / Math.Max(1, _memory.Count)));
            _memory.Set(key, entry, options);
            if (_memory.TryGetValue(key, out _))
            {
                return true;
            }
        }

        return false;
    }

    /// <summary>
    /// Joins the read in flight for <paramref name="key"/>, or starts it. <paramref name="read"/> runs once, under a token
    /// that is cancelled only when every caller has left; its answer is stored when <paramref name="cacheableText"/>
    /// returns text for it. Await <see cref="ReadFlight{T}.WaitAsync"/> with the caller's own token.
    /// </summary>
    internal ReadFlight<T> Join<T>(string key, TimeSpan ttl, Func<CancellationToken, Task<T>> read, Func<T, string?> cacheableText)
    {
        Flight<T> flight;
        bool lead = false;
        lock (_flightLock)
        {
            if (_flights.TryGetValue(key, out var found) && found is Flight<T> running && running.TryJoin())
            {
                flight = running;
            }
            else
            {
                flight = new Flight<T>(BeforeFlightCancel);
                flight.TryJoin();
                _flights[key] = flight;
                lead = true;
            }
        }

        if (lead)
        {
            /* Not awaited here: RunAsync completes the flight's source on every path and never throws. */
            _ = RunAsync(key, ttl, flight, read, cacheableText);
        }

        return new ReadFlight<T>(flight, lead);
    }

    private async Task RunAsync<T>(string key, TimeSpan ttl, Flight<T> flight, Func<CancellationToken, Task<T>> read, Func<T, string?> cacheableText)
    {
        try
        {
            var answer = await read(flight.Token);
            if (cacheableText(answer) is { } text)
            {
                TryStore(key, text, ttl);
            }

            _flights.TryRemove(new KeyValuePair<string, object>(key, flight));
            flight.Source.TrySetResult(answer);
        }
        catch (OperationCanceledException ex)
        {
            _flights.TryRemove(new KeyValuePair<string, object>(key, flight));
            flight.Source.TrySetCanceled(ex.CancellationToken);
        }
#pragma warning disable CA1031 // handed to every waiter, which rethrows it
        catch (Exception ex)
#pragma warning restore CA1031
        {
            _flights.TryRemove(new KeyValuePair<string, object>(key, flight));
            flight.Source.TrySetException(ex);
        }
    }

    public void Dispose() => _memory.Dispose();

    private sealed record Entry(string Text, DateTimeOffset ExpiresAt);

    /// <summary>A flight's waiter count, whatever its answer type.</summary>
    private interface IFlight
    {
        int Waiters { get; }
    }

#pragma warning disable CA1001 // the source has no timer and a late Leave may still cancel it after the read ends
    internal sealed class Flight<T> : IFlight
#pragma warning restore CA1001
    {
        private readonly Action? _beforeCancel;
        private readonly CancellationTokenSource _cts = new();
        private readonly object _gate = new();
        private int _waiters;

        /* Round 2, L1: set inside the lock by the Leave that takes the waiter count to zero, before the token is cancelled
           outside it, so a caller that arrives in between does not join a read that is about to be cancelled. */
        private bool _closing;

        internal Flight(Action? beforeCancel = null) => _beforeCancel = beforeCancel;

        public int Waiters
        {
            get
            {
                lock (_gate)
                {
                    return _waiters;
                }
            }
        }

        /* RunContinuationsAsynchronously: a waiter's continuation must not run inline on the thread that ends the read. */
        internal TaskCompletionSource<T> Source { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task<T> Task => Source.Task;

        internal CancellationToken Token => _cts.Token;

        internal bool TryJoin()
        {
            lock (_gate)
            {
                if (_closing || _cts.IsCancellationRequested)
                {
                    return false;
                }

                _waiters++;
                return true;
            }
        }

        /// <summary>One waiter left; the last one out cancels the read.</summary>
        internal void Leave()
        {
            bool last;
            lock (_gate)
            {
                last = --_waiters == 0 && !Task.IsCompleted;
                _closing |= last;
            }

            if (last)
            {
                _beforeCancel?.Invoke();
                _cts.Cancel();
            }
        }
    }
}

/// <summary>One caller's place in a <see cref="ReadResultCache.Join{T}"/> flight.</summary>
internal readonly struct ReadFlight<T>
{
    private readonly ReadResultCache.Flight<T> _flight;

    internal ReadFlight(ReadResultCache.Flight<T> flight, bool isLead)
    {
        _flight = flight;
        IsLead = isLead;
    }

    /// <summary>True for the caller that started the read: it records the read's real outcome; a joined caller records
    /// only its own (<see cref="ReadResultCache.JoinedOutcome"/>).</summary>
    internal bool IsLead { get; }

    /// <summary>Waits for the shared answer with the caller's own token. A caller that leaves stops waiting at once,
    /// except the lead, which stays until the read ends (the read records through the lead's <see cref="ReadScope"/> and
    /// uses its <c>RequestServices</c>) and then throws.</summary>
    internal async Task<T> WaitAsync(CancellationToken cancellationToken)
    {
        var flight = _flight;
        try
        {
            return await flight.Task.WaitAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            /* Left here, once, rather than from a token registration: a registration disposed while the token is cancelling
               can be dropped before its callback runs, and a missed leave would keep the read alive for nobody. */
            flight.Leave();
            if (!IsLead)
            {
                throw;
            }

            try
            {
                await flight.Task;
            }
#pragma warning disable CA1031 // the lead has left: the read's own end is recorded by the read
            catch (Exception)
#pragma warning restore CA1031
            {
            }

            throw;
        }
    }
}
