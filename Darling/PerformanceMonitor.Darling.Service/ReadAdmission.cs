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
using System.Linq;
using System.Threading;
using System.Threading.RateLimiting;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Npgsql;
using PerformanceMonitor.Darling.Storage;

namespace PerformanceMonitor.Darling.Service;

/// <summary>
/// Read admission (#5506 part 3): at most <c>reads.maxHeavyReads</c> long reads run at once across the MCP and web hosts
/// of this process. On 2026-10-02 one MCP client held the store's disk at its limit for about 5 minutes with a burst of
/// 7-day reads; this is the cap on that.
///
/// <para><b>Which reads wait.</b> A read is long (heavy) when the 7-day p90 of its <c>total_ms</c> for its (surface,
/// route, window band) is 2,000 ms or more, from the <c>ok</c> and fallback outcomes only. The table is built from the
/// stored histogram plus this process's unflushed samples, at boot and at every hourly flush; a failed build keeps the
/// last table. A pair with no history takes the band prior: <c>7d</c> and <c>7d+</c> are heavy, the rest light. Light
/// reads never touch the limiter.</para>
///
/// <para><b>Queue and refusal.</b> Heavy reads take a slot of one <see cref="ConcurrencyLimiter"/>, oldest first. Up to
/// <see cref="QueueLimit"/> more wait, each for at most <see cref="DefaultWaitCap"/>. A read is refused as busy when the
/// queue is full, when the expected wait (the reads running and queued, times the largest p90 among them, over the limit)
/// already passes the cap, or when the cap passes while it waits.</para>
///
/// <para><b>Nesting.</b> A flow that holds a slot takes no second one: a read inside a read (a composed panel run from a
/// tool, an analysis reading through another reader) passes straight through, so a limit of 1 cannot deadlock. The
/// marker is a mutable object in an <see cref="AsyncLocal{T}"/>, set synchronously in the caller's flow, so the granted
/// flag set after the await is seen by every child of that flow (the <see cref="ReadScope"/> pattern).</para>
///
/// <para>The recorded elapsed time of an admitted read starts after admission, and admission never adds to
/// <see cref="ReadScope.WaitMs"/>: queue time is recorded only as the read's one <c>queue_wait</c> sample.</para>
/// </summary>
public sealed class ReadAdmission
{
    /// <summary>The default of <c>reads.maxHeavyReads</c>.</summary>
    public const int DefaultMaxHeavyReads = 2;

    /// <summary>How many heavy reads may wait for a slot.</summary>
    internal const int QueueLimit = 16;

    /// <summary>How long a heavy read may wait for a slot.</summary>
    internal static readonly TimeSpan DefaultWaitCap = TimeSpan.FromSeconds(45);

    /// <summary>The share of the hourly store sweep's budget the table rebuild may use (#5506 part 3). The 7-day scan
    /// is the one slow step on the flush's connection; its own slice keeps a long scan from taking the budget the
    /// read-latency flush needs.</summary>
    internal static readonly TimeSpan RebuildSliceBudget = TimeSpan.FromSeconds(60);

    /// <summary>The p90 above which a (surface, route, band) is heavy.</summary>
    internal const long HeavyThresholdMs = 2_000;

    /// <summary>The smallest retry-after a refusal names.</summary>
    internal const int MinRetryAfterSeconds = 5;

    /// <summary>The p90 window the table is built from.</summary>
    internal const int ClassificationDays = 7;

    /// <summary>The stored outcomes the table is built from: completed reads only. A timeout or an error is not a
    /// measure of the read's cost, and the admission outcomes are not reads at all.</summary>
    internal static readonly string[] ClassifiedOutcomes = ["ok", "fallback_raw", "gate_failed"];

    /// <summary>The status word of a refused MCP read.</summary>
    internal const string BusyStatus = "busy";

    /// <summary>
    /// D8: the reads that skip admission, each with its reason. They are the health reads, the ones an operator opens to
    /// find out WHY reads are slow or refused, so they must answer while the slots are full; each is a small read of the
    /// service's own tables. The web mirror of each (<c>/api/read/&lt;name&gt;</c>) uses the same name. Everything else
    /// that opens a <see cref="ReadScope"/> is admitted, and the learned p90 decides whether it waits.
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, string> SkippedRoutes = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["get_read_latency"] = "health read: the read-latency and admission counters themselves",
        ["get_slow_reads"] = "health read: the slow-read record, the next stop after get_read_latency",
        ["get_collection_health"] = "health read: whether collection is running",
        ["get_collection_log"] = "health read: the collection log behind collection health",
        ["get_store_metrics"] = "health read: the store's own size and pressure",
        ["get_store_log"] = "health read: the store's own log",
        ["list_servers"] = "health read: the registry every other read names a server from",
    };

    /// <summary>
    /// The routes that are heavy from their first call, whatever the learned p90 or the window band says (#6236): each is a
    /// read whose cost does not follow a time window, so neither the stored histogram nor the band prior can classify it.
    /// The investigation export reads up to 2,000 cases through the store, so it takes a heavy slot from the start.
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, string> AlwaysHeavyRoutes = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        [DarlingWebEndpoints.InvestigationExportRoute] = "reads up to 2,000 cases with their reports, briefs and feedback into one file",
    };

    /// <summary>
    /// #6800: the routes whose whole-fleet form (no server named) is cached for the TTL in the table when the window asked for is at
    /// least <see cref="FleetMinWindowHours"/> hours (<see cref="FleetTtlApplies"/>), not by the p90 table or the band prior. The fleet <c>get_resource_use_by_tag</c> reads its
    /// Query Store section over the whole window and took about 61 s cold on a 42-server store (7.5 s warm). With no history its
    /// 24-hour band is light, so it was never kept, and once the learned p90 made the pair heavy it was kept for the band's 30 s in a
    /// 30 s wall-clock bucket that had passed before a 61 s read could store anything. Either way every visit, every user and every
    /// page picker ran it again. The one-server form takes about
    /// 1.3 s and is left to the band as before, and so is a window under 24 hours. The floor is the length of the window asked for, not
    /// its band: the <c>24h</c> band starts just above 1 hour, so a band gate would also hold the page's Past 4 hours preset
    /// (<c>?hours=4</c>) for 5 minutes. 24 hours is where the Query Store section starts (the tool reads it only from
    /// <c>hours_back</c> 24) and where the 61 s cold cost was measured, so a shorter window neither pays that cost nor gets the long TTL.
    /// A read anchored with <c>as_of</c> is measured by the same <c>hours</c> span.
    /// <para>The 5 minutes, and what it costs: the answer has six sections. Query Store and deadlocks come from 5-minute collectors,
    /// but the plan cache, procedures, blocked process report and blocking snapshot sections read 1-minute collectors at a 24-hour
    /// window (the hourly rollup serves the plan cache and procedures only past the raw tier, from the 7-day band up). So a stored
    /// answer can be up to five collection cycles old for those four sections: a blocked process report or a deadlock that landed a
    /// minute ago can be missing from the fleet page for up to 5 minutes, where the band TTL made it 30 s. What the page shows is a
    /// 24-hour total by tag, which moves by under 1/288th in 5 minutes, and a live blocking or deadlock event has its own reads and
    /// alerts, which this does not touch. The entry is held for its TTL from the moment it is stored, not for what is left of a
    /// wall-clock bucket (its key carries no bucket, <c>ReadCacheKey</c> <c>heldFromStore</c>, and the entry expires at store time
    /// plus the TTL, as an <c>as_of</c> entry does): with a bucket key a cold read of 61 to 118 s started late in a 5-minute bucket
    /// stores an entry nothing can reach (20 to 39% of cold reads) and the rest live about 55 to 95 s. A TTL of 1 minute would match
    /// the fastest collector, but a reload or a second user more than a minute after the read finished would pay the cold read again,
    /// and the page itself re-renders every 60 s, so the entry would expire about as each poll arrives. A write (the tag map, a rule)
    /// moves the cache's generation and drops the entry at once.</para>
    /// </summary>
    internal static readonly IReadOnlyDictionary<string, TimeSpan> FleetCachedRoutes = new Dictionary<string, TimeSpan>(StringComparer.Ordinal)
    {
        ["get_resource_use_by_tag"] = TimeSpan.FromMinutes(5),
    };

    /// <summary>The shortest window, in hours, whose whole-fleet read takes the route's own TTL (#6800). A window under it keeps the
    /// band's TTL and the band's rule for whether it is cached at all. It is where the by-tag rollup's Query Store section starts
    /// (<c>QueryStoreIntervalWide.TopMinWindow</c>, the field <c>DarlingQueryTagRollupReader.QueryStoreMinWindow</c> reads), so a retune of
    /// that threshold moves this floor with it.</summary>
    internal static readonly double FleetMinWindowHours = PerformanceMonitor.Darling.Storage.QueryStoreIntervalWide.TopMinWindow.TotalHours;

    /// <summary>Whether the route's own TTL in <see cref="FleetCachedRoutes"/> applies to a read: <paramref name="fleetRead"/> (from
    /// <see cref="IsFleetRead"/>, which already holds the 24-hour floor) on a route in the table.</summary>
    internal static bool FleetTtlApplies(string route, bool fleetRead) =>
        fleetRead && FleetCachedRoutes.ContainsKey(route);

    /// <summary>Whether a read of <paramref name="route"/> is the whole-fleet form of a route in <see cref="FleetCachedRoutes"/> over a
    /// window of at least <see cref="FleetMinWindowHours"/>: no server named (null, empty or blank), and <paramref name="hours"/>, the
    /// window the read asks for or its declared default (<see cref="ReadWindowBand.HoursOf"/>), at least <see cref="FleetMinWindowHours"/>. No window, or one that is
    /// not a number, is not long enough.</summary>
    internal static bool IsFleetRead(string route, string? server, double? hours) =>
        FleetCachedRoutes.ContainsKey(route) && string.IsNullOrWhiteSpace(server) && hours >= FleetMinWindowHours;

    /// <summary>Whether a tool or web read goes through admission: not a health read (<see cref="SkippedRoutes"/>) and not
    /// an MCP write tool (writes never wait).</summary>
    internal static bool IsAdmitted(string name) =>
        !SkippedRoutes.ContainsKey(name) && !Mcp.DarlingMcpWritePolicy.IsWrite(name);

    /// <summary>The MCP envelope of a refusal: <c>busy</c>, the sentence, and the retry-after and counts as hints.</summary>
    internal static string McpBusyText(ReadAdmissionRefusal refusal) =>
        PerformanceMonitor.Common.McpHelpers.Status(BusyStatus, refusal.Sentence,
            new { retry_after_seconds = refusal.RetryAfterSeconds, running = refusal.Running, queued = refusal.Queued });

    private static readonly AsyncLocal<Marker?> s_marker = new();

    private readonly TimeProvider _time;
    private readonly TimeSpan _waitCap;
    private readonly ILogger? _logger;
    private readonly object _configureLock = new();
    private volatile LimiterState _state;
    private volatile IReadOnlyDictionary<(ReadSurface Surface, string Route, string Band), long> _p90 =
        new Dictionary<(ReadSurface, string, string), long>();
    private readonly ConcurrentDictionary<long, long> _expectedMs = new();
    private readonly ConcurrentDictionary<(ReadSurface Surface, string Route), RouteCounters> _routes = new();
    private long _nextId;
    private int _running;
    private int _queued;

    public ReadAdmission()
        : this(TimeProvider.System, DefaultWaitCap, null)
    {
    }

    public ReadAdmission(ILogger<ReadAdmission>? logger)
        : this(TimeProvider.System, DefaultWaitCap, logger)
    {
    }

    internal ReadAdmission(TimeProvider time, TimeSpan waitCap, ILogger? logger, int maxHeavyReads = DefaultMaxHeavyReads)
    {
        _time = time ?? throw new ArgumentNullException(nameof(time));
        _waitCap = waitCap;
        _logger = logger;
        _state = LimiterState.Create(maxHeavyReads);
        Cache = new ReadResultCache(time);
    }

    /// <summary>#5506 part 3: the process's read cache. It caches the pairs this classifies as heavy
    /// and, since #6800, a whole-fleet read of a route in <see cref="FleetCachedRoutes"/> over a long window (<see cref="IsCached"/>),
    /// on the same clock.</summary>
    internal ReadResultCache Cache { get; }

    /// <summary>Whether an answer of (<paramref name="surface"/>, <paramref name="route"/>, <paramref name="band"/>) is
    /// served from and stored in <see cref="Cache"/>: a cacheable route (<see cref="ReadResultCache.IsCacheableRoute"/>)
    /// whose pair is heavy, or a whole-fleet read of a route in <see cref="FleetCachedRoutes"/>
    /// over a window of <see cref="FleetMinWindowHours"/> or more (<paramref name="fleetRead"/>, from <see cref="IsFleetRead"/>,
    /// <see cref="FleetTtlApplies"/>). It does not depend
    /// on the limit: with admission off, heavy answers are still cached. Caching a fleet read takes no slot: whether
    /// the read waits for one stays with <see cref="IsHeavy"/> (<see cref="AdmitAsync"/>).</summary>
    internal bool IsCached(ReadSurface surface, string route, string? band, bool fleetRead = false) =>
        Cache.Enabled
        && !string.IsNullOrEmpty(route)
        && ReadResultCache.IsCacheableRoute(route)
        && (FleetTtlApplies(route, fleetRead)
            || IsHeavy(surface, route, string.IsNullOrEmpty(band) ? ReadWindowBand.None : band, out _));

    /// <summary>The limit in force (0 = off).</summary>
    internal int Limit => _state.Limit;

    /// <summary>Sets the limit from <c>reads.maxHeavyReads</c>. Each host calls it when it starts; both read the same
    /// file, so the second call is a no-op. A negative value is treated as 0 (off). A changed limit swaps in a new
    /// limiter; reads holding a slot of the old one release it there.</summary>
    internal void Configure(int maxHeavyReads)
    {
        var limit = Math.Max(0, maxHeavyReads);
        lock (_configureLock)
        {
            if (_state.Limit != limit)
            {
                _state = LimiterState.Create(limit);
            }
        }
    }

    /// <summary>Whether a read of (<paramref name="surface"/>, <paramref name="route"/>, <paramref name="band"/>) is
    /// heavy, and the p90 it is expected to take (the threshold for a pair known only by its band prior).</summary>
    internal bool IsHeavy(ReadSurface surface, string route, string band, out long expectedMs)
    {
        if (AlwaysHeavyRoutes.ContainsKey(route))
        {
            expectedMs = HeavyThresholdMs;
            return true;
        }

        if (_p90.TryGetValue((surface, route, band), out var p90))
        {
            expectedMs = p90;
            /* #5506 part 3: the table holds a bucket UPPER bound (capped at the largest max seen), so a real p90 anywhere
               in (1,500, 2,000] reads as 2,000. Strictly above the threshold is the only value a real p90 over it
               produces. */
            return p90 > HeavyThresholdMs;
        }

        expectedMs = HeavyThresholdMs;
        return IsHeavyByBand(band);
    }

    /// <summary>The prior for a pair with no history: a week or more of data is heavy, anything shorter light.</summary>
    internal static bool IsHeavyByBand(string? band) =>
        string.Equals(band, ReadWindowBand.SevenDays, StringComparison.Ordinal)
        || string.Equals(band, ReadWindowBand.Longer, StringComparison.Ordinal);

    /// <summary>
    /// Admits one read. Call it after <see cref="ReadScope.Open"/> and the band, and dispose the ticket when the read
    /// ends. A light read, a nested read, or a limit of 0 passes at once. A heavy read takes a slot, waits for one, or is
    /// refused (<see cref="ReadAdmissionTicket.Refusal"/>); a caller that leaves while it waits gets
    /// <see cref="ReadAdmissionTicket.CallerLeft"/>. Never throws for a refusal or a cancellation.
    /// </summary>
    internal Task<ReadAdmissionTicket> AdmitAsync(ReadSurface surface, string route, string? band, CancellationToken cancellationToken)
    {
        var state = _state;
        if (state.Limiter is null || string.IsNullOrEmpty(route))
        {
            return Task.FromResult(ReadAdmissionTicket.Passed);
        }

        var marker = s_marker.Value;
        if (marker is { Held: true })
        {
            return Task.FromResult(ReadAdmissionTicket.Passed);
        }

        if (!IsHeavy(surface, route, string.IsNullOrEmpty(band) ? ReadWindowBand.None : band, out var expectedMs))
        {
            return Task.FromResult(ReadAdmissionTicket.Passed);
        }

        /* Set here, synchronously, so the marker lives in the CALLER's flow: an assignment inside the async method below
           would be undone when it returns. */
        if (marker is null)
        {
            marker = new Marker();
            s_marker.Value = marker;
        }

        return AdmitHeavyAsync(state, surface, route, expectedMs, marker, cancellationToken);
    }

    private async Task<ReadAdmissionTicket> AdmitHeavyAsync(
        LimiterState state, ReadSurface surface, string route, long expectedMs, Marker marker, CancellationToken cancellationToken)
    {
        var counters = _routes.GetOrAdd((surface, route), static _ => new RouteCounters());
        var id = Interlocked.Increment(ref _nextId);

        var immediate = state.Limiter!.AttemptAcquire(1);
        if (immediate.IsAcquired)
        {
            return Grant(immediate, id, expectedMs, marker, counters, queueMs: null);
        }

        immediate.Dispose();

        var estimateMs = ExpectedWaitMs(state.Limit);
        if (Volatile.Read(ref _queued) >= QueueLimit || estimateMs > _waitCap.TotalMilliseconds)
        {
            Interlocked.Increment(ref counters.Rejected);
            return ReadAdmissionTicket.Refused(Refusal(state.Limit, estimateMs), queueMs: 0);
        }

        /* The cap's timer starts before the read counts as queued, so whoever sees it queued sees its timer running. */
        using var cap = new CancellationTokenSource(_waitCap, _time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, cap.Token);
        var started = _time.GetTimestamp();
        _expectedMs[id] = expectedMs;
        Interlocked.Increment(ref _queued);
        RateLimitLease lease;
        try
        {
            lease = await state.Limiter.AcquireAsync(1, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            Interlocked.Decrement(ref _queued);
            _expectedMs.TryRemove(id, out _);
            var waitedMs = ElapsedMs(started);
            if (cancellationToken.IsCancellationRequested)
            {
                return ReadAdmissionTicket.Left(waitedMs);
            }

            /* The cap passed: refused with a fresh estimate of the wait. */
            Interlocked.Increment(ref counters.Rejected);
            return ReadAdmissionTicket.Refused(Refusal(state.Limit, ExpectedWaitMs(state.Limit)), waitedMs);
        }

        Interlocked.Decrement(ref _queued);
        var queueMs = ElapsedMs(started);
        if (!lease.IsAcquired)
        {
            /* The limiter's own queue was full: a race with another read past the check above. */
            lease.Dispose();
            _expectedMs.TryRemove(id, out _);
            Interlocked.Increment(ref counters.Rejected);
            return ReadAdmissionTicket.Refused(Refusal(state.Limit, ExpectedWaitMs(state.Limit)), queueMs);
        }

        Interlocked.Increment(ref counters.Queued);
        return Grant(lease, id, expectedMs, marker, counters, queueMs);
    }

    private ReadAdmissionTicket Grant(RateLimitLease lease, long id, long expectedMs, Marker marker, RouteCounters counters, long? queueMs)
    {
        _expectedMs[id] = expectedMs;
        Interlocked.Increment(ref _running);
        Interlocked.Increment(ref counters.Admitted);
        marker.Held = true;
        return ReadAdmissionTicket.Granted(queueMs, () =>
        {
            marker.Held = false;
            _expectedMs.TryRemove(id, out _);
            Interlocked.Decrement(ref _running);
            lease.Dispose();
        });
    }

    /// <summary>D3: the reads running and queued, times the largest p90 among them, over the limit.</summary>
    private double ExpectedWaitMs(int limit)
    {
        var ahead = Volatile.Read(ref _running) + Volatile.Read(ref _queued);
        if (ahead <= 0 || limit <= 0)
        {
            return 0;
        }

        var largest = _expectedMs.Values.DefaultIfEmpty(HeavyThresholdMs).Max();
        return (double)ahead * largest / limit;
    }

    private ReadAdmissionRefusal Refusal(int limit, double estimateMs)
    {
        var retryAfter = Math.Max(MinRetryAfterSeconds, (int)Math.Ceiling(estimateMs / 1000.0));
        var running = Volatile.Read(ref _running);
        var queued = Volatile.Read(ref _queued);
        return new ReadAdmissionRefusal(
            $"The service is running as many long reads as it allows ({limit}), and {queued} more are waiting. Try again in about {retryAfter} seconds.",
            retryAfter, running, queued);
    }

    private long ElapsedMs(long started) => (long)_time.GetElapsedTime(started).TotalMilliseconds;

    /// <summary>Counts a cache hit for the route (lane 2 of #5506 part 3 calls it).</summary>
    internal void NoteCacheHit(ReadSurface surface, string route) =>
        Interlocked.Increment(ref _routes.GetOrAdd((surface, route), static _ => new RouteCounters()).CacheHits);

    /// <summary>Counts a cache miss for the route (lane 2 of #5506 part 3 calls it).</summary>
    internal void NoteCacheMiss(ReadSurface surface, string route) =>
        Interlocked.Increment(ref _routes.GetOrAdd((surface, route), static _ => new RouteCounters()).CacheMisses);

    /// <summary>This process's live admission counts since it started: the limit, the reads running and waiting now,
    /// and per route the heavy reads admitted, queued and refused (and the cache hits and misses).</summary>
    public ReadAdmissionStats Stats()
    {
        var routes = _routes
            .Select(pair => new ReadAdmissionRouteStats(
                pair.Key.Surface.ToString().ToLowerInvariant(), pair.Key.Route,
                Interlocked.Read(ref pair.Value.Admitted), Interlocked.Read(ref pair.Value.Queued),
                Interlocked.Read(ref pair.Value.Rejected), Interlocked.Read(ref pair.Value.CacheHits),
                Interlocked.Read(ref pair.Value.CacheMisses)))
            .OrderBy(r => r.Surface, StringComparer.Ordinal)
            .ThenBy(r => r.Route, StringComparer.Ordinal)
            .ToList();
        return new ReadAdmissionStats(_state.Limit, Volatile.Read(ref _running), Volatile.Read(ref _queued), routes);
    }

    /// <summary>Replaces the classification table (a test, or <see cref="RefreshAsync"/>).</summary>
    internal void SetTable(IReadOnlyDictionary<(ReadSurface Surface, string Route, string Band), long> p90) => _p90 = p90;

    /// <summary>Builds the p90 table from per-row histograms: rows of one (surface, route, band) are summed bucket by
    /// bucket, and the p90 is <see cref="ReadLatencyPercentiles.FromBuckets"/> capped at the largest max seen. A row
    /// whose array does not match the fixed bounds is skipped.</summary>
    internal static Dictionary<(ReadSurface Surface, string Route, string Band), long> BuildTable(
        IEnumerable<(ReadSurface Surface, string Route, string Band, long[] Counts, long MaxMs)> rows)
    {
        var bounds = ReadLatencyAccumulator.BucketUpperBoundsMs;
        var sums = new Dictionary<(ReadSurface, string, string), (long[] Counts, long MaxMs)>();
        foreach (var row in rows)
        {
            if (row.Counts is null || row.Counts.Length != bounds.Length + 1 || string.IsNullOrEmpty(row.Route))
            {
                continue;
            }

            var key = (row.Surface, row.Route, string.IsNullOrEmpty(row.Band) ? ReadWindowBand.None : row.Band);
            if (!sums.TryGetValue(key, out var sum))
            {
                sum = (new long[bounds.Length + 1], 0);
            }

            for (var i = 0; i < row.Counts.Length; i++)
            {
                sum.Counts[i] += row.Counts[i];
            }

            sums[key] = (sum.Counts, Math.Max(sum.MaxMs, row.MaxMs));
        }

        var table = new Dictionary<(ReadSurface, string, string), long>();
        foreach (var (key, sum) in sums)
        {
            if (ReadLatencyPercentiles.FromBuckets(sum.Counts, bounds, 0.9, sum.MaxMs) is { } p90)
            {
                table[key] = p90.UpperBoundMs;
            }
        }

        return table;
    }

    /// <summary>
    /// Rebuilds the table from the last <see cref="ClassificationDays"/> of <c>collect.read_latency</c> plus
    /// <paramref name="live"/>'s unflushed samples. Call it BEFORE the flush drains them, or they are counted in
    /// neither. Never throws: a failed read keeps the last table and logs a warning.
    /// </summary>
    internal async Task<bool> RefreshAsync(NpgsqlConnection connection, ReadLatencyAccumulator? live, CancellationToken cancellationToken)
    {
        try
        {
            var rows = new List<(ReadSurface, string, string, long[], long)>();
            await using (var command = new NpgsqlCommand(ClassificationSql, connection))
            {
                command.CommandTimeout = ReadLatencyAccumulator.FlushTimeoutSeconds;
                command.Parameters.AddWithValue(_time.GetUtcNow().UtcDateTime.AddDays(-ClassificationDays));
                command.Parameters.AddWithValue(ClassifiedOutcomes);
                await using var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
                while (await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    if (Enum.TryParse<ReadSurface>(reader.GetString(0), ignoreCase: true, out var surface))
                    {
                        rows.Add((surface, reader.GetString(1), reader.GetString(2), (long[])reader.GetValue(3), reader.GetInt64(4)));
                    }
                }
            }

            if (live is not null)
            {
                foreach (var row in live.Snapshot())
                {
                    if (row.Outcome is ReadOutcome.Ok or ReadOutcome.FallbackRaw or ReadOutcome.GateFailed)
                    {
                        rows.Add((row.Surface, row.Route, row.WindowBand, row.BucketCounts, row.MaxMs));
                    }
                }
            }

            _p90 = BuildTable(rows);
            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            _logger?.LogWarning("The long-read table was not rebuilt, so the last one stays in force: {Message}", ex.Message);
            return false;
        }
    }

    /// <summary>The hourly rebuild under a slice of <paramref name="budget"/> of its own (#5506 part 3). The slice ending,
    /// or a fault <paramref name="rebuild"/> lets through, keeps the last table and returns, so the flush that follows
    /// still runs; only <paramref name="budget"/> itself ending (the sweep's, or shutdown) propagates. Returns whether the
    /// rebuild finished. A canceled slice can leave the caller's connection broken: the caller reopens it before the flush.</summary>
    internal static async Task<bool> RunRebuildSliceAsync(
        Func<CancellationToken, Task<bool>> rebuild, TimeSpan slice, ILogger? logger, CancellationToken budget)
    {
        using var sliceBudget = CancellationTokenSource.CreateLinkedTokenSource(budget);
        sliceBudget.CancelAfter(slice);
        try
        {
            return await rebuild(sliceBudget.Token).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException
            || (sliceBudget.IsCancellationRequested && !budget.IsCancellationRequested))
        {
            logger?.LogWarning("The long-read table rebuild did not finish, so the last table stays in force: {Message}", ex.Message);
            return false;
        }
    }

    /// <summary>Boot: <see cref="RefreshAsync(NpgsqlConnection, ReadLatencyAccumulator?, CancellationToken)"/> on a
    /// connection of its own. Never throws.</summary>
    internal async Task RefreshAsync(NpgsqlDataSource postgres, ReadLatencyAccumulator? live, CancellationToken cancellationToken)
    {
        try
        {
            await using var connection = await postgres.OpenConnectionAsync(cancellationToken).ConfigureAwait(false);
            await RefreshAsync(connection, live, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException || !cancellationToken.IsCancellationRequested)
        {
            _logger?.LogWarning("The long-read table was not built at start, so the window-size rule applies: {Message}", ex.Message);
        }
    }

    internal const string ClassificationSql = @"
SELECT surface, route, window_band, bucket_counts, max_ms
FROM collect.read_latency
WHERE metric_time >= $1
AND   outcome = ANY($2)";

    private sealed class Marker
    {
        public volatile bool Held;
    }

    private sealed class RouteCounters
    {
        public long Admitted;
        public long Queued;
        public long Rejected;
        public long CacheHits;
        public long CacheMisses;
    }

    private sealed class LimiterState
    {
        public int Limit { get; private init; }

        public ConcurrencyLimiter? Limiter { get; private init; }

        public static LimiterState Create(int limit) => new()
        {
            Limit = limit,
            Limiter = limit <= 0
                ? null
                : new ConcurrencyLimiter(new ConcurrencyLimiterOptions
                {
                    PermitLimit = limit,
                    QueueLimit = QueueLimit,
                    QueueProcessingOrder = QueueProcessingOrder.OldestFirst,
                }),
        };
    }
}

/// <summary>The answer of <see cref="ReadAdmission.AdmitAsync"/>. Dispose it when the read ends; that frees the slot.</summary>
internal sealed class ReadAdmissionTicket : IDisposable
{
    /// <summary>A read that took no slot: light, nested, or with the limit off.</summary>
    internal static readonly ReadAdmissionTicket Passed = new(null, null, false, null);

    private Action? _release;

    private ReadAdmissionTicket(long? queueMs, ReadAdmissionRefusal? refusal, bool callerLeft, Action? release)
    {
        QueueMs = queueMs;
        Refusal = refusal;
        CallerLeft = callerLeft;
        _release = release;
    }

    /// <summary>The time the read waited for its slot; null when it did not wait. A queued read records it as one
    /// <c>queue_wait</c> sample; a refused or departed read records it as the elapsed time of its <c>busy</c> or
    /// <c>cancelled</c> sample.</summary>
    internal long? QueueMs { get; }

    /// <summary>Set when the read was refused: answer busy (MCP) or 429 (web) and do not run it.</summary>
    internal ReadAdmissionRefusal? Refusal { get; }

    /// <summary>Set when the caller left while the read waited: record <c>cancelled</c> and end the request.</summary>
    internal bool CallerLeft { get; }

    /// <summary>Whether the read may run.</summary>
    internal bool MayRun => Refusal is null && !CallerLeft;

    internal static ReadAdmissionTicket Granted(long? queueMs, Action release) => new(queueMs, null, false, release);

    internal static ReadAdmissionTicket Refused(ReadAdmissionRefusal refusal, long queueMs) => new(queueMs, refusal, false, null);

    internal static ReadAdmissionTicket Left(long queueMs) => new(queueMs, null, true, null);

    public void Dispose() => Interlocked.Exchange(ref _release, null)?.Invoke();
}

/// <summary>The live admission counts of this process (#5506 part 3), for <c>get_read_latency</c>'s <c>admission</c>
/// object. Since the process started; never stored.</summary>
public sealed record ReadAdmissionStats(int Limit, int Running, int Queued, IReadOnlyList<ReadAdmissionRouteStats> Routes);

/// <summary>One route's live admission counts: heavy reads admitted (with or without a wait), queued, and refused, and
/// the cache's hits and misses (zero until the read cache exists).</summary>
public sealed record ReadAdmissionRouteStats(string Surface, string Route, long Admitted, long Queued, long Rejected, long CacheHits, long CacheMisses);
