/*
 * Copyright (c) 2026 Erik Darling, Darling Data LLC
 *
 * This file is part of the SQL Server Performance Monitor.
 *
 * Licensed under the MIT License. See LICENSE file in the project root for full license information.
 */

using System;
using System.Collections.Concurrent;

namespace PerformanceMonitor.Collectors;

/// <summary>
/// Calculates delta values for cumulative metrics between collection intervals, caching previous
/// values in memory. The shared implementation of <see cref="ICollectorDeltaCalculator"/> both
/// SKUs run (extracted verbatim from Lite's DeltaCalculator, which now derives from this), so the
/// baseline / counter-reset / gap-policy semantics can never drift between portable Lite and the
/// Darling service. Hosts that survive restarts by re-seeding baselines from their own store call
/// the protected <see cref="Seed"/> (Lite: DuckDB; Darling: Postgres).
/// </summary>
public class CollectorDeltaCalculator : ICollectorDeltaCalculator
{
    /// <summary>
    /// The gap past which a cached baseline is treated as too stale to subtract from, shared by every
    /// delta call site in this assembly so the policy cannot drift collector by collector.
    ///
    /// <para>One hour, chosen from measurement rather than intuition. The previous value — 300 s,
    /// hard-coded at all 41 call sites — sat almost exactly on the fleet's median sweep gap, so it
    /// fired during ordinary operation instead of after the restarts it was written for. Measured over
    /// 99,717 consecutive perfmon gaps across 52 production servers and 7 days: p50 <b>299 s</b>,
    /// p90 580 s, p99 830 s, p99.9 1,190 s, max 2,514 s. The share of ordinary gaps each candidate
    /// rejects: <b>300 s → 50.0%</b>, 600 s → 8.3%, 900 s → 0.6%, 1,800 s → 0.0%, 3,600 s → 0.0%.
    /// Half of every delta collector's output was a fabricated zero (#2233, #2234).</para>
    ///
    /// <para>An hour clears the observed maximum with room to spare while still catching what the
    /// guard is actually for: a server unreachable for hours, or a baseline restored from a store row
    /// old enough that attributing its whole accrual to one interval would read as a spike. Note the
    /// direction of the harm this replaces — a rejected gap returns 0, and a 0 is indistinguishable
    /// from a genuinely idle interval, so the guard did not merely lose data, it invented quiet.</para>
    /// </summary>
    public const int DefaultMaxGapSeconds = 3600;

    /// <summary>
    /// How far back a restart re-seed reads when restoring baselines from a host's own store.
    ///
    /// <para>Fifteen minutes, and since <see cref="DefaultMaxGapSeconds"/> became an hour this window
    /// — not the gap policy — is what bounds restart recovery. It used to be the other way round: at a
    /// 300 s policy every seed row older than five minutes was rejected on arrival, so most of this
    /// window was work whose result was thrown away. Now every row it returns can produce a real
    /// delta, which is the point.</para>
    ///
    /// <para>Left at fifteen minutes deliberately. It sits well inside one store chunk, which is the
    /// property that matters: it lets TimescaleDB exclude the rest of a multi-hundred-GB hypertable
    /// rather than scan every chunk on a 30-second command timeout — the field failure in #1772.
    /// Widening it to chase the hour-long policy would trade that back.</para>
    /// </summary>
    public static readonly TimeSpan SeedLookback = TimeSpan.FromMinutes(15);

    /// <summary>
    /// The cutoff a seed read binds to its <c>collection_time &gt;= $1</c> bound, defined once so the
    /// two hosts cannot drift. Naive UTC by the product-wide storage convention — Kind is stripped
    /// deliberately, because Npgsql 6+ rejects a <c>Kind=Utc</c> value against a <c>timestamp</c>
    /// column, and DuckDB stores the same naive-UTC values.
    /// </summary>
    public static DateTime SeedCutoff()
        => DateTime.SpecifyKind(DateTime.UtcNow - SeedLookback, DateTimeKind.Unspecified);

    /// <summary>
    /// Cache structure: serverId -> collectorName -> key -> (previousValue, timestamp)
    /// </summary>
    private readonly ConcurrentDictionary<int, ConcurrentDictionary<string, ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>>> _cache = new();

    /// <summary>
    /// When this (server, collector) pair was last looked at, and the look before that:
    /// serverId -> collectorName -> (current pass, previous pass).
    ///
    /// <para>Needed by <see cref="CalculateDeltaWithSeriesAge"/> to answer "did this counter series begin
    /// since we last looked?" for a key that has no history of its own. The PREVIOUS pass is the useful
    /// one, and it is tracked separately from the per-key timestamps because a brand-new key has none.</para>
    ///
    /// <para>Advanced only when the collection time actually CHANGES, which is what makes it stable
    /// across the many rows of one pass: a collector calls in per row, and if the first row rolled the
    /// window forward every later row in the same pass would compare against its own pass and see a zero
    /// gap — quietly disabling the credit for every row but the first.</para>
    /// </summary>
    private readonly ConcurrentDictionary<int, ConcurrentDictionary<string, (DateTime Current, DateTime? Previous)>> _passes = new();

    /// <summary>
    /// Rolls the (server, collector) pass window forward when <paramref name="collectionTime"/> is a new
    /// pass, and returns the previous pass — the boundary a series age is measured against.
    /// </summary>
    private DateTime? PreviousPass(int serverId, string collectorName, DateTime? collectionTime)
    {
        if (!collectionTime.HasValue)
        {
            return null;
        }

        var byCollector = _passes.GetOrAdd(serverId, _ => new ConcurrentDictionary<string, (DateTime, DateTime?)>());
        var updated = byCollector.AddOrUpdate(
            collectorName,
            _ => (collectionTime.Value, null),
            (_, existing) => existing.Current == collectionTime.Value
                ? existing
                : (collectionTime.Value, existing.Current));

        return updated.Previous;
    }

    /// <summary>
    /// Removes all cached entries for a server (e.g., when the server tab is closed).
    /// Next collection will re-seed from database if needed.
    /// </summary>
    public void ClearServer(int serverId)
    {
        _cache.TryRemove(serverId, out _);
        /* The pass window goes with the baselines it is interpreted against. Left behind, a re-added
           server's first pass would measure a series age against a look from before it was removed and
           credit a full counter to an interval that never happened. */
        _passes.TryRemove(serverId, out _);
    }

    /// <summary>
    /// Calculates the delta between the current value and the previous cached value.
    /// First-ever sighting (no baseline): returns 0 and stores the value as the new baseline.
    /// Counter reset (value decreased): returns 0 to avoid inflated deltas from plan cache churn.
    /// Gap detection: if collectionTime and maxGapSeconds are provided and the gap since the
    /// last cached value exceeds maxGapSeconds, returns 0 to avoid inflated deltas after restarts.
    /// Thread-safe via atomic AddOrUpdate.
    /// <para>All three of those zeros mean "no delta is knowable here", which a <c>long</c> cannot say
    /// any other way — and none of them is the same claim as "this interval was idle". Use
    /// <see cref="CalculateDeltaWithInterval"/> when a caller has to tell them apart: the reported
    /// interval is 0 in exactly these cases and non-zero whenever the delta is real, so a stored
    /// (delta, interval) pair of (0, 0) reads as unknown while (0, n) reads as genuinely idle. That
    /// pairing is what makes a zero interpretable downstream (#2234).</para>
    /// </summary>
    public long CalculateDelta(int serverId, string collectorName, string key, long currentValue,
        DateTime? collectionTime = null, int maxGapSeconds = 0)
        => CalculateDeltaWithInterval(serverId, collectorName, key, currentValue, out _, collectionTime, maxGapSeconds);

    /// <summary>
    /// Same as <see cref="CalculateDelta"/>, but also reports the number of seconds between the
    /// previous cached collection and the current one via <paramref name="intervalSeconds"/>.
    /// The interval is 0 on the first sighting of a key or after a gap reset (no prior baseline to
    /// measure against), mirroring the delta's own 0 in those cases. Callers can divide a delta by
    /// this interval to derive a per-second rate (e.g. CPU-ms per wall-clock second).
    /// Thread-safe via atomic AddOrUpdate.
    /// </summary>
    public long CalculateDeltaWithInterval(int serverId, string collectorName, string key, long currentValue,
        out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        => Core(serverId, collectorName, key, currentValue, seriesAgeSeconds: null, out intervalSeconds,
            collectionTime, maxGapSeconds);

    /// <inheritdoc />
    public long CalculateDeltaWithSeriesAge(int serverId, string collectorName, string key, long currentValue,
        int? seriesAgeSeconds, out int intervalSeconds, DateTime? collectionTime = null, int maxGapSeconds = 0)
        => Core(serverId, collectorName, key, currentValue, seriesAgeSeconds, out intervalSeconds,
            collectionTime, maxGapSeconds);

    private long Core(int serverId, string collectorName, string key, long currentValue,
        int? seriesAgeSeconds, out int intervalSeconds, DateTime? collectionTime, int maxGapSeconds)
    {
        /* Read (and roll) the pass window BEFORE touching the key cache: the Add path below needs the
           previous pass, and a key that is new has no timestamp of its own to supply it. Always called,
           even when no series age was passed, so the window advances on every pass rather than only on
           the passes that happen to use the hint. */
        var previousPass = PreviousPass(serverId, collectorName, collectionTime);

        var serverCache = _cache.GetOrAdd(serverId, _ => new ConcurrentDictionary<string, ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>>());
        var collectorCache = serverCache.GetOrAdd(collectorName, _ => new ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>());

        long delta = 0;
        int interval = 0;

        collectorCache.AddOrUpdate(
            key,
            /* Add: first time seeing this key — store the baseline only and return 0.
               All callers track cumulative counters (perfmon, wait stats, file IO, etc.).

               #2235 exception, and the ONLY case where a first sighting can report real work: when the
               caller supplies a series age younger than the gap since our previous pass, the counter
               demonstrably STARTED inside that gap, so its whole value accrued there and its baseline
               was 0 rather than currentValue. Without this a recompiling plan reports 0 forever — it
               presents a new plan_handle, hence a new key, on nearly every sighting. A real interval is
               reported alongside it because this delta IS knowable; the (0, 0) pairing stays reserved
               for the cases that genuinely are not. */
            _ =>
            {
                delta = 0;
                interval = 0;

                if (seriesAgeSeconds.HasValue && seriesAgeSeconds.Value >= 0
                    && collectionTime.HasValue && previousPass.HasValue)
                {
                    var gap = (collectionTime.Value - previousPass.Value).TotalSeconds;

                    /* Bounded by the same gap policy as the reset branch: past it, attributing a whole
                       cumulative counter to one interval is the inflated spike that guard exists to
                       prevent — a plan compiled during an hour-long outage is not an hour of work in
                       the next minute. */
                    if (gap > 0 && (maxGapSeconds <= 0 || gap <= maxGapSeconds) && seriesAgeSeconds.Value <= gap)
                    {
                        delta = currentValue;
                        interval = (int)gap;
                    }
                }

                return (currentValue, collectionTime);
            },
            /* Update: compute delta atomically */
            (_, previous) =>
            {
                /* Gap detection: if too much time has passed since the last cached value,
                   treat this as a new baseline to avoid inflated deltas after app restarts */
                if (maxGapSeconds > 0 && collectionTime.HasValue && previous.Timestamp.HasValue
                    && (collectionTime.Value - previous.Timestamp.Value).TotalSeconds > maxGapSeconds)
                {
                    delta = 0;
                    interval = 0;
                    return (currentValue, collectionTime);
                }

                /* Seconds between the previous and current collection, when both timestamps exist —
                   the wall-clock span this delta accrued over. */
                interval = (collectionTime.HasValue && previous.Timestamp.HasValue)
                    ? (int)(collectionTime.Value - previous.Timestamp.Value).TotalSeconds
                    : 0;

                if (currentValue < previous.Value)
                {
                    /* Counter reset (plan cache eviction/re-entry): the work between the two readings
                       is unknowable, not zero. Report no interval either, so the pair stays honest —
                       a 0 delta over a REAL interval is a claim that nothing happened for that long,
                       and this is the one case where that claim would be false. That invariant
                       (interval 0 <=> no delta knowable) is what lets a reader tell a fabricated zero
                       from an idle one, and every consumer already maps 0 to NULL via
                       NULLIF(sample_interval_seconds, 0). */
                    delta = 0;
                    interval = 0;
                }
                else
                {
                    delta = currentValue - previous.Value;
                }

                return (currentValue, collectionTime);
            });

        intervalSeconds = interval;
        return delta;
    }

    /// <summary>
    /// Seeds a single value into the cache without computing a delta — the restart-survival hook
    /// hosts use to restore baselines from their own store.
    /// </summary>
    protected void Seed(int serverId, string collectorName, string key, long value, DateTime? timestamp = null)
    {
        var serverCache = _cache.GetOrAdd(serverId, _ => new ConcurrentDictionary<string, ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>>());
        var collectorCache = serverCache.GetOrAdd(collectorName, _ => new ConcurrentDictionary<string, (long Value, DateTime? Timestamp)>());
        collectorCache[key] = (value, timestamp);
    }
}
